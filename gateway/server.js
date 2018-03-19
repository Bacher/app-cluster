const express   = require('express');
const RpcClient = require('rpc-easy/Client');
const { Etcd3 } = require('etcd3');
const _         = require('lodash');

const API_SERVER_PREFIX = 'apiServer/';

//const gateId = crypto.randomBytes(4).toString('hex');

const API_SERVER_STATUS = {
    OK:       1,
    DRAINING: 2,
    DOWN:     3,
};

const etcd = new Etcd3();

const TOKES_MAP = new Map([
    ['123', 2],
    ['124', 3],
]);

const apiServers = new Map();
const usersRoutes = new Map();
const requestsQueue = new Map();

async function initEtcd() {
    const watcher = await etcd.watch().prefix(API_SERVER_PREFIX).create();

    let inited = false;

    watcher.on('connected', () => console.log('successfully reconnected!'));
    watcher.on('disconnected', () => console.log('disconnected...'));
    watcher.on('put', res => {
        if (!inited) {
            return;
        }

        const id = res.key.toString().match(/^apiServer\/(.*)$/)[1];

        addNewApiServer(id, res.value.toString());
    });
    watcher.on('delete', async res => {
        if (!inited) {
            return;
        }

        const id = res.key.toString().match(/^apiServer\/(.*)$/)[1];

        const apiServer = apiServers.get(id);

        apiServer.status = API_SERVER_STATUS.DOWN;

        const waits = [];

        for (let [userId, apiServerId] of usersRoutes) {
            if (apiServerId === id) {
                waits.push(rerouteUser(userId).catch(err => {
                    console.error(err);
                }));
            }
        }

        await Promise.all(waits);
    });

    const apiServersInfo = await etcd.getAll().prefix(API_SERVER_PREFIX);
    inited = true;

    for (let key in apiServersInfo) {
        if (apiServersInfo.hasOwnProperty(key)) {
            const id = key.match(/^apiServer\/(.*)$/)[1];

            addNewApiServer(id, apiServersInfo[key]);
        }
    }
}

function addNewApiServer(id, address) {
    console.log('Api Server added:', id, address);
    const [host, port] = address.split(':');

    const rpcClient = new RpcClient({
        host,
        port,
        autoReconnect: true,
        useQueue:      true,
    });

    rpcClient.on('message', data => {
        console.log('Api Server Message:', data);

        switch (data.code) {
            case 'TERMINATING':
                apiServers.status = API_SERVER_STATUS.DRAINING;
                break;
            case 'USERS_FREE':
                onUsersFreeMessage(id, data.usersIds).catch(err => {
                    console.error(err);
                });
                break;
            default:
                console.error('Invalid message from api server:', data);
        }
    });

    rpcClient.connect();

    apiServers.set(id, {
        id,
        host,
        port,
        rpc:    rpcClient,
        status: API_SERVER_STATUS.OK,
    });
}

initEtcd().catch(err => {
    console.error(err);
    process.exit(1);
});

async function onUsersFreeMessage(apiServerId, usersIds) {
    for (let userId of usersIds) {
        const apiServer = usersRoutes.get(userId);

        if (apiServer.id === apiServerId) {
            await rerouteUser(userId);
        }
    }
}

const app = express();

app.get('/api/sayHello.json', async (req, res) => {
    const userId = TOKES_MAP.get(req.query.token);

    if (!userId) {
        res.status(400);
        res.json({
            errorCode: 'UNAUTHORIZED',
        });
        return;
    }

    let apiServer = null;
    let serverId  = usersRoutes.get(userId);

    if (serverId) {
        apiServer = apiServers.get(serverId);

        if (apiServer.status !== API_SERVER_STATUS.OK) {
            let queue = requestsQueue.get(userId);

            if (!queue) {
                queue = [];
                requestsQueue.set(userId, queue);
            }

            queue.push({ userId, res });
            // TODO: НАЧАТЬ (ИЛИ ПРИСОЕДИНИТЬСЯ К ОЖИДАНИЮ) ПЕРЕБРОС СТЕЙТА И РЕРОУТИНГ
            console.log('NEED_REROUTING');
            apiServer = null;
        }
    } else {
        apiServer = findLiveAppServer();

        if (apiServer) {
            usersRoutes.set(userId, apiServer.id);
        }
    }

    if (!apiServer) {
        res.status(500);
        res.json({
            errorCode: 'NO_API_SERVERS',
        });
        return;
    }

    await makeRequest(apiServer, userId, res);
});

function findLiveAppServer() {
    return _.sample(Array.from(apiServers.values()).filter(a => a.status === API_SERVER_STATUS.OK));
}

app.listen(9080, err => {
    if (err) {
        throw err;
    }
    console.log('Server listen at 0.0.0.0:9080, try http://localhost:9080/api/sayHello.json');
});

async function makeRequest(apiServer, userId, res) {
    try {
        const data = await apiServer.rpc.request('sayHello', {
            userId,
        });

        res.json({
            status:   'OK',
            response: data,
        });
    } catch(err) {
        console.error('rpc request error:', err);

        try {
            res.status(500);
            res.json({
                errorCode: 'ERROR',
            });
        } catch(err) {
            res.destroy();
        }
    }
}

async function rerouteUser(userId) {
    usersRoutes.delete(userId);

    if (requestsQueue.has(userId)) {
        const requests  = requestsQueue.has(userId);
        const apiServer = findLiveAppServer();

        if (!apiServer) {
            for (let { res } of requests) {
                res.status(500);
                res.json({
                    errorCode: 'NO_API_SERVERS',
                });
            }
            return;
        }

        usersRoutes.set(userId, apiServer.id);

        for (let { userId, res } of requests) {
            await makeRequest(apiServer, userId, res);
        }
    }
}
