const express   = require('express');
const RpcClient = require('rpc-easy/Client');
const { Etcd3 } = require('etcd3');
const _         = require('lodash');
const Memcached = require('memcached');

const API_SERVER_PREFIX = 'apiServer/';

//const gateId = crypto.randomBytes(4).toString('hex');

const API_SERVER_STATUS = {
    OK:       1,
    DRAINING: 2,
    DOWN:     3,
};

const port = process.env['PORT'] || '9080';

const etcd = new Etcd3();
const memcached = new Memcached();

const TOKES_MAP = new Map([
    ['123', 2],
    ['124', 3],
]);

const apiServers    = new Map();
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

        for (let [userId, requests] of requestsQueue) {
            for (let req of requests) {
                if (req.waitApiServerId === apiServer.id) {
                    waits.push(rerouteUser(userId));
                    break;
                }
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
                onUsersFreeMessage(data.usersIds).catch(err => {
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

async function onUsersFreeMessage(usersIds) {
    for (let userId of usersIds) {
        await rerouteUser(userId);
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

    const apiServer = await routeUser(userId);

    if (!apiServer) {
        res.status(500);
        res.json({
            errorCode: 'NO_API_SERVERS',
        });
        return;
    }

    if (apiServer.status === API_SERVER_STATUS.OK) {
        await makeRequest(apiServer, userId, res);

    } else {
        let queue = requestsQueue.get(userId);

        if (!queue) {
            queue = [];
            requestsQueue.set(userId, queue);
        }

        queue.push({ userId, res, waitApiServerId: apiServer.id });
    }
});

function findLiveAppServer() {
    return _.sample(Array.from(apiServers.values()).filter(a => a.status === API_SERVER_STATUS.OK));
}

app.listen(port, err => {
    if (err) {
        throw err;
    }
    console.log(`Server listen at 0.0.0.0:${port}, try http://localhost:${port}/api/sayHello.json`);
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
    if (requestsQueue.has(userId)) {
        const apiServer = await routeUser(userId);
        const requests  = requestsQueue.get(userId);
        requestsQueue.delete(userId);

        if (!apiServer) {
            for (let { res } of requests) {
                res.status(500);
                res.json({
                    errorCode: 'NO_API_SERVERS',
                });
            }
            return;
        }

        for (let { userId, res } of requests) {
            await makeRequest(apiServer, userId, res);
        }
    }
}

const routeUsersCalls = new Map();

function routeUser(userId) {
    let promise = routeUsersCalls.get(userId);

    if (!promise) {
        promise = _routeUser(userId);
        routeUsersCalls.set(userId, promise);
    }

    promise.catch().then(() => {
        routeUsersCalls.delete(userId);
    });

    return promise;
}

async function _routeUser(userId, remain = 3) {
    return new Promise((resolve, reject) => {
        const key = `route/${userId}`;

        memcached.get(key, (err, apiServerId) => {
            console.log('memcached.get() =>', apiServerId);

            if (err) {
                reject(err);
                return;
            }

            if (apiServerId) {
                const apiServer = apiServers.get(apiServerId);

                if (apiServer) {
                    if (apiServer.status === API_SERVER_STATUS.OK || apiServer.status === API_SERVER_STATUS.DRAINING) {
                        resolve(apiServer);
                        return;
                    }
                }
            }

            const apiServer = findLiveAppServer();

            if (!apiServer) {
                resolve(null);
                return;
            }

            memcached.set(key, apiServer.id, 2 * 60, err => {
                if (err) {
                    recover(err);
                    return;
                }

                setTimeout(() => {
                    resolve(routeUser(userId, remain - 1));
                }, 100);
            });
        });

        function recover(err) {
            if (err) {
                console.error(err);
            }

            if (remain === 0) {
                reject(new Error('CYCLED'));
                return;
            }

            setTimeout(() => {
                resolve(routeUser(userId, remain - 1));
            }, 100);
        }
    });
}
