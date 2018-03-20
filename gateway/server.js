const express   = require('express');
const RpcClient = require('rpc-easy/Client');
const { Etcd3 } = require('etcd3');
const _         = require('lodash');
const Memcached = require('memcached');
const UserRequestsStore = require('./UserRequestsStore');
const UserRequest = require('./UserRequest');

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
const requestsStore = new UserRequestsStore();

const waitApiServerResponses = new Set();

let terminating = false;

async function initEtcd() {
    const watcher = await etcd.watch().prefix(API_SERVER_PREFIX).create();

    let inited = false;

    watcher.on('connected', () => console.log('successfully reconnected!'));
    watcher.on('disconnected', () => console.log('disconnected...'));
    watcher.on('put', data => {
        if (!inited || terminating) {
            return;
        }

        const id = data.key.toString().match(/^apiServer\/(.*)$/)[1];

        addNewApiServer(id, data.value.toString());
    });
    watcher.on('delete', async data => {
        if (!inited) {
            return;
        }

        const id = data.key.toString().match(/^apiServer\/(.*)$/)[1];

        const apiServer = apiServers.get(id);

        apiServer.status = API_SERVER_STATUS.DOWN;

        if (terminating) {
            return;
        }

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
    if (terminating) {
        res.status(500);
        res.json({
            errorCode: 'REDIRECT',
        });
        return;
    }

    const userId = TOKES_MAP.get(req.query.token);

    if (!userId) {
        res.status(400);
        res.json({
            errorCode: 'UNAUTHORIZED',
        });
        return;
    }

    const request = new UserRequest(requestsStore, userId, res);

    const apiServer = await routeUser(userId);

    if (!apiServer) {
        request.error(500, 'NO_API_SERVERS');
        return;
    }

    if (apiServer.status === API_SERVER_STATUS.OK) {
        await makeRequest(apiServer, userId, request);

    } else {
        let queue = requestsQueue.get(userId);

        if (!queue) {
            queue = [];
            requestsQueue.set(userId, queue);
        }

        queue.push({ userId, request, waitApiServerId: apiServer.id });
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

async function makeRequest(apiServer, userId, request) {
    const requestInfo = {
        request,
        promise: null,
    };

    waitApiServerResponses.add(requestInfo);

    try {
        requestInfo.promise = apiServer.rpc.request('sayHello', {
            userId,
        });

        const data = await requestInfo.promise;

        waitApiServerResponses.delete(requestInfo);

        request.end({
            status:   'OK',
            response: data,
        });

    } catch(err) {
        console.error('rpc request error:', err);

        waitApiServerResponses.delete(requestInfo);

        request.error(500, 'ERROR');
    }
}

async function rerouteUser(userId) {
    if (requestsQueue.has(userId)) {
        const apiServer = await routeUser(userId);
        const requests  = requestsQueue.get(userId);
        requestsQueue.delete(userId);

        if (!apiServer) {
            for (let { request } of requests) {
                request.error(500, 'NO_API_SERVERS');
            }
            return;
        }

        for (let { userId, request } of requests) {
            await makeRequest(apiServer, userId, request);
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

    promise.catch(_.noop).then(() => {
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
                    resolve(_routeUser(userId, remain - 1));
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
                resolve(_routeUser(userId, remain - 1));
            }, 100);
        }
    });
}

let nextForceExit = false;

process.on('SIGINT', async () => {
    terminating = true;

    if (nextForceExit) {
        process.exit(1);
    }

    nextForceExit = true;

    for (let [, queue] of requestsQueue) {
        for (let { request } of queue) {
            request.error(500, 'REDIRECT');
        }
    }

    requestsQueue.clear();

    const waits = Array.from(waitApiServerResponses.keys()).map(data => {
        return data.promise.catch(_.noop).then(() => {
            waitApiServerResponses.delete(data);
        });
    });

    if (waits.length) {
        try {
            await Promise.race([Promise.all(waits), timeout(5000)]);

        } catch(err) {
            console.error(err);

            for (let data of waitApiServerResponses) {
                data.request.error(500, 'TIMEOUT');
            }

            await shutdown(1);
        }
    }

    await shutdown();
});

async function shutdown(exitCode = 0) {
    for (let request of requestsStore.getInProgress()) {
        request.error(500, 'REDIRECT');
    }

    await sleep(500);

    process.exit(exitCode);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function timeout(ms) {
    return new Promise((resolve, reject) => {
        setTimeout(() => reject(new Error('TIMEOUT')), ms);
    });
}
