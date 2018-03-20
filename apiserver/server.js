const crypto    = require('crypto');
const os        = require('os');
const _         = require('lodash');
const Server    = require('rpc-easy/Server');
const { Etcd3 } = require('etcd3');
const redis     = require('redis');
const bluebird  = require('bluebird');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

const etcd = new Etcd3({
    hosts: process.env['ETCD_ADDR'] || 'localhost:2379',
});
const rd = redis.createClient({
    host: process.env['REDIS_ADDR'] || 'localhost',
});

const rpcServer = new Server();

const appServerId      = crypto.randomBytes(4).toString('hex');
const etcdServerKey    = `apiServer/${appServerId}`;
let etcdServerKeyLease = null;

let terminating = false;
let refreshUsersRoutesInterval = null;

const gates = new Set();

const userCache = new Map();

const requestsInProgress = new Set();

const interfaces = os.networkInterfaces();
let iface = null;

for (let interfaceName in interfaces) {
    for (let face of interfaces[interfaceName]) {
        if (!face.internal && face.family === 'IPv4') {
            iface = face;
            break;
        }
    }
}

if (!iface) {
    console.error('Interface not found');
    process.exit(10);
}

rpcServer.on('connection', conn => {
    conn.setRequestHandler((apiName, data) => {
        const apiCall = {
            promise: null,
            resolve: null,
            reject:  null,
            aborted: false,
            done:    false,
        };

        apiCall.promise = new Promise(async (resolve, reject) => {
            apiCall.resolve = resolve;
            apiCall.reject  = reject;

            try {
                console.log('Api call:', apiName, data);

                let user = userCache.get(data.userId);
                if (!user) {
                    try {
                        user = await loadUserCache(data.userId);
                    } catch(err) {
                        console.error('load user cache failed:', err);
                    }

                    if (!user) {
                        user = await loadUser(data.userId);
                    }

                    userCache.set(data.userId, user);
                }

                const result = await doApi(user);
                apiCall.done = true;
                resolve(result);

            } catch(err) {
                apiCall.done = true;
                // TODO reject ? or resolve with error object?
                reject(err);
            }
        });

        requestsInProgress.add(apiCall);

        apiCall.promise.catch(_.noop).then(() => {
            requestsInProgress.delete(apiCall);
        });

        return apiCall.promise;
    });

    conn.on('error', err => {
        console.error('Connection error:', err);
        // TODO Do something
    });

    conn.on('close', () => {
        gates.delete(conn);
    });

    gates.add(conn);
});

rpcServer.on('error', err => {
    console.error('rpc error:', err);
    // TODO Do something
});

rpcServer.listen({
    host:      iface.address,
    port:      Number(process.env['PORT']) || 12000,
    exclusive: true,
}, async err => {
    if (err) {
        console.error('Listen error:', err);
    } else {
        console.log(`Api Server [${appServerId}] started`);
    }

    const addrInfo = rpcServer.address();
    const address  = `${addrInfo.address}:${addrInfo.port}`;

    console.log('Server address:', address);

    try {
        etcdServerKeyLease = etcd.lease();
        await etcdServerKeyLease.put(etcdServerKey).value(address);
    } catch(err) {
        console.error(err);
        process.exit(1);
    }

    refreshUsersRoutesInterval = setInterval(refreshUserRoutes, 60 * 1000);
});

let nextForceExit = false;

process.on('SIGINT', async () => {
    terminating = true;

    if (nextForceExit) {
        process.exit(1);
    }

    console.log('Terminating');

    clearInterval(refreshUsersRoutesInterval);

    nextForceExit = true;

    gateBroadcast({
        code: 'TERMINATING',
    });

    await sleep(1000);

    try {
        await withTimeout(5000, waitAllInProgress());
    } catch(err) {
        console.error(err);
    }

    const chunks = _.chunk(Array.from(userCache.values()), 10);

    for (let chunk of chunks) {
        const wait = [];

        for (let user of chunk) {
            wait.push(saveUserCache(user));
        }

        await Promise.all(wait);

        gateBroadcast({
            code:     'USERS_FREE',
            usersIds: chunk.map(user => user.userId),
        });
    }

    if (etcdServerKeyLease) {
        try {
            await sleep(500);
            await etcdServerKeyLease.revoke();
        } catch(err) {
            console.error('revoke error', err);
        }
    }

    await shutdown();
});

async function waitAllInProgress() {
    while (requestsInProgress.size) {
        await Promise.all(Array.from(requestsInProgress.keys()).map(request => request.promise));
    }
}

function gateBroadcast(data) {
    for (let gate of gates) {
        gate.send(data);
    }
}

async function loadUserCache(userId) {
    const json = await rd.getAsync(`user/${userId}`);
    console.log('user cache loaded', `user/${userId}`, json);

    if (json) {
        deleteUserCache(userId).catch(err => {
            console.error('deleteUserCache', err);
        });

        return JSON.parse(json);
    } else {
        return null;
    }
}

async function saveUserCache(user) {
    console.log('set:', `user/${user.userId}`, JSON.stringify(user));
    await rd.setAsync(`user/${user.userId}`, JSON.stringify(user), 'EX', 120);
}

async function deleteUserCache(userId) {
    await rd.delAsync(`user/${userId}`);
}

function refreshUserRoutes() {
    for (let userId of userCache.keys()) {
        rd.set(`route/${userId}`, appServerId, 'EX', 90, err => {
            if (err) {
                console.error('cache set error:', err);
            }
        });
    }
}

async function loadUser(userId) {
    // STUB
    await sleep(500);

    return {
        userId: userId,
        inc:    0,
    };
}

async function doApi(user) {
    user.inc++;

    await sleep(3000);

    return {
        status:      'OK',
        inc:         user.inc,
        from:        'api server',
        apiServerId: appServerId,
    };
}

async function shutdown(exitCode = 0) {
    await sleep(500);
    process.exit()
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function timeout(ms) {
    return new Promise((resolve, reject) => {
        setTimeout(() => reject(new Error('TIMEOUT')), ms);
    });
}

function withTimeout(ms, result) {
    return Promise.race([timeout(ms), result]);
}
