const crypto    = require('crypto');
const Server    = require('rpc-easy/Server');
const Memcached = require('memcached');

const { Etcd3 } = require('etcd3');
const memcached = new Memcached();

const etcd = new Etcd3();

const rpcServer = new Server();

const appServerId      = crypto.randomBytes(4).toString('hex');
const etcdServerKey    = `apiServer/${appServerId}`;
let etcdServerKeyLease = null;

const gates = new Set();

const userCache = new Map();

rpcServer.on('connection', conn => {
    conn.setRequestHandler(async (apiName, data) => {
        console.log('Api call:', apiName, data);

        let user = userCache.get(data.userId);
        if (!user) {
            try {
                user = await loadUserCache(data.userId);
            } catch(err) {
                console.error('load user cache from memcached failed:', err);
            }

            if (!user) {
                user = {
                    userId: data.userId,
                    inc:    0,
                };
            }

            userCache.set(data.userId, user);
        }

        user.inc++;

        return {
            status:      'OK',
            inc:         user.inc,
            from:        'api server',
            apiServerId: appServerId,
        };
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
    host: '127.0.0.1',
    port: 0,
    exclusive: true,
}, async err => {
    if (err) {
        console.error('Listen error:', err);
    } else {
        console.log(`Api Server [${appServerId}] started`);
    }

    const address = rpcServer.address();

    try {
        etcdServerKeyLease = etcd.lease();
        await etcdServerKeyLease.put(etcdServerKey).value(`${address.address}:${address.port}`);
    } catch(err) {
        console.error(err);
        process.exit(1);
    }
});

let nextForceExit = false;

process.on('SIGINT', async () => {
    if (nextForceExit) {
        process.exit(1);
    }

    nextForceExit = true;

    gateBroadcast({
        code: 'TERMINATING',
    });

    for (let [userId, user] of userCache) {
        console.log(`saving user ${userId}...`);
        await saveUserCache(user);

        gateBroadcast({
            code:     'USERS_FREE',
            usersIds: [userId],
        });
    }

    await sleep(1000);

    if (etcdServerKeyLease) {
        try {
            await etcdServerKeyLease.revoke();
        } catch(err) {
            console.error(err);
        }
    }

    process.exit(0);
});

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function gateBroadcast(data) {
    for (let gate of gates) {
        gate.send(data);
    }
}

function loadUserCache(userId) {
    return new Promise((resolve, reject) => {
        memcached.get(`user/${userId}`, (err, json) => {
            if (err) {
                reject(err);
            } else {
                console.log('memcached loaded', `user/${userId}`, json);
                if (json) {
                    resolve(JSON.parse(json));
                    deleteUserCache(userId).catch(err => {
                        console.error(err);
                    });
                } else {
                    resolve(null);
                }
            }
        });
    });
}

function saveUserCache(user) {
    return new Promise((resolve, reject) => {
        console.log('set:', `user/${user.userId}`, JSON.stringify(user));
        memcached.set(`user/${user.userId}`, JSON.stringify(user), 120, err => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

function deleteUserCache(userId) {
    return new Promise((resolve, reject) => {
        memcached.del(`user/${userId}`, err => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}
