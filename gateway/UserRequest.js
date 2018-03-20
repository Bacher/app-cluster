
class UserRequest {

    constructor(store, userId, res) {
        this._store = store;
        this.userId = userId;
        this._res = res;

        this._store.add(this);
    }

    error(httpCode, code) {
        this._res.status(httpCode);
        this._json({
            errorCode: code,
        });
    }

    end(data) {
        this._json(data);
    }

    _json(data) {
        this._store.remove(this);

        try {
            this._res.json(data)
        } catch(err) {
            this._res.destroy();
        }
    }

}

module.exports = UserRequest;
