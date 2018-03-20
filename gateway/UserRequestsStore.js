
class UserRequestsStore {

    constructor() {
        this._store = new Set();
    }

    add(request) {
        this._store.add(request);
    }

    remove(request) {
        this._store.delete(request);
    }

    getInProgress() {
        return this._store;
    }

}

module.exports = UserRequestsStore;
