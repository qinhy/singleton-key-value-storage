class SingletonStorageController {
    
    exists(key) { console.log(`[${this.constructor.name}]: not implemented`); }
    set(key, value) { console.log(`[${this.constructor.name}]: not implemented`); }
    get(key) { console.log(`[${this.constructor.name}]: not implemented`); }
    delete(key) { console.log(`[${this.constructor.name}]: not implemented`); }
    keys(pattern = '*') { console.log(`[${this.constructor.name}]: not implemented`); }
    clean() { this.keys('*').forEach(k => this.delete(k)); }
    dumps() { var res = {}; this.keys('*').forEach(k => res[k] = this.get(k)); return JSON.stringify(res); }
    loads(jsonString = '{}') { this.clean(); Object.entries(JSON.parse(jsonString)).forEach(d => this.set(d[0], d[1])); }

    _randuuid(prefix = '') {
        return prefix + 'xxxx-xxxx-xxxx-xxxx-xxxx'.replace(/x/g, function () {
            return Math.floor(Math.random() * 16).toString(16);
        });
    }
}
class JavascriptDictStorage {
    constructor() {
        this.uuid = this._randuuid();
        this.store = {};
    }
    _randuuid(prefix = '') {
        return prefix + 'xxxx-xxxx-xxxx-xxxx-xxxx'.replace(/x/g, function () {
            return Math.floor(Math.random() * 16).toString(16);
        });
    }
    get() {
        return this.store;
    }
}
class SingletonJavascriptDictStorage {
    static _instance = null;
    static _meta = {};

    constructor() {
        if (!SingletonJavascriptDictStorage._instance) {
            SingletonJavascriptDictStorage._instance = this;
            this.store = {};
        }
        return SingletonJavascriptDictStorage._instance;
    }
    get() {
        return this.store;
    }
}

class SingletonJavascriptDictStorageController extends SingletonStorageController {
    constructor(model) {
        super();
        this.model = model;
    }

    exists(key) { return key in this.model.get(); }

    set(key, value) {
        this.model.get()[key] = value;
    }

    get(key) { return this.model.get()[key] || null; }

    delete(key) {
        if (key in this.model.get()) { delete this.model.get()[key]; }
    }

    keys(pattern = '*') {
        const regex = new RegExp(pattern.replace(/\*/g, '.*'));
        return Object.keys(this.model.get()).filter(key => key.match(regex));
    }
}


class EventDispatcherController {
    static ROOT_KEY = 'Event';

    constructor(client = null) {
        if (client === null) {
            client = new SingletonJavascriptDictStorageController(new JavascriptDictStorage());
        }
        this.client = client;
    }

    events() {
        return this.client.keys('*').map(k => [k, this.client.get(k)]);
    }

    _find_event(uuid) {
        const es = this.client.keys(`*:${uuid}`);
        return es.length === 0 ? [null] : es;
    }

    get_event(uuid) {
        return this._find_event(uuid).map(k => this.client.get(k));
    }

    delete_event(uuid) {
        return this._find_event(uuid).forEach(k => this.client.delete(k));
    }

    set_event(event_name, callback, id = null) {
        if (id === null) id = uuidv4();
        this.client.set(`${EventDispatcherController.ROOT_KEY}:${event_name}:${id}`, callback);
        return id;
    }

    dispatch(event_name, ...args) {
        this.client.keys(`${EventDispatcherController.ROOT_KEY}:${event_name}:*`).forEach(event_full_uuid => {
            this.client.get(event_full_uuid)(...args);
        });
    }

    clean() {
        return this.client.clean();
    }
}

class KeysHistoryController {
    constructor(client = null) {
        if (client === null) {
            client = new SingletonJavascriptDictStorageController(new JavascriptDictStorage());
        }
        this.client = client;
    }

    _str2base64(key) {
        return btoa(key);
    }

    reset() {
        this.client.set('_History:', {});
    }

    set_history(key, result) {
        if (result) {
            this.client.set(`_History:${this._str2base64(key)}`, { result });
        }
        return result;
    }

    get_history(key) {
        const res = this.client.get(`_History:${this._str2base64(key)}`);
        return res ? res.result : null;
    }

    try_history(key, result_func = () => null) {
        let res = this.get_history(key);
        if (res === null) {
            res = result_func();
            if (res) this.set_history(key, res);
        }
        return res;
    }
}

class SingletonVueStorage {
    static _instance = null;
    static _meta = {};

    constructor() {
        if (!SingletonVueStorage._instance) {
            SingletonVueStorage._instance = this;
            if (Vue) {
                console.log("add Vue support")
                const { ref } = Vue;
                this.store = ref({});
            }
            else {
                console.log("no Vue support")
                this.store = null;
            }

        }
        return SingletonVueStorage._instance;
    }
    get() {
        return this.store.value;
    }
}

class SingletonVueStorageController extends SingletonJavascriptDictStorageController {
    constructor(model) {
        super();
        this.model = model;
    }
}

class SingletonIndexedDBStorage {
    static _instance = null;
    static dbName = 'SingletonIndexedDBStorageDatabase';
    static storeName = 'SingletonIndexedDBStorage';

    constructor() {
        if (!SingletonIndexedDBStorage._instance) {
            SingletonIndexedDBStorage._instance = this;
            this.dbPromise = this.initializeDB();
        }
        return SingletonIndexedDBStorage._instance;
    }

    initializeDB() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(SingletonIndexedDBStorage.dbName, 1);
            request.onerror = (event) => reject('Database error: ' + event.target.errorCode);
            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                if (!db.objectStoreNames.contains(SingletonIndexedDBStorage.storeName)) {
                    db.createObjectStore(SingletonIndexedDBStorage.storeName, { keyPath: 'id' });
                }
            };
            request.onsuccess = (event) => resolve(event.target.result);
        });
    }

    getDB() {
        return this.dbPromise;
    }
}

class SingletonIndexedDBStorageController extends SingletonStorageController {
    constructor(model) {
        super();
        this.model = model;
    }

    exists(key) {
        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName]);
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);
            const request = objectStore.get(key);
            return new Promise((resolve, reject) => {
                request.onsuccess = () => resolve(request.result !== undefined);
                request.onerror = () => reject(request.error);
            });
        });
    }

    set(key, value) {
        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName], 'readwrite');
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);
            const request = objectStore.put({ id: key, value: value });
            return new Promise((resolve, reject) => {
                request.onsuccess = () => {
                    resolve();
                };
                request.onerror = () => reject(request.error);
            });
        });
    }

    get(key) {
        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName]);
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);
            const request = objectStore.get(key);
            return new Promise((resolve, reject) => {
                request.onsuccess = () => resolve(request.result ? request.result.value : null);
                request.onerror = () => reject(request.error);
            });
        });
    }

    delete(key) {
        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName], 'readwrite');
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);
            const request = objectStore.delete(key);
            return new Promise((resolve, reject) => {
                request.onsuccess = () => {
                    resolve();
                };
                request.onerror = () => reject(request.error);
            });
        });
    }

    keys(pattern = '*') {
        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName]);
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);
            const request = objectStore.openCursor();
            const regex = new RegExp(pattern.replace(/\*/g, '.*'));
            const keys = [];

            return new Promise((resolve, reject) => {
                request.onsuccess = (event) => {
                    const cursor = event.target.result;
                    if (cursor) {
                        if (cursor.key.match(regex)) {
                            keys.push(cursor.key);
                        }
                        cursor.continue();
                    } else {
                        resolve(keys);
                    }
                };
                request.onerror = () => reject(request.error);
            });
        });
    }

    clean() {
        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName], 'readwrite');
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);
            const request = objectStore.openCursor();

            return new Promise((resolve, reject) => {
                request.onsuccess = (event) => {
                    const cursor = event.target.result;
                    if (cursor) {
                        // Delete each entry one by one
                        const deleteRequest = cursor.delete();
                        deleteRequest.onsuccess = () => {
                            cursor.continue();
                        };
                        deleteRequest.onerror = () => reject(deleteRequest.error);
                    } else {
                        // No more entries to delete
                        resolve();
                    }
                };
                request.onerror = () => reject(request.error);
            });
        });
    }

    // Dumps all data from the store to a JSON string
    dumps() {
        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName]);
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);
            const request = objectStore.openCursor();
            const result = {};

            return new Promise((resolve, reject) => {
                request.onsuccess = (event) => {
                    const cursor = event.target.result;
                    if (cursor) {
                        result[cursor.key] = cursor.value.value;
                        cursor.continue();
                    } else {
                        resolve(JSON.stringify(result)); // Resolve the final JSON string of all stored data
                    }
                };
                request.onerror = () => reject(request.error);
            });
        });
    }

    // Loads data from a JSON string into the store
    loads(jsonString = '{}') {
        const entries = Object.entries(JSON.parse(jsonString));
        if (entries.length === 0) return Promise.resolve();

        return this.model.getDB().then(db => {
            const transaction = db.transaction([SingletonIndexedDBStorage.storeName], 'readwrite');
            const objectStore = transaction.objectStore(SingletonIndexedDBStorage.storeName);

            return Promise.all(entries.map(([key, value]) => {
                return new Promise((resolve, reject) => {
                    const request = objectStore.put({ id: key, value: value });
                    request.onsuccess = () => {
                        resolve();
                    };
                    request.onerror = () => reject(request.error);
                });
            }));
        }).then(() => {
            // console.log('All data has been loaded into the database');
        }).catch(error => {
            console.error('Error loading data into the database:', error);
        });
    }
}

class SingletonKeyValueStorage extends SingletonStorageController {

    constructor() {
        super();
        this.conn = null;
        this.js_backend();
    }

    init() {
        this.event_dispa = new EventDispatcherController();
        this._hist = new KeysHistoryController();
    }

    js_backend() { this.init(); this.conn = new SingletonJavascriptDictStorageController(new SingletonJavascriptDictStorage()); return this; }
    vue_backend() { this.init(); this.conn = new SingletonVueStorageController(new SingletonVueStorage()); return this; }
    indexedDB_backend() { this.init(); this.conn = new SingletonIndexedDBStorageController(new SingletonIndexedDBStorage()); return this; }
    
    add_slave(slave, event_names = ['set', 'delete']) {
        if (!slave.uuid) slave.uuid = this.randuuid();
        event_names.forEach(m => {
            if (slave[m]) {
                this.event_dispa.set_event(m, slave[m], slave.uuid);
            }
        });
    }

    delete_slave(slave) {
        this.event_dispa.delete_event(slave.uuid);
    }

    _edit(func_name, key, value = null) {
        this._hist.reset();
        const func = this.conn[func_name];
        const args = value ? [key, value] : [key];
        const res = func.apply(this.conn, args);
        this.event_dispa.dispatch(func_name, ...args);
        return res;
    }
    set(key, value) { return this._edit('set', key, value); }
    delete(key) { return this._edit('delete', key); }

    exists(key) { return this._hist.try_history(key, () => this.conn.exists(key)); }
    keys(regx = '*') { return this._hist.try_history(regx, () => this.conn.keys(regx)); }

    get(key) { return this.conn.get(key); }
    clean() { return this.conn.clean(); }
    dumps() { return this.conn.dumps(); }
    loads(json_str) { return this.conn.loads(json_str); }
    randuuid() { return this.conn._randuuid(); }
}

// Tests for SingletonKeyValueStorage 
[new SingletonKeyValueStorage().js_backend(), new SingletonKeyValueStorage().vue_backend(), new SingletonKeyValueStorage().indexedDB_backend()]
    .forEach(storage => {
        console.log(`Testing ${storage.conn.constructor.name}...`);

        // Test 1: Set and Get
        storage.set('key1', { data: 'value1' });
        const value1 = storage.get('key1');
        if (value1.constructor.name == 'Promise') {
            value1.then(d =>
                console.assert(JSON.stringify(d) === JSON.stringify({ data: 'value1' }), 'Test 1 Failed: Set or Get does not work correctly')
            );
        }
        else {
            console.assert(JSON.stringify(value1) === JSON.stringify({ data: 'value1' }), 'Test 1 Failed: Set or Get does not work correctly');
        }

        // Test 2: Exists
        const exists1 = storage.exists('key1');
        const exists2 = storage.exists('key2');

        if (exists1.constructor.name == 'Promise') {
            exists1.then(d =>
                console.assert(d === true, 'Test 2 Failed: Exists does not return true for existing key')
            );
        }
        else {
            console.assert(exists1 === true, 'Test 2 Failed: Exists does not return true for existing key');
        }

        if (exists2.constructor.name == 'Promise') {
            exists2.then(d =>
                console.assert(d === false, 'Test 2 Failed: Exists does not return false for non-existing key')
            );
        }
        else {
            console.assert(exists2 === false, 'Test 2 Failed: Exists does not return false for non-existing key');
        }

        // Test 3: Delete
        storage.delete('key1');
        const valueAfterDelete = storage.get('key1');

        if (valueAfterDelete && valueAfterDelete.constructor.name == 'Promise') {
            valueAfterDelete.then(d =>
                console.assert(d === null, 'Test 3 Failed: Delete does not remove the key properly')
            );
        }
        else {
            console.assert(valueAfterDelete === null, 'Test 3 Failed: Delete does not remove the key properly');
        }

        // Test 4: Keys
        storage.set('test1', { data: '123' });
        storage.set('test2', { data: '456' });
        storage.set('something', { data: '789' });
        const keys = storage.keys('test*');

        if (keys && keys.constructor.name == 'Promise') {
            keys.then(d =>
                console.assert(d.includes('test1') && d.includes('test2') && d.length === 2, 'Test 4 Failed: Keys does not filter correctly')
            );
        }
        else {
            console.assert(keys.includes('test1') && keys.includes('test2') && keys.length === 2, 'Test 4 Failed: Keys does not filter correctly');
        }

        // Test 5: Clean
        storage.clean();
        const keysAfterClean = storage.keys('*');
        if (keysAfterClean && keysAfterClean.constructor.name == 'Promise') {
            keysAfterClean.then(d =>
                console.assert(d.length === 0, 'Test 5 Failed: Clean does not clear all keys')
            );
        }
        else {
            console.assert(keysAfterClean.length === 0, 'Test 5 Failed: Clean does not clear all keys');
        }

        // Test 6: dumps and loads
        if (keysAfterClean.constructor.name != 'Promise') {
            // Adding some test data
            storage.set('key1', 'value1');
            storage.set('key2', 'value2');
            storage.set('key3', 'value3');
            // Dumping the data
            const jsonString = storage.dumps();
            storage.clean();
            storage.loads(jsonString)
            console.assert(storage.dumps() === jsonString, 'Test 6 Failed: dumps data != loads data');
        }
        else {
            // Adding some test data
            storage.set('key1', 'value1').then(d => {
                storage.set('key2', 'value2').then(d => {
                    storage.set('key3', 'value3').then(d => {
                        // console.log('Initial data set.');                  
                        // Dumping the data
                        storage.dumps().then(jsonString => {
                            // console.log('Data dumped:', jsonString);
                            storage.clean().then(d => {
                                storage.loads(jsonString).then(d => {
                                    storage.dumps().then(verifiedJsonString => {
                                        console.assert(verifiedJsonString === jsonString, 'Test 6 Failed: dumps data != loads data');
                                        storage.clean();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        }

        console.log("All tests completed.");
    });
