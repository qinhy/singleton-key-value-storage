class SingletonStorageController {
    slaves() { return this.model.slaves; }
    add_slave(slave) { if(!slave.uuid)slave.uuid=self._randuuid();this.slaves().push(slave);}
    delete_slave(slave) { this.model.slaves = this.model.slaves.filter(s=>s.uuid!=slave.uuid)}

    _set_slaves(key, value) {
        this.slaves().forEach(slave => {
            if (slave.set) { slave.set(key, value); }
        });
    }
    _delete_slaves(key) {
        this.slaves().forEach(slave => {
            if (slave.delete) { slave.delete(key); }
        });
    }
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

class SingletonJavascriptDictStorage {
    static _instance = null;
    static _meta = {};

    constructor() {
        if (!SingletonJavascriptDictStorage._instance) {
            SingletonJavascriptDictStorage._instance = this;
            this.store = {};
            this.slaves = [];
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
        this._set_slaves(key, value);
    }

    get(key) { return this.model.get()[key] || null; }

    delete(key) {
        if (key in this.model.get()) { delete this.model.get()[key]; }
        this._delete_slaves(key);
    }

    keys(pattern = '*') {
        const regex = new RegExp(pattern.replace(/\*/g, '.*'));
        return Object.keys(this.model.get()).filter(key => key.match(regex));
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

            this.slaves = [];
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
            this.slaves = [];
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
                    this._set_slaves(key, value);
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
                    this._delete_slaves(key);
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
                            // Continue deleting next entry
                            this._delete_slaves(cursor.key);
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
                        this._set_slaves(key, value);
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
        this.js_backend();
    }

    js_backend() { this.client = new SingletonJavascriptDictStorageController(new SingletonJavascriptDictStorage()); return this; }
    vue_backend() { this.client = new SingletonVueStorageController(new SingletonVueStorage()); return this; }
    indexedDB_backend() { this.client = new SingletonIndexedDBStorageController(new SingletonIndexedDBStorage()); return this; }

    slaves() { return this.client.slaves(); }
    add_slave(slave) { if(!slave.uuid)slave.uuid=self._randuuid();this.slaves().push(slave);}
    delete_slave(slave) { this.client.model.slaves = this.client.model.slaves.filter(s=>s.uuid!=slave.uuid)}

    exists(key) { return this.client.exists(key); }
    set(key, value) { return this.client.set(key, value); }
    get(key) { return this.client.get(key); }
    delete(key) { return this.client.delete(key); }
    keys(pattern = '*') { return this.client.keys(pattern); }
    clean() { return this.client.clean(); }
    dumps() { return this.client.dumps(); }
    loads(jsonStr) { return this.client.loads(jsonStr); }
}

// Tests for SingletonKeyValueStorage 
[new SingletonKeyValueStorage().js_backend(), new SingletonKeyValueStorage().vue_backend(), new SingletonKeyValueStorage().indexedDB_backend()]
    .forEach(storage => {
        console.log(`Testing ${storage.client.constructor.name}...`);

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
