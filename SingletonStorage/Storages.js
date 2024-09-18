// A utility function to handle errors in JavaScript
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
        const regex = new RegExp('^'+pattern.replace(/\*/g, '.*'));
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
        if (id === null) id = this.client._randuuid();
        this.client.set(`${EventDispatcherController.ROOT_KEY}:${event_name}:${id}`, callback);
        return id;
    }

    dispatch(event_name, ...args) {
        this.client.keys(`${EventDispatcherController.ROOT_KEY}:${event_name}:*`).forEach(event_full_uuid => {
            this.client.get(event_full_uuid)(...args);
        });
    }

    async async_dispatch(event_name, ...args) {
        return await Promise.all(
            this.client.keys(`${EventDispatcherController.ROOT_KEY}:${event_name}:*`).map(
                async(event_full_uuid) => this.client.get(event_full_uuid)(...args))
            );
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

class LocalVersionController {
    constructor(client = null) {
        if (client === null) {
            client = new SingletonJavascriptDictStorageController(new JavascriptDictStorage());
        }
        this.client = client;
        this.client.set('_Operations', { ops: [] });
    }

    add_operation(operation, revert = null) {
        const opuuid = this.client._randuuid();
        this.client.set(`_Operation:${opuuid}`, { forward: operation, revert: revert });
        const ops = this.client.get('_Operations');
        ops.ops.push(opuuid);
        this.client.set('_Operations', ops);
    }

    revert_one_operation(revertCallback) {
        const ops = this.client.get('_Operations').ops;
        const opuuid = ops[ops.length - 1];
        const op = this.client.get(`_Operation:${opuuid}`);
        const revert = op.revert;
        // Perform revert
        revertCallback(revert);
        ops.pop();
        this.client.set('_Operations', { ops: ops });
    }

    get_versions() {
        return this.client.get('_Operations').ops;
    }

    revert_operations_untill(opuuid, revertCallback) {
        const ops = [...this.client.get('_Operations').ops];
        if (ops.includes(opuuid)) {
            for (let i = ops.length - 1; i >= 0; i--) {
                if (ops[i] === opuuid) break;
                this.revert_one_operation(revertCallback);
            }
        } else {
            throw new Error(`No such version of ${opuuid}`);
        }
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

class SingletonFastAPIStorageController extends SingletonStorageController {
    constructor(apiBaseUrl = '') {
        super();
        this.apiBaseUrl = apiBaseUrl; // Base URL for the FastAPI endpoints
    }

    async set(key, value) {
        // @api.post("/store/set/")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/set/${key}`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ key, value }),
            });

            if (!response.ok) {
                throw new Error(`Failed to set key: ${response.statusText}`);
            }
        } catch (error) {
            console.error(`[${this.constructor.name}]: Error setting key ${key} - ${error.message}`);
        }
    }

    async get(key) {
        // @api.get("/store/get/{key}")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/get/${encodeURIComponent(key)}`);
            if (!response.ok) {
                throw new Error(`Failed to get key: ${response.statusText}`);
            }
            const data = await response.json();
            return data;
        } catch (error) {
            console.log(`[${this.constructor.name}]: Error getting key ${key} - ${error.message}`);
            return null;
        }
    }

    async delete(key) {
        // @api.delete("/store/delete/{key}")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/delete/${encodeURIComponent(key)}`, {
                method: 'DELETE',
            });

            if (!response.ok) {
                throw new Error(`Failed to delete key: ${response.statusText}`);
            }
            const data = await response.json();
            return data.delete;

        } catch (error) {
            console.error(`[${this.constructor.name}]: Error deleting key ${key} - ${error.message}`);
        }
    }

    async clean() {
        // @api.delete("/store/clean")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/clean`, {
                method: 'DELETE',
            });

            if (!response.ok) {
                throw new Error(`Failed to clean`);
            }
            const data = await response.json();
            return data.clean;

        } catch (error) {
            console.error(`[${this.constructor.name}]: Error clean - ${error.message}`);
        }
    }

    async exists(key) {
        // @api.get("/store/exists/{key}")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/exists/${encodeURIComponent(key)}`);
            if (!response.ok) {
                throw new Error(`Failed to check if key exists: ${response.statusText}`);
            }
            const data = await response.json();
            return data.exists; // Assuming the response contains { "exists": true/false }
        } catch (error) {
            console.error(`[${this.constructor.name}]: Error checking existence of key ${key} - ${error.message}`);
        }
    }

    async keys(pattern = '*') {
        // @api.get("/store/keys/{pattern}")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/keys/${encodeURIComponent(pattern)}`);
            if (!response.ok) {
                throw new Error(`Failed to get keys: ${response.statusText}`);
            }
            const data = await response.json();
            return data; // Assuming the response contains { "keys": [...] }
        } catch (error) {
            console.log(`[${this.constructor.name}]: Error getting keys with pattern ${pattern} - ${error.message}`);
            return [];
        }
    }

    async dumps() {
        // @api.post("/store/loads/")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/dumps/`);
            if (!response.ok) {
                throw new Error(`Failed to dump data: ${response.statusText}`);
            }
            const data = await response.json();
            return data.dumps; // Assuming the response contains { "dumps": "..." }
        } catch (error) {
            console.error(`[${this.constructor.name}]: Error dumping data - ${error.message}`);
        }
    }

    async loads(jsonString = '{}') {
        // @api.get("/store/dumps/")
        try {
            const response = await fetch(`${this.apiBaseUrl}/store/loads/`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ data: jsonString }),
            });

            if (!response.ok) {
                throw new Error(`Failed to load data: ${response.statusText}`);
            }
        } catch (error) {
            console.error(`[${this.constructor.name}]: Error loading data - ${error.message}`);
        }
    }
}

class SingletonKeyValueStorage extends SingletonStorageController {
    constructor(version_controll = false) {
        super();
        this.version_controll = version_controll;
        this.conn = null;
        this.js_backend();
    }

    _switch_backend(name = 'js', ...args) {
        this.event_dispa = new EventDispatcherController();
        this._hist = new KeysHistoryController();
        this._verc = new LocalVersionController();
        const backs = {
            'js': () => new SingletonJavascriptDictStorageController(new SingletonJavascriptDictStorage()),
            'fastapi': () => new SingletonFastAPIStorageController()
        };
        const back = backs[name.toLowerCase()] || (() => null);
        const backend_instance = back();
        if (backend_instance === null) {
            throw new Error(`No backend of ${name}, available backends: ${Object.keys(backs)}`);
        }
        return backend_instance;
    }

    js_backend() {
        this.conn = this._switch_backend('js');
    }
    fastapi_backend() {
        this.conn = this._switch_backend('fastapi');
    }

    _print(msg) {
        console.log(`[${this.constructor.name}]: ${msg}`);
    }

    add_slave(slave, event_names = ['set', 'delete']) {
        console.log(`add a slave with events of ${event_names}`);
        if (!slave.uuid) {
            try {
                slave.uuid = this.conn._randuuid();
            } catch (e) {
                this._print(`Cannot set uuid to ${slave}. Skipping this slave.`);
                return false;
            }
        }
        for (const event_name of event_names) {
            if (typeof slave[event_name] === 'function') {
                this.event_dispa.set_event(event_name, slave[event_name].bind(slave), slave.uuid);
            } else {
                this._print(`No function "${event_name}" in ${slave}. Skipping it.`);
            }
        }
        return true;
    }

    delete_slave(slave) {
        this.event_dispa.delete_event(slave.uuid);
    }

    _edit_local(func_name, key = null, value = null) {
        if (!['set', 'delete', 'clean', 'load', 'loads'].includes(func_name)) {
            this._print(`No function "${func_name}". Returning.`);
            return;
        }
        this._hist.reset();
        const func = this.conn[func_name].bind(this.conn);
        const args = [key, value].filter(x => x !== null);
        return func(...args);
    }

    _edit(func_name, key = null, value = null) {
        const args = [key, value].filter(x => x !== null);
        const res = this._edit_local(func_name,key,value);
        // this.event_dispa.dispatch(func_name, ...args)

        const version = this.get_current_version();
        this.event_dispa.async_dispatch(func_name, ...args).catch(e=>{
            this.revert_operations_untill(version);
        });
        return res;
    }

    _try_edit_error(args) {
        if (this.version_controll) {
            const func = args[0];
            if (func === 'set') {
                const [_, key, value] = args;
                let revert = null;
                if (this.exists(key)) {
                    revert = [func, key, this.get(key)];
                } else {
                    revert = ['delete', key];
                }
                this._verc.add_operation(args, revert);
            } else if (func === 'delete') {
                const [_, key] = args;
                const revert = ['set', key, this.get(key)];
                this._verc.add_operation(args, revert);
            } else if (['clean', 'load', 'loads'].includes(func)) {
                const revert = ['loads', this.dumps()];
                this._verc.add_operation(args, revert);
            }
        }

        try {
            this._edit(...args);
            return true;
        } catch (e) {
            this._print(e);
            return false;
        }
    }

    revert_one_operation() {
        this._verc.revert_one_operation(revert => this._edit(...revert));
    }

    get_current_version() {
        const vs = this._verc.get_versions();
        return vs.length === 0 ? null : vs[vs.length - 1];
    }

    revert_operations_untill(opuuid) {
        this._verc.revert_operations_untill(opuuid, revert => this._edit(...revert));
    }

    // True or False (in case of error)
    set(key, value) { return this._try_edit_error(['set', key, value]); }
    delete(key) { return this._try_edit_error(['delete', key]); }
    clean() { return this._try_edit_error(['clean']); }
    load(json_path) { return this._try_edit_error(['load', json_path]); }
    loads(json_str) { return this._try_edit_error(['loads', json_str]); }

    _try_obj_error(func) {
        try {
            return func();
        } catch (e) {
            this._print(e);
            return null;
        }
    }

    // Object or None (in case of error)
    exists(key) { return this._try_obj_error(() => this.conn.exists(key)); }
    keys(regx = '*') { return this._try_obj_error(() => this.conn.keys(regx)); }
    get(key) { return this._try_obj_error(() => this.conn.get(key)); }
    dumps() { return this._try_obj_error(() => this.conn.dumps()); }
    dump(json_path) { return this._try_obj_error(() => this.conn.dump(json_path)); }
}

// Tests for SingletonKeyValueStorage 
class Tests {
    constructor() {
        this.store = new SingletonKeyValueStorage();
    }

    test_all(num = 1) {
        this.test_js(num);
    }

    test_js(num = 1) {
        this.store.js_backend();
        for (let i = 0; i < num; i++) this.test_all_cases();
    }

    test_all_cases() {
        this.test_set_and_get();
        this.test_exists();
        this.test_delete();
        this.test_keys();
        this.test_get_nonexistent();
        this.test_dump_and_load();
        this.test_version();
        this.test_slaves();
    }

    test_set_and_get() {
        this.store.set('test1', { data: 123 });
        console.assert(JSON.stringify(this.store.get('test1')) === JSON.stringify({ data: 123 }),
            "The retrieved value should match the set value.");
    }

    test_exists() {
        this.store.set('test2', { data: 456 });
        console.assert(this.store.exists('test2') === true, "Key should exist after being set.");
    }

    test_delete() {
        this.store.set('test3', { data: 789 });
        this.store.delete('test3');
        console.assert(this.store.exists('test3') === false, "Key should not exist after being deleted.");
    }

    test_keys() {
        this.store.set('alpha', { info: 'first' });
        this.store.set('abeta', { info: 'second' });
        this.store.set('gamma', { info: 'third' });
        const expected_keys = ['alpha', 'abeta'];
        console.assert(
            JSON.stringify(this.store.keys('a*').sort()) === JSON.stringify(expected_keys.sort()),
            "Should return the correct keys matching the pattern."
        );
    }

    test_get_nonexistent() {
        console.assert(this.store.get('nonexistent') === null, "Getting a non-existent key should return null.");
    }

    test_dump_and_load() {
        const raw = {
            "test1": { "data": 123 },
            "test2": { "data": 456 },
            "alpha": { "info": "first" },
            "abeta": { "info": "second" },
            "gamma": { "info": "third" }
        };

        this.store.clean();
        console.assert(this.store.dumps() === '{}', "Should return the correct keys and values.");

        this.store.clean();
        this.store.loads(JSON.stringify(raw));
        console.assert(JSON.stringify(JSON.parse(this.store.dumps())) === JSON.stringify(raw),
            "Should return the correct keys and values.");
    }

    test_slaves() {
        if (this.store.conn.constructor.name === 'SingletonPythonDictStorageController') return;

        const store2 = new SingletonKeyValueStorage();
        this.store.add_slave(store2);

        this.store.set('alpha', { info: 'first' });
        this.store.set('abeta', { info: 'second' });
        this.store.set('gamma', { info: 'third' });
        this.store.delete('abeta');

        console.assert(
            JSON.stringify(JSON.parse(this.store.dumps())) === JSON.stringify(JSON.parse(store2.dumps())),
            "Should return the correct keys and values."
        );
    }

    test_version() {
        this.store.version_controll = true;
        this.store.clean();
        this.store.set('alpha', { info: 'first' });
        const data = this.store.dumps();
        const version = this.store.get_current_version();

        this.store.set('abeta', { info: 'second' });
        this.store.set('gamma', { info: 'third' });
        this.store.revert_operations_untill(version);

        console.assert(
            JSON.stringify(JSON.parse(this.store.dumps())) === JSON.stringify(JSON.parse(data)),
            "Should return the same keys and values."
        );
    }
}

// Running tests
new Tests().test_all();
