class SingletonStorageController {
    add_slave(slave) { this.model.slaves.push(slave); }
    _set_slaves(key, value) {
        this.model.slaves.forEach(slave => {
            if (slave.set) { slave.set(key, value); }
        });
    }
    _delete_slaves(key) {
        this.model.slaves.forEach(slave => {
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
    loads(jsonString = '{}') {this.clean();Object.entries(JSON.parse(jsonString)).forEach(d => this.set(d[0], d[1]));}
    
    randuuid(prefix = ''){
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
    get(){
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
        if(Vue){
            console.log("add Vue support")
            const { ref } = Vue;
            this.store = ref({});
        }
        else{
            console.log("no Vue support")
            this.store = null;
        }
        
        this.slaves = [];
        }
        return SingletonVueStorage._instance;
    }
    get(){
        return this.store.value;
    }
}

class SingletonVueStorageController extends SingletonJavascriptDictStorageController {
    constructor(model) {
        super();
        this.model = model;
    }
}

class SingletonKeyValueStorage extends SingletonStorageController {
    constructor() {
        super();
        this.js_backend();
    }

    js_backend() { this.client = new SingletonJavascriptDictStorageController(new SingletonJavascriptDictStorage()); return this;}
    vue_backend() { this.client = new SingletonVueStorageController(new SingletonVueStorage()); return this;}

    exists(key) { return this.client.exists(key); }
    set(key, value) { this.client.set(key, value); }
    get(key) { return this.client.get(key); }
    delete(key) { this.client.delete(key); }
    keys(pattern = '*') { return this.client.keys(pattern); }
    clean() { this.client.clean(); }
    dumps() { return this.client.dumps(); }
    loads(jsonStr) { this.client.loads(jsonStr); }
}

// Tests for SingletonKeyValueStorage 
[new SingletonKeyValueStorage().js_backend(),new SingletonKeyValueStorage().vue_backend()]
.forEach(storage => {
    console.log(`Testing ${storage.client.constructor.name}...`);

    // Test 1: Set and Get
    storage.set('key1', { data: 'value1' });
    const value1 = storage.get('key1');
    console.assert(JSON.stringify(value1) === JSON.stringify({ data: 'value1' }), 'Test 1 Failed: Set or Get does not work correctly');

    // Test 2: Exists
    const exists1 = storage.exists('key1');
    const exists2 = storage.exists('key2');
    console.assert(exists1 === true, 'Test 2 Failed: Exists does not return true for existing key');
    console.assert(exists2 === false, 'Test 2 Failed: Exists does not return false for non-existing key');

    // Test 3: Delete
    storage.delete('key1');
    const valueAfterDelete = storage.get('key1');
    console.assert(valueAfterDelete === null, 'Test 3 Failed: Delete does not remove the key properly');

    // Test 4: Keys
    storage.set('test1', { data: '123' });
    storage.set('test2', { data: '456' });
    storage.set('something', { data: '789' });
    const keys = storage.keys('test*');
    console.assert(keys.includes('test1') && keys.includes('test2') && keys.length === 2, 'Test 4 Failed: Keys does not filter correctly');

    // Test 5: Clean
    storage.clean();
    const keysAfterClean = storage.keys('*');
    console.assert(keysAfterClean.length === 0, 'Test 5 Failed: Clean does not clear all keys');

    console.log("All tests completed.");
});
