// from https://github.com/qinhy/singleton-key-value-storage.git
// Function to get current time in UTC
function now_utc() {
    return new Date().toISOString();  // Returns UTC time in ISO string format
}
class Model4Basic {
    static AbstractObj = class {
        constructor(data = {}) {
            this._id = data._id || null;
            this.rank = data.rank || [0];
            this.create_time = data.create_time || now_utc();
            this.update_time = data.update_time || now_utc();
            this.status = data.status || "";
            this.metadata = data.metadata || {};
            this._controller = null;  // Initially null
        }

        _randuuid(prefix = '') {
            return prefix + 'xxxx-xxxx-xxxx-xxxx-xxxx'.replace(/x/g, function () {
                return Math.floor(Math.random() * 16).toString(16);
            });
        }

        model_dump_json_dict() {
            // Use Object.keys() to filter out keys that start with an underscore (_)
            const publicData = {};
            for (const key in this) {
                if (this.hasOwnProperty(key) && !key.startsWith('_')) {
                    publicData[key] = this[key];
                }
            }
            return publicData;  // Return only the public data
        }

        class_name() {
            return this.constructor.name;
        }

        set_id(id) {
            if (this._id !== null) {
                throw new Error('This object is already set! Cannot set again!');
            }
            this._id = id;
            return this;
        }

        gen_new_id() {
            return `${this.class_name()}:${this._randuuid()}`;
        }

        get_id() {
            if (this._id === null) {
                throw new Error('This object is not set!');
            }
            return this._id;
        }

        get_controller() {
            return this._controller;
        }

        init_controller(store) {
            this._controller = new Controller4Basic.AbstractObjController(store, this);
        }
    };
}
class Controller4Basic {
    static AbstractObjController = class {
        constructor(store, model) {
            this.model = model;
            this._store = store;
        }

        update(kwargs) {
            console.assert(this.model !== null, 'controller has null model!');
            for (const [key, value] of Object.entries(kwargs)) {
                if (key in this.model) {
                    this.model[key] = value;
                }
            }
            this._update_timestamp();
            this.store();
        }

        _update_timestamp() {
            console.assert(this.model !== null, 'controller has null model!');
            this.model.update_time = now_utc();
        }

        store() {
            console.assert(this.model._id !== null, 'Model ID is required!');
            this._store.set(this.model._id, this.model.model_dump_json_dict());
            return this;
        }

        delete() {
            this._store.delete(this.model.get_id());
            this.model._controller = null;
        }

        update_metadata(key, value) {
            const updated_metadata = { ...this.model.metadata, [key]: value };
            this.update({ metadata: updated_metadata });
            return this;
        }
    };
}
class BasicStore extends SingletonKeyValueStorage {
    constructor() {
        super();
        this.js_backend();
    }

    _get_class(id) {
        const class_type = id.split(':')[0];
        const classes = {
            'AbstractObj': Model4Basic.AbstractObj
        };
        const res = classes[class_type];
        if (!res) throw new Error(`No such class of ${class_type}`);
        return res;
    }

    _get_as_obj(id, data_dict) {
        const obj = new (this._get_class(id))(data_dict);
        obj.set_id(id).init_controller(this);
        return obj;
    }

    _add_new_obj(obj, id = null) {
        id = id === null ? obj.gen_new_id() : id;
        const d = obj.model_dump_json_dict();
        this.set(id, d);
        return this._get_as_obj(id, d);
    }

    add_new_obj(obj, id = null) {
        if (obj._id !== null) throw new Error(`obj._id is ${obj._id}, must be none`);
        return this._add_new_obj(obj, id);
    }

    find(id) {
        const raw = this.get(id);
        if (raw === null) return null;
        return this._get_as_obj(id, raw);
    }

    find_all(id = 'AbstractObj:*') {
        return this.keys(id).map((key) => this.find(key));
    }
}

class TestBasicStore {
    constructor() {
        this.store = new BasicStore();
    }

    test_all(num = 1) {
        this.test_python(num);
    }

    test_python(num = 1) {
        this.store.js_backend();
        for (let i = 0; i < num; i++) this.test_all_cases();
        this.store.clean();
    }

    test_all_cases() {
        this.store.clean();
        this.test_add_and_get();
        this.test_find_all();
        this.test_delete();
        this.test_get_nonexistent();
        this.test_dump_and_load();
    }

    test_get_nonexistent() {
        console.assert(this.store.find('nonexistent') === null, "Getting a non-existent key should return null.");
    }

    test_add_and_get() {
        const obj = this.store.add_new_obj(new Model4Basic.AbstractObj());
        const objr = this.store.find(obj.get_id());
        console.assert(
            JSON.stringify(obj.model_dump_json_dict()) === JSON.stringify(objr.model_dump_json_dict()),
            "The retrieved value should match the set value."
        );
    }

    test_find_all() {
        this.store.add_new_obj(new Model4Basic.AbstractObj());
        console.assert(
            this.store.find_all().length === 2,
            "The retrieved value should match number of objs."
        );
    }

    test_dump_and_load() {
        const a = this.store.find_all();
        const js = this.store.dumps();
        this.store.clean();
        this.store.loads(js);
        const b = this.store.find_all();
        console.assert(
            a.every((x, idx) => JSON.stringify(x.model_dump_json_dict()) === JSON.stringify(b[idx].model_dump_json_dict())),
            "The same before dumps and loads."
        );
    }

    test_delete() {
        const obj = this.store.find_all()[0];
        obj.get_controller().delete();
        console.assert(!this.store.exists(obj.get_id()), "Key should not exist after being deleted.");
    }
}

// Run the tests
new TestBasicStore().test_all();
