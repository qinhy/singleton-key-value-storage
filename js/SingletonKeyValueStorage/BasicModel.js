// from https://github.com/qinhy/singleton-key-value-storage.git
// Function to get current time in UTC
function now_utc() {
    // Get the current time in milliseconds since the Unix epoch
    const now = new Date();
    // Get the current time with higher precision (including microseconds)
    const milliseconds = now.getMilliseconds(); // Milliseconds part (0-999)
    const microseconds = Math.floor((performance.now() % 1) * 1000); // Simulate microseconds
    // Format the date to an ISO string
    let isoString = now.toISOString(); // e.g., "2024-09-18T16:49:21.552Z"
    // Replace the milliseconds part with microsecond precision
    return isoString.replace(/\.\d{3}Z/, `.${String(milliseconds).padStart(3, '0')}${String(microseconds).padStart(3, '0')}Z`);
}
class Controller4Basic {
    static AbstractObjController = class {
        constructor(store, model) {
            this.model = model; // Model4Basic.AbstractObj
            this._store = store; // BasicStore
        }

        storage() {
            return this._store;
        }

        update(kwargs) {
            if (!this.model) throw new Error('Controller has null model!');

            for (let key in kwargs) {
                if (kwargs.hasOwnProperty(key) && this.model.hasOwnProperty(key)) {
                    this.model[key] = kwargs[key];
                }
            }
            this._update_timestamp();
            this.store();
            return this;
        }

        _update_timestamp() {
            if (!this.model) throw new Error('Controller has null model!');
            this.model.update_time = now_utc();
        }

        store() {
            if (!this.model._id) throw new Error('Model ID is not set!');
            this.storage().set(this.model._id, this.model.model_dump_json_dict());
            return this;
        }

        delete() {
            this.storage().delete(this.model.get_id());
            this.model._controller = null;
        }

        update_metadata(key, value) {
            const updated_metadata = { ...this.model.metadata, [key]: value };
            this.update({ metadata: updated_metadata });
            return this;
        }
    };

    static AbstractGroupController = class extends Controller4Basic.AbstractObjController {
        constructor(store, model) {
            super(store, model);
            this.model = model; // Model4Basic.AbstractGroup
            this._store = store; // BasicStore
        }

        *yield_children_recursive(depth = 0) {
            for (let child_id of this.model.children_id) {
                if (!this.storage().exists(child_id)) continue;

                const child = this.storage().find(child_id);
                if (child.hasOwnProperty('parent_id') && child.hasOwnProperty('children_id')) {
                    const group = child.get_controller();
                    yield* group.yield_children_recursive(depth + 1);
                }
                yield { child, depth };
            }
        }

        delete_recursive() {
            for (let { child } of this.yield_children_recursive()) {
                child.get_controller().delete();
            }
            this.delete();
        }

        get_children_recursive() {
            const children_list = [];
            for (let child_id of this.model.children_id) {
                if (!this.storage().exists(child_id)) continue;

                const child = this.storage().find(child_id);
                if (child.hasOwnProperty('parent_id') && child.hasOwnProperty('children_id')) {
                    const group = child.get_controller();
                    children_list.push(group.get_children_recursive());
                } else {
                    children_list.push(child);
                }
            }
            return children_list;
        }

        get_children() {
            if (!this.model) throw new Error('Controller has a null model!');
            return this.model.children_id.map((child_id) => this.storage().find(child_id));
        }

        get_child(child_id) {
            return this.storage().find(child_id);
        }

        add_child(child_id) {
            return this.update({ children_id: [...this.model.children_id, child_id] });
        }

        delete_child(child_id) {
            if (!this.model.children_id.includes(child_id)) return this;

            const remaining_ids = this.model.children_id.filter((cid) => cid !== child_id);
            const child_con = this.storage().find(child_id).get_controller();

            if (child_con.delete_recursive) {
                child_con.delete_recursive();
            } else {
                child_con.delete();
            }

            this.update({ children_id: remaining_ids });
            return this;
        }
    };
}

class Model4Basic {
    static AbstractObj = class {
        constructor(data) {
            this._id = data?._id || null;
            this.rank = data?.rank || [0];
            this.create_time = data?.create_time || now_utc();
            this.update_time = data?.update_time || now_utc();
            this.status = data?.status || "";
            this.metadata = data?.metadata || {};
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
            if (this._id) throw new Error('This object ID is already set!');
            this._id = id;
            return this;
        }

        gen_new_id() {
            return `${this.class_name()}:${this._randuuid()}`;
        }

        get_id() {
            if (!this._id) throw new Error('This object ID is not set!');
            return this._id;
        }

        get_controller() {
            return this._controller;
        }

        init_controller(store) {
            const controller_class = this._get_controller_class(Controller4Basic);
            this._controller = new controller_class(store, this);
        }

        _get_controller_class(model_class = Controller4Basic) {
            const class_type = this.constructor.name + 'Controller';
            const res = Object.values(model_class).find(c => c.name === class_type);
            if (!res) throw new Error(`No such class of ${class_type}`);
            return res;
        }
    };

    static AbstractGroup = class extends Model4Basic.AbstractObj {
        constructor(data) {
            super(data);
            this.author_id = data?.author_id || '';
            this.parent_id = data?.parent_id || '';
            this.children_id = data?.children_id || [];
            this._controller = data?._controller || null;
        }

        get_controller() {
            return this._controller;
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
            'AbstractObj': Model4Basic.AbstractObj,
            'AbstractGroup': Model4Basic.AbstractGroup
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

    add_new_group(obj, id = null) {
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
        this.test_group();
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

    test_group() {
        // Reset and prepare the store
        this.store.clean();
        
        // Create a new object and a new group
        const obj = this.store.add_new_obj(new Model4Basic.AbstractObj());
        const group = this.store.add_new_group(new Model4Basic.AbstractGroup());
        
        // Add the object as a child to the group
        group.get_controller().add_child(obj.get_id());
        
        // Assert that the child matches the original object
        console.assert(
          JSON.stringify(group.get_controller().get_child(group.children_id[0]).model_dump_json_dict()) ===
            JSON.stringify(obj.model_dump_json_dict()),
          "The retrieved value should match the child value."
        );
        
        // Add a new group as a child to the original group
        const group2_id = this.store.add_new_group(new Model4Basic.AbstractGroup()).get_id();
        group.get_controller().add_child(group2_id);
        
        // Add a second object to the second group
        const obj2 = this.store.add_new_obj(new Model4Basic.AbstractObj());
        group.get_controller().get_child(group2_id).get_controller().add_child(obj2.get_id());
        
        // Retrieve the second group from the store
        const group2 = this.store.find(group2_id);
        
        // Assert that the first group's children match the added object and group
        const childrenDump = group.get_controller().get_children().map(child => child.model_dump_json_dict());
        console.assert(
          JSON.stringify(childrenDump) ===
            JSON.stringify([obj.model_dump_json_dict(), group2.model_dump_json_dict()]),
          "check get_children."
        );
        
        // Retrieve all children recursively
        const children = group.get_controller().get_children_recursive();
        console.log(children);
        
        // Assert the first child matches the first object
        console.assert(
          JSON.stringify(children[0].model_dump_json_dict()) ===
            JSON.stringify(obj.model_dump_json_dict()),
          "The retrieved first value should match the child value."
        );
        
        // Assert the second child is a list (which would be the second group and its children)
        console.assert(
          Array.isArray(children[1]),
          "The retrieved second value should be a list."
        );
        
        // Assert the first child of the second group matches the second object
        console.assert(
          JSON.stringify(children[1][0].model_dump_json_dict()) ===
            JSON.stringify(obj2.model_dump_json_dict()),
          "The retrieved second child value should match the child value."
        );
        
        // Delete the second group from the first group
        group.get_controller().delete_child(group2_id);
        
        // Assert that after deletion, only the first object remains in the first group
        console.assert(
          JSON.stringify(group.get_controller().get_children()[0].model_dump_json_dict()) ===
            JSON.stringify(obj.model_dump_json_dict()),
          "The retrieved value should match the child value."
        );
      }      

}

// Run the tests
new TestBasicStore().test_all();
