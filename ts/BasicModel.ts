import { SingletonKeyValueStorage } from './Storages';

function now_utc(): string {
    // Get the current time in milliseconds since the Unix epoch
    const now = new Date();
    // Get the current time with higher precision (including microseconds)
    const milliseconds = now.getMilliseconds(); // Milliseconds part (0-999)
    const microseconds = Math.floor((performance.now() % 1) * 1000); // Simulate microseconds
    // Format the date to an ISO string
    let isoString = now.toISOString(); // e.g., "2024-09-18T16:49:21.552Z"
    // Replace the milliseconds part with microsecond precision
    return isoString.replace(
        /\.\d{3}Z/,
        `.${String(milliseconds).padStart(3, '0')}${String(microseconds).padStart(3, '0')}Z`
    );
}

export namespace Controller4Basic {
    export class AbstractObjController {
        model: Model4Basic.AbstractObj | null;
        private _store: BasicStore;

        constructor(store: BasicStore, model: Model4Basic.AbstractObj) {
            this.model = model; // Model4Basic.AbstractObj
            this._store = store; // BasicStore
        }

        storage(): BasicStore {
            return this._store;
        }

        update(kwargs: any): this {
            if (!this.model) throw new Error('Controller has null model!');

            for (const key in kwargs) {
                if (kwargs.hasOwnProperty(key) && this.model.hasOwnProperty(key)) {
                    (this.model as any)[key] = kwargs[key];
                }
            }
            this._update_timestamp();
            this.store();
            return this;
        }

        private _update_timestamp(): void {
            if (!this.model) throw new Error('Controller has null model!');
            this.model.update_time = now_utc();
        }

        store(): this {
            if (!this.model?._id) throw new Error('Model ID is not set!');
            this.storage().set(this.model._id, this.model.model_dump_json_dict());
            return this;
        }

        delete(): void {
            if (!this.model) throw new Error('Controller has null model!');
            this.storage().delete(this.model.get_id());
            this.model._controller = null;
        }

        update_metadata(key: string, value: any): this {
            if (!this.model) throw new Error('Controller has null model!');
            const updated_metadata = { ...this.model.metadata, [key]: value };
            this.update({ metadata: updated_metadata });
            return this;
        }
    };

    export class AbstractGroupController extends Controller4Basic.AbstractObjController {
        // model: Model4Basic.AbstractGroup;

        constructor(store: BasicStore, model: Model4Basic.AbstractGroup) {
            super(store, model);
        }

        *yield_children_recursive(depth = 0): Generator<{ child: any; depth: number }> {
            const model = this.model as Model4Basic.AbstractGroup;
            if (!model) throw new Error('Controller has null model!');
            for (const child_id of model.children_id || []) {
                if (!this.storage().exists(child_id)) continue;

                const child = this.storage().find(child_id);
                if (child && child.hasOwnProperty('parent_id') && child.hasOwnProperty('children_id')) {
                    const group = child.get_controller();
                    yield* group.yield_children_recursive(depth + 1);
                }
                yield { child, depth };
            }
        }

        delete_recursive(): void {
            if (!this.model) throw new Error('Controller has null model!');
            for (const { child } of this.yield_children_recursive()) {
                child.get_controller().delete();
            }
            this.delete();
        }

        get_children_recursive(): any[] {
            const model = this.model as Model4Basic.AbstractGroup;
            if (!model) throw new Error('Controller has null model!');
            const children_list: any[] = [];
            for (const child_id of model.children_id || []) {
                
                if (!this.storage().exists(child_id)) continue;

                const child = this.storage().find(child_id);
                if (child && child.hasOwnProperty('parent_id') && child.hasOwnProperty('children_id')) {
                    const group :Controller4Basic.AbstractGroupController = child.get_controller();
                    children_list.push(group.get_children_recursive());
                } else {
                    children_list.push(child);
                }
            }
            return children_list;
        }

        get_children(): any[] {
            const model = this.model as Model4Basic.AbstractGroup;
            if (!model) throw new Error('Controller has a null model!');
            return (model.children_id || []).map((child_id) => this.storage().find(child_id));
        }

        get_child(child_id: string): any {
            return this.storage().find(child_id);
        }

        add_child(child_id: string): this {
            const model = this.model as Model4Basic.AbstractGroup;
            if (!model) throw new Error('Controller has null model!');
            return this.update({ children_id: [...(model.children_id || []), child_id] });
        }

        delete_child(child_id: string): this {
            const model = this.model as Model4Basic.AbstractGroup;
            if (!model || !model.children_id?.includes(child_id)) return this;

            const remaining_ids = (model.children_id || []).filter((cid) => cid !== child_id);
            const child = this.storage().find(child_id);
            if(!child)return this;

            const child_con = child.get_controller();

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

export namespace Model4Basic {
    export class AbstractObj {
        _id: string | null;
        rank: number[];
        create_time: string;
        update_time: string;
        status: string;
        metadata: Record<string, any>;
        _controller: any;

        constructor(data?: Partial<Model4Basic.AbstractObj>) {
            this._id = data?._id || null;
            this.rank = data?.rank || [0];
            this.create_time = data?.create_time || now_utc();
            this.update_time = data?.update_time || now_utc();
            this.status = data?.status || "";
            this.metadata = data?.metadata || {};
            this._controller = null;
        }

        private _randuuid(prefix = ''): string {
            return prefix + 'xxxx-xxxx-xxxx-xxxx-xxxx'.replace(/x/g, () => {
                return Math.floor(Math.random() * 16).toString(16);
            });
        }

        model_dump_json_dict(): Record<string, any> {
            const publicData: Record<string, any> = {};
            for (const key in this) {
                if (Object.prototype.hasOwnProperty.call(this, key) && !key.startsWith('_')) {
                    publicData[key] = (this as any)[key];
                }
            }
            return publicData;
        }

        class_name(): string {
            return this.constructor.name;
        }

        set_id(id: string): this {
            if (this._id) throw new Error('This object ID is already set!');
            this._id = id;
            return this;
        }

        gen_new_id(): string {
            return `${this.class_name()}:${this._randuuid()}`;
        }

        get_id(): string {
            if (!this._id) throw new Error('This object ID is not set!');
            return this._id;
        }

        get_controller(): any {
            return this._controller;
        }

        init_controller(store: any): void {
            const controller_class = this._get_controller_class(Controller4Basic);
            this._controller = new controller_class(store, this);
        }

        protected _get_controller_class(model_class:any): any {
            const class_type = `${this.constructor.name}Controller`;
            const res = Object.values(model_class).find(
                (c) => (c as any).name === class_type
            );
            if (!res) throw new Error(`No such class of ${class_type}`);
            return res;
        }
    };

    export class AbstractGroup extends Model4Basic.AbstractObj {
        author_id: string;
        parent_id: string;
        children_id: string[];

        constructor(data?: Partial<Model4Basic.AbstractGroup>) {
            super(data);
            this.author_id = data?.author_id || '';
            this.parent_id = data?.parent_id || '';
            this.children_id = data?.children_id || [];
            this._controller = data?._controller || null;
        }

        get_controller(): any {
            return this._controller;
        }
    };
}


export class BasicStore extends SingletonKeyValueStorage {
    constructor() {
        super();
        this.tempTsBackend();
    }

    protected _get_class(id: string): typeof Model4Basic.AbstractObj | typeof Model4Basic.AbstractGroup {
        const class_type = id.split(':')[0];
        const classes: Record<string, typeof Model4Basic.AbstractObj | typeof Model4Basic.AbstractGroup> = {
            'AbstractObj': Model4Basic.AbstractObj,
            'AbstractGroup': Model4Basic.AbstractGroup
        };
        const res = classes[class_type];
        if (!res) throw new Error(`No such class of ${class_type}`);
        return res;
    }

    protected _get_as_obj(id: string, data_dict: Record<string, any>): Model4Basic.AbstractObj {
        const ClassConstructor = this._get_class(id);
        const obj = new ClassConstructor(data_dict);
        obj.set_id(id).init_controller(this);
        return obj;
    }

    protected _add_new_obj(obj: Model4Basic.AbstractObj, id: string | null = null): Model4Basic.AbstractObj {
        id = id === null ? obj.gen_new_id() : id;
        const data = obj.model_dump_json_dict();
        this.set(id, data);
        return this._get_as_obj(id, data);
    }

    add_new_obj(obj: any, id: string | null = null): any {
        if (obj._id !== null) throw new Error(`obj._id is ${obj._id}, must be none`);
        return this._add_new_obj(obj, id);
    }

    add_new_group(obj: Model4Basic.AbstractGroup, id: string | null = null): Model4Basic.AbstractGroup {
        if (obj._id !== null) throw new Error(`obj._id is ${obj._id}, must be none`);
        return this._add_new_obj(obj, id) as Model4Basic.AbstractGroup;
    }

    find(id: string): Model4Basic.AbstractObj | null {
        const raw = this.get(id);
        if (raw === null) return null;
        return this._get_as_obj(id, raw);
    }

    find_all(id: string = 'AbstractObj:*'): Model4Basic.AbstractObj[] {
        return this.keys(id).map((key) => this.find(key)).filter((obj)=> obj !== null);
    }
}

class TestBasicStore {
    private store: BasicStore;

    constructor() {
        this.store = new BasicStore();
    }

    test_all(num: number = 1): void {
        this.test_tempts(num);
        console.log("[TestBasicStore]: All tests OK");
        
    }

    test_tempts(num: number = 1): void {
        this.store.tempTsBackend();
        for (let i = 0; i < num; i++) this.test_all_cases();
        this.store.clean();
    }

    test_all_cases(): void {
        this.store.clean();
        this.test_add_and_get();
        this.test_find_all();
        this.test_delete();
        this.test_get_nonexistent();
        this.test_dump_and_load();
        this.test_group();
    }

    test_get_nonexistent(): void {
        console.assert(
            this.store.find('nonexistent') === null,
            "Getting a non-existent key should return null."
        );
    }

    test_add_and_get(): void {
        const obj = this.store.add_new_obj(new Model4Basic.AbstractObj());
        const objr = this.store.find(obj.get_id());
        console.assert(
            objr !== null &&
            JSON.stringify(obj.model_dump_json_dict()) === JSON.stringify(objr.model_dump_json_dict()),
            "The retrieved value should match the set value."
        );
    }

    test_find_all(): void {
        this.store.add_new_obj(new Model4Basic.AbstractObj());
        console.assert(
            this.store.find_all().length === 2,
            "The retrieved value should match number of objs."
        );
    }

    test_dump_and_load(): void {
        const a = this.store.find_all();
        const js = this.store.dumps();
        this.store.clean();
        this.store.loads(js);
        const b = this.store.find_all();
        console.assert(
            a.every(
                (x, idx) =>
                    JSON.stringify(x.model_dump_json_dict()) ===
                    JSON.stringify(b[idx].model_dump_json_dict())
            ),
            "The same before dumps and loads."
        );
    }

    test_delete(): void {
        const obj = this.store.find_all()[0];
        obj.get_controller().delete();
        console.assert(
            !this.store.exists(obj.get_id()),
            "Key should not exist after being deleted."
        );
    }

    test_group(): void {
        this.store.clean();

        const obj = this.store.add_new_obj(new Model4Basic.AbstractObj());
        const group = this.store.add_new_group(new Model4Basic.AbstractGroup());

        group.get_controller().add_child(obj.get_id());

        console.assert(
            JSON.stringify(group.get_controller()
                .get_child(group.children_id[0]).model_dump_json_dict()) ===
            JSON.stringify(obj.model_dump_json_dict()),
            "The retrieved value should match the child value."
        );

        const group2_id = this.store.add_new_group(new Model4Basic.AbstractGroup()).get_id();
        group.get_controller().add_child(group2_id);
        
        const obj2 = this.store.add_new_obj(new Model4Basic.AbstractObj());
        group.get_controller().get_child(group2_id).get_controller().add_child(obj2.get_id());

        const group2 = this.store.find(group2_id);

        const childrenDump = group
            .get_controller()
            .get_children()
            .map((child) => child.model_dump_json_dict());
        console.assert(
            JSON.stringify(childrenDump) ===
            JSON.stringify([obj.model_dump_json_dict(), group2.model_dump_json_dict()]),
            "check get_children."
        );

        const children = group.get_controller().get_children_recursive();
        // console.log(children);

        console.assert(
            JSON.stringify(children[0].model_dump_json_dict()) ===
            JSON.stringify(obj.model_dump_json_dict()),
            "The retrieved first value should match the child value."
        );

        console.assert(Array.isArray(children[1]), "The retrieved second value should be a list.");

        console.assert(
            JSON.stringify(children[1][0].model_dump_json_dict()) ===
            JSON.stringify(obj2.model_dump_json_dict()),
            "The retrieved second child value should match the child value."
        );

        group.get_controller().delete_child(group2_id);

        console.assert(
            JSON.stringify(group.get_controller().get_children()[0].model_dump_json_dict()) ===
            JSON.stringify(obj.model_dump_json_dict()),
            "The retrieved value should match the child value."
        );
    }
}

// Run the tests
new TestBasicStore().test_all();
