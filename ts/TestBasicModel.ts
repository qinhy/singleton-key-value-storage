import { BasicStore,Model4Basic } from './BasicModel';

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
