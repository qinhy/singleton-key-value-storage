mod rjson;
mod storage;

use rjson::*;
use storage::*;
use serde_json::{json, Value};
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn main() {
    let mut failed = 0usize;
    let mut ran = 0usize;

    macro_rules! run {
        ($name:expr, $fn:path) => {{
            ran += 1;
            print!("test {name: <36} ... ", name = $name);
            let res = catch_unwind(AssertUnwindSafe(|| $fn()));
            match res {
                Ok(()) => {
                    println!("ok");
                }
                Err(_) => {
                    println!("FAILED");
                    failed += 1;
                }
            }
        }};
    }

    run!("msg_queue_basics_and_events", test_msg_queue_basics_and_events);
    run!("set_get_exists_delete_keys", test_set_get_exists_delete_keys);
    run!("dump_and_load", test_dump_and_load);
    run!("slaves_via_events", test_slaves_via_events);
    run!("versioning_and_memory_limit", test_versioning_and_memory_limit);

    println!("\nresult: {passed} passed; {failed} failed; {ran} total",
        passed = ran - failed,
        failed = failed,
        ran = ran
    );

    if failed > 0 {
        std::process::exit(1);
    }
}

// ===== tests-as-functions =====

fn test_msg_queue_basics_and_events() {
    // fresh in-memory queue
    let store = MemoryLimitedDictStorageController::new(
        DictStorage::new(),
        1024.0,
        Policy::Lru,
        None,
        None,
    );
    let mut mq = MessageQueueController::new(store, None);

    // FIFO + size
    mq.push("default", json!({"n": 1}));
    mq.push("default", json!({"n": 2}));
    mq.push("default", json!({"n": 3}));
    assert_eq!(mq.queue_size("default"), 3);

    assert_eq!(mq.pop("default").unwrap(), json!({"n": 1}));
    assert_eq!(mq.pop("default").unwrap(), json!({"n": 2}));
    assert_eq!(mq.pop("default").unwrap(), json!({"n": 3}));
    assert!(mq.pop("default").is_none());
    assert_eq!(mq.queue_size("default"), 0);

    // peek does not remove
    mq.push("default", json!({"a": 1}));
    assert_eq!(mq.peek("default").unwrap(), json!({"a": 1}));
    assert_eq!(mq.queue_size("default"), 1);
    assert_eq!(mq.pop("default").unwrap(), json!({"a": 1}));

    // clear resets
    mq.push("default", json!({"x": 1}));
    mq.push("default", json!({"y": 2}));
    mq.clear("default");
    assert_eq!(mq.queue_size("default"), 0);
    assert!(mq.pop("default").is_none());

    // event flow (capture kinds)
    use std::sync::{Arc, Mutex};
    let events: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(vec![]));
    {
        let ev = events.clone();
        mq.add_listener(
            "default",
            "pushed",
            move |_| ev.lock().unwrap().push("pushed"),
            None,
        );
    }
    {
        let ev = events.clone();
        mq.add_listener(
            "default",
            "popped",
            move |_| ev.lock().unwrap().push("popped"),
            None,
        );
    }
    {
        let ev = events.clone();
        mq.add_listener(
            "default",
            "empty",
            move |_| ev.lock().unwrap().push("empty"),
            None,
        );
    }
    {
        let ev = events.clone();
        mq.add_listener(
            "default",
            "cleared",
            move |_| ev.lock().unwrap().push("cleared"),
            None,
        );
    }

    mq.push("default", json!({"m": 1}));
    mq.push("default", json!({"m": 2}));
    let a = mq.pop("default").unwrap();
    let b = mq.pop("default").unwrap();
    mq.clear("default");

    assert_eq!(a, json!({"m": 1}));
    assert_eq!(b, json!({"m": 2}));

    let kinds = events.lock().unwrap().clone();
    assert_eq!(
        kinds,
        vec!["pushed", "pushed", "popped", "popped", "empty", "cleared"]
    );

    // multiple queues are isolated
    mq.push("q1", json!({"a": 1}));
    mq.push("q2", json!({"b": 2}));
    assert_eq!(mq.queue_size("q1"), 1);
    assert_eq!(mq.queue_size("q2"), 1);
    assert_eq!(mq.pop("q1").unwrap(), json!({"a": 1}));
    assert_eq!(mq.queue_size("q2"), 1);
}

fn test_set_get_exists_delete_keys() {
    let mut store = SingletonKeyValueStorage::new(false, None);

    // set & get
    store.set("test1", json!({"data": 123}));
    assert_eq!(store.get("test1").unwrap(), json!({"data": 123}));

    // exists
    store.set("test2", json!({"data": 456}));
    assert!(store.exists("test2"));

    // delete
    store.set("test3", json!({"data": 789}));
    store.delete("test3");
    assert!(!store.exists("test3"));

    // keys with pattern
    store.set("alpha", json!({"info": "first"}));
    store.set("abeta", json!({"info": "second"}));
    store.set("gamma", json!({"info": "third"}));

    let mut ks = store.keys("a*");
    ks.sort();
    assert_eq!(ks, vec!["abeta".to_string(), "alpha".to_string()]);

    // get nonexistent
    assert!(store.get("nonexistent").is_none());
}

fn test_dump_and_load() {
    let mut store = SingletonKeyValueStorage::new(false, None);

    // seed
    store.set("test1", json!({"data": 123}));
    store.set("test2", json!({"data": 456}));
    store.set("alpha", json!({"info": "first"}));
    store.set("abeta", json!({"info": "second"}));
    store.set("gamma", json!({"info": "third"}));

    // dump
    let path = "test.json";
    store.dump(path).unwrap();

    // clean
    store.clean();
    assert_eq!(store.dumps(), "{}");

    // load from file
    let _ = store.load(path); // returns bool in our wrapper; ignore
    let raw: Value = serde_json::from_str(
        r#"{"test1":{"data":123},"test2":{"data":456},"alpha":{"info":"first"},"abeta":{"info":"second"},"gamma":{"info":"third"}}"#
    ).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&store.dumps()).unwrap(), raw);

    // clean + loads
    store.clean();
    store.loads(&raw.to_string());
    assert_eq!(serde_json::from_str::<Value>(&store.dumps()).unwrap(), raw);

    // cleanup
    let _ = fs::remove_file(path);
}

fn test_slaves_via_events() {
    use std::sync::{Arc, Mutex};

    let mut master = SingletonKeyValueStorage::new(false, None);
    let slave = Arc::new(Mutex::new(SingletonKeyValueStorage::new(false, None)));

    // mirror "set" and "delete" from master -> slave using event payloads
    {
        let s = slave.clone();
        master.set_event(
            "set",
            move |msg| {
                if let Some(m) = msg {
                    if let Some(k) = m.get("key").and_then(|x| x.as_str()) {
                        if let Some(v) = m.get("value") {
                            s.lock().unwrap().set(k, v.clone());
                        }
                    }
                }
            },
            None,
        );
    }
    {
        let s = slave.clone();
        master.set_event(
            "delete",
            move |msg| {
                if let Some(m) = msg {
                    if let Some(k) = m.get("key").and_then(|x| x.as_str()) {
                        s.lock().unwrap().delete(k);
                    }
                }
            },
            None,
        );
    }

    master.set("alpha", json!({"info": "first"}));
    master.set("abeta", json!({"info": "second"}));
    master.set("gamma", json!({"info": "third"}));
    master.delete("abeta");

    let m = serde_json::from_str::<Value>(&master.dumps()).unwrap();
    let s = serde_json::from_str::<Value>(&slave.lock().unwrap().dumps()).unwrap();
    assert_eq!(m, s);
}

fn test_versioning_and_memory_limit() {
    // versioning on
    let mut store = SingletonKeyValueStorage::new(true, None);
    store.clean();

    store.set("alpha", json!({"info": "first"}));
    let data1 = store.dumps();
    let v1 = store.get_current_version().unwrap();

    store.set("abeta", json!({"info": "second"}));
    let v2 = store.get_current_version().unwrap();
    let data2 = store.dumps();

    store.set("gamma", json!({"info": "third"}));
    store.local_to_version(&v1).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&store.dumps()).unwrap(),
        serde_json::from_str::<Value>(&data1).unwrap()
    );

    store.local_to_version(&v2).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&store.dumps()).unwrap(),
        serde_json::from_str::<Value>(&data2).unwrap()
    );

    // independent memory-limit test on an LVC instance
    fn make_big_payload(kib: usize) -> String {
        "X".repeat(1024).repeat(kib)
    }

    let mut lvc = LocalVersionController::new(0.2); // 0.2 MB
    for i in 0..3 {
        let small = make_big_payload(62); // ~0.062 MB
        let res = lvc.add_operation(
            Operation::Set(format!("small_{i}"), json!(small)),
            Some(Operation::Delete(format!("small_{i}"))),
        );
        assert!(res.is_none(), "no warning for small payloads");
    }
    let big = make_big_payload(600); // ~0.6 MB
    let res = lvc.add_operation(
        Operation::Set("too_big".to_string(), json!(big)),
        Some(Operation::Delete("too_big".to_string())),
    );
    let msg = res.expect("should warn about memory usage");
    assert!(
        msg.starts_with("[LocalVersionController] Warning: memory usage"),
        "warning message starts correctly"
    );
}
