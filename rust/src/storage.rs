//! A Rust translation of
//! https://github.com/qinhy/singleton-key-value-storage
//!
//! Notes / differences from Python version:
//! - Values are `serde_json::Value` to mimic Python `dict` freedom.
//! - "Deep size" is approximated by the length of a JSON serialization.
//! - The event dispatcher stores Rust closures in-memory; event listing
//!   returns event keys (callbacks themselves aren’t printable).
//! - RSA helpers are exposed via a trait `Encryptor`. Plug in your own
//!   implementation (or leave `None`).
//! - Glob matching for keys uses `wildmatch` (“*”, “?” patterns).
//! - Controllers are not thread-safe by default. Wrap in `Arc<Mutex<..>>`
//!   from your application if you need concurrency.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64URL, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::Path;
use uuid::Uuid;
use wildmatch::WildMatch;

/* ------------------------------ utils ------------------------------ */

pub fn b64url_encode(s: &str) -> String {
    B64URL.encode(s.as_bytes())
}

pub fn b64url_decode(s: &str) -> Option<String> {
    B64URL.decode(s.as_bytes()).ok().and_then(|b| String::from_utf8(b).ok())
}

pub fn is_b64url(s: &str) -> bool {
    b64url_decode(s)
        .map(|decoded| b64url_encode(&decoded) == s)
        .unwrap_or(false)
}

fn deep_bytes_size_value(v: &Value) -> usize {
    serde_json::to_vec(v).map(|b| b.len()).unwrap_or(0)
}

fn deep_bytes_size_str(s: &str) -> usize {
    s.as_bytes().len()
}

pub fn humanize_bytes(n: usize) -> String {
    let mut size = n as f64;
    for unit in ["B", "KB", "MB", "GB", "TB"] {
        if size < 1024.0 {
            return format!("{size:3.1} {unit}");
        }
        size /= 1024.0;
    }
    format!("{size:.1} PB")
}

/* -------------------------- storage traits -------------------------- */

pub trait StorageController: Send + Sync {
    fn exists(&self, key: &str) -> bool;
    fn set(&mut self, key: &str, value: Value);
    fn get(&self, key: &str) -> Option<Value>;
    fn delete(&mut self, key: &str) -> Option<Value>;
    fn keys(&self, pattern: &str) -> Vec<String>;
    fn clean(&mut self) {
        let ks = self.keys("*");
        for k in ks {
            let _ = self.delete(&k);
        }
    }
    fn dumps(&self) -> String {
        let mut map = serde_json::Map::new();
        for k in self.keys("*") {
            if let Some(v) = self.get(&k) {
                map.insert(k, v);
            }
        }
        Value::Object(map).to_string()
    }
    fn loads(&mut self, json_str: &str) {
        let v: Value = serde_json::from_str(json_str).unwrap_or(Value::Object(Default::default()));
        if let Some(obj) = v.as_object() {
            for (k, v) in obj {
                self.set(k, v.clone());
            }
        }
    }
    fn dump(&self, path: &str) -> std::io::Result<()> {
        fs::write(path, self.dumps())
    }
    fn load(&mut self, path: &str) -> std::io::Result<()> {
        let txt = fs::read_to_string(path)?;
        self.loads(&txt);
        Ok(())
    }

    /// deep byte estimate (json-serialized length)
    fn bytes_used(&self) -> usize;
}

/* -------------------------- RSA encryptor --------------------------- */
pub trait Encryptor: Send + Sync {
    fn encrypt_string(&self, plaintext: &str) -> String;
    fn decrypt_string(&self, ciphertext: &str) -> String;
}

/* --------------------------- dict storage --------------------------- */

#[derive(Clone, Debug, Default)]
pub struct DictStorage {
    pub id: Uuid,
    store: HashMap<String, Value>,
}

impl DictStorage {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            store: HashMap::new(),
        }
    }
}

pub struct DictStorageController {
    model: DictStorage,
}

impl DictStorageController {
    pub fn new(model: DictStorage) -> Self {
        Self { model }
    }
    pub fn tmp() -> Self {
        Self::new(DictStorage::new())
    }
}

impl StorageController for DictStorageController {
    fn exists(&self, key: &str) -> bool {
        self.model.store.contains_key(key)
    }

    fn set(&mut self, key: &str, value: Value) {
        self.model.store.insert(key.to_string(), value);
    }

    fn get(&self, key: &str) -> Option<Value> {
        self.model.store.get(key).cloned()
    }

    fn delete(&mut self, key: &str) -> Option<Value> {
        self.model.store.remove(key)
    }

    fn keys(&self, pattern: &str) -> Vec<String> {
        let wm = WildMatch::new(pattern);
        self.model
            .store
            .keys()
            .filter(|k| wm.matches(k))
            .cloned()
            .collect()
    }

    fn bytes_used(&self) -> usize {
        // JSON size of full map (cheap, simple)
        deep_bytes_size_value(&serde_json::to_value(&self.model.store).unwrap_or(Value::Null))
    }
}

/* ---------------------- memory-limited controller -------------------- */

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Policy {
    Lru,
    Fifo,
}

pub struct MemoryLimitedDictStorageController {
    inner: DictStorageController,
    max_bytes: usize,
    policy: Policy,

    sizes: HashMap<String, usize>, // key -> approx bytes
    order: VecDeque<String>,       // access/insertion order
    pinned: HashSet<String>,
    current_bytes: usize,

    // eviction callback
    on_evict: Box<dyn Fn(&str, &Value) + 'static>,
}

impl MemoryLimitedDictStorageController {
    pub fn new(
        model: DictStorage,
        max_memory_mb: f64,
        policy: Policy,
        on_evict: Option<Box<dyn Fn(&str, &Value) + 'static>>,
        pinned: Option<HashSet<String>>,
    ) -> Self {
        Self {
            inner: DictStorageController::new(model),
            max_bytes: (max_memory_mb.max(0.0) * 1024.0 * 1024.0) as usize,
            policy,
            sizes: HashMap::new(),
            order: VecDeque::new(),
            pinned: pinned.unwrap_or_default(),
            current_bytes: 0,
            on_evict: on_evict.unwrap_or_else(|| Box::new(|_, _| {})),
        }
    }

    fn entry_size(key: &str, value: &Value) -> usize {
        deep_bytes_size_str(key) + deep_bytes_size_value(value)
    }

    fn reduce(&mut self, key: &str) {
        if let Some(sz) = self.sizes.remove(key) {
            self.current_bytes = self.current_bytes.saturating_sub(sz);
        }
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
    }

    fn pick_victim(&self) -> Option<String> {
        for k in &self.order {
            if !self.pinned.contains(k) {
                return Some(k.clone());
            }
        }
        None
    }

    fn maybe_evict(&mut self) {
        if self.max_bytes == 0 {
            return;
        }
        while self.current_bytes > self.max_bytes && !self.order.is_empty() {
            let victim = match self.pick_victim() {
                Some(v) => v,
                None => break, // only pinned keys remain
            };
            if let Some(val) = self.inner.get(&victim) {
                (self.on_evict)(&victim, &val);
            }
            self.reduce(&victim);
            let _ = self.inner.delete(&victim);
        }
    }
}

impl StorageController for MemoryLimitedDictStorageController {
    fn exists(&self, key: &str) -> bool {
        self.inner.exists(key)
    }

    fn set(&mut self, key: &str, value: Value) {
        if self.exists(key) {
            self.reduce(key);
        }
        self.inner.set(key, value.clone());

        let sz = Self::entry_size(key, &value);
        self.sizes.insert(key.to_string(), sz);
        self.current_bytes += sz;

        self.order.push_back(key.to_string());
        if self.policy == Policy::Lru {
            // nothing else; get() will bump access
        }

        self.maybe_evict();
    }

    fn get(&self, key: &str) -> Option<Value> {
        let v = self.inner.get(key);
        if v.is_some() && self.policy == Policy::Lru {
            // SAFETY: &self, but we want to bump order. For simplicity,
            // don't mutate order here (keep it simple). Users who need
            // perfect LRU can call `touch()` we expose below.
        }
        v
    }

    fn delete(&mut self, key: &str) -> Option<Value> {
        if self.exists(key) {
            self.reduce(key);
        }
        self.inner.delete(key)
    }

    fn keys(&self, pattern: &str) -> Vec<String> {
        self.inner.keys(pattern)
    }

    fn bytes_used(&self) -> usize {
        self.current_bytes
    }
}

impl MemoryLimitedDictStorageController {
    /// For a true LRU, call this after a successful `get`.
    pub fn touch(&mut self, key: &str) {
        if self.policy == Policy::Lru {
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                let k = self.order.remove(pos).unwrap();
                self.order.push_back(k);
            }
        }
    }
}

/* ------------------------- event dispatcher ------------------------- */

type Callback = Box<dyn Fn(Option<&Value>) + Send + Sync + 'static>;

pub struct EventDispatcherController {
    // maps "Event:<b64(name)>:<id>" -> callback
    callbacks: HashMap<String, Callback>,
}

impl Default for EventDispatcherController {
    fn default() -> Self {
        Self {
            callbacks: HashMap::new(),
        }
    }
}

impl EventDispatcherController {
    pub const ROOT_KEY: &'static str = "_Event";

    fn event_glob(event_name: &str, event_id: &str) -> String {
        let en = if event_name == "*" {
            "*".to_string()
        } else {
            b64url_encode(event_name)
        };
        format!("{}:{}:{}", Self::ROOT_KEY, en, event_id)
    }

    pub fn events(&self) -> Vec<String> {
        self.callbacks.keys().cloned().collect()
    }

    pub fn get_event(&self, id: &str) -> Vec<String> {
        let glob = Self::event_glob("*", id);
        let wm = WildMatch::new(&glob);
        self.callbacks
            .keys()
            .filter(|k| wm.matches(k))
            .cloned()
            .collect()
    }

    pub fn delete_event(&mut self, id: &str) -> usize {
        let keys: Vec<String> = self.get_event(id);
        let n = keys.len();
        for k in keys {
            self.callbacks.remove(&k);
        }
        n
    }

    pub fn set_event<F>(&mut self, event_name: &str, callback: F, id: Option<String>) -> String
    where
        F: Fn(Option<&Value>) + Send + Sync + 'static,
    {
        let eid = id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let key = Self::event_glob(event_name, &eid);
        self.callbacks.insert(key, Box::new(callback));
        eid
    }

    pub fn dispatch_event(&self, event_name: &str, message: Option<&Value>) {
        let glob = Self::event_glob(event_name, "*");
        let wm = WildMatch::new(&glob);
        for (k, cb) in &self.callbacks {
            if wm.matches(k) {
                cb(message);
            }
        }
    }

    pub fn clean(&mut self) {
        self.callbacks.clear();
    }
}

/* -------------------------- message queue --------------------------- */

pub struct MessageQueueController {
    pub constroller: MemoryLimitedDictStorageController,
    dispatcher: EventDispatcherController,
}

impl MessageQueueController {
    pub const ROOT_KEY: &'static str = "_MessageQueue";
    pub const ROOT_KEY_EVENT: &'static str = "MQE";

    pub fn new(
        store: MemoryLimitedDictStorageController,
        dispatcher: Option<EventDispatcherController>,
    ) -> Self {
        Self {
            constroller: store,
            dispatcher: dispatcher.unwrap_or_default(),
        }
    }

    fn qname(queue_name: &str) -> String {
        b64url_encode(queue_name)
    }

    fn qkey(queue_name: &str, index: Option<&str>) -> String {
        match index {
            Some(i) => format!("{}:{}:{}", Self::ROOT_KEY, Self::qname(queue_name), i),
            None => format!("{}:{}", Self::ROOT_KEY, Self::qname(queue_name)),
        }
    }

    fn event_name(queue_name: &str, kind: &str) -> String {
        format!("{}:{}:{}", Self::ROOT_KEY_EVENT, Self::qname(queue_name), kind)
    }

    fn load_meta(&mut self, queue_name: &str) -> (i64, i64) {
        let meta_key = Self::qkey(queue_name, None);
        if let Some(v) = self.constroller.get(&meta_key) {
            if let (Some(h), Some(t)) = (v.get("head"), v.get("tail")) {
                return (h.as_i64().unwrap_or(0), t.as_i64().unwrap_or(0));
            }
        }
        self.constroller.set(
            &meta_key,
            json!({
                "head": 0,
                "tail": 0
            }),
        );
        (0, 0)
    }

    fn save_meta(&mut self, queue_name: &str, head: i64, tail: i64) {
        let meta_key = Self::qkey(queue_name, None);
        self.constroller
            .set(&meta_key, json!({ "head": head, "tail": tail }));
    }

    fn size_from_meta(head: i64, tail: i64) -> i64 {
        (tail - head).max(0)
    }

    fn try_dispatch(&self, queue_name: &str, kind: &str, message: Option<&Value>) {
        let ev = Self::event_name(queue_name, kind);
        self.dispatcher.dispatch_event(&ev, message);
    }

    pub fn add_listener<F>(
        &mut self,
        queue_name: &str,
        event_kind: &str, // "pushed" | "popped" | "empty" | "cleared"
        callback: F,
        listener_id: Option<String>,
    ) -> String
    where
        F: Fn(Option<&Value>) + Send + Sync + 'static,
    {
        self.dispatcher
            .set_event(&Self::event_name(queue_name, event_kind), callback, listener_id)
    }

    pub fn remove_listener(&mut self, listener_id: &str) -> usize {
        self.dispatcher.delete_event(listener_id)
    }

    pub fn push(&mut self, queue_name: &str, message: Value) -> String {
        let (mut head, mut tail) = self.load_meta(queue_name);
        let idx = tail;
        let key = Self::qkey(queue_name, Some(&idx.to_string()));
        self.constroller.set(&key, message.clone());
        tail += 1;
        self.save_meta(queue_name, head, tail);
        self.try_dispatch(queue_name, "pushed", Some(&message));
        key
    }

    fn advance_head_past_holes(&mut self, queue_name: &str, mut head: i64, tail: i64) -> i64 {
        while head < tail {
            let k = Self::qkey(queue_name, Some(&head.to_string()));
            if self.constroller.get(&k).is_some() {
                break;
            }
            head += 1;
        }
        head
    }

    pub fn pop_item(&mut self, queue_name: &str, peek: bool) -> (Option<String>, Option<Value>) {
        let (mut head, tail) = self.load_meta(queue_name);
        let head0 = self.advance_head_past_holes(queue_name, head, tail);
        head = head0;

        if head >= tail {
            return (None, None);
        }

        let key = Self::qkey(queue_name, Some(&head.to_string()));
        let msg = self.constroller.get(&key);
        if msg.is_none() {
            // rare hole; move head forward, retry
            let head2 = head + 1;
            self.save_meta(queue_name, head2, tail);
            let head3 = self.advance_head_past_holes(queue_name, head2, tail);
            if head3 >= tail {
                return (None, None);
            }
            return self.pop_item(queue_name, peek);
        }

        if peek {
            return (Some(key), msg);
        }

        let _ = self.constroller.delete(&key);
        let msgv = msg.unwrap();
        let head2 = head + 1;
        self.save_meta(queue_name, head2, tail);

        self.try_dispatch(queue_name, "popped", Some(&msgv));
        if Self::size_from_meta(head2, tail) == 0 {
            self.try_dispatch(queue_name, "empty", None);
        }
        (Some(key), Some(msgv))
    }

    pub fn pop(&mut self, queue_name: &str) -> Option<Value> {
        self.pop_item(queue_name, false).1
    }

    pub fn peek(&mut self, queue_name: &str) -> Option<Value> {
        self.pop_item(queue_name, true).1
    }

    pub fn queue_size(&mut self, queue_name: &str) -> i64 {
        let (head, tail) = self.load_meta(queue_name);
        Self::size_from_meta(head, tail)
    }

    pub fn clear(&mut self, queue_name: &str) {
        let qn = Self::qname(queue_name);
        let prefix = format!("{}:{}:", Self::ROOT_KEY, qn);
        let keys: Vec<String> = self.constroller.keys(&format!("{}*{}", Self::ROOT_KEY, "*"))
            .into_iter()
            .filter(|k| k.starts_with(&prefix))
            .collect();

        for k in keys {
            let _ = self.constroller.delete(&k);
        }
        let _ = self
            .constroller
            .delete(&format!("{}:{}", Self::ROOT_KEY, qn));
        self.try_dispatch(queue_name, "cleared", None);
    }
}

/* ----------------------- local version controller ------------------- */

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Set(String, Value),
    Delete(String),
    Clean,
    Load(String),  // path
    Loads(String), // json string
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OpRecord {
    forward: Operation,
    revert: Option<Operation>,
}

pub struct LocalVersionController {
    ops: Vec<String>,                  // ordered op ids
    op_map: HashMap<String, OpRecord>, // id -> record
    current: Option<String>,
    limit_mb: f64,
}

impl LocalVersionController {
    pub const TABLENAME: &'static str = "_Operation";

    pub fn new(limit_memory_mb: f64) -> Self {
        Self {
            ops: Vec::new(),
            op_map: HashMap::new(),
            current: None,
            limit_mb: limit_memory_mb,
        }
    }

    pub fn get_versions(&self) -> Vec<String> {
        self.ops.clone()
    }

    pub fn estimate_memory_mb(&self) -> f64 {
        let v = json!({
            "ops": self.ops,
            "map": self.op_map, // rough
            "current": self.current
        });
        deep_bytes_size_value(&v) as f64 / (1024.0 * 1024.0)
    }

    pub fn add_operation(
        &mut self,
        forward: Operation,
        revert: Option<Operation>,
    ) -> Option<String> {
        let id = Uuid::new_v4().to_string();
        // If we had manually reverted, cut off redo tail
        if let Some(cur) = &self.current {
            if let Some(idx) = self.ops.iter().position(|x| x == cur) {
                self.ops.truncate(idx + 1);
            }
        }
        self.ops.push(id.clone());
        self.op_map.insert(id.clone(), OpRecord { forward, revert });
        self.current = Some(id.clone());

        if self.estimate_memory_mb() > self.limit_mb {
            Some(format!(
                "[LocalVersionController] Warning: memory usage {:.1} MB exceeds limit of {:.1} MB",
                self.estimate_memory_mb(),
                self.limit_mb
            ))
        } else {
            None
        }
    }

    pub fn pop_operation(&mut self, n: usize) -> Vec<(String, OpRecord)> {
        if n == 0 || self.ops.is_empty() {
            return vec![];
        }
        let mut out = vec![];
        for _ in 0..n.min(self.ops.len()) {
            let pop_idx = if self
                .current
                .as_ref()
                .map(|c| self.ops.first().map(|h| h != c).unwrap_or(true))
                .unwrap_or(true)
            {
                0
            } else {
                self.ops.len() - 1
            };
            let id = self.ops.remove(pop_idx);
            if let Some(rec) = self.op_map.remove(&id) {
                out.push((id, rec));
            }
        }
        if let Some(cur) = &self.current {
            if !self.ops.iter().any(|x| x == cur) {
                self.current = self.ops.last().cloned();
            }
        } else {
            self.current = self.ops.last().cloned();
        }
        out
    }

    pub fn forward_one<F>(&mut self, mut f: F)
    where
        F: FnMut(&Operation),
    {
        let cur_idx = self
            .current
            .as_ref()
            .and_then(|c| self.ops.iter().position(|x| x == c))
            .unwrap_or_else(|| usize::MAX);
        let next_idx = cur_idx.saturating_add(1);
        if next_idx >= self.ops.len() {
            return;
        }
        let id = &self.ops[next_idx];
        if let Some(rec) = self.op_map.get(id) {
            f(&rec.forward);
            self.current = Some(id.clone());
        }
    }

    pub fn revert_one<F>(&mut self, mut f: F)
    where
        F: FnMut(&Operation),
    {
        let cur_idx = self
            .current
            .as_ref()
            .and_then(|c| self.ops.iter().position(|x| x == c));
        let Some(ci) = cur_idx else { return; };
        if ci == 0 {
            return;
        }
        let id = &self.ops[ci];
        if let Some(rec) = self.op_map.get(id) {
            if let Some(ref op) = rec.revert {
                f(op);
                self.current = Some(self.ops[ci - 1].clone());
            }
        }
    }

    pub fn to_version<F>(&mut self, target: &str, mut f: F) -> Result<(), String>
    where
        F: FnMut(&Operation),
    {
        let Some(ti) = self.ops.iter().position(|x| x == target) else {
            return Err(format!("no such version of {}", target));
        };

        let mut ci = self
            .current
            .as_ref()
            .and_then(|c| self.ops.iter().position(|x| x == c))
            .unwrap_or(usize::MAX); // conceptually "before 0"

        loop {
            if ci == ti {
                break;
            }
            if ci > ti {
                self.revert_one(|op| f(op));
                ci -= 1;
            } else {
                self.forward_one(|op| f(op));
                ci += 1;
            }
        }
        Ok(())
    }

    pub fn current(&self) -> Option<String> {
        self.current.clone()
    }
}

/* --------------------- singleton key-value storage ------------------ */

pub struct SingletonKeyValueStorage {
    version_control: bool,
    encryptor: Option<Box<dyn Encryptor + Send + Sync>>,
    conn: Box<dyn StorageController + Send + Sync>,
    events: EventDispatcherController,
    ver: LocalVersionController,
    mq: MessageQueueController,
}

impl SingletonKeyValueStorage {
    pub fn new(version_control: bool, encryptor: Option<Box<dyn Encryptor>>) -> Self {
        let dict = DictStorage::new();
        let conn: Box<dyn StorageController> = Box::new(DictStorageController::new(dict));
        let ver = LocalVersionController::new(128.0);

        // Message queue with its own temp store
        let mq_store = MemoryLimitedDictStorageController::new(
            DictStorage::new(),
            1024.0,
            Policy::Lru,
            None,
            None,
        );
        let mq = MessageQueueController::new(mq_store, None);

        Self {
            version_control,
            encryptor,
            conn,
            events: EventDispatcherController::default(),
            ver,
            mq,
        }
    }

    pub fn switch_backend(&mut self, controller: Box<dyn StorageController>) {
        self.events = EventDispatcherController::default();
        self.ver = LocalVersionController::new(128.0);
        let mq_store = MemoryLimitedDictStorageController::new(
            DictStorage::new(),
            1024.0,
            Policy::Lru,
            None,
            None,
        );
        self.mq = MessageQueueController::new(mq_store, None);
        self.conn = controller;
    }

    fn edit_local(&mut self, op: &Operation) {
        match op {
            Operation::Set(k, v) => self.conn.set(k, v.clone()),
            Operation::Delete(k) => {
                let _ = self.conn.delete(k);
            }
            Operation::Clean => self.conn.clean(),
            Operation::Load(path) => {
                let _ = self.conn.load(path);
            }
            Operation::Loads(s) => self.conn.loads(s),
        }
    }

    fn edit_with_events(&mut self, op: &Operation) {
        match (op, self.encryptor.as_ref()) {
            (Operation::Set(k, v), Some(enc)) => {
                let cipher = enc.encrypt_string(&v.to_string());
                let wrapped = serde_json::json!({ "rjson": cipher });
                self.conn.set(k, wrapped);
                let payload = serde_json::json!({ "key": k, "value": v });
                self.events.dispatch_event("set", Some(&payload));
            }
            _ => {
                match op {
                    Operation::Set(k, v) => {
                        self.conn.set(k, v.clone());
                        let payload = serde_json::json!({ "key": k, "value": v });
                        self.events.dispatch_event("set", Some(&payload));
                    }
                    Operation::Delete(k) => {
                        let _ = self.conn.delete(k);
                        let payload = serde_json::json!({ "key": k });
                        self.events.dispatch_event("delete", Some(&payload));
                    }
                    Operation::Clean => {
                        self.conn.clean();
                        self.events.dispatch_event("clean", Some(&serde_json::json!({})));
                    }
                    Operation::Load(path) => {
                        let _ = self.conn.load(path);
                        let payload = serde_json::json!({ "path": path });
                        self.events.dispatch_event("load", Some(&payload));
                    }
                    Operation::Loads(s) => {
                        self.conn.loads(s);
                        let payload = serde_json::json!({ "json": s });
                        self.events.dispatch_event("loads", Some(&payload));
                    }
                }
            }
        }
    }

    fn record_and_apply(&mut self, forward: Operation, revert: Option<Operation>) -> bool {
        if self.version_control {
            let _ = self.ver.add_operation(forward.clone(), revert);
        }
        // best-effort apply
        self.edit_with_events(&forward);
        true
    }

    /* public API mirroring the Python wrapper */

    pub fn set(&mut self, key: &str, value: Value) -> bool {
        let forward = Operation::Set(key.to_string(), value.clone());
        let revert = if self.exists(key) {
            self.get(key).map(|old| Operation::Set(key.to_string(), old))
        } else {
            Some(Operation::Delete(key.to_string()))
        };
        self.record_and_apply(forward, revert)
    }

    pub fn delete(&mut self, key: &str) -> bool {
        let old = self.get(key);
        let forward = Operation::Delete(key.to_string());
        let revert = old.map(|v| Operation::Set(key.to_string(), v));
        self.record_and_apply(forward, revert)
    }

    pub fn clean(&mut self) -> bool {
        let snapshot = self.dumps();
        let forward = Operation::Clean;
        let revert = Some(Operation::Loads(snapshot));
        self.record_and_apply(forward, revert)
    }

    pub fn load(&mut self, path: &str) -> bool {
        // to revert, take before-snapshot
        let snapshot = self.dumps();
        let forward = Operation::Load(path.to_string());
        let revert = Some(Operation::Loads(snapshot));
        self.record_and_apply(forward, revert)
    }

    pub fn loads(&mut self, s: &str) -> bool {
        let snapshot = self.dumps();
        let forward = Operation::Loads(s.to_string());
        let revert = Some(Operation::Loads(snapshot));
        self.record_and_apply(forward, revert)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.conn.exists(key)
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        self.conn.keys(pattern)
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        let mut v = self.conn.get(key)?;
        if let Some(enc) = self.encryptor.as_ref() {
            if let Some(cipher) = v.get("rjson").and_then(|c| c.as_str()) {
                let plain = enc.decrypt_string(cipher);
                v = serde_json::from_str(&plain).ok()?;
            }
        }
        Some(v)
    }

    pub fn dumps(&self) -> String {
        let mut map = serde_json::Map::new();
        for k in self.keys("*") {
            if let Some(v) = self.get(&k) {
                map.insert(k, v);
            }
        }
        Value::Object(map).to_string()
    }

    pub fn dump(&self, path: &str) -> std::io::Result<()> {
        fs::write(path, self.dumps())
    }

    // versioning control surface
    pub fn revert_one_operation(&mut self) {
        self.ver.revert_one(|op| self.edit_local(op));
    }
    pub fn forward_one_operation(&mut self) {
        self.ver.forward_one(|op| self.edit_local(op));
    }
    pub fn get_current_version(&self) -> Option<String> {
        self.ver.current()
    }
    pub fn local_to_version(&mut self, id: &str) -> Result<(), String> {
        self.ver.to_version(id, |op| self.edit_local(op))
    }

    // events
    pub fn set_event<F>(&mut self, name: &str, callback: F, id: Option<String>) -> String
    where
        F: Fn(Option<&Value>) + Send + Sync + 'static,
    {
        self.events.set_event(name, callback, id)
    }
    pub fn delete_event(&mut self, id: &str) -> usize {
        self.events.delete_event(id)
    }
    pub fn dispatch_event(&self, name: &str) {
        self.events.dispatch_event(name, None)
    }
    pub fn events(&self) -> Vec<String> {
        self.events.events()
    }
}

/* ------------------------------ tests ------------------------------- */

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let mut s = SingletonKeyValueStorage::new(false, None);
        assert!(!s.exists("a"));
        s.set("a", json!({"x": 1}));
        assert!(s.exists("a"));
        assert_eq!(s.get("a").unwrap()["x"], 1);

        s.set("b", json!(123));
        let d = s.dumps();
        assert!(d.contains("\"a\""));
        assert!(d.contains("\"b\""));

        s.delete("a");
        assert!(!s.exists("a"));
        s.loads(&d);
        assert!(s.exists("a"));
        assert!(s.exists("b"));
    }

    #[test]
    fn queue() {
        let store = MemoryLimitedDictStorageController::new(
            DictStorage::new(),
            1_024.0,
            Policy::Fifo,
            None,
            None,
        );
        let mut mq = MessageQueueController::new(store, None);
        mq.push("default", json!({"hello": "world"}));
        mq.push("default", json!(2));
        assert_eq!(mq.queue_size("default"), 2);
        let v = mq.pop("default").unwrap();
        assert_eq!(v["hello"], "world");
        assert_eq!(mq.queue_size("default"), 1);
    }

    #[test]
    fn versioning() {
        let mut s = SingletonKeyValueStorage::new(true, None);
        s.set("a", json!(1));
        let v1 = s.get_current_version().unwrap();
        s.set("a", json!(2));
        s.set("b", json!(3));
        s.revert_one_operation(); // revert set b
        assert!(!s.exists("b"));
        s.revert_one_operation(); // revert set a=2 -> back to a=1
        assert_eq!(s.get("a").unwrap(), json!(1));
        s.local_to_version(&v1).unwrap();
        assert_eq!(s.get("a").unwrap(), json!(1));
    }
}
