use std::collections::HashMap;
use std::fs;
// use std::path::Path;
use uuid::Uuid;
// use serde_json::json;
use serde_json::Value;
use std::sync::RwLock;
use lazy_static::lazy_static;


pub trait AbstractStorageController {
    fn is_singleton(&self) -> bool;
    fn exists(&self, key: &str) -> bool;
    fn set(&mut self, key: &str, value: Value);
    fn get(&self, key: &str) -> Option<Value>;
    fn delete(&mut self, key: &str);
    fn keys(&self, pattern: &str) -> Vec<String>;
    fn clean(&mut self);
    fn dumps(&self) -> String;
    fn loads(&mut self, json_string: &str);
    fn dump(&self, path: &str);
    fn load(&mut self, path: &str);
}


lazy_static! {
    static ref _RustDict_UUID: Uuid = Uuid::new_v4();
    static ref _RustDict_STORE: RwLock<Option<HashMap<String, Value>>> = RwLock::new(None);
    static ref _RustDict_IS_SINGLETON: RwLock<bool> = RwLock::new(true);
}
pub struct RustDictStorage {    
    pub uuid: Uuid,
    pub store: HashMap<String, Value>,
    pub is_singleton: bool,
}

impl RustDictStorage {
    pub fn new(id: Option<Uuid>, store: Option<HashMap<String, Value>>, is_singleton: Option<bool>) -> Self {
        Self {
            uuid: id.unwrap_or_else(Uuid::new_v4),
            store: store.unwrap_or_else(HashMap::new),
            is_singleton: is_singleton.unwrap_or(false),
        }
    }

    pub fn get_singleton() -> Self {
        let uuid = *_RustDict_UUID;
        let store = _RustDict_STORE.read().unwrap().clone();
        let is_singleton = *_RustDict_IS_SINGLETON.read().unwrap();
        Self::new(Some(uuid), store, Some(is_singleton))
    }
}

pub struct RustDictStorageController {
    model: RustDictStorage,
}

impl RustDictStorageController {
    pub fn new(model: RustDictStorage) -> Self {
        Self { model }
    }
}

impl AbstractStorageController for RustDictStorageController {
    fn is_singleton(&self) -> bool {
        self.model.is_singleton
    }

    fn exists(&self, key: &str) -> bool {
        self.model.store.contains_key(key)
    }

    fn set(&mut self, key: &str, value: Value) {
        self.model.store.insert(key.to_string(), value);
    }

    fn get(&self, key: &str) -> Option<Value> {
        self.model.store.get(key).cloned()
    }

    fn delete(&mut self, key: &str) {
        self.model.store.remove(key);
    }

    fn keys(&self, _pattern: &str) -> Vec<String> {
        self.model.store.keys().cloned().collect()
    }

    fn clean(&mut self) {
        self.model.store.clear();
    }

    fn dumps(&self) -> String {
        serde_json::to_string(&self.model.store).unwrap_or_else(|_| "{}".to_string())
    }

    fn loads(&mut self, json_string: &str) {
        if let Ok(map) = serde_json::from_str::<HashMap<String, Value>>(json_string) {
            self.model.store = map;
        }
    }

    fn dump(&self, path: &str) {
        let json_string = self.dumps();
        fs::write(path, json_string).expect("Unable to write file");
    }

    fn load(&mut self, path: &str) {
        if let Ok(contents) = fs::read_to_string(path) {
            self.loads(&contents);
        }
    }
}
