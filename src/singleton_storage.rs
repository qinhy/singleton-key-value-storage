use std::collections::HashMap;
use std::fs;
// use std::path::Path;
use uuid::Uuid;
// use serde_json::json;
use serde_json::Value;
use std::sync::{RwLock, Arc};
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
    static ref _RustDict_STORE: Arc<RwLock<HashMap<String, Value>>> = Arc::new(RwLock::new(HashMap::new()));
}
pub struct RustDictStorage {    
    pub uuid: Uuid,
    pub store: Option<HashMap<String, Value>>,
    pub store_lock: Option<Arc<RwLock<HashMap<String, Value>>>>,
    pub is_singleton: bool,
}

impl RustDictStorage {    
    pub fn new() -> Self {
        Self {
            uuid: Uuid::new_v4(),
            store: Some(HashMap::new()),
            store_lock: None,
            is_singleton: false,
        }
    }
    pub fn get_singleton() -> Self {
        let uuid = *_RustDict_UUID;
        let store_lock = Arc::clone(&*_RustDict_STORE);
        Self{uuid, store:None, store_lock:Some(store_lock), is_singleton:true}
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

impl RustDictStorageController {
    pub fn is_singleton(&self) -> bool {
        self.model.is_singleton
    }

    pub fn exists(&self, key: &str) -> bool {
        if self.is_singleton() {
            return self.model.store_lock
                .as_ref()
                .map_or(false, |store_lock| store_lock.read().unwrap().contains_key(key));
        }
        self.model.store
            .as_ref()
            .map_or(false, |store| store.contains_key(key))
    }

    pub fn set(&mut self, key: &str, value: Value) {
        if self.is_singleton() {
            if let Some(store_lock) = &self.model.store_lock {
                store_lock.write().unwrap().insert(key.to_string(), value);
            }
        } else if let Some(store) = &mut self.model.store {
            store.insert(key.to_string(), value);
        }
    }

    pub fn get(&self, key: &str) -> Option<Value> {
        if self.is_singleton() {
            self.model.store_lock
                .as_ref()
                .and_then(|store_lock| store_lock.read().unwrap().get(key).cloned())
        } else {
            self.model.store
                .as_ref()
                .and_then(|store| store.get(key).cloned())
        }
    }

    pub fn delete(&mut self, key: &str) {
        if self.is_singleton() {
            if let Some(store_lock) = &self.model.store_lock {
                store_lock.write().unwrap().remove(key);
            }
        } else if let Some(store) = &mut self.model.store {
            store.remove(key);
        }
    }

    pub fn keys(&self, _pattern: &str) -> Vec<String> {
        if self.is_singleton() {
            self.model.store_lock
                .as_ref()
                .map_or_else(Vec::new, |store_lock| store_lock.read().unwrap().keys().cloned().collect())
        } else {
            self.model.store
                .as_ref()
                .map_or_else(Vec::new, |store| store.keys().cloned().collect())
        }
    }

    pub fn clean(&mut self) {
        if self.is_singleton() {
            if let Some(store_lock) = &self.model.store_lock {
                store_lock.write().unwrap().clear();
            }
        } else if let Some(store) = &mut self.model.store {
            store.clear();
        }
    }

    pub fn dumps(&self) -> String {
        serde_json::to_string(&self.model.store).unwrap_or_else(|_| "{}".to_string())
    }

    pub fn loads(&mut self, json_string: &str) {
        if let Ok(map) = serde_json::from_str::<HashMap<String, Value>>(json_string) {
            // self.model.store = map;
        }
    }

    pub fn dump(&self, path: &str) {
        let json_string = self.dumps();
        fs::write(path, json_string).expect("Unable to write file");
    }

    pub fn load(&mut self, path: &str) {
        if let Ok(contents) = fs::read_to_string(path) {
            self.loads(&contents);
        }
    }
}
