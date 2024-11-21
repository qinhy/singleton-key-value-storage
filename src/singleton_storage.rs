use std::collections::HashMap;
use std::fs;
// use std::path::Path;
use uuid::Uuid;
// use serde_json::json;
use lazy_static::lazy_static;
use serde_json::Value;
use std::sync::{Arc, RwLock};

pub trait AbstractStorageController<T> {
    fn is_singleton(&self) -> bool;
    fn exists(&self, key: &str) -> bool;
    fn set(&mut self, key: &str, value: T);
    fn get(&self, key: &str) -> Option<T>;
    fn delete(&mut self, key: &str);
    fn keys(&self, pattern: &str) -> Vec<String>;
    fn clean(&mut self);
    fn dumps(&self) -> String;
    fn loads(&mut self, json_string: &str);
    fn dump(&self, path: &str);
    fn load(&mut self, path: &str);
}

// Define your custom type for T
type GenericHashMap<T> = HashMap<String, T>;
type GenericArcHashMap<T> = Arc<RwLock<HashMap<String, T>>>;

pub struct RustDictStorage<T> {
    pub uuid: Uuid,
    pub store: Option<GenericHashMap<T>>,
    pub store_lock: Option<GenericArcHashMap<T>>,
    pub is_singleton: bool,
}

impl<T> RustDictStorage<T> {
    pub fn new() -> Self {
        Self {
            uuid: Uuid::new_v4(),
            store: Some(HashMap::new()),
            store_lock: None,
            is_singleton: false,
        }
    }
}

lazy_static! {
    static ref _RustDict_Json_UUID: uuid::Uuid = uuid::Uuid::new_v4();
    static ref _RustDict_Json_STORE: GenericArcHashMap<Value> = Arc::new(RwLock::new(HashMap::new()));
}

impl RustDictStorage<Value> {
    pub fn get_singleton() -> Self {
        Self {
            uuid: *_RustDict_Json_UUID,
            store: None,
            store_lock: Some(_RustDict_Json_STORE.clone()),
            is_singleton: true,
        }
    }
}

pub struct RustDictStorageController<T> {
    pub(crate) model: RustDictStorage<T>,
}

impl<T> AbstractStorageController<T> for RustDictStorageController<T> {

    fn is_singleton(&self) -> bool {
        self.model.is_singleton
    }

    fn exists(&self, key: &str) -> bool {
        if self.is_singleton() {
            return self.model.store_lock.as_ref().map_or(false, |store_lock| {
                store_lock.read().unwrap().contains_key(key)
            });
        }
        self.model
            .store
            .as_ref()
            .map_or(false, |store| store.contains_key(key))
    }

    fn set(&mut self, key: &str, value: T) {
        if self.is_singleton() {
            if let Some(store_lock) = &self.model.store_lock {
                store_lock.write().unwrap().insert(key.to_string(), value);
            }
        } else {
            if let Some(store) = &mut self.model.store {
                store.insert(key.to_string(), value);
            }
        }
    }

    fn delete(&mut self, key: &str) {
        if self.is_singleton() {
            if let Some(store_lock) = &self.model.store_lock {
                store_lock.write().unwrap().remove(key);
            }
        } else if let Some(store) = &mut self.model.store {
            store.remove(key);
        }
    }

    fn keys(&self, _pattern: &str) -> Vec<String> {
        if self.is_singleton() {
            self.model
                .store_lock
                .as_ref()
                .map_or_else(Vec::new, |store_lock| {
                    store_lock.read().unwrap().keys().cloned().collect()
                })
        } else {
            self.model
                .store
                .as_ref()
                .map_or_else(Vec::new, |store| store.keys().cloned().collect())
        }
    }

    fn clean(&mut self) {
        if self.is_singleton() {
            if let Some(store_lock) = &self.model.store_lock {
                store_lock.write().unwrap().clear();
            }
        } else if let Some(store) = &mut self.model.store {
            store.clear();
        }
    }

    fn get(&self, key: &str) -> Option<T> {
        None
    }

    fn dumps(&self) -> String {
        "not implemeted!".to_string()
        // serde_json::to_string(&self.model.store).unwrap_or_else(|_| "{}".to_string())
    }

    fn loads(&mut self, json_string: &str) {
        if let Ok(map) = serde_json::from_str::<HashMap<String, Value>>(json_string) {
            // self.model.store = map;
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

impl<Value: Clone> RustDictStorageController<Value> {
    pub fn get(&self, key: &str) -> Option<Value> {
        if self.is_singleton() {
            self.model
                .store_lock
                .as_ref()
                .and_then(|store_lock| store_lock.read().unwrap().get(key).cloned())
        } else {
            self.model
                .store
                .as_ref()
                .and_then(|store| store.get(key).cloned())
        }
    }
}
