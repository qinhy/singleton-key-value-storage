use lazy_static::lazy_static;
use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

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
type GenericArcHashMap<T> = Arc<RwLock<HashMap<String, T>>>;

pub struct DictStorage<T> {
    pub uuid: Uuid,
    pub store: Option<GenericArcHashMap<T>>,
    pub is_singleton: bool,
}

impl<T> DictStorage<T> {
    pub fn new() -> Self {
        Self {
            uuid: Uuid::new_v4(),
            store: Some(Arc::new(RwLock::new(HashMap::new()))),
            is_singleton: false,
        }
    }
}

lazy_static! {
    static ref _RustDict_Json_UUID: uuid::Uuid = uuid::Uuid::new_v4();
    static ref _RustDict_Json_STORE: GenericArcHashMap<Value> =
        Arc::new(RwLock::new(HashMap::new()));
}

impl DictStorage<Value> {
    pub fn get_singleton() -> Self {
        Self {
            uuid: *_RustDict_Json_UUID,
            store: Some(_RustDict_Json_STORE.clone()),
            is_singleton: true,
        }
    }
}

pub struct DictStorageController<T> {
    pub(crate) model: DictStorage<T>,
}

impl<T> AbstractStorageController<T> for DictStorageController<T> {
    fn is_singleton(&self) -> bool {
        self.model.is_singleton
    }

    fn exists(&self, key: &str) -> bool {
        self.model
            .store
            .as_ref()
            .map_or(false, |store| store.read().unwrap().contains_key(key))
    }

    fn set(&mut self, key: &str, value: T) {
        if let Some(store) = &self.model.store {
            store.write().unwrap().insert(key.to_string(), value);
        }
    }

    fn delete(&mut self, key: &str) {
        if let Some(store) = &self.model.store {
            store.write().unwrap().remove(key);
        }
    }

    fn keys(&self, pattern: &str) -> Vec<String> {
        let mut ks: Vec<String> = self.model.store.as_ref().map_or_else(Vec::new, |store| {
            store.read().unwrap().keys().cloned().collect()
        });
        // Perform regex filtering on keys
        let regex = Regex::new(pattern).unwrap();
        ks.retain(|key| regex.is_match(key));

        // Return the filtered keys
        ks
    }

    fn clean(&mut self) {
        if let Some(store) = &self.model.store {
            store.write().unwrap().clear();
        };
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

impl<Value: Clone> DictStorageController<Value> {
    pub fn get(&self, key: &str) -> Option<Value> {
        self.model
            .store
            .as_ref()
            .and_then(|store| store.read().unwrap().get(key).cloned())
    }
}
