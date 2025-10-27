use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json;
use serde_json::Value;
use uuid::Uuid;
use crate::singleton_storage::{AbstractStorageController, DictStorageController};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AbstractObj {
    pub id: String,
    pub rank: Vec<i32>,
    pub create_time: DateTime<Utc>,
    pub update_time: DateTime<Utc>,
    pub status: String,
    pub metadata: HashMap<String, String>,
}

impl AbstractObj {
    pub fn new() -> Self {
        Self {
            id: "None".to_string(),
            rank: vec![0],
            create_time: Utc::now(),
            update_time: Utc::now(),
            status: String::new(),
            metadata: HashMap::new(),
            // controller: None,
        }
    }

    pub fn get_controller<'a>(& self, store: &'a mut BasicStore) -> AbstractObjController<'a>{
        AbstractObjController::new(self.clone(),store)
    }

    pub fn set_id(&mut self, id: &str) {
        assert!(self.id=="None".to_string(), "this obj is already set! cannot set again!");
        self.id = id.to_string();
    }

    pub fn gen_new_id(&self) -> String {
        format!("{}:{}", self.class_name(), Uuid::new_v4())
    }

    pub fn get_id(&self) -> String {
        assert!(self.id!="None".to_string(), "this obj is not set!");
        self.id.to_string()
    }

    fn class_name(&self) -> &str {
        let name = std::any::type_name::<Self>();
        
        if let Some(result) = name.split("::").last() {
            return  result;
        }
        else {
            return "";
        }
    }
}

pub struct AbstractObjController<'a> {
    pub model: AbstractObj,
    store: &'a mut BasicStore,
}

impl<'a> AbstractObjController<'a> {
    pub fn new(model: AbstractObj, store: &'a mut BasicStore) -> Self {
        Self { model, store }
    }

    fn update(&mut self, properties: HashMap<String, String>) {
        for (key, value) in properties {
            if key == "id" {
                self.model.id = value;
            } else if key == "status" {
                self.model.status = value;
            } else {
                self.model.metadata.insert(key, value);
            }
        }
        self.model.update_time = Utc::now();
        self.store();
    }

    pub fn delete(&mut self) {
        self.store.delete(&self.model.id);
    }

    fn store(&mut self) {
        self.store.store(&self.model);
    }
}

pub struct BasicStore {
    storage: DictStorageController<Value>,
}

impl BasicStore {
    pub fn new(storage:DictStorageController<Value>) -> Self {
        Self {storage}
    }

    pub fn store(&mut self, obj: &AbstractObj) {
        match serde_json::to_value(&obj) {
            Ok(json_value) => {
                self.storage.set(&obj.get_id(), json_value);
            }
            Err(e) => {
                eprintln!("Failed to convert to JSON: {}", e);
            }
        }
    }

    pub fn find<T>(&mut self, id: &str) -> Option<T> 
    where
        T: serde::de::DeserializeOwned, // Ensure T implements DeserializeOwned
    {
        // Get the JSON value from storage
        let json_value = self.storage.get(id)?;
    
        // Try to deserialize the JSON value into the expected type
        match serde_json::from_value::<T>(json_value.clone()) {
            Ok(obj) => Some(obj), // Successfully deserialized, return the object
            Err(e) => {
                eprintln!("Failed to deserialize value for id {}: {}", id, e);
                None // Return None if deserialization fails
            }
        }
    }

    pub fn delete(&mut self, id: &str) {
        self.storage.delete(id);
    }
}
