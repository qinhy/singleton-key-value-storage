mod singleton_storage;
mod basic_store;

use singleton_storage::{RustDictStorage, RustDictStorageController, AbstractStorageController};
use basic_store::{AbstractObj, BasicStore};
use serde_json::json;

fn main() {
    // Initialize storage and controller
    let storage = RustDictStorage::get_singleton();
    println!("Storage UUID: {:?}", storage.uuid);
    let mut storage_conn = RustDictStorageController::new(storage);

    // Perform operations on the controller
    storage_conn.set("key1", json!({"example": "data"}));
    if let Some(value) = storage_conn.get("key1") {
        println!("Value for 'key1': {:?}", value);
    } else {
        println!("No value found for 'key1'");
    }

    // Create an AbstractObj and generate ID
    let mut obj = AbstractObj::new();
    let obj_id = obj.gen_new_id();
    obj.set_id(&obj_id);
    println!("Generated ID for obj: {:?}", obj.get_id());

    // Store the object using BasicStore
    let mut basic_store = BasicStore::new(storage_conn);
    basic_store.store(&obj);

    // Search and conditionally delete object
    if let Some(mut obj_conn) = basic_store.find(&obj.get_id()) {
        println!("Found object with ID: {:?}", obj_conn.model.id);
        obj_conn.delete();
    } else {
        println!("Object with ID {:?} not found", obj.get_id());
    }

    // Verify deletion
    match basic_store.find(&obj.get_id()) {
        Some(obj_conn) => println!("Object still exists with ID: {:?}", obj_conn.model.id),
        None => println!("Object with ID {:?} not found", obj.get_id()),
    }
}
// use std::collections::HashMap;
// use serde_json::Value;

// trait Model {
//     fn as_any(&self) -> &dyn std::any::Any;
// }
// #[derive(Debug)]
// struct User {
//     name: String,
//     age: u32,
// }

// impl Model for User {
//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }
// }
// #[derive(Debug)]
// struct Product {
//     title: String,
//     price: f64,
// }

// impl Model for Product {
//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }
// }

// fn find(id: u32) -> Box<dyn Model> {
//     if id == 1 {
//         Box::new(User {
//             name: "Alice".to_string(),
//             age: 30,
//         })
//     } else {
//         Box::new(Product {
//             title: "Gadget".to_string(),
//             price: 99.99,
//         })
//     }
// }

// fn main() {
//     let found = find(1);
//     if let Some(user) = found.as_any().downcast_ref::<User>() {
//         println!("Found user: {:?}", user);
//     }
//     else {
//         println!("No found");
//     }
//     // else if let Some(product) = found.as_any().downcast_ref::<Product>() {
//     //     println!("Found product: {:?}", product);
//     // }
// }
