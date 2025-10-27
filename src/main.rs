mod singleton_storage;
mod basic_store;
mod rsa;

use singleton_storage::{DictStorage, DictStorageController, AbstractStorageController};
use basic_store::{AbstractObj, BasicStore};
use serde_json::json;

use rsa::PEMFileReader;
use rsa::SimpleRSAChunkEncryptor;


fn test_store() {
    // Initialize storage and controller
    let storage = DictStorage::get_singleton();
    println!("Storage UUID: {:?}", storage.uuid);
    let mut storage_conn = DictStorageController{model:storage};

    // Perform operations on the controller
    storage_conn.set("key1", json!({"example": "data"}));
    if let Some(value) = storage_conn.get("key1") {
        println!("Value for 'key1': {:?}", value);
    } else {
        println!("No value found for 'key1'");
    }

    // check singleton
    {
        let tmp_storage = DictStorage::get_singleton();
        let mut tmp_conn = DictStorageController{model:tmp_storage};
        if let Some(value) = tmp_conn.get("key1") {
            println!("Value for 'key1': {:?}", value);
        } else {
            println!("No value found for 'key1'");
        }
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
    match basic_store.find::<AbstractObj>(&obj.get_id()) {
        Some(obj) => {            
            let mut obj_conn = obj.get_controller(&mut basic_store);
            println!("Found object with ID: {:?}", obj_conn.model.id);
            println!("{:?}", obj_conn.model);
            obj_conn.delete();
        },
        None => println!("Object with ID {:?} not found", obj.get_id()),
    }

    // Verify deletion
    match basic_store.find::<AbstractObj>(&obj.get_id()) {
        Some(obj) => println!(
            "Object still exists with ID: {:?}", obj.get_id()),
        None => println!("Object with ID {:?} not found", obj.get_id()),
    }
}
fn test_rsa() -> Result<(), Box<dyn std::error::Error>> {
    // Load keys from .pem files
    let public_key_path = "./tmp/public_key.pem";
    let private_key_path = "./tmp/private_key.pem";

    let public_key_reader = PEMFileReader::new(public_key_path)?;
    let private_key_reader = PEMFileReader::new(private_key_path)?;

    let public_key = public_key_reader.load_public_pkcs8_key()?;
    let private_key = private_key_reader.load_private_pkcs8_key()?;

    // Instantiate the encryptor with the loaded keys
    let encryptor = SimpleRSAChunkEncryptor::new(
        Some(public_key.clone()), Some(private_key.clone()))?;

    // Encrypt and decrypt a sample string
    let plaintext = "Hello, RSA encryption with .pem support!";
    println!("Original Plaintext: [{}]", plaintext);

    // Encrypt the plaintext
    let encrypted_text = encryptor.encrypt_string(plaintext,true)?;
    println!("\nEncrypted (Base64 encoded): [{}]", encrypted_text);

    // Decrypt the encrypted text
    let decrypted_text = encryptor.decrypt_string(&encrypted_text)?;
    println!("\nDecrypted Text: [{}]", decrypted_text);

    Ok(())
}

fn main() {
    if let Err(e) = test_rsa() {
        eprintln!("Error: {}", e);
    }
    test_store();
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
