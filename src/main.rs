mod singleton_storage;

fn main() {
    // Example usage of the AbstractStorage and RustDictStorageController
    use singleton_storage::{RustDictStorage, RustDictStorageController, AbstractStorageController};
    use serde_json::json;

    let storage = RustDictStorage::get_singleton();    
    println!("storage of {:?}", storage.uuid);
    
    let mut controller = RustDictStorageController::new(storage);

    // Example operations
    controller.set("key1", json!({"example": "data"}));
    println!("Value for 'key1': {:?}", controller.get("key1"));
}
