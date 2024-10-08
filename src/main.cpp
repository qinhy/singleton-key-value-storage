#include <iostream>
#include <string>
#include <json.hpp>
#include <uuid.h>
#include <Storages.hpp>

using json = nlohmann::json;
using namespace uuids;

int main() {
    // Initialize singleton storage instance
    SingletonCppDictStorage& storage = SingletonCppDictStorage::getInstance();
    
    // Initialize controller with the singleton storage
    SingletonCppDictStorageController controller(storage);
    
    // Set some key-value pairs
    controller.set("user1", json{{"name", "Alice"}, {"age", 30}});
    controller.set("user2", json{{"name", "Bob"}, {"age", 25}});
    
    // Check if a key exists
    std::string key = "user1";
    if (controller.exists(key)) {
        std::cout << "Key " << key << " exists in storage." << std::endl;
    } else {
        std::cout << "Key " << key << " does not exist in storage." << std::endl;
    }
    
    // Retrieve and print the value for a key
    json value = controller.get("user1");
    if (!value.is_null()) {
        std::cout << "Value for key " << key << ": " << value.dump() << std::endl;
    } else {
        std::cout << "No value found for key " << key << std::endl;
    }
    
    // Dump all data to a JSON string
    std::string dumpedData = controller.dumps();
    std::cout << "Dumped Data: " << dumpedData << std::endl;

    // Load from dumped data string
    std::string jsonData = R"({"user3": {"name": "Charlie", "age": 35}})";
    controller.loads(jsonData);    
    // Print all keys
    std::vector<std::string> allKeys = controller.keys();
    std::cout << "All Keys in Storage:" << std::endl;
    for (const auto& k : allKeys) {
        std::cout << "- " << k << std::endl;
    }
    
    // Delete a key
    controller.deleteKey("user1");
    std::cout << "Deleted key 'user1'" << std::endl;

    
    std::cout << controller.keys("user*")[0] << std::endl;

    // Verify deletion
    if (!controller.exists("user1")) {
        std::cout << "Key 'user1' was successfully deleted." << std::endl;
    }

    return 0;
}
