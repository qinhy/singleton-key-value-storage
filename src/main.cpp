#include <iostream>
#include <string>
#include <json.hpp>
#include <Storages.hpp>

using json = nlohmann::json;

int main() {
    std::cout << "######## start." << std::endl;

    {        
        SingletonKeyValueStorage controller;
        controller.set("user2", json{{"name", "Bob"}, {"age", 25}});
    }

    SingletonKeyValueStorage controller;
    std::cout << controller.uuid << std::endl;
    
    std::cout << "######## Set some key-value pairs" << std::endl;
    controller.set("user1", json{{"name", "Alice"}, {"age", 30}, {"nums", {1,2,3}}});
    
    std::cout << "######## Check if a key exists" << std::endl;
    std::string key = "user1";
    if (controller.exists(key)) {
        std::cout << "Key " << key << " exists in storage." << std::endl;
    } else {
        std::cout << "Key " << key << " does not exist in storage." << std::endl;
    }
    
    std::cout << "######## Retrieve and print the value for a key" << std::endl;
    json value = controller.get("user1");
    if (!value.is_null()) {
        std::cout << "Value for key " << key << ": " << value.dump() << std::endl;
    } else {
        std::cout << "No value found for key " << key << std::endl;
    }
    
    std::cout << "######## Dump all data to a JSON string" << std::endl;
    std::string dumpedData = controller.dumps();
    std::cout << "Dumped Data: " << dumpedData << std::endl;

    std::cout << "######## Load from dumped data string" << std::endl;
    std::string jsonData = R"({"user3": {"name": "Charlie", "age": 35}})";
    controller.loads(jsonData);    
    std::cout << "######## Print all keys" << std::endl;
    std::vector<std::string> allKeys = controller.keys();
    std::cout << "All Keys in Storage:" << std::endl;
    for (const auto& k : allKeys) {
        std::cout << "- " << k << std::endl;
    }
    
    std::cout << "######## Delete a key" << std::endl;
    controller.deleteKey("user1");
    std::cout << "Deleted key 'user1'" << std::endl;

    std::cout << "######## Verify deletion" << std::endl;
    if (!controller.exists("user1")) {
        std::cout << "Key 'user1' was successfully deleted." << std::endl;
    }

    std::cout << "######## Test keys" << std::endl;    
    std::cout << controller.keys("user*")[0] << std::endl;

    std::cout << controller.dumps() << std::endl;

    return 0;
}
