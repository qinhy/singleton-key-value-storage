#include <iostream>
#include <string>
#include <json.hpp>
#include <Storages.hpp>
#include <map>
#include <vector>
#include <memory>

using json = nlohmann::json;

auto commands = "set, get, exists, delete, keys, dumps, loads, clean, exit";

    // Initialize the storage controller
    std::shared_ptr<SingletonKeyValueStorage>
    init_storage()
{
    auto controllerfs = std::make_shared<SingletonKeyValueStorage>();
    controllerfs->file_backend();
    SingletonKeyValueStorage controller;
    controller.add_slave(controllerfs);
    return controllerfs;
}

// Function to handle console commands
void handle_command(std::shared_ptr<SingletonKeyValueStorage> controller, const std::string &command)
{
    if (command == "set")
    {
        std::string key, value;
        std::cout << "Enter key: ";
        std::getline(std::cin, key);
        std::cout << "Enter value (in JSON format): ";
        std::getline(std::cin, value);
        try
        {
            json json_value = json::parse(value);
            controller->set(key, json_value);
            std::cout << "Set key " << key << " with value " << json_value.dump() << std::endl;
        }
        catch (json::parse_error &e)
        {
            std::cout << "Invalid JSON format: " << e.what() << std::endl;
        }
    }
    else if (command == "get")
    {
        std::string key;
        std::cout << "Enter key: ";
        std::getline(std::cin, key);
        json value = controller->get(key);
        if (!value.is_null())
        {
            std::cout << "Value for key " << key << ": " << value.dump() << std::endl;
        }
        else
        {
            std::cout << "No value found for key " << key << std::endl;
        }
    }
    else if (command == "exists")
    {
        std::string key;
        std::cout << "Enter key: ";
        std::getline(std::cin, key);
        if (controller->exists(key))
        {
            std::cout << "Key " << key << " exists in storage." << std::endl;
        }
        else
        {
            std::cout << "Key " << key << " does not exist in storage." << std::endl;
        }
    }
    else if (command == "delete")
    {
        std::string key;
        std::cout << "Enter key: ";
        std::getline(std::cin, key);
        controller->deleteKey(key);
        std::cout << "Deleted key " << key << std::endl;
    }
    else if (command == "keys")
    {
        std::string key;
        std::cout << "Enter key pattern: ";
        std::getline(std::cin, key);
        std::vector<std::string> allKeys = controller->keys(key);
        std::cout << "All Keys in Storage:" << std::endl;
        for (const auto &k : allKeys)
        {
            std::cout << "- " << k << std::endl;
        }
    }
    else if (command == "dumps")
    {
        std::string dumpedData = controller->dumps();
        std::cout << "Dumped Data: " << dumpedData << std::endl;
    }
    else if (command == "loads")
    {
        std::string jsonData;
        std::cout << "Enter JSON data string: ";
        std::getline(std::cin, jsonData);
        try
        {
            controller->loads(jsonData);
            std::cout << "Loaded JSON data into storage." << std::endl;
        }
        catch (json::parse_error &e)
        {
            std::cout << "Invalid JSON format: " << e.what() << std::endl;
        }
    }
    else if (command == "clean")
    {
        controller->clean();
        std::cout << "Clean all data." << std::endl;
    }
    else if (command == "exit")
    {
        std::cout << "Exiting..." << std::endl;
    }
    else
    {
        std::cout << "Invalid command. Available commands: " << commands << std::endl;
    }
}

int main()
{
    // Initialize storage
    auto controller = init_storage();

    // Console loop
    std::string command;
    while (true)
    {
        std::cout << "\n> Enter command (" << commands << "): ";
        std::getline(std::cin, command);

        if (command == "exit")
        {
            break;
        }

        handle_command(controller, command);
    }

    return 0;
}

// #include <iostream>
// #include <string>
// #include <json.hpp>
// #include <Storages.hpp>

// using json = nlohmann::json;

// int main() {
//     std::cout << "######## start." << std::endl;
//     auto controllerfs = std::make_shared<SingletonKeyValueStorage>() ;
//     controllerfs->file_backend();

//     {
//         SingletonKeyValueStorage controller;
//         controller.add_slave(controllerfs);
//         controller.set("user2", json{{"name", "Bob"}, {"age", 25}});
//     }

//     SingletonKeyValueStorage controller;
//     controller.add_slave(controllerfs);
//     std::cout << controller.uuid() << std::endl;

//     std::cout << "######## Set some key-value pairs" << std::endl;
//     controller.set("user1", json{{"name", "Alice"}, {"age", 30}, {"nums", {1,2,3}}});

//     std::cout << "######## Check if a key exists" << std::endl;
//     std::string key = "user1";
//     if (controller.exists(key)) {
//         std::cout << "Key " << key << " exists in storage." << std::endl;
//     } else {
//         std::cout << "Key " << key << " does not exist in storage." << std::endl;
//     }

//     std::cout << "######## Retrieve and print the value for a key" << std::endl;
//     json value = controller.get("user1");
//     if (!value.is_null()) {
//         std::cout << "Value for key " << key << ": " << value.dump() << std::endl;
//     } else {
//         std::cout << "No value found for key " << key << std::endl;
//     }

//     std::cout << "######## Dump all data to a JSON string" << std::endl;
//     std::string dumpedData = controller.dumps();
//     std::cout << "Dumped Data: " << dumpedData << std::endl;

//     std::cout << "######## Load from dumped data string" << std::endl;
//     std::string jsonData = R"({"user3": {"name": "Charlie", "age": 35}})";
//     controller.loads(jsonData);
//     std::cout << "######## Print all keys" << std::endl;
//     std::vector<std::string> allKeys = controller.keys();
//     std::cout << "All Keys in Storage:" << std::endl;
//     for (const auto& k : allKeys) {
//         std::cout << "- " << k << std::endl;
//     }

//     std::cout << "######## Delete a key" << std::endl;
//     controller.deleteKey("user1");
//     std::cout << "Deleted key 'user1'" << std::endl;

//     std::cout << "######## Verify deletion" << std::endl;
//     if (!controller.exists("user1")) {
//         std::cout << "Key 'user1' was successfully deleted." << std::endl;
//     }

//     std::cout << "######## Test keys" << std::endl;
//     std::cout << controller.keys("user*")[0] << std::endl;

//     std::cout << controller.dumps() << std::endl;

//     return 0;
// }
