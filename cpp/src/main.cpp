#include <iostream>
#include <string>
#include <json.hpp>
#include <Storages.hpp>
#include <map>
#include <vector>
#include <memory>
#include <functional>

using json = nlohmann::json;

// Helper functions
std::string prompt(const std::string &message)
{
    std::string input;
    std::cout << message;
    std::getline(std::cin, input);
    return input;
}

void handle_json_parse_error(const json::parse_error &e)
{
    std::cout << "Invalid JSON format: " << e.what() << std::endl;
}

void handle_invalid_argument(const std::invalid_argument &e)
{
    std::cout << "ERROR: " << e.what() << std::endl;
}

// Command map
using Shared_store_ptr = std::shared_ptr<SingletonKeyValueStorage>;
using CommandFunction = std::function<void(Shared_store_ptr)>;

std::map<std::string, CommandFunction> command_map = {
    {"set", [](Shared_store_ptr controller)
     {
         std::string key = prompt("Enter key: ");
         std::string value = prompt("Enter value (in JSON format): ");
         try
         {
             json json_value = json::parse(value);
             controller->set(key, json_value);
             std::cout << "Set key " << key << " : " << json_value.dump() << std::endl;
         }
         catch (json::parse_error &e)
         {
             handle_json_parse_error(e);
         }
         catch (std::invalid_argument &e)
         {
             handle_invalid_argument(e);
         }
     }},

    {"get", [](Shared_store_ptr controller)
     {
         std::string key = prompt("Enter key: ");
         json value = controller->get(key);
         if (!value.is_null())
         {
             std::cout << "Value for key " << key << ": " << value.dump() << std::endl;
         }
         else
         {
             std::cout << "No value found for key " << key << std::endl;
         }
     }},

    {"exists", [](Shared_store_ptr controller)
     {
         std::string key = prompt("Enter key: ");
         if (controller->exists(key))
         {
             std::cout << "Key " << key << " exists in storage." << std::endl;
         }
         else
         {
             std::cout << "Key " << key << " does not exist in storage." << std::endl;
         }
     }},

    {"delete", [](Shared_store_ptr controller)
     {
         std::string key = prompt("Enter key: ");
         controller->deleteKey(key);
         std::cout << "Deleted key " << key << std::endl;
     }},

    {"keys", [](Shared_store_ptr controller)
     {
         std::string pattern = prompt("Enter key pattern: ");
         std::vector<std::string> allKeys = controller->keys(pattern);
         std::cout << "All Keys in Storage:" << std::endl;
         for (const auto &k : allKeys)
         {
             std::cout << "- " << k << std::endl;
         }
     }},

    {"dumps", [](Shared_store_ptr controller)
     {
         std::string dumpedData = controller->dumps();
         std::cout << "Dumped Data: " << dumpedData << std::endl;
     }},

    {"loads", [](Shared_store_ptr controller)
     {
         std::string jsonData = prompt("Enter JSON data string: ");
         try
         {
             controller->loads(jsonData);
             std::cout << "Loaded JSON data into storage." << std::endl;
         }
         catch (json::parse_error &e)
         {
             handle_json_parse_error(e);
         }
     }},

    {"clean", [](Shared_store_ptr controller)
     {
         controller->clean();
         std::cout << "Cleaned all data." << std::endl;
     }},

    {"ver", [](Shared_store_ptr controller)
     {
         std::cout << "Current: " << controller->get_current_version() << std::endl;
     }},

    {"rev", [](Shared_store_ptr controller)
     {
         std::cout << "Current: " << controller->get_current_version() << std::endl;
         controller->revert_one_operation();
         std::cout << "Reverted to: " << controller->get_current_version() << std::endl;
     }},

    {"exit", [](Shared_store_ptr)
     {
         std::cout << "Exiting..." << std::endl;
     }}};

// Function to auto-generate command list
std::string generate_command_list(const std::map<std::string, CommandFunction> &command_map)
{
    std::ostringstream oss;
    for (const auto &cmd : command_map)
    {
        if (oss.tellp() > 0) oss << ", ";  // Add comma separator between commands
        oss << cmd.first;
    }
    return oss.str();
}

// Function to handle console commands
void handle_command(Shared_store_ptr controller, const std::string &command)
{
    auto cmd = command_map.find(command);
    if (cmd != command_map.end())
    {
        cmd->second(controller);
    }
    else
    {
        std::cout << "Invalid command. Available commands: " << generate_command_list(command_map) << std::endl;
    }
}


void test_rsa()
{
    // Define paths to the PEM files
    std::string public_key_path = "public_key.pem";
    std::string private_key_path = "private_key.pem";

    // Load the public key from the PEM file
    PEMFileReader public_key_reader(public_key_path);
    auto public_key = public_key_reader.load_public_key_from_pkcs8();

    // Load the private key from the PEM file
    PEMFileReader private_key_reader(private_key_path);
    auto private_key = private_key_reader.load_private_key_from_pkcs8();

    // Instantiate the encryptor with the loaded keys
    SimpleRSAChunkEncryptor encryptor(public_key, private_key);

    // Define the plaintext to be encrypted
    std::string plaintext = "Hello, RSA encryption with .pem support!";
    std::cout << "Original Plaintext: [" << plaintext << "]" << std::endl;

    // Encrypt the plaintext
    std::string encrypted_text = encryptor.encrypt_string(plaintext);
    std::cout << "\nEncrypted (Base64 encoded): [" << encrypted_text << "]" << std::endl;

    // Decrypt the encrypted text
    std::string decrypted_text = encryptor.decrypt_string(encrypted_text);
    std::cout << "\nDecrypted Text: [" << decrypted_text << "]" << std::endl;
}

int test()
{
    std::cout << "######## start." << std::endl;
    auto controllerfs = std::make_shared<SingletonKeyValueStorage>();
    controllerfs->file_backend();

    {
        SingletonKeyValueStorage controller;
        controller.add_slave(controllerfs);
        controller.set("user2", json{{"name", "Bob"}, {"age", 25}});
    }

    SingletonKeyValueStorage controller;
    controller.add_slave(controllerfs);
    std::cout << controller.uuid() << std::endl;

    std::cout << "######## Set some key-value pairs" << std::endl;
    controller.set("user1", json{{"name", "Alice"}, {"age", 30}, {"nums", {1, 2, 3}}});

    std::cout << "######## Check if a key exists" << std::endl;
    std::string key = "user1";
    if (controller.exists(key))
    {
        std::cout << "Key " << key << " exists in storage." << std::endl;
    }
    else
    {
        std::cout << "Key " << key << " does not exist in storage." << std::endl;
    }

    std::cout << "######## Retrieve and print the value for a key" << std::endl;
    json value = controller.get("user1");
    if (!value.is_null())
    {
        std::cout << "Value for key " << key << ": " << value.dump() << std::endl;
    }
    else
    {
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
    for (const auto &k : allKeys)
    {
        std::cout << "- " << k << std::endl;
    }

    std::cout << "######## Delete a key" << std::endl;
    controller.deleteKey("user1");
    std::cout << "Deleted key 'user1'" << std::endl;

    std::cout << "######## Verify deletion" << std::endl;
    if (!controller.exists("user1"))
    {
        std::cout << "Key 'user1' was successfully deleted." << std::endl;
    }

    std::cout << "######## Test keys" << std::endl;
    std::cout << controller.keys("user*")[0] << std::endl;

    std::cout << controller.dumps() << std::endl;

    test_rsa();

    return 0;
}

int main()
{
    // test();
    // Initialize storage
    auto controllerfs = std::make_shared<SingletonKeyValueStorage>();
    controllerfs->file_backend();
    auto controllermemo = std::make_shared<SingletonKeyValueStorage>();
    controllermemo->loads(controllerfs->dumps());
    controllermemo->add_slave(controllerfs, {"set", "deleteKey", "clean", "loads"});
    auto controller = controllermemo;

    // Console loop
    std::string command;
    while (true)
    {
        std::cout << "\n> Enter command (" << generate_command_list(command_map) << "): ";
        std::getline(std::cin, command);

        if (command == "exit")
        {
            break;
        }

        handle_command(controller, command);
    }

    return 0;
}
