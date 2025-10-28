// main.cpp (updated for SingletonKeyValueStorage)
#include <json.hpp>
#include <Storages.hpp>
#include <rjson.hpp>

#include <iostream>
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <functional>

using json = nlohmann::json;

// -------- Helpers ----------
std::string prompt(const std::string &message) {
    std::string input;
    std::cout << message;
    std::getline(std::cin, input);
    return input;
}

void handle_json_parse_error(const json::parse_error &e) {
    std::cout << "Invalid JSON format: " << e.what() << std::endl;
}

void handle_invalid_argument(const std::invalid_argument &e) {
    std::cout << "ERROR: " << e.what() << std::endl;
}

// -------- Commands ----------
using Shared_store_ptr = std::shared_ptr<SingletonKeyValueStorage>;
using CommandFunction = std::function<void(Shared_store_ptr)>;

std::map<std::string, CommandFunction> command_map = {
    {"set", [](Shared_store_ptr controller) {
        std::string key = prompt("Enter key: ");
        std::string value = prompt("Enter value (in JSON format): ");
        try {
            json json_value = json::parse(value);
            bool ok = controller->set(key, json_value);
            if (ok) {
                std::cout << "Set key " << key << " : " << json_value.dump() << std::endl;
            } else {
                std::cout << "Set failed.\n";
            }
        } catch (json::parse_error &e) {
            handle_json_parse_error(e);
        } catch (std::invalid_argument &e) {
            handle_invalid_argument(e);
        }
    }},

    {"get", [](Shared_store_ptr controller) {
        std::string key = prompt("Enter key: ");
        auto val = controller->get(key);
        if (val) {
            std::cout << "Value for key " << key << ": " << val->dump() << std::endl;
        } else {
            std::cout << "No value found for key " << key << std::endl;
        }
    }},

    {"exists", [](Shared_store_ptr controller) {
        std::string key = prompt("Enter key: ");
        if (controller->exists(key)) {
            std::cout << "Key " << key << " exists in storage." << std::endl;
        } else {
            std::cout << "Key " << key << " does not exist in storage." << std::endl;
        }
    }},

    {"delete", [](Shared_store_ptr controller) {
        std::string key = prompt("Enter key: ");
        bool ok = controller->erase(key);
        std::cout << (ok ? "Deleted key " : "No such key (nothing deleted): ") << key << std::endl;
    }},

    {"keys", [](Shared_store_ptr controller) {
        std::string pattern = prompt("Enter key pattern (e.g. * or user*): ");
        std::vector<std::string> allKeys = controller->keys(pattern);
        std::cout << "Keys (" << allKeys.size() << "):\n";
        for (const auto &k : allKeys) std::cout << "- " << k << std::endl;
    }},

    {"dumps", [](Shared_store_ptr controller) {
        std::string dumpedData = controller->dumps();
        std::cout << "Dumped Data: " << dumpedData << std::endl;
    }},

    {"loads", [](Shared_store_ptr controller) {
        std::string jsonData = prompt("Enter JSON data string: ");
        try {
            bool ok = controller->loads(jsonData);
            std::cout << (ok ? "Loaded JSON data into storage." : "Load failed.") << std::endl;
        } catch (json::parse_error &e) {
            handle_json_parse_error(e);
        }
    }},

    {"clean", [](Shared_store_ptr controller) {
        bool ok = controller->clean();
        std::cout << (ok ? "Cleaned all data." : "Clean failed.") << std::endl;
    }},

    {"ver", [](Shared_store_ptr controller) {
        auto v = controller->get_current_version();
        std::cout << "Current: " << (v ? *v : std::string("(none)")) << std::endl;
    }},

    {"rev", [](Shared_store_ptr controller) {
        auto before = controller->get_current_version();
        std::cout << "Current: " << (before ? *before : std::string("(none)")) << std::endl;
        controller->revert_one_operation();
        auto after = controller->get_current_version();
        std::cout << "Reverted to: " << (after ? *after : std::string("(none)")) << std::endl;
    }},

    {"fwd", [](Shared_store_ptr controller) {
        auto before = controller->get_current_version();
        std::cout << "Current: " << (before ? *before : std::string("(none)")) << std::endl;
        controller->forward_one_operation();
        auto after = controller->get_current_version();
        std::cout << "Forwarded to: " << (after ? *after : std::string("(none)")) << std::endl;
    }},

    {"exit", [](Shared_store_ptr) {
        std::cout << "Exiting..." << std::endl;
    }}
};

// List available commands
std::string generate_command_list(const std::map<std::string, CommandFunction> &command_map) {
    std::ostringstream oss;
    for (auto it = command_map.begin(); it != command_map.end(); ++it) {
        if (it != command_map.begin()) oss << ", ";
        oss << it->first;
    }
    return oss.str();
}

void handle_command(Shared_store_ptr controller, const std::string &command) {
    auto cmd = command_map.find(command);
    if (cmd != command_map.end()) {
        cmd->second(controller);
    } else {
        std::cout << "Invalid command. Available commands: " << generate_command_list(command_map) << std::endl;
    }
}


void test_rsa() {
    // === Uses your existing RSA utilities (PEMFileReader + SimpleRSAChunkEncryptor) ===
    std::string public_key_path  = "../tmp/public_key.pem";
    std::string private_key_path = "../tmp/private_key.pem";

    rjson::PEMFileReader public_key_reader(public_key_path);
    auto public_key = public_key_reader.load_public_pkcs8_key();

    rjson::PEMFileReader private_key_reader(private_key_path);
    auto private_key = private_key_reader.load_private_pkcs8_key();

    rjson::SimpleRSAChunkEncryptor encryptor(public_key, private_key);

    std::string plaintext = "Hello, RSA encryption with .pem support!";
    std::cout << "Original Plaintext: [" << plaintext << "]\n";

    std::string encrypted_text = encryptor.encrypt_string(plaintext);
    std::cout << "\nEncrypted (Base64 encoded): [" << encrypted_text << "]\n";

    std::string decrypted_text = encryptor.decrypt_string(encrypted_text);
    std::cout << "\nDecrypted Text: [" << decrypted_text << "]\n";
}


int main() {
    Tests{}.test_all();
    // test();
    // test_rsa();

    // Turn on version control to enable rev/fwd history
    // auto controller = std::make_shared<SingletonKeyValueStorage>(true /*version_control*/);

    // // Console loop
    // std::string command;
    // while (true) {
    //     std::cout << "\n> Enter command (" << generate_command_list(command_map) << "): ";
    //     if (!std::getline(std::cin, command)) break;
    //     if (command == "exit") break;
    //     handle_command(controller, command);
    // }
    return 0;
}
