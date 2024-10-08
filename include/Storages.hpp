#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <regex>
#include <vector>

// Include header-only JSON and UUID libraries
#include <json.hpp>
#include <uuid.h>

using json = nlohmann::json;

std::mt19937 generator;
uuids::uuid_random_generator gen{generator};

// Base class
class SingletonStorageController {
public:
    SingletonStorageController() {}

    virtual bool exists(const std::string& key) {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
        return false;
    }

    virtual void set(const std::string& key, const json& value) {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
    }

    virtual json get(const std::string& key) {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
        return nullptr;
    }

    virtual void deleteKey(const std::string& key) {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
    }

    virtual std::vector<std::string> keys(const std::string& pattern = "*") {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
        return {};
    }

    void clean() {
        for (const auto& key : keys("*")) {
            deleteKey(key);
        }
    }

    std::string dumps() {
        json jsonObject;
        for (const auto& key : keys("*")) {
            jsonObject[key] = get(key);
        }
        return jsonObject.dump();
    }

    void loads(const std::string& jsonString) {
        json jsonObject = json::parse(jsonString);
        for (auto& [key, value] : jsonObject.items()) {
            set(key, value);
        }
    }

    void dump(const std::string& path) {
        std::ofstream file(path);
        file << dumps();
        file.close();
    }

    void load(const std::string& path) {
        std::ifstream file(path);
        std::string jsonString((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        loads(jsonString);
    }
};

class CppDictStorage {
public:
    CppDictStorage() : uuidString(uuids::to_string(gen())) {}

    std::string uuidString;

private:
    std::unordered_map<std::string, json> store;
};

class SingletonCppDictStorage {
public:
    static SingletonCppDictStorage& getInstance() {
        static SingletonCppDictStorage instance;
        return instance;
    }

    std::string uuidString;
    std::unordered_map<std::string, json> store;

private:
    SingletonCppDictStorage() : uuidString(uuids::to_string(gen())) {}
};

class SingletonCppDictStorageController : public SingletonStorageController {
public:
    explicit SingletonCppDictStorageController(SingletonCppDictStorage& model) : model(model) {}

    bool exists(const std::string& key) override {
        return model.store.find(key) != model.store.end();
    }

    void set(const std::string& key, const json& value) override {
        model.store[key] = value;
    }

    json get(const std::string& key) override {
        auto it = model.store.find(key);
        return (it != model.store.end()) ? it->second : json();
    }

    void deleteKey(const std::string& key) override {
        model.store.erase(key);
    }

    std::vector<std::string> keys(const std::string& pattern = "*") override {
        std::vector<std::string> result;
        std::string regexPattern = std::regex_replace(pattern, std::regex(R"(\*)"), ".*");
        regexPattern = std::regex_replace(regexPattern, std::regex(R"(\?)"), ".");
        std::regex regexPatternCompiled(regexPattern);

        for (const auto& item : model.store) {
            if (std::regex_match(item.first, regexPatternCompiled)) {
                result.push_back(item.first);
            }
        }
        return result;
    }


private:
    SingletonCppDictStorage& model;
};
