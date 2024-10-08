#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <regex>
#include <vector>
#include <mutex>
#include <fstream>
#include <filesystem>

// Include header-only JSON and UUID libraries
#include <json.hpp>
#include <uuid.h>

using json = nlohmann::json;
namespace fs = std::filesystem;

std::mt19937 generator;
uuids::uuid_random_generator gen{generator};

// Base class
class SingletonStorageController
{
public:
    std::string uuid;

    SingletonStorageController() : uuid(uuids::to_string(gen()))
    {
        // Register local member functions
        register_local_events();
    }

    virtual bool exists(const std::string &key)
    {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
        return false;
    }

    virtual void set(const std::string &key, const json &value)
    {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
    }

    virtual json get(const std::string &key)
    {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
        return nullptr;
    }

    virtual void deleteKey(const std::string &key)
    {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
    }

    virtual std::vector<std::string> keys(const std::string &pattern = "*")
    {
        std::cout << "[" << typeid(*this).name() << "]: not implemented" << std::endl;
        return {};
    }

    void clean()
    {
        for (const auto &key : keys("*"))
        {
            deleteKey(key);
        }
    }

    std::string dumps()
    {
        json jsonObject;
        for (const auto &key : keys("*"))
        {
            jsonObject[key] = get(key);
        }
        return jsonObject.dump();
    }

    void loads(const std::string &jsonString)
    {
        json jsonObject = json::parse(jsonString);
        for (auto &[key, value] : jsonObject.items())
        {
            set(key, value);
        }
    }

    void dump(const std::string &path)
    {
        std::ofstream file(path);
        file << dumps();
        file.close();
    }

    void load(const std::string &path)
    {
        std::ifstream file(path);
        std::string jsonString((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        loads(jsonString);
    }

    // Checks if a particular event is available
    bool has_event(const std::string &event_name) const
    {
        return local_event_map.find(event_name) != local_event_map.end();
    }

    void call_event(const std::string &event_name, const std::string &key = "", const json &value = {})
    {
        if (local_event_map.find(event_name) != local_event_map.end())
        {
            local_event_map[event_name](key, value);
        }
        else
        {
            std::cerr << "[" << typeid(*this).name() << "]: Event '" << event_name << "' not found or not implemented." << std::endl;
        }
    }

protected:
    // Register local member functions to their event names
    void register_local_events()
    {
        local_event_map["set"] = [this](const std::string &key, const json &value)
        { this->set(key, value); };
        local_event_map["delete"] = [this](const std::string &key, const json &)
        { this->deleteKey(key); };
        local_event_map["load"] = [this](const std::string &path, const json &)
        { this->load(path); };
        local_event_map["loads"] = [this](const std::string &, const json &jsonString)
        { this->loads(jsonString.dump()); };
        local_event_map["dumps"] = [this](const std::string &, const json &)
        { this->dumps(); };
        local_event_map["exists"] = [this](const std::string &key, const json &)
        { this->exists(key); };
        // Add more mappings as needed for other local functions.
    }

    // Map to store function pointers for local functions based on event name
    std::unordered_map<std::string, std::function<void(const std::string &, const json &)>> local_event_map;
};
class CppDictStorage
{
public:
    CppDictStorage() : uuidString(uuids::to_string(gen())) {}

    std::string uuidString;
    std::unordered_map<std::string, json> store;
};

class SingletonCppDictStorage : public CppDictStorage
{
public:
    static SingletonCppDictStorage &getInstance()
    {
        static SingletonCppDictStorage instance;
        return instance;
    }
};

class SingletonCppDictStorageController : public SingletonStorageController
{
public:
    explicit SingletonCppDictStorageController(CppDictStorage &model) : model(model) {}

    bool exists(const std::string &key) override
    {
        return model.store.find(key) != model.store.end();
    }

    void set(const std::string &key, const json &value) override
    {
        model.store[key] = value;
    }

    json get(const std::string &key) override
    {
        auto it = model.store.find(key);
        return (it != model.store.end()) ? it->second : json();
    }

    void deleteKey(const std::string &key) override
    {
        model.store.erase(key);
    }

    std::vector<std::string> keys(const std::string &pattern = "*") override
    {
        std::vector<std::string> result;
        std::string regexPattern = std::regex_replace(pattern, std::regex(R"(\*)"), ".*");
        regexPattern = std::regex_replace(regexPattern, std::regex(R"(\?)"), ".");
        std::regex regexPatternCompiled(regexPattern);

        for (const auto &item : model.store)
        {
            if (std::regex_match(item.first, regexPatternCompiled))
            {
                result.push_back(item.first);
            }
        }
        return result;
    }

private:
    CppDictStorage &model;
};

class SingletonFunctionStorage
{
public:
    static SingletonFunctionStorage &getInstance()
    {
        static SingletonFunctionStorage instance;
        return instance;
    }

    bool exists(const std::string &key)
    {
        return store.find(key) != store.end();
    }

    void set(const std::string &key, const std::function<void()> &func)
    {
        store[key] = func;
    }

    std::function<void()> get(const std::string &key)
    {
        return exists(key) ? store[key] : nullptr;
    }

    void deleteKey(const std::string &key)
    {
        store.erase(key);
    }

    std::vector<std::string> keys(const std::string &pattern = "*")
    {
        std::vector<std::string> result;
        std::string regexPattern = std::regex_replace(pattern, std::regex(R"(\*)"), ".*");
        regexPattern = std::regex_replace(regexPattern, std::regex(R"(\?)"), ".");
        std::regex regexPatternCompiled(regexPattern);

        for (const auto &item : store)
        {
            if (std::regex_match(item.first, regexPatternCompiled))
            {
                result.push_back(item.first);
            }
        }
        return result;
    }

private:
    std::unordered_map<std::string, std::function<void()>> store;
};

class EventDispatcherController
{
public:
    static const std::string ROOT_KEY;

    explicit EventDispatcherController(std::shared_ptr<SingletonFunctionStorage> client = nullptr)
    {
        if (!client)
        {
            client = std::make_shared<SingletonFunctionStorage>(SingletonFunctionStorage::getInstance());
        }
        this->client = client;
    }

    std::vector<std::string> events()
    {
        return client->keys("*");
    }

    void delete_event(const std::string &event_name)
    {
        client->deleteKey(event_name);
    }

    void set_event(const std::string &event_name, const std::function<void()> &callback, const std::string &id = "")
    {
        std::string event_id = id.empty() ? uuids::to_string(gen()) : id;
        client->set(ROOT_KEY + ":" + event_name + ":" + event_id, callback);
    }

    void dispatch(const std::string &event_name)
    {
        for (const auto &key : client->keys(ROOT_KEY + ":" + event_name + ":*"))
        {
            auto callback = client->get(key);
            if (callback)
            {
                callback();
            }
        }
    }

    void clean()
    {
        for (const auto &key : events())
        {
            client->deleteKey(key);
        }
    }

private:
    std::shared_ptr<SingletonFunctionStorage> client;
};

const std::string EventDispatcherController::ROOT_KEY = "Event";
class LocalVersionController
{
public:
    explicit LocalVersionController(std::shared_ptr<SingletonStorageController> client = nullptr)
    {
        if (!client)
        {
            auto dictStorage = std::make_shared<CppDictStorage>();
            client = std::make_shared<SingletonCppDictStorageController>(*dictStorage);
        }
        this->client = client;
        json initial_ops = {{"ops", json::array()}};
        this->client->set("_Operations", initial_ops);
    }

    void add_operation(const std::tuple<std::string, std::string> &operation,
                       const std::tuple<std::string, std::string> &revert = std::tuple<std::string, std::string>())
    {
        auto uuid_str = uuids::to_string(gen());

        json opData = {
            {"forward", std::get<0>(operation)},
            {"revert", std::get<0>(revert)}};

        this->client->set("_Operation:" + uuid_str, opData);

        json ops = this->client->get("_Operations");
        ops["ops"].push_back(uuid_str);
        this->client->set("_Operations", ops);
    }

    void revert_one_operation(const std::function<void(const json &)> &revert_callback)
    {
        auto ops = this->client->get("_Operations")["ops"].get<std::vector<std::string>>();
        std::string opuuid = ops.back();
        json op = this->client->get("_Operation:" + opuuid);
        json revert = op["revert"];

        revert_callback(revert);

        ops.pop_back();
        this->client->set("_Operations", {{"ops", ops}});
    }

    std::vector<std::string> get_versions()
    {
        json ops = this->client->get("_Operations")["ops"];
        return ops.get<std::vector<std::string>>();
    }

    void revert_operations_until(const std::string &opuuid, const std::function<void(const json &)> &revert_callback)
    {
        auto ops = get_versions();
        auto it = std::find(ops.rbegin(), ops.rend(), opuuid);
        if (it == ops.rend())
        {
            throw std::invalid_argument("No such version: " + opuuid);
        }

        for (auto i = ops.rbegin(); i != it; ++i)
        {
            revert_one_operation(revert_callback);
        }
    }

private:
    std::shared_ptr<SingletonStorageController> client;
};

class SingletonFileStorage {
public:
    std::string directory;

    static SingletonFileStorage& getInstance(const std::string& directory="./store") {
        static SingletonFileStorage instance(directory);
        return instance;
    }
    
    // Helper function to convert glob pattern to regex pattern
    std::string glob_to_regex(const std::string& glob="*") const {
         
        std::string regex = "^";
        for (char ch : glob+".json") {
            switch (ch) {
                case '*': regex += ".*"; break;
                case '?': regex += "."; break;
                case '.': regex += "\\."; break;
                default: regex += ch; break;
            }
        }
        regex += "$";
        return regex;
    }
};

class SingletonFileStorageController : public SingletonStorageController {
public:
    explicit SingletonFileStorageController(SingletonFileStorage &model) : model(model) {}

    void set(const std::string& key, const json& value) override {
        std::ofstream file(model.directory + "/" + key + ".json");
        if (file.is_open()) {
            file << value.dump();
            file.close();
        }
    }

    json get(const std::string& key) override {
        std::ifstream file(model.directory + "/" + key + ".json");
        json value;
        if (file.is_open()) {
            file >> value;
            file.close();
        }
        return value;
    }

    void deleteKey(const std::string& key) override {
        std::string filename = model.directory + "/" + key + ".json";
        std::remove(filename.c_str());
    }

    std::vector<std::string> keys(const std::string& pattern = "*") override {
        std::vector<std::string> matchingKeys;
        std::regex regexPattern(model.glob_to_regex(pattern));

        for (const auto& entry : fs::directory_iterator(model.directory)) {
            if (entry.is_regular_file() && std::regex_match(entry.path().filename().string(), regexPattern)) {
                std::string filename = entry.path().filename().string();
                // Remove the ".json" extension to return only the key name
                matchingKeys.push_back(filename.substr(0, filename.find_last_of(".")));
            }
        }
        return matchingKeys;
    }

private:
    SingletonFileStorage &model;
};

class SingletonKeyValueStorage : public SingletonStorageController
{
public:
    SingletonKeyValueStorage() : conn(nullptr)
    {
        cpp_backend();
    }

    void cpp_backend()
    {
        conn = _switch_backend("cpp");
    }
    void file_backend()
    {
        conn = _switch_backend("file");
    }

    void add_slave(const std::shared_ptr<SingletonStorageController> &slave,
                   const std::vector<std::string> &event_names = {"set", "delete"})
    {
        if (slave->uuid.empty())
        {
            try
            {
                auto uuid_str = uuids::to_string(gen());
                slave->uuid = uuid_str;
            }
            catch (...)
            {
                _print("Cannot set UUID to slave. Skipping this slave.");
                return;
            }
        }
        for (const auto &event : event_names)
        {
            if (slave->has_event(event))
            {
                event_dispa.set_event(event, [=]
                                      { slave->call_event(event); }, slave->uuid);
            }
            else
            {
                _print("No function for event \"" + event + "\" in slave. Skipping.");
            }
        }
    }

    void delete_slave(const std::shared_ptr<SingletonStorageController> &slave)
    {
        event_dispa.delete_event(slave->uuid);
    }

    // void revert_one_operation() {
    //     _verc.revert_one_operation([=](const json& revert) { _edit(revert); });
    // }

    // std::string get_current_version() {
    //     auto versions = _verc.get_versions();
    //     return versions.empty() ? "" : versions.back();
    // }

    // void revert_operations_until(const std::string& opuuid) {
    //     _verc.revert_operations_until(opuuid, [=](const json& revert) { _edit(revert); });
    // }

    void set(const std::string &key, const json &value){_edit("set",key,value);}
    void deleteKey(const std::string &key){_edit("deleteKey",key);}
    void clean(){_edit("clean");}
    void load(const std::string &json_path){_edit("load",json_path);}
    void loads(const std::string &json_str){_edit("loads",json_str);}
    bool exists(const std::string &key) override{return conn->exists(key);}
    std::vector<std::string> keys(const std::string &pattern = "*") override{return conn->keys(pattern);}
    json get(const std::string &key) override{return conn->get(key);}
    std::string dumps(){return conn->dumps();}

private:
    std::shared_ptr<SingletonStorageController> conn;
    EventDispatcherController event_dispa;
    // KeysHistoryController _hist;
    // LocalVersionController _verc;

    std::shared_ptr<SingletonStorageController> _switch_backend(const std::string &name)
    {
        if (name == "cpp")
        {
            return std::make_shared<SingletonCppDictStorageController>(SingletonCppDictStorage::getInstance());
        }
        if (name == "file")
        {
            return std::make_shared<SingletonFileStorageController>(SingletonFileStorage::getInstance());
        }
        throw std::invalid_argument("No backend of " + name);
    }

    void _print(const std::string &msg)
    {
        std::cout << "[" << typeid(*this).name() << "]: " << msg << std::endl;
    }

    template <typename Func>
    bool _try_obj_error(Func func)
    {
        try
        {
            return func();
        }
        catch (const std::exception &e)
        {
            _print(e.what());
            return false;
        }
    }

    bool _try_edit_error(const std::tuple<std::string, std::string, json> &args)
    {
        // Unpack arguments
        const std::string &func_name = std::get<0>(args);
        const std::string &key = std::get<1>(args);
        const json &value = std::get<2>(args);

        // Perform local version control based on the function
        try
        {
            // if (func_name == "set") {
            //     json revert;
            //     if (exists(key)) {
            //         revert = json::array({"set", key, get(key)});
            //     } else {
            //         revert = json::array({"delete", key});
            //     }
            //     _verc.add_operation({func_name, key, value}, revert);
            // } else if (func_name == "delete") {
            //     json revert = json::array({"set", key, get(key)});
            //     _verc.add_operation({func_name, key, value}, revert);
            // } else if (func_name == "clean" || func_name == "load" || func_name == "loads") {
            //     json revert = json::array({"loads", dumps()});
            //     _verc.add_operation({func_name, key, value}, revert);
            // }

            // Execute the edit
            _edit(func_name, key, value);
            return true;
        }
        catch (const std::exception &e)
        {
            _print(e.what());
            return false;
        }
    }

    void _edit(const std::string &func_name, const std::string &key = "", const json &value = json())
    {
        if (func_name != "set" && func_name != "deleteKey" && func_name != "clean"
             && func_name != "load" && func_name != "loads")
        {
            _print("No function of \"" + func_name + "\". Returning.");
            return;
        }

        // _hist.reset();  // Reset history for tracking changes

        // Handle based on function name
        if (func_name == "set")
        {
            conn->set(key, value);
        }
        else if (func_name == "deleteKey")
        {
            conn->deleteKey(key);
        }
        else if (func_name == "clean")
        {
            // Assuming clean is an available method in conn
            conn->clean();
        }
        else if (func_name == "load")
        {
            conn->load(key); // Here, `key` would be the path to the file
        }
        else if (func_name == "loads")
        {
            conn->loads(key); // Here, `value` is expected to contain the JSON string
        }

        // Dispatch the event via EventDispatcherController
        // event_dispa.dispatch(func_name, key, value);
    }
};