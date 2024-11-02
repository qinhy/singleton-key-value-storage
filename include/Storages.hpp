#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <regex>
#include <vector>
#include <mutex>
#include <filesystem>
#include <functional>
#include <random>
#include <optional>

// Include header-only JSON and UUID libraries
#include <json.hpp>
#include <uuid.h>
#include <utils.hpp>

using json = nlohmann::json;
using String = std::string;
namespace fs = std::filesystem;

static String generateUUID() noexcept
{
    static std::mt19937 generator{std::random_device{}()};
    static uuids::uuid_random_generator uuidGen{generator};
    return uuids::to_string(uuidGen());
}

// TemplateStorage class definition
template <typename StoreType>
class TemplateStorage
{
public:
    TemplateStorage() : _uuid(generateUUID()) {}

    const String &uuid() const noexcept { return _uuid; }
    StoreType &store() noexcept { return _store; }

    static std::shared_ptr<TemplateStorage> getSingletonInstance()
    {
        static std::shared_ptr<TemplateStorage> instance(new TemplateStorage());
        return instance;
    }

private:
    String _uuid;
    StoreType _store;
};

template <typename DataType, typename StorageType>
class TemplateStorageController
{
public:
    TemplateStorageController(std::shared_ptr<StorageType> model)
        : model(std::move(model)), _uuid(generateUUID())
    {
        register_local_events();
    }

    virtual bool exists(const String &key)
    {
        not_implemented("exists");
        return false;
    }
    virtual void set(const String &key, const DataType &value) { not_implemented("set"); }
    virtual DataType get(const String &key)
    {
        not_implemented("get");
        return {};
    }
    virtual void deleteKey(const String &key) { not_implemented("deleteKey"); }
    virtual std::vector<String> keys(const String &pattern = "*")
    {
        not_implemented("keys");
        return {};
    }
    virtual String dumps()
    {
        not_implemented("dumps");
        return "{}";
    }
    virtual void loads(const String &jsonString) { not_implemented("loads"); }

    // Save and load from file
    void dump(const String &path) const
    {
        std::ofstream file(path);
        file << dumps();
    }

    void load(const String &path)
    {
        std::ifstream file(path);
        String jsonString((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
        loads(jsonString);
    }

    std::string dump_RSA(const std::string &path, const std::string &public_pkcs8_key_path)
    {
        std::string data = dumps();

        PEMFileReader public_key_reader(public_pkcs8_key_path);
        auto public_key = public_key_reader.load_public_key_from_pkcs8();

        SimpleRSAChunkEncryptor encryptor(public_key);

        std::string encrypted_data = encryptor.encrypt_string(data);

        std::ofstream tf(path);
        if (!tf.is_open())
        {
            throw std::runtime_error("Unable to open file for writing: " + path);
        }
        tf << encrypted_data;
        tf.close();

        return data;
    }

    void load_RSA(const std::string &path, const std::string &private_pkcs8_key_path)
    {
        PEMFileReader private_key_reader(private_pkcs8_key_path);
        auto private_key = private_key_reader.load_private_key_from_pkcs8();

        SimpleRSAChunkEncryptor encryptor({}, private_key);

        std::ifstream tf(path);
        if (!tf.is_open())
        {
            throw std::runtime_error("Unable to open file for reading: " + path);
        }
        std::string encrypted_data((std::istreambuf_iterator<char>(tf)), std::istreambuf_iterator<char>());
        tf.close();

        std::string decrypted_data = encryptor.decrypt_string(encrypted_data);

        loads(decrypted_data);
    }

    void clean()
    {
        for (const auto &key : keys())
        {
            deleteKey(key);
        }
    }

    bool has_event(const String &event_name) const noexcept
    {
        return local_event_map.find(event_name) != local_event_map.end();
    }

    void call_event(const String &event_name, const String &key = "", const DataType &value = {})
    {
        auto it = local_event_map.find(event_name);
        if (it != local_event_map.end())
        {
            it->second(key, value);
        }
        else
        {
            std::cerr << "[" << typeid(*this).name() << "]: Event '" << event_name << "' not found or not implemented." << std::endl;
        }
    }

    const String &uuid() const noexcept { return _uuid; }

protected:
    std::shared_ptr<StorageType> model;
    String _uuid;

    void not_implemented(const String &name) const noexcept
    {
        std::cout << "[" << name << "]: not implemented" << std::endl;
    }

    void register_local_events()
    {
        local_event_map["set"] = [this](const String &key, const DataType &value)
        { this->set(key, value); };
        local_event_map["deleteKey"] = [this](const String &key, const DataType &)
        { this->deleteKey(key); };
        local_event_map["load"] = [this](const String &path, const DataType &)
        { this->load(path); };
        local_event_map["loads"] = [this](const String &data, const DataType &)
        { this->loads(data); };
        local_event_map["clean"] = [this](const String &, const DataType &)
        { this->clean(); };
        local_event_map["dumps"] = [this](const String &, const DataType &)
        { this->dumps(); };
        local_event_map["exists"] = [this](const String &key, const DataType &)
        { this->exists(key); };
    }

    std::unordered_map<String, std::function<void(const String &, const DataType &)>> local_event_map;
};

template <typename DataType>
using TemplateDictStorage = TemplateStorage<std::unordered_map<String, DataType>>;

template <typename DataType>
class TemplateDictStorageController : public TemplateStorageController<DataType, TemplateDictStorage<DataType>>
{
public:
    explicit TemplateDictStorageController(std::shared_ptr<TemplateDictStorage<DataType>> model)
        : TemplateStorageController<DataType, TemplateDictStorage<DataType>>(model), model(std::move(model)) {}

    bool exists(const String &key) override
    {
        const auto &store = model->store();
        return store.find(key) != store.end();
    }

    void set(const String &key, const DataType &value) override
    {
        auto &store = model->store();
        store[key] = std::move(value); // Use std::move to avoid unnecessary copies
    }

    void deleteKey(const String &key) override
    {
        auto &store = model->store();
        store.erase(key);
    }

    DataType get(const String &key) override
    {
        const auto &store = model->store();
        auto it = store.find(key);
        if (it != store.end())
        {
            return it->second; // Return value if found
        }
        return nullptr;
    }

    std::vector<String> keys(const String &pattern = "*") override
    {
        const auto &store = model->store();
        std::vector<String> result;

        if (pattern == "*")
        {
            // If pattern is '*', return all keys without regex matching
            result.reserve(store.size()); // Reserve size to avoid multiple allocations
            for (const auto &item : store)
            {
                result.push_back(item.first);
            }
            return result;
        }

        // Otherwise, perform regex matching on keys
        String regexPattern = std::regex_replace(pattern, std::regex(R"(\*)"), ".*");
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

protected:
    std::shared_ptr<TemplateDictStorage<DataType>> model;
};

using JsonDictStorage = TemplateDictStorage<json>;

class JsonDictStorageController : public TemplateDictStorageController<json>
{
public:
    JsonDictStorageController(std::shared_ptr<JsonDictStorage> model)
        : TemplateDictStorageController<json>(std::move(model)) {}

    void loads(const String &jsonString) override
    {
        try
        {
            // Parse the input JSON string
            json jsonObject = json::parse(jsonString);

            // Iterate through the parsed JSON object and set each key-value pair
            for (auto &[key, value] : jsonObject.items())
            {
                set(key, value);
            }
        }
        catch (const json::parse_error &e)
        {
            std::cerr << "Error parsing JSON: " << e.what() << std::endl;
        }
    }

    String dumps() override
    {
        // Create a new JSON object
        json jsonObject = json::object();

        // Iterate through all keys and insert them into the JSON object
        for (const auto &key : keys("*"))
        {
            jsonObject[key] = get(key);
        }

        // Return the string representation of the JSON object
        return jsonObject.dump();
    }
};
using FunctionType = std::function<void(const String &, const json &)>;
using FunctionStorage = TemplateDictStorage<FunctionType>;
using FunctionStorageController = TemplateDictStorageController<FunctionType>;

class EventDispatcherController
{
public:
    static const String ROOT_KEY;

    explicit EventDispatcherController(std::shared_ptr<FunctionStorageController> client = nullptr)
    {
        if (!client)
        {
            client = std::make_shared<FunctionStorageController>(FunctionStorage::getSingletonInstance());
        }
        this->client = std::move(client);
    }

    // Get all registered events
    std::vector<String> events() const
    {
        return client->keys(ROOT_KEY + ":*");
    }

    // Delete a specific event
    void delete_event(const String &event_name)
    {
        client->deleteKey(event_name);
    }

    // Set a new event callback, generating a UUID if no ID is provided
    void set_event(const String &event_name, FunctionType callback, const String &id = "")
    {
        String event_id = id.empty() ? generateUUID() : id;
        String key = construct_event_key(event_name, event_id);
        client->set(key, std::move(callback));
    }

    // Dispatch an event to all registered callbacks
    void dispatch(const String &event_name, const String &key = "", const json &value = json())
    {
        String pattern = ROOT_KEY + ":" + event_name + ":*";
        for (const auto &k : client->keys(pattern))
        {
            auto callback_opt = client->get(k);
            if (callback_opt)
            {
                callback_opt(key, value); // Safely dereference the optional
            }
        }
    }

    // Remove all events
    void clean()
    {
        for (const auto &key : events())
        {
            client->deleteKey(key);
        }
    }

private:
    std::shared_ptr<FunctionStorageController> client;

    // Helper to construct event key
    String construct_event_key(const String &event_name, const String &event_id) const
    {
        return ROOT_KEY + ":" + event_name + ":" + event_id;
    }
};
const String EventDispatcherController::ROOT_KEY = "Event";

class LocalVersionController : public JsonDictStorageController
{
public:
    // Constructor initializes with default operations list
    explicit LocalVersionController(std::shared_ptr<JsonDictStorage> model)
        : JsonDictStorageController(std::move(model))
    {
        json initial_ops = {{"ops", json::array()}};
        set(OPERATIONS_KEY, initial_ops);
    }

    // Adds a new operation and its revert operation
    void add_operation(const std::tuple<String, String, json> &operation,
                       const std::tuple<String, String, json> &revert = std::make_tuple("", "", json()))
    {
        String uuid_str = generateUUID();
        json opData = {{"forward", operation}, {"revert", revert}};
        set(OPERATION_KEY_PREFIX + uuid_str, opData);

        json ops = get(OPERATIONS_KEY);
        ops["ops"].push_back(uuid_str);
        set(OPERATIONS_KEY, ops);
    }

    // Reverts the last operation in the list using the provided callback
    void revert_one_operation(const std::function<void(const String &, const String &, const json &)> &revert_callback)
    {
        auto versions = get_versions();
        if (versions.empty())
            return;

        auto ops = versions;
        String opuuid = ops.back();
        auto op = get(OPERATION_KEY_PREFIX + opuuid);
        auto revert = op["revert"];
        if (revert.is_null() || revert[0].get<String>().empty())
        {
            throw std::runtime_error("No valid revert operation found.");
        }

        String func_name = revert[0];
        String key = revert[1];
        json value = revert[2];

        revert_callback(func_name, key, value);

        ops.pop_back(); // Remove last operation
        set(OPERATIONS_KEY, {{"ops", ops}});
    }

    // Returns a list of all operation UUIDs
    std::vector<String> get_versions()
    {
        json ops = get(OPERATIONS_KEY)["ops"];
        return ops.get<std::vector<String>>();
    }

    // Reverts operations until the given operation UUID
    void revert_operations_until(const String &opuuid,
                                 const std::function<void(const String &, const String &, const json &)> &revert_callback)
    {
        auto ops = get_versions();
        auto it = std::find(ops.rbegin(), ops.rend(), opuuid);
        if (it == ops.rend())
        {
            throw std::invalid_argument("No such version: " + opuuid);
        }

        // Revert operations one by one until reaching the target UUID
        while (it != ops.rbegin())
        {
            revert_one_operation(revert_callback);
            ops.pop_back();
            it = std::find(ops.rbegin(), ops.rend(), opuuid); // Update iterator after each revert
        }

        set(OPERATIONS_KEY, {{"ops", ops}});
    }

private:
    static constexpr const char *OPERATIONS_KEY = "_Operations";
    static constexpr const char *OPERATION_KEY_PREFIX = "_Operation:";
};

class FileStorageController : public JsonDictStorageController
{
public:
    explicit FileStorageController(std::shared_ptr<JsonDictStorage> model)
        : JsonDictStorageController(std::move(model))
    {
        set_directory();
    }

    // Set the storage directory, defaulting to "./store"
    void set_directory(const String &directory = "./store")
    {
        model->store()["directory"] = directory;
    }

    // Retrieve the storage directory, ensuring it exists
    String get_directory() const
    {
        auto it = model->store().find("directory");
        if (it == model->store().end() || it->second.is_null())
        {
            throw std::invalid_argument("No directory specified!");
        }

        String directory = it->second.get<String>();

        if (directory.empty())
        {
            throw std::invalid_argument("Directory is empty!");
        }

        if (!fs::exists(directory))
        {
            std::cout << "Directory does not exist, creating: " << directory << std::endl;
            fs::create_directories(directory);
        }

        return directory;
    }

    // Generate a full file path based on the key
    String get_directory_id(const String &key) const
    {
        return get_directory() + "/" + key + ".json";
    }

    // Override set method to write data to a file
    void set(const String &key, const json &value) override
    {
        std::ofstream file(get_directory_id(key));
        if (!file.is_open())
        {
            throw std::runtime_error("Failed to open file for writing: " + get_directory_id(key));
        }
        file << value.dump();
    }

    // Override get method to read data from a file
    json get(const String &key) override
    {
        std::ifstream file(get_directory_id(key));
        if (!file.is_open())
        {
            throw std::runtime_error("Failed to open file for reading: " + get_directory_id(key));
        }

        json value;
        file >> value;
        return value;
    }

    // Override deleteKey to remove the file associated with the key
    void deleteKey(const String &key) override
    {
        String filename = get_directory_id(key);
        if (fs::exists(filename))
        {
            fs::remove(filename);
        }
        else
        {
            throw std::runtime_error("File does not exist: " + filename);
        }
    }

    // Override keys to retrieve all files matching the glob pattern
    std::vector<String> keys(const String &pattern = "*") override
    {
        std::vector<String> matchingKeys;
        std::regex regexPattern(glob_to_regex(pattern));

        for (const auto &entry : fs::directory_iterator(get_directory()))
        {
            if (entry.is_regular_file())
            {
                String filename = entry.path().filename().string();
                if (std::regex_match(filename, regexPattern))
                {
                    matchingKeys.push_back(entry.path().stem().string()); // Use stem() to get the base filename without extension
                }
            }
        }
        return matchingKeys;
    }

    // Convert glob pattern (e.g., "*.json") to regex
    String glob_to_regex(const String &glob = "*") const
    {
        String regex = "^";
        for (char ch : glob + ".json")
        {
            switch (ch)
            {
            case '*':
                regex += ".*";
                break;
            case '?':
                regex += ".";
                break;
            case '.':
                regex += "\\.";
                break;
            default:
                regex += ch;
                break;
            }
        }
        regex += "$";
        return regex;
    }

private:
    // The model pointer is managed by the base class; no need to store it here
};

class SingletonKeyValueStorage
{
public:
    SingletonKeyValueStorage() : conn(nullptr), _verc(std::make_shared<JsonDictStorage>())
    {
        cpp_backend();
    }

    String uuid() const { return conn->uuid(); }
    void cpp_backend() { conn = _switch_backend("cpp"); }
    void file_backend() { conn = _switch_backend("file"); }

    void add_slave(const std::shared_ptr<SingletonKeyValueStorage> &slave, const std::vector<String> &event_names = {"set", "deleteKey"})
    {
        if (slave->uuid().empty())
        {
            throw std::invalid_argument("No slave UUID.");
        }

        for (const auto &event : event_names)
        {
            if (slave->conn->has_event(event))
            {
                event_dispa.set_event(event, [=](const String &key, const json &value)
                                      { slave->conn->call_event(event, key, value); }, slave->uuid());
            }
            else
            {
                _print("No function for event \"" + event + "\" in slave. Skipping.");
            }
        }
    }

    void delete_slave(const auto &slave) { event_dispa.delete_event(slave->uuid()); }

    void revert_one_operation()
    {
        _verc.revert_one_operation([=](const String &func_name, const String &key = "", const json &value = json())
                                   { _edit(func_name, key, value); });
    }

    String get_current_version()
    {
        auto versions = _verc.get_versions();
        return versions.empty() ? "No versions" : versions.back();
    }

    void revert_operations_until(const String &opuuid)
    {
        _verc.revert_operations_until(opuuid, [=](const String &func_name, const String &key = "", const json &value = json())
                                      { _edit(func_name, key, value); });
    }

    // Public API to interact with the storag
    void set(const String &key, const json &value) { _edit("set", key, value); }
    void deleteKey(const String &key) { _edit("deleteKey", key); }
    void clean() { _edit("clean"); }
    void load(const String &json_path) { _edit("load", json_path); }
    void loads(const String &json_str) { _edit("loads", json_str); }
    bool exists(const String &key) const { return conn->exists(key); }
    std::vector<String> keys(const String &pattern = "*") const { return conn->keys(pattern); }
    json get(const String &key) const { return conn->get(key); }
    String dumps() const { return conn->dumps(); }

private:
    std::shared_ptr<JsonDictStorageController> conn;
    EventDispatcherController event_dispa;
    LocalVersionController _verc;

    std::shared_ptr<JsonDictStorageController> _switch_backend(const String &name)
    {
        if (name == "cpp")
        {
            return std::make_shared<JsonDictStorageController>(JsonDictStorage::getSingletonInstance());
        }
        if (name == "file")
        {
            return std::make_shared<FileStorageController>(std::make_shared<JsonDictStorage>());
        }
        throw std::invalid_argument("No backend named: " + name);
    }

    void _print(const String &msg) const
    {
        std::cout << "[" << typeid(*this).name() << "]: " << msg << std::endl;
    }

    void _edit(const String &func_name, const String &key = "", const json &value = json::parse("{}"))
    {
        // Perform operation and track history
        if (func_name == "set")
        {
            json revert;
            if (exists(key))
            {
                revert = json::array({"set", key, get(key)});
            }
            else
            {
                revert = json::array({"deleteKey", key, json()});
            }
            _verc.add_operation(std::make_tuple(func_name, key, value), revert);
            conn->set(key, value);
        }
        else if (func_name == "deleteKey")
        {
            json revert = json::array({"set", key, get(key)});
            _verc.add_operation(std::make_tuple(func_name, key, value), revert);
            conn->deleteKey(key);
        }
        else if (func_name == "clean" || func_name == "load" || func_name == "loads")
        {
            json revert = json::array({"loads", dumps(), json()});
            _verc.add_operation(std::make_tuple(func_name, key, value), revert);
            if (func_name == "clean")
                conn->clean();
            if (func_name == "load")
                conn->load(key);
            if (func_name == "loads")
                conn->loads(key);
        }
        event_dispa.dispatch(func_name, key, value);
    }
};
