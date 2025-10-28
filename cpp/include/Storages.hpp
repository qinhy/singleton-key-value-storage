// C++17 translation of https://github.com/qinhy/singleton-key-value-storage
#pragma once
#include <string>
#include <unordered_map>
#include <map>
#include <vector>
#include <set>
#include <list>
#include <functional>
#include <optional>
#include <random>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <stdexcept>
#include <cctype>
#include <cstdint>
#include <cassert>

// Include header-only JSON and UUID libraries
#include <json.hpp>
#include <uuid.h>
#include <rjson.hpp>

using json = nlohmann::json;

// ===================== Utilities =====================
inline std::string uuid_v4(){
    static std::mt19937 generator{std::random_device{}()};
    static uuids::uuid_random_generator uuidGen{generator};
    return uuids::to_string(uuidGen());
}

// ---- base64url (no padding) ----
inline const std::string& b64_table() {
    static const std::string tbl =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    return tbl;
}
inline std::string base64_encode(const std::string& in) {
    std::string out;
    out.reserve(((in.size() + 2) / 3) * 4);
    int val = 0, valb = -6;
    for (uint8_t c : in) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(b64_table()[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6) out.push_back(b64_table()[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}
inline std::string base64_decode(const std::string& in) {
    std::vector<int> T(256, -1);
    for (int i = 0; i < 64; i++) T[b64_table()[i]] = i;
    std::string out;
    out.reserve(in.size() * 3 / 4);
    int val = 0, valb = -8;
    for (uint8_t c : in) {
        if (T[c] == -1) { if (c == '=') break; else continue; }
        val = (val << 6) + T[c];
        valb += 6;
        if (valb >= 0) {
            out.push_back(char((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
    return out;
}
inline std::string b64url_encode(const std::string& s) {
    std::string b64 = base64_encode(s);
    // url-safe
    for (char& c : b64) {
        if (c == '+') c = '-';
        else if (c == '/') c = '_';
    }
    // strip padding
    while (!b64.empty() && b64.back() == '=') b64.pop_back();
    return b64;
}
inline std::string b64url_decode(const std::string& s) {
    std::string x = s;
    for (char& c : x) {
        if (c == '-') c = '+';
        else if (c == '_') c = '/';
    }
    while (x.size() % 4) x.push_back('=');
    return base64_decode(x);
}
inline bool is_b64url(const std::string& s) {
    try {
        return b64url_encode(b64url_decode(s)) == s;
    } catch (...) {
        return false;
    }
}

inline size_t deep_size_of_json(const json& j);

inline size_t deep_size_of_string(const std::string& s) {
    return sizeof(std::string) + s.size();
}
inline size_t deep_size_of_json(const json& j) {
    switch (j.type()) {
        case json::value_t::null:    return 0;
        case json::value_t::boolean: return sizeof(bool);
        case json::value_t::number_integer: 
        case json::value_t::number_unsigned:
        case json::value_t::number_float: return sizeof(double);
        case json::value_t::string:  return deep_size_of_string(j.get_ref<const std::string&>());
        case json::value_t::array: {
            size_t sum = sizeof(json);
            for (const auto& v : j) sum += deep_size_of_json(v);
            return sum;
        }
        case json::value_t::object: {
            size_t sum = sizeof(json);
            for (auto it = j.begin(); it != j.end(); ++it) {
                sum += deep_size_of_string(it.key());
                sum += deep_size_of_json(it.value());
            }
            return sum;
        }
        default: return sizeof(json);
    }
}
inline std::string humanize_bytes(size_t n) {
    static const char* units[] = {"B","KB","MB","GB","TB","PB"};
    double size = static_cast<double>(n);
    int i = 0;
    while (size >= 1024.0 && i < 5) { size /= 1024.0; ++i; }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1) << size << " " << units[i];
    return oss.str();
}

// Wildcard match: supports '*' and '?'
inline bool wildcard_match(const char* pat, const char* str) {
    if (*pat == '\0') return *str == '\0';
    if (*pat == '*') {
        while (*pat == '*') ++pat;
        if (*pat == '\0') return true;
        while (*str) {
            if (wildcard_match(pat, str)) return true;
            ++str;
        }
        return false;
    } else if (*pat == '?' || *pat == *str) {
        return *str ? wildcard_match(pat + 1, str + 1) : false;
    } else {
        return false;
    }
}
inline bool wildcard_match(const std::string& pattern, const std::string& s) {
    return wildcard_match(pattern.c_str(), s.c_str());
}

// ===================== Abstract Storage =====================

struct AbstractStorage {
    std::string uuid = uuid_v4();
    bool is_singleton = false;
    virtual ~AbstractStorage() = default;
    virtual size_t bytes_used(bool deep=true) const = 0; // approx
};

// DictStorage: map<string, json>
struct DictStorage : public AbstractStorage {
    using Store = std::unordered_map<std::string, json>;
    std::shared_ptr<Store> store;

    // shared singleton backing
    static std::shared_ptr<Store>& singleton_store() {
        static std::shared_ptr<Store> s = std::make_shared<Store>();
        return s;
    }

    DictStorage(std::shared_ptr<Store> st = nullptr, bool singleton=false) {
        store = st ? st : std::make_shared<Store>();
        is_singleton = singleton;
    }

    DictStorage get_singleton() const {
        DictStorage s(DictStorage::singleton_store(), true);
        s.uuid = uuid; // mimic sharing class-level uuid semantic
        return s;
    }

    size_t bytes_used(bool deep=true) const override {
        if (!deep) return sizeof(*this) + store->size() * (sizeof(Store::value_type));
        size_t sum = sizeof(*this);
        for (auto& kv : *store) {
            sum += deep_size_of_string(kv.first);
            sum += deep_size_of_json(kv.second);
        }
        return sum;
    }
};

// ===================== Controllers =====================

struct AbstractStorageController {
    virtual ~AbstractStorageController() = default;

    virtual bool is_singleton() const = 0;
    virtual bool exists(const std::string& key) const = 0;
    virtual void set(const std::string& key, const json& value) = 0;
    virtual std::optional<json> get(const std::string& key) const = 0;
    virtual bool erase(const std::string& key) = 0;

    virtual std::vector<std::string> keys(const std::string& pattern="*") const = 0;

    virtual void clean() {
        auto ks = keys("*");
        for (auto& k : ks) erase(k);
    }

    virtual std::string dumps() const {
        json root = json::object();
        for (auto& k : keys("*")) {
            auto v = get(k);
            if (v) root[k] = *v;
        }
        return root.dump();
    }
    virtual void loads(const std::string& s) {
        json root = json::parse(s);
        for (auto it = root.begin(); it != root.end(); ++it) set(it.key(), it.value());
    }
    virtual void dump_file(const std::string& path) const {
        std::ofstream ofs(path);
        ofs << dumps();
    }
    virtual void load(const std::string& path) {
        std::ifstream ifs(path);
        std::stringstream buf; buf << ifs.rdbuf();
        loads(buf.str());
    }
    
    virtual void dump_rjson(const std::string& path,
                            const std::string& public_pkcs8_key_path,
                            bool compress=true) const {
        rjson::dump_rJSON(dumps(), path, public_pkcs8_key_path, compress);
    }

    virtual void load_rjson(const std::string& path,
                            const std::string& private_pkcs8_key_path) {
        const std::string plain_json = rjson::load_rJSON(path, private_pkcs8_key_path);
        loads(plain_json);
    }

    // Approximate memory
    virtual size_t bytes_used(bool deep=true) const = 0;
};

struct DictStorageController : public AbstractStorageController {
    DictStorage model;

    explicit DictStorageController(const DictStorage& m) : model(m) {}

    bool is_singleton() const override { return model.is_singleton; }

    bool exists(const std::string& key) const override {
        return model.store->find(key) != model.store->end();
    }

    void set(const std::string& key, const json& value) override {
        (*model.store)[key] = value;
    }

    std::optional<json> get(const std::string& key) const override {
        auto it = model.store->find(key);
        if (it == model.store->end()) return std::nullopt;
        return it->second;
    }

    bool erase(const std::string& key) override {
        auto it = model.store->find(key);
        if (it == model.store->end()) return false;
        model.store->erase(it);
        return true;
    }

    std::vector<std::string> keys(const std::string& pattern="*") const override {
        std::vector<std::string> out;
        out.reserve(model.store->size());
        for (auto& kv : *model.store) {
            if (wildcard_match(pattern, kv.first)) out.push_back(kv.first);
        }
        return out;
    }

    size_t bytes_used(bool deep=true) const override {
        return model.bytes_used(deep);
    }

    // Builders to mirror Python
    static DictStorageController build_tmp() {
        return DictStorageController(DictStorage{});
    }
    static DictStorageController build() {
        DictStorage tmp;
        return DictStorageController(tmp.get_singleton());
    }
};

// ---- Memory-limited dict with LRU/FIFO eviction ----

struct MemoryLimitedDictStorageController : public DictStorageController {
    enum class Policy { LRU, FIFO };

    size_t max_bytes;
    Policy policy;
    std::function<void(const std::string&, const json&)> on_evict;
    std::set<std::string> pinned;

    std::unordered_map<std::string, size_t> sizes;
    std::list<std::string> order; // front = oldest
    std::unordered_map<std::string, std::list<std::string>::iterator> where;
    size_t current_bytes = 0;

    MemoryLimitedDictStorageController(
        const DictStorage& model,
        double max_memory_mb = 1024.0,
        const std::string& pol = "lru",
        std::function<void(const std::string&, const json&)> onEvict = [](auto, auto){},
        std::set<std::string> pinnedKeys = {}
    )
    : DictStorageController(model),
      max_bytes(static_cast<size_t>(std::max(0.0, max_memory_mb) * 1024.0 * 1024.0)),
      policy( (pol == "fifo" || pol == "FIFO") ? Policy::FIFO : Policy::LRU ),
      on_evict(std::move(onEvict)),
      pinned(std::move(pinnedKeys))
    {}

    size_t entry_size(const std::string& k, const json& v) const {
        return deep_size_of_string(k) + deep_size_of_json(v);
    }

    void reduce_key(const std::string& key) {
        auto itS = sizes.find(key);
        if (itS != sizes.end()) {
            if (current_bytes >= itS->second) current_bytes -= itS->second;
            sizes.erase(itS);
        }
        auto itW = where.find(key);
        if (itW != where.end()) {
            order.erase(itW->second);
            where.erase(itW);
        }
    }

    void maybe_evict() {
        if (max_bytes == 0) return;
        while (current_bytes > max_bytes && !order.empty()) {
            // victim from head; skip pinned
            auto it = order.begin();
            while (it != order.end() && pinned.count(*it)) ++it;
            if (it == order.end()) break;
            const std::string victim = *it;

            auto val = DictStorageController::get(victim);
            reduce_key(victim);
            DictStorageController::erase(victim);
            if (val) on_evict(victim, *val);
        }
    }

    void set(const std::string& key, const json& value) override {
        if (exists(key)) reduce_key(key);
        DictStorageController::set(key, value);

        size_t sz = entry_size(key, value);
        sizes[key] = sz;
        current_bytes += sz;

        order.push_back(key);
        where[key] = std::prev(order.end());
        if (policy == Policy::LRU) {
            // (already at end)
        }
        maybe_evict();
    }

    std::optional<json> get(const std::string& key) const override {
        auto v = DictStorageController::get(key);
        if (v && policy == Policy::LRU) {
            auto self = const_cast<MemoryLimitedDictStorageController*>(this);
            auto itW = self->where.find(key);
            if (itW != self->where.end()) {
                self->order.splice(self->order.end(), self->order, itW->second);
                itW->second = std::prev(self->order.end());
            }
        }
        return v;
    }

    bool erase(const std::string& key) override {
        if (!exists(key)) return false;
        reduce_key(key);
        return DictStorageController::erase(key);
    }

    void clean() override {
        auto ks = keys("*");
        for (auto& k : ks) DictStorageController::erase(k);
        sizes.clear(); where.clear(); order.clear(); current_bytes = 0;
    }

    size_t bytes_used(bool /*deep*/=true) const override {
        return current_bytes;
    }
};

// ===================== Event Dispatcher =====================

struct EventDispatcherController {
    static constexpr const char* ROOT_KEY = "_Event";
    // We keep callbacks in-memory. (Python version stores callables in store.)
    using Callback = std::function<void(const json&)>;

    std::unordered_map<std::string, Callback> callbacks; // key -> cb
    mutable std::unordered_map<std::string, std::string> b64_cache{{"*","*"}};

    std::string event_glob(const std::string& event_name="*", const std::string& event_id="*") const {
        auto& cache = const_cast<std::unordered_map<std::string,std::string>&>(b64_cache);
        if (!cache.count(event_name)) cache[event_name] = b64url_encode(event_name);
        return std::string(ROOT_KEY) + ":" + cache[event_name] + ":" + event_id;
    }

    std::vector<std::pair<std::string, Callback>> events() const {
        std::vector<std::pair<std::string, Callback>> out;
        out.reserve(callbacks.size());
        for (auto& kv : callbacks) out.emplace_back(kv.first, kv.second);
        return out;
    }

    std::vector<Callback> get_event(const std::string& event_id) const {
        // find keys matching "*:<event_id>"
        std::vector<Callback> out;
        for (auto& kv : callbacks) {
            auto& k = kv.first;
            auto pos1 = k.find(':');
            auto pos2 = k.find(':', pos1 == std::string::npos ? 0 : pos1 + 1);
            if (pos2 != std::string::npos) {
                std::string eid = k.substr(pos2 + 1);
                if (eid == event_id) out.push_back(kv.second);
            }
        }
        return out;
    }

    int erase_event(const std::string& id) {
        return callbacks.erase(id);
    }

    std::string set_event(const std::string& event_name, Callback cb, const std::optional<std::string>& event_id = std::nullopt) {
        std::string eid = event_id.value_or(uuid_v4());
        auto& cache = b64_cache;
        if (!cache.count(event_name)) cache[event_name] = b64url_encode(event_name);
        std::string key = std::string(ROOT_KEY) + ":" + cache[event_name] + ":" + eid;
        callbacks[key] = std::move(cb);
        return eid;
    }

    void dispatch_event(const std::string& event_name, const json& payload=json::object()) {
        auto& cache = b64_cache;
        if (!cache.count(event_name)) cache[event_name] = b64url_encode(event_name);
        const std::string prefix = std::string(ROOT_KEY) + ":" + cache[event_name] + ":";
        // call all listeners with matching prefix
        // (Python uses store.keys(pattern); we do a simple prefix test)
        for (auto& kv : callbacks) {
            if (kv.first.rfind(prefix, 0) == 0) {
                try { kv.second(payload); } catch (...) {}
            }
        }
    }
};

// ===================== Message Queue =====================

struct MessageQueueController : public MemoryLimitedDictStorageController {
    static constexpr const char* ROOT_KEY = "_MessageQueue";
    static constexpr const char* ROOT_KEY_EVENT = "MQE";

    EventDispatcherController dispatcher;
    mutable std::unordered_map<std::string,std::string> b64_cache{{"*","*"}};

    MessageQueueController(const DictStorage& model,
                           double max_memory_mb = 1024.0,
                           const std::string& pol = "lru",
                           std::function<void(const std::string&, const json&)> onEvict = [](auto, auto){},
                           std::set<std::string> pinnedKeys = {},
                           std::optional<EventDispatcherController> disp = std::nullopt)
    : MemoryLimitedDictStorageController(model, max_memory_mb, pol, onEvict, std::move(pinnedKeys)),
      dispatcher(disp.value_or(EventDispatcherController{}))
    {}

    std::string qname(const std::string& q) const {
        auto& cache = const_cast<std::unordered_map<std::string,std::string>&>(b64_cache);
        if (!cache.count(q)) {
            std::string enc = b64url_encode(q);
            cache[q] = enc;
            cache[enc] = q;
        }
        return cache[q];
    }
    std::string qkey(const std::string& q, std::optional<std::string> idx = std::nullopt) const {
        std::string k = std::string(ROOT_KEY) + ":" + qname(q);
        if (idx) k += ":" + *idx;
        return k;
    }
    std::string event_name(const std::string& q, const std::string& kind) const {
        return std::string(ROOT_KEY_EVENT) + ":" + qname(q) + ":" + kind;
    }

    json load_meta(const std::string& q) {
        auto m = DictStorageController::get(qkey(q));
        if (!m || !m->is_object()) {
            json nm = {{"head",0}, {"tail",0}};
            DictStorageController::set(qkey(q), nm);
            return nm;
        }
        json meta = *m;
        if (!meta.contains("head") || !meta.contains("tail") ||
            !meta["head"].is_number_integer() || !meta["tail"].is_number_integer() ||
            meta["head"].get<int64_t>() < 0 || meta["tail"].get<int64_t>() < meta["head"].get<int64_t>()) {
            meta = {{"head",0}, {"tail",0}};
            DictStorageController::set(qkey(q), meta);
        }
        return meta;
    }
    void save_meta(const std::string& q, const json& meta) {
        DictStorageController::set(qkey(q), meta);
    }
    int size_from_meta(const json& meta) const {
        return std::max<int64_t>(0, meta["tail"].get<int64_t>() - meta["head"].get<int64_t>());
    }
    void try_dispatch(const std::string& q, const std::string& kind, const std::optional<std::string>& key, const std::optional<json>& msg) {
        try {
            json payload = json::object();
            if (msg) payload["message"] = *msg;
            dispatcher.dispatch_event(event_name(q, kind), payload);
        } catch (...) {}
    }

    std::string add_listener(const std::string& queue_name,
                             EventDispatcherController::Callback cb,
                             const std::string& event_kind = "pushed",
                             const std::optional<std::string>& listener_id = std::nullopt) {
        return dispatcher.set_event(event_name(queue_name, event_kind), std::move(cb), listener_id);
    }
    int remove_listener(const std::string& listener_id) {
        return dispatcher.erase_event(listener_id);
    }

    std::string push(const json& message, const std::string& q="default") {
        json meta = load_meta(q);
        int64_t idx = meta["tail"].get<int64_t>();
        std::string key = qkey(q, std::to_string(idx));
        DictStorageController::set(key, message);
        meta["tail"] = idx + 1;
        save_meta(q, meta);
        try_dispatch(q, "pushed", key, message);
        return key;
    }

    std::pair<std::optional<std::string>, std::optional<json>> pop_item(const std::string& q="default", bool peek=false) {
        json meta = load_meta(q);
        // advance head past holes
        while (meta["head"] < meta["tail"]) {
            std::string k = qkey(q, std::to_string((int64_t)meta["head"]));
            if (DictStorageController::get(k)) break;
            meta["head"] = (int64_t)meta["head"] + 1;
        }
        if (meta["head"] >= meta["tail"]) return {std::nullopt, std::nullopt};

        std::string key = qkey(q, std::to_string((int64_t)meta["head"]));
        auto msg = DictStorageController::get(key);
        if (!msg) {
            meta["head"] = (int64_t)meta["head"] + 1;
            save_meta(q, meta);
            // try again or return empty if at end
            while (meta["head"] < meta["tail"]) {
                std::string k = qkey(q, std::to_string((int64_t)meta["head"]));
                if (DictStorageController::get(k)) break;
                meta["head"] = (int64_t)meta["head"] + 1;
            }
            if (meta["head"] >= meta["tail"]) return {std::nullopt, std::nullopt};
            return pop_item(q, peek);
        }

        if (peek) return {key, msg};

        DictStorageController::erase(key);
        meta["head"] = (int64_t)meta["head"] + 1;
        save_meta(q, meta);

        try_dispatch(q, "popped", key, msg);
        if (size_from_meta(meta) == 0) try_dispatch(q, "empty", std::nullopt, std::nullopt);
        return {key, msg};
    }

    std::optional<json> pop(const std::string& q="default") { return pop_item(q).second; }
    std::optional<json> peek(const std::string& q="default") { return pop_item(q, true).second; }
    int queue_size(const std::string& q="default") { return size_from_meta(load_meta(q)); }

    void clear(const std::string& q="default") {
        auto ks = keys(std::string(ROOT_KEY) + ":" + qname(q) + ":*");
        for (auto& k : ks) DictStorageController::erase(k);
        DictStorageController::erase(qkey(q));
        try_dispatch(q, "cleared", std::nullopt, std::nullopt);
    }

    std::vector<std::string> list_queues() const {
        std::set<std::string> qs;
        for (auto& k : keys(std::string(ROOT_KEY) + ":*")) {
            auto parts = std::vector<std::string>{};
            std::string tmp = k;
            size_t pos = 0;
            while (true) {
                auto p = tmp.find(':', pos);
                if (p == std::string::npos) { parts.push_back(tmp.substr(pos)); break; }
                parts.push_back(tmp.substr(pos, p - pos));
                pos = p + 1;
            }
            if (parts.size() >= 2 && parts[0] == ROOT_KEY) {
                const std::string& enc = parts[1];
                auto it = b64_cache.find(enc);
                if (it != b64_cache.end()) qs.insert(it->second);
                else qs.insert(enc); // best effort
            }
        }
        return std::vector<std::string>(qs.begin(), qs.end());
    }
};

// ===================== Local Version Controller =====================

struct LocalVersionController {
    static constexpr const char* TABLENAME = "_Operation";
    static constexpr const char* KEY = "ops";
    static constexpr const char* FORWARD = "forward";
    static constexpr const char* REVERT = "revert";

    std::unique_ptr<MemoryLimitedDictStorageController> client;
    double limit_memory_MB;
    std::optional<std::string> current_version;

    explicit LocalVersionController(
        std::unique_ptr<MemoryLimitedDictStorageController> client_ = nullptr,
        double limitMB = 128.0,
        const std::string& eviction_policy = "fifo"
    )
    : limit_memory_MB(limitMB)
    {
        if (!client_) {
            DictStorage model;
            client = std::make_unique<MemoryLimitedDictStorageController>(
                model, limitMB, eviction_policy,
                [this](const std::string& key, const json& /*v*/) { this->on_evict(key); },
                std::set<std::string>{TABLENAME}
            );
        } else {
            client = std::move(client_);
        }
        auto table = client->get(TABLENAME).value_or(json::object());
        if (!table.contains(KEY)) client->set(TABLENAME, json{{KEY, json::array()}});
    }

    void on_evict(const std::string& key) {
        const std::string prefix = std::string(TABLENAME) + ":";
        if (key.rfind(prefix, 0) != 0) return;
        const std::string op_id = key.substr(prefix.size());
        auto ops = get_versions();
        auto it = std::find(ops.begin(), ops.end(), op_id);
        if (it != ops.end()) {
            ops.erase(it);
            set_versions(ops);
        }
        if (current_version && *current_version == op_id) {
            throw std::runtime_error("auto removed current_version");
        }
    }

    std::vector<std::string> get_versions() const {
        auto t = client->get(TABLENAME).value_or(json::object());
        if (!t.contains(KEY)) return {};
        std::vector<std::string> out;
        for (auto& v : t[KEY]) out.push_back(v.get<std::string>());
        return out;
    }
    void set_versions(const std::vector<std::string>& ops) {
        client->set(TABLENAME, json{{KEY, ops}});
    }

    std::tuple<std::vector<std::string>, int, std::optional<int>, std::optional<json>>
    find_version(const std::optional<std::string>& version_uuid) const {
        auto versions = get_versions();
        int current_idx = -1;
        if (current_version) {
            auto it = std::find(versions.begin(), versions.end(), *current_version);
            if (it != versions.end()) current_idx = int(it - versions.begin());
        }
        std::optional<int> target_idx;
        json op;
        if (version_uuid) {
            auto it = std::find(versions.begin(), versions.end(), *version_uuid);
            if (it != versions.end()) {
                target_idx = int(it - versions.begin());
                auto opj = client->get(std::string(TABLENAME) + ":" + *version_uuid);
                if (opj) op = *opj;
            }
        }
        return {versions, current_idx, target_idx, op.is_null() ? std::optional<json>{} : std::optional<json>{op}};
    }

    double estimate_memory_MB() const {
        return double(client->bytes_used(true)) / (1024.0 * 1024.0);
    }

    // operation format: ["set", key, value] etc.
    std::optional<std::string> add_operation(const json& operation, const std::optional<json>& revert = std::nullopt, bool verbose=false) {
        const std::string opuuid = uuid_v4();
        client->set(std::string(TABLENAME) + ":" + opuuid, json{{FORWARD, operation}, {REVERT, revert ? *revert : json()}});
        auto ops = get_versions();
        if (current_version) {
            auto it = std::find(ops.begin(), ops.end(), *current_version);
            if (it != ops.end()) ops.erase(it + 1, ops.end());
        }
        ops.push_back(opuuid);
        set_versions(ops);
        current_version = opuuid;
        if (estimate_memory_MB() > limit_memory_MB) {
            std::ostringstream oss;
            oss << "[LocalVersionController] Warning: memory usage " << std::fixed << std::setprecision(1)
                << estimate_memory_MB() << " MB exceeds limit of " << limit_memory_MB << " MB";
            if (verbose) { /* print if you want */ }
            return oss.str();
        }
        return std::nullopt;
    }

    std::vector<std::pair<std::string, json>> pop_operation(int n=1) {
        if (n <= 0) return {};
        auto ops = get_versions();
        if (ops.empty()) return {};
        std::vector<std::pair<std::string, json>> popped;
        for (int i=0; i<std::min<int>(n, (int)ops.size()); ++i) {
            int pop_idx = (!ops.empty() && (!current_version || ops[0] != *current_version)) ? 0 : (int)ops.size()-1;
            std::string op_id = ops[pop_idx];
            std::string op_key = std::string(TABLENAME) + ":" + op_id;
            auto op_record = client->get(op_key).value_or(json::object());
            popped.emplace_back(op_id, op_record);
            ops.erase(ops.begin() + pop_idx);
            client->erase(op_key);
        }
        set_versions(ops);
        if (!current_version || std::find(ops.begin(), ops.end(), *current_version) == ops.end()) {
            current_version = ops.empty() ? std::optional<std::string>{} : std::optional<std::string>{ops.back()};
        }
        return popped;
    }

    template<class ForwardCB>
    void forward_one_operation(ForwardCB cb) {
        auto [versions, cur_idx, _t, _o] = find_version(current_version);
        int next_idx = cur_idx + 1;
        if (next_idx >= (int)versions.size()) return;
        auto op = client->get(std::string(TABLENAME) + ":" + versions[next_idx]);
        if (!op || !op->contains(FORWARD)) return;
        cb((*op)[FORWARD]);
        current_version = versions[next_idx];
    }

    template<class RevertCB>
    void revert_one_operation(RevertCB cb) {
        auto [versions, cur_idx, _t, op] = find_version(current_version);
        if (cur_idx <= 0 || !op || !op->contains(REVERT)) return;
        cb((*op)[REVERT]);
        current_version = versions[cur_idx - 1];
    }

    template<class VersionCB>
    void to_version(const std::string& version_uuid, VersionCB cb) {
        auto [versions, cur_idx, target_idx, _] = find_version(version_uuid);
        if (!target_idx) throw std::runtime_error("no such version: " + version_uuid);
        if (cur_idx < 0) cur_idx = -1;
        while (cur_idx != *target_idx) {
            if (cur_idx < *target_idx) { forward_one_operation(cb); ++cur_idx; }
            else { revert_one_operation(cb); --cur_idx; }
        }
    }
};

// ===================== SingletonKeyValueStorage =====================
// Assumes: json = nlohmann::json

struct SingletonKeyValueStorage {
    // --- config
    bool version_control = false;

    rjson::SimpleRSAChunkEncryptor* encryptor = nullptr;

    // --- runtime
    std::unique_ptr<AbstractStorageController> conn;
    EventDispatcherController _event_dispa;
    LocalVersionController _verc;
    MessageQueueController message_queue;

    // --- ctor / backend switch (matches Python flow)
    explicit SingletonKeyValueStorage(bool version_control_ = false,
                                      rjson::SimpleRSAChunkEncryptor* enc = nullptr)
        : version_control(version_control_), encryptor(enc),
          _event_dispa(EventDispatcherController{}),
          message_queue(DictStorageController::build_tmp().model)
    {
        switch_backend(std::make_unique<DictStorageController>(
            DictStorageController::build()));
    }

    SingletonKeyValueStorage& switch_backend(std::unique_ptr<AbstractStorageController> controller) {
        _event_dispa   = EventDispatcherController{};
        _verc          = LocalVersionController{};
        message_queue  = MessageQueueController(DictStorageController::build_tmp().model);
        conn           = std::move(controller);
        return *this;
    }

    // --- tiny logger
    void _print(const std::string& msg) const {
        std::cerr << "[" << typeid(*this).name() << "]: " << msg << "\n";
    }

    // --- "slave" helpers (explicit id + callbacks, since no reflection)
    using Callback = EventDispatcherController::Callback;

    bool delete_slave(const std::string& id) {
        return _event_dispa.erase_event(id) > 0;
    }

    // Register multiple callbacks under a single id (default events: "set","erase")
    bool add_slave(const std::string& id,
                   const std::vector<std::pair<std::string, Callback>>& event_map = {}) {
        bool ok = true;
        if (id.empty()) {
            _print("cannot register slave: empty id");
            return false;
        }
        if (event_map.empty()) {
            // nothing to attach => warn but don't fail
            _print("add_slave called with empty event_map — nothing to register");
            return true;
        }
        for (const auto& [name, cb] : event_map) {
            try {
                _event_dispa.set_event(name, cb, id);
            } catch (const std::exception& e) {
                _print(std::string("failed to set_event '") + name + "': " + e.what());
                ok = false;
            } catch (...) {
                _print(std::string("failed to set_event '") + name + "': unknown error");
                ok = false;
            }
        }
        return ok;
    }

    // ---------- Unified editing pipeline (Python-like) ----------

    // local raw edit (no encryption, no events, no VC)
    bool _edit_local(const std::string& func_name,
                     const std::optional<std::string>& key = std::nullopt,
                     const std::optional<json>& value = std::nullopt)
    {
        try {
            if (func_name == "set") {
                if (!key || !value) return false;
                conn->set(*key, *value);
                return true;
            } else if (func_name == "erase") {
                if (!key) return false;
                return conn->erase(*key);
            } else if (func_name == "clean") {
                conn->clean();
                return true;
            } else if (func_name == "load") {
                if (!key) return false;           // here 'key' holds path for parity with Python
                conn->load(*key);
                return true;
            } else if (func_name == "loads") {
                if (!value) return false;         // value can be string or json
                if (value->is_string()) conn->loads(value->get<std::string>());
                else                     conn->loads(value->dump());
                return true;
            } else {
                _print("no func of '" + func_name + "'. return.");
                return false;
            }
        } catch (const std::exception& e) {
            _print(e.what());
            return false;
        } catch (...) {
            _print("unknown error in _edit_local");
            return false;
        }
    }

    // encryption + event dispatch
    bool _edit(const std::string& func_name,
               const std::optional<std::string>& key = std::nullopt,
               std::optional<json> value = std::nullopt)
    {
        // wrap only for "set"
        std::optional<json> to_store = value;
        if (encryptor && func_name == "set" && value) {
            to_store = json{{"rjson", encryptor->encrypt_string(value->dump())}};
        }

        bool ok = _edit_local(func_name, key, to_store);

        // event payload mirrors Python spirit: pass the raw args (unencrypted)
        try {
            json payload = json::object();
            if (key)   payload["key"] = *key;
            if (value) payload["value"] = *value;
            _event_dispa.dispatch_event(func_name, payload);
        } catch (...) {
            // don't fail the operation due to event dispatch errors
        }

        return ok;
    }

    // VC wrapper + try/catch (main entry)
    bool _try_edit_error(const std::string& func_name,
                         const std::optional<std::string>& key = std::nullopt,
                         const std::optional<json>& value = std::nullopt)
    {
        // --- local version control bookkeeping, store ops as arrays like ["set", key, value]
        auto to_args = [&](const std::string& f,
                           const std::optional<std::string>& k,
                           const std::optional<json>& v) -> json {
            json a = json::array();
            a.push_back(f);
            if (k) a.push_back(*k);
            if (v) a.push_back(*v);
            return a;
        };
        auto current_snapshot_revert = [&]() -> json {
            return json::array({"loads", dumps()});
        };

        if (version_control) {
            if (func_name == "set") {
                if (!key) return false;
                json revert;
                if (exists(*key)) {
                    revert = json::array({"set", *key, get(*key).value_or(json())});
                } else {
                    revert = json::array({"erase", *key});
                }
                _verc.add_operation(to_args("set", key, value), revert);

            } else if (func_name == "erase") {
                if (!key) return false;
                json revert = json::array({"set", *key, get(*key).value_or(json())});
                _verc.add_operation(to_args("erase", key, std::nullopt), revert);

            } else if (func_name == "clean" || func_name == "load" || func_name == "loads") {
                json revert = current_snapshot_revert();
                _verc.add_operation(to_args(func_name, key, value), revert);
            }
        }

        try {
            return _edit(func_name, key, value);
        } catch (const std::exception& e) {
            _print(e.what());
            return false;
        } catch (...) {
            _print("unknown error in _try_edit_error");
            return false;
        }
    }

    // ---------- Public API (Python method names / behavior) ----------

    // True/False (on error)
    bool set(const std::string& key, const json& value)  { return _try_edit_error("set",   key, value); }
    bool erase(const std::string& key)                   { return _try_edit_error("erase", key, std::nullopt); }
    bool del_(const std::string& key)                    { return erase(key); } // convenience alias

    bool clean()                                         { return _try_edit_error("clean"); }

    bool load(const std::string& json_path)              { return _try_edit_error("load",  json_path, std::nullopt); }
    bool loads(const std::string& json_str)              { return _try_edit_error("loads", std::nullopt, json_str); }

    // Version navigation
    void revert_one_operation() {
        _verc.revert_one_operation([this](const json& rev){
            // rev like ["set", key, value] or ["erase", key] or ["loads", json_string]...
            _apply_array_to_local(rev);
        });
    }
    void forward_one_operation() {
        _verc.forward_one_operation([this](const json& fwd){
            _apply_array_to_local(fwd);
        });
    }
    std::optional<std::string> get_current_version() const { return _verc.current_version; }
    void local_to_version(const std::string& opuuid) {
        _verc.to_version(opuuid, [this](const json& op){
            _apply_array_to_local(op);
        });
    }

    // ---------- Safe load helpers (like _try_load_error) ----------

    template<typename F, typename R>
    R _try_load_error(F&& f, R fallback) const noexcept {
        try { return f(); }
        catch (const std::exception& e) { const_cast<SingletonKeyValueStorage*>(this)->_print(e.what()); return fallback; }
        catch (...) { const_cast<SingletonKeyValueStorage*>(this)->_print("unknown error in _try_load_error"); return fallback; }
    }

    // Object / None(in error)
    bool exists(const std::string& key) const {
        return _try_load_error([&]{ return conn->exists(key); }, false);
    }

    std::vector<std::string> keys(const std::string& pattern="*") const {
        return _try_load_error([&]{ return conn->keys(pattern); }, std::vector<std::string>{});
    }

    std::optional<json> get(const std::string& key) const {
        auto val = _try_load_error([&]{ return conn->get(key); }, std::optional<json>{});
        if (val && encryptor && val->is_object() && val->contains("rjson")) {
            return _try_load_error([&]{
                auto dec = encryptor->decrypt_string((*val)["rjson"].get<std::string>());
                return std::optional<json>(json::parse(dec));
            }, std::optional<json>{});
        }
        return val;
    }

    std::string dumps() const {
        return _try_load_error([&]{
            json root = json::object();
            for (const auto& k : keys("*")) {
                if (auto v = get(k)) root[k] = *v;
            }
            return root.dump();
        }, std::string{"{}"});
    }

    // ---------- Events facade (names + surface mirrored to Python) ----------
    auto events()                                    { return _event_dispa.events(); }
    auto get_event(const std::string& id)            { return _event_dispa.get_event(id); }
    int  erase_event(const std::string& id)         { return _event_dispa.erase_event(id); }
    std::string set_event(const std::string& name, Callback cb, const std::optional<std::string>& id = std::nullopt) {
        return _event_dispa.set_event(name, std::move(cb), id);
    }
    void dispatch_event(const std::string& name, const json& payload = json::object()) {
        _event_dispa.dispatch_event(name, payload);
    }
    void clean_events()                              { _event_dispa = EventDispatcherController{}; }

private:
    // apply ["set", key, value] etc. *locally* (no encryption, no events)
    void _apply_array_to_local(const json& arr) {
        if (!arr.is_array() || arr.empty()) return;
        const std::string f = arr[0].get<std::string>();
        if (f == "set") {
            if (arr.size() < 3) return;
            _edit_local("set",   arr[1].get<std::string>(), arr[2]);
        } else if (f == "erase") {
            if (arr.size() < 2) return;
            _edit_local("erase", arr[1].get<std::string>());
        } else if (f == "clean") {
            _edit_local("clean");
        } else if (f == "load") {
            if (arr.size() < 2) return;
            _edit_local("load",  arr[1].get<std::string>());
        } else if (f == "loads") {
            if (arr.size() < 2) return;
            if (arr[1].is_string())
                _edit_local("loads", std::nullopt, arr[1].get<std::string>());
            else
                _edit_local("loads", std::nullopt, arr[1]);
        }
    }
};


// ===================== Python-Tests port =====================
// Requires: nlohmann::json and singleton_kv_storage.hpp

struct Tests {
    std::unique_ptr<SingletonKeyValueStorage> store;

    int failures = 0;
    int assertions = 0;

    Tests() {
        store = std::make_unique<SingletonKeyValueStorage>(false /*version_control*/);
    }

    // ---------- tiny assert helpers ----------
    void fail(const std::string& msg) {
        ++failures;
        std::cout << "[FAIL] " << msg << "\n";
    }
    void pass(const std::string& msg) {
        (void)msg; // keep output clean; uncomment if you want per-assert logs
        // std::cout << "[OK] " << msg << "\n";
    }
    void assert_true(bool cond, const std::string& msg) {
        ++assertions; cond ? pass(msg) : fail(msg);
    }
    void assert_false(bool cond, const std::string& msg) {
        ++assertions; (!cond) ? pass(msg) : fail(msg);
    }
    void assert_eq_str(const std::string& a, const std::string& b, const std::string& msg) {
        ++assertions; (a == b) ? pass(msg) : fail(msg + "  (got: \"" + a + "\", expect: \"" + b + "\")");
    }
    void assert_eq_json(const nlohmann::json& a, const nlohmann::json& b, const std::string& msg) {
        ++assertions; (a == b) ? pass(msg) : fail(msg + "  (got: " + a.dump() + ", expect: " + b.dump() + ")");
    }
    void assert_opt_json_eq(const std::optional<nlohmann::json>& a, const nlohmann::json& b, const std::string& msg) {
        ++assertions; (a && *a == b) ? pass(msg) : fail(msg + "  (got: " + (a? a->dump() : "null") + ", expect: " + b.dump() + ")");
    }
    void assert_is_none(const std::optional<nlohmann::json>& a, const std::string& msg) {
        ++assertions; (!a.has_value()) ? pass(msg) : fail(msg + "  (got: " + a->dump() + ", expect: null)");
    }

    // ---------- helpers ----------
    static bool dump_to_file(SingletonKeyValueStorage& s, const std::string& path) {
        std::ofstream ofs(path);
        ofs << s.dumps();
        return ofs.good();
    }

    // ---------- tests ----------
    void test_all(int num=1) {
        test_dict(num);
    }

    void test_dict(int num=1) {
        std::cout << "###### test_dict ######\n";
        // DictStorage.build() -> singleton backing
        store->switch_backend(std::make_unique<DictStorageController>(
            DictStorageController::build()
        ));

        test_msg();
        for (int i=0; i<num; ++i) test_all_cases();
    }

    void test_msg() {
        std::cout << "start : self.test_msg()\n";

        // FIFO & size
        store->message_queue.push(json{{"n",1}});
        store->message_queue.push(json{{"n",2}});
        store->message_queue.push(json{{"n",3}});

        assert_eq_json(store->message_queue.queue_size() , 3, "Size should reflect number of enqueued items.");
        assert_opt_json_eq(store->message_queue.pop(), json{{"n",1}}, "Queue must be FIFO: first pop returns first pushed.");
        assert_opt_json_eq(store->message_queue.pop(), json{{"n",2}}, "Second pop should return second item.");
        assert_opt_json_eq(store->message_queue.pop(), json{{"n",3}}, "Third pop should return third item.");
        assert_is_none(store->message_queue.pop(), "Popping an empty queue should return None.");
        assert_eq_json(store->message_queue.queue_size(), 0, "Size should be zero after popping all items.");

        // Peek
        store->message_queue.push(json{{"a",1}});
        assert_opt_json_eq(store->message_queue.peek(), json{{"a",1}}, "Peek should return earliest message without removing it.");
        assert_eq_json(store->message_queue.queue_size(), 1, "Peek should not change the queue size.");
        assert_opt_json_eq(store->message_queue.pop(), json{{"a",1}}, "Pop should still return the same earliest message after peek.");

        // Clear
        store->message_queue.push(json{{"x",1}});
        store->message_queue.push(json{{"y",2}});
        store->message_queue.clear();
        assert_eq_json(store->message_queue.queue_size(), 0, "Clear should remove all items from the queue.");
        assert_is_none(store->message_queue.pop(), "After clear, popping should return None.");

        // Capture normal event flow (we'll just ensure callbacks are invoked)
        std::vector<json> events;
        auto capture = [&events](const json& payload){ events.push_back(payload); };
        store->message_queue.add_listener("default", capture, "pushed");
        store->message_queue.add_listener("default", capture, "popped");
        store->message_queue.add_listener("default", capture, "empty");
        store->message_queue.add_listener("default", capture, "cleared");
        store->message_queue.push(json{{"m",1}});
        store->message_queue.push(json{{"m",2}});
        auto a = store->message_queue.pop();
        auto b = store->message_queue.pop();
        store->message_queue.clear();
        // (Python had specific order asserts commented out; we mirror that.)

        // Listener failure should not break queue ops (isolated queue)
        std::string queue = std::string("t_listener_fail_") + uuid_v4().substr(0,8);
        auto bad = [](const json&) { throw std::runtime_error("boom"); };
        store->message_queue.add_listener(queue, bad, "pushed");

        store->message_queue.push(json{{"ok", true}}, queue);
        assert_eq_json(store->message_queue.queue_size(queue), 1, "ops should succeed even if a listener fails.");
        assert_opt_json_eq(store->message_queue.pop(queue), json{{"ok", true}}, "pop returns pushed message (listener threw).");

        // Multiple queues are isolated
        store->message_queue.push(json{{"a",1}}, "q1");
        store->message_queue.push(json{{"b",2}}, "q2");
        assert_eq_json(store->message_queue.queue_size("q1"), 1, "q1 should have one item.");
        assert_eq_json(store->message_queue.queue_size("q2"), 1, "q2 should have one item.");
        assert_opt_json_eq(store->message_queue.pop("q1"), json{{"a",1}}, "Popping q1 should return its own item.");
        assert_eq_json(store->message_queue.queue_size("q2"), 1, "Popping q1 should not affect q2.");
    }

    void test_all_cases() {
        std::cout << "start : self.test_set_and_get()\n";      test_set_and_get();
        std::cout << "start : self.test_exists()\n";           test_exists();
        std::cout << "start : self.test_erase()\n";           test_erase();
        std::cout << "start : self.test_keys()\n";             test_keys();
        std::cout << "start : self.test_get_nonexistent()\n";  test_get_nonexistent();
        std::cout << "start : self.test_dump_and_load()\n";    test_dump_and_load();
        std::cout << "start : self.test_version()\n";          test_version();
        std::cout << "start : self.test_slaves()\n";           test_slaves();
        std::cout << "start : self.store.clean()\n";           store->clean();
        std::cout << "end all.\n";
    }

    void test_set_and_get() {
        store->set("test1", nlohmann::json{{"data",123}});
        assert_opt_json_eq(store->get("test1"), nlohmann::json{{"data",123}}, "The retrieved value should match the set value.");
    }

    void test_exists() {
        store->set("test2", nlohmann::json{{"data",456}});
        assert_true(store->exists("test2"), "Key should exist after being set.");
    }

    void test_erase() {
        store->set("test3", nlohmann::json{{"data",789}});
        store->erase("test3");
        assert_false(store->exists("test3"), "Key should not exist after being erased.");
    }

    void test_keys() {
        store->set("alpha", nlohmann::json{{"info","first"}});
        store->set("abeta", nlohmann::json{{"info","second"}});
        store->set("gamma", nlohmann::json{{"info","third"}});
        std::vector<std::string> ks = store->keys("a*");
        std::sort(ks.begin(), ks.end());
        std::vector<std::string> expected = {"abeta","alpha"};
        assert_true(ks == expected, "Should return the correct keys matching the pattern.");
    }

    void test_get_nonexistent() {
        auto v = store->get("nonexistent");
        assert_is_none(v, "Getting a non-existent key should return None.");
    }

    void test_dump_and_load() {
        json raw = {
            {"test1", {{"data",123}}},
            {"test2", {{"data",456}}},
            {"alpha", {{"info","first"}}},
            {"abeta", {{"info","second"}}},
            {"gamma", {{"info","third"}}}
        };
        // dump to file
        assert_true(dump_to_file(*store, "test.json"), "dump file created");
        store->clean();
        assert_eq_str(store->dumps(), "{}", "Should return {} after clean.");
        // load from file
        store->load("test.json");
        assert_eq_json(nlohmann::json::parse(store->dumps()), raw, "Should return the correct keys and values (file load).");
        // loads from string
        store->clean();
        store->loads(raw.dump());
        assert_eq_json(nlohmann::json::parse(store->dumps()), raw, "Should return the correct keys and values (loads).");
    }

    void test_slaves() {
        // Set up a follower store with a temporary backend
        auto store2 = std::make_shared<SingletonKeyValueStorage>(false);
        store2->switch_backend(std::make_unique<DictStorageController>(
            DictStorageController::build_tmp()
        ));
        // Wire replication via events (equivalent to Python add_slave)
        store->set_event("set", [store2](const nlohmann::json& p){
            auto key = p.value("key", std::string{});
            if (!key.empty() && p.contains("value")) store2->set(key, p["value"]);
        });
        store->set_event("erase", [store2](const nlohmann::json& p){
            auto key = p.value("key", std::string{});
            if (!key.empty()) store2->erase(key);
        });

        store->set("alpha", nlohmann::json{{"info","first"}});
        store->set("abeta", nlohmann::json{{"info","second"}});
        store->set("gamma", nlohmann::json{{"info","third"}});
        store->erase("abeta");

        auto a = nlohmann::json::parse(store->dumps());
        auto b = nlohmann::json::parse(store2->dumps());
        assert_eq_json(a, b, "Should return the correct keys and values (slave replication).");
    }

    void test_version() {

        store->clean();
        store->version_control = true;

        store->set("alpha", json{{"info","first"}});
        std::string data1 = store->dumps();
        auto v1 = store->get_current_version();

        store->set("abeta", json{{"info","second"}});
        auto v2 = store->get_current_version();
        std::string data2 = store->dumps();

        store->set("gamma", json{{"info","third"}});
        if (v1) store->local_to_version(*v1);
        assert_eq_json(json::parse(store->dumps()), json::parse(data1), "Should return the same keys and values (to v1).");

        if (v2) store->local_to_version(*v2);
        assert_eq_json(json::parse(store->dumps()), json::parse(data2), "Should return the same keys and values (to v2).");

        // memory limit warning scenario
        auto make_big_payload = [](int size_kb)->std::string {
            return std::string(1024 * size_kb, 'X');
        };
        store->_verc.limit_memory_MB = 0.2; // 0.2 MB
        auto& lvc2 = store->_verc;

        for (int i=0;i<3;++i) {
            std::string small_payload = make_big_payload(62); // ~0.062 MB
            auto res = lvc2.add_operation(json::array({"write", "small_" + std::to_string(i), small_payload}),
                                          json::array({"erase","small_" + std::to_string(i)}));
            assert_true(!res.has_value(), "Should not return any warning message for small payloads.");
        }

        std::string big_payload = make_big_payload(600); // ~0.6 MB
        auto res = lvc2.add_operation(json::array({"write", "too_big", big_payload}),
                                      json::array({"erase", "too_big"}));
        std::string expect_prefix = "[LocalVersionController] Warning: memory usage";
        assert_true(res.has_value() && res->rfind(expect_prefix, 0) == 0,
                    "Should return warning message about memory usage.");
    }
};

// Run all tests; returns number of failures (0 means all good)
int run_ported_python_tests(int num=1) {
    Tests t;
    t.test_all(num);
    std::cout << "\n==== TEST SUMMARY ====\n";
    std::cout << "Assertions: " << t.assertions << "\n";
    std::cout << "Failures:   " << t.failures << "\n";
    std::cout << "======================\n";
    return t.failures;
}
