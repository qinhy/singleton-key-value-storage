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

inline std::string uuid_v4() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist;
    uint64_t a = dist(rng), b = dist(rng);
    // Set version and variant bits
    a = (a & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL; // version 4
    b = (b & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL; // variant 1
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8)  << ((a >> 32) & 0xFFFFFFFFULL) << "-"
        << std::setw(4)  << ((a >> 16) & 0xFFFFULL)     << "-"
        << std::setw(4)  << ((a >>  0) & 0xFFFFULL)     << "-"
        << std::setw(4)  << ((b >> 48) & 0xFFFFULL)     << "-"
        << std::setw(12) << ( b        & 0x0000FFFFFFFFFFFFULL);
    return oss.str();
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
    virtual void load_file(const std::string& path) {
        std::ifstream ifs(path);
        std::stringstream buf; buf << ifs.rdbuf();
        loads(buf.str());
    }

    // ---- Optional RSA wrappers: plug your own encryptor if needed
    struct SimpleRSAChunkEncryptor {
        virtual ~SimpleRSAChunkEncryptor() = default;
        virtual std::string encrypt_string(const std::string& s) = 0;
        virtual std::string decrypt_string(const std::string& s) = 0;
    };

    virtual void dump_RSA(const std::string& path, SimpleRSAChunkEncryptor& enc) const {
        std::ofstream ofs(path);
        ofs << enc.encrypt_string(dumps());
    }
    virtual void load_RSA(const std::string& path, SimpleRSAChunkEncryptor& enc) {
        std::ifstream ifs(path);
        std::stringstream buf; buf << ifs.rdbuf();
        loads(enc.decrypt_string(buf.str()));
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

struct PythonMemoryLimitedDictStorageController : public DictStorageController {
    enum class Policy { LRU, FIFO };

    size_t max_bytes;
    Policy policy;
    std::function<void(const std::string&, const json&)> on_evict;
    std::set<std::string> pinned;

    std::unordered_map<std::string, size_t> sizes;
    std::list<std::string> order; // front = oldest
    std::unordered_map<std::string, std::list<std::string>::iterator> where;
    size_t current_bytes = 0;

    PythonMemoryLimitedDictStorageController(
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
            auto self = const_cast<PythonMemoryLimitedDictStorageController*>(this);
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

    int delete_event(const std::string& id) {
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

struct MessageQueueController : public PythonMemoryLimitedDictStorageController {
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
    : PythonMemoryLimitedDictStorageController(model, max_memory_mb, pol, onEvict, std::move(pinnedKeys)),
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
        return dispatcher.delete_event(listener_id);
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

    std::unique_ptr<PythonMemoryLimitedDictStorageController> client;
    double limit_memory_MB;
    std::optional<std::string> current_version;

    explicit LocalVersionController(
        std::unique_ptr<PythonMemoryLimitedDictStorageController> client_ = nullptr,
        double limitMB = 128.0,
        const std::string& eviction_policy = "fifo"
    )
    : limit_memory_MB(limitMB)
    {
        if (!client_) {
            DictStorage model;
            client = std::make_unique<PythonMemoryLimitedDictStorageController>(
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

struct SingletonKeyValueStorage {
    bool version_control = false;

    // Optional encryptor for {"rjson": "..."} wrapping
    struct Encryptor {
        virtual ~Encryptor() = default;
        virtual std::string encrypt_string(const std::string&) = 0;
        virtual std::string decrypt_string(const std::string&) = 0;
    };
    Encryptor* encryptor = nullptr;

    // active backend
    std::unique_ptr<AbstractStorageController> conn;
    EventDispatcherController event_disp;
    LocalVersionController verc;
    MessageQueueController mq;

    SingletonKeyValueStorage(bool version_control_=false, Encryptor* enc=nullptr)
    : version_control(version_control_), encryptor(enc),
      conn(std::make_unique<DictStorageController>(DictStorageController::build())),
      event_disp(),
      verc(),
      mq(DictStorageController::build_tmp().model)
    {}

    SingletonKeyValueStorage& switch_backend(std::unique_ptr<AbstractStorageController> controller) {
        event_disp = EventDispatcherController{};
        verc = LocalVersionController{};
        mq = MessageQueueController(DictStorageController::build_tmp().model);
        conn = std::move(controller);
        return *this;
    }

    // --- helpers
    bool exists(const std::string& key) {
        try { return conn->exists(key); } catch (...) { return false; }
    }
    std::vector<std::string> keys(const std::string& pattern="*") {
        try { return conn->keys(pattern); } catch (...) { return {}; }
    }

    std::optional<json> get(const std::string& key) {
        try {
            auto v = conn->get(key);
            if (!v) return std::nullopt;
            if (encryptor && v->is_object() && v->contains("rjson")) {
                auto s = (*v)["rjson"].get<std::string>();
                return json::parse(encryptor->decrypt_string(s));
            }
            return v;
        } catch (...) {
            return std::nullopt;
        }
    }

    std::string dumps() {
        try {
            json root = json::object();
            for (auto& k : keys("*")) {
                auto v = get(k);
                if (v) root[k] = *v;
            }
            return root.dump();
        } catch (...) { return "{}"; }
    }

    bool set(const std::string& key, json value) {
        auto args = json::array({"set", key, value});
        if (version_control) {
            json revert;
            if (exists(key)) {
                revert = json::array({"set", key, get(key).value_or(json())});
            } else {
                revert = json::array({"delete", key});
            }
            verc.add_operation(args, revert);
        }
        try {
            if (encryptor) {
                json wrapped = json{{"rjson", encryptor->encrypt_string(value.dump())}};
                conn->set(key, wrapped);
            } else {
                conn->set(key, value);
            }
            event_disp.dispatch_event("set", json{{"key", key}, {"value", value}});
            return true;
        } catch (const std::exception& e) { (void)e; return false; }
    }

    bool erase(const std::string& key) {
        auto args = json::array({"delete", key});
        if (version_control) {
            json revert = json::array({"set", key, get(key).value_or(json())});
            verc.add_operation(args, revert);
        }
        try {
            bool ok = conn->erase(key);
            event_disp.dispatch_event("delete", json{{"key", key}});
            return ok;
        } catch (...) { return false; }
    }

    bool clean() {
        auto args = json::array({"clean"});
        if (version_control) {
            json revert = json::array({"loads", dumps()});
            verc.add_operation(args, revert);
        }
        try {
            conn->clean();
            event_disp.dispatch_event("clean");
            return true;
        } catch (...) { return false; }
    }

    bool load_file(const std::string& path) {
        auto args = json::array({"load", path});
        if (version_control) {
            json revert = json::array({"loads", dumps()});
            verc.add_operation(args, revert);
        }
        try { conn->load_file(path); event_disp.dispatch_event("load"); return true; }
        catch (...) { return false; }
    }
    bool loads(const std::string& s) {
        auto args = json::array({"loads", s});
        if (version_control) {
            json revert = json::array({"loads", dumps()});
            verc.add_operation(args, revert);
        }
        try { conn->loads(s); event_disp.dispatch_event("loads"); return true; }
        catch (...) { return false; }
    }

    // Version navigation
    void revert_one_operation() {
        verc.revert_one_operation([this](const json& rev){
            // rev like ["set", key, value] or ["delete", key] or ["loads", json_string]
            if (!rev.is_array() || rev.empty()) return;
            std::string f = rev[0].get<std::string>();
            if (f == "set")      this->conn->set(rev[1].get<std::string>(), rev[2]);
            else if (f == "delete") this->conn->erase(rev[1].get<std::string>());
            else if (f == "clean")  this->conn->clean();
            else if (f == "load")   this->conn->load_file(rev[1].get<std::string>());
            else if (f == "loads")  this->conn->loads(rev[1].is_string()? rev[1].get<std::string>() : rev[1].dump());
        });
    }
    void forward_one_operation() {
        verc.forward_one_operation([this](const json& fwd){
            if (!fwd.is_array() || fwd.empty()) return;
            std::string f = fwd[0].get<std::string>();
            if (f == "set")      this->conn->set(fwd[1].get<std::string>(), fwd[2]);
            else if (f == "delete") this->conn->erase(fwd[1].get<std::string>());
            else if (f == "clean")  this->conn->clean();
            else if (f == "load")   this->conn->load_file(fwd[1].get<std::string>());
            else if (f == "loads")  this->conn->loads(fwd[1].is_string()? fwd[1].get<std::string>() : fwd[1].dump());
        });
    }

    std::optional<std::string> current_version() const { return verc.current_version; }
    void local_to_version(const std::string& opuuid) {
        verc.to_version(opuuid, [this](const json& op){
            if (!op.is_array() || op.empty()) return;
            std::string f = op[0].get<std::string>();
            if (f == "set")      this->conn->set(op[1].get<std::string>(), op[2]);
            else if (f == "delete") this->conn->erase(op[1].get<std::string>());
            else if (f == "clean")  this->conn->clean();
            else if (f == "load")   this->conn->load_file(op[1].get<std::string>());
            else if (f == "loads")  this->conn->loads(op[1].is_string()? op[1].get<std::string>() : op[1].dump());
        });
    }

    // Events facade
    std::vector<std::pair<std::string, EventDispatcherController::Callback>> events() { return event_disp.events(); }
    std::vector<EventDispatcherController::Callback> get_event(const std::string& id) { return event_disp.get_event(id); }
    int delete_event(const std::string& id) { return event_disp.delete_event(id); }
    std::string set_event(const std::string& name, EventDispatcherController::Callback cb, const std::optional<std::string>& id=std::nullopt) {
        return event_disp.set_event(name, std::move(cb), id);
    }
    void dispatch_event(const std::string& name, const json& payload=json::object()) { event_disp.dispatch_event(name, payload); }
    void clean_events() { /* not stored -> nothing to wipe aside from replacing controller */ event_disp = EventDispatcherController{}; }
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
        store->mq.push(json{{"n",1}});
        store->mq.push(json{{"n",2}});
        store->mq.push(json{{"n",3}});

        assert_eq_json(store->mq.queue_size() , 3, "Size should reflect number of enqueued items.");
        assert_opt_json_eq(store->mq.pop(), json{{"n",1}}, "Queue must be FIFO: first pop returns first pushed.");
        assert_opt_json_eq(store->mq.pop(), json{{"n",2}}, "Second pop should return second item.");
        assert_opt_json_eq(store->mq.pop(), json{{"n",3}}, "Third pop should return third item.");
        assert_is_none(store->mq.pop(), "Popping an empty queue should return None.");
        assert_eq_json(store->mq.queue_size(), 0, "Size should be zero after popping all items.");

        // Peek
        store->mq.push(json{{"a",1}});
        assert_opt_json_eq(store->mq.peek(), json{{"a",1}}, "Peek should return earliest message without removing it.");
        assert_eq_json(store->mq.queue_size(), 1, "Peek should not change the queue size.");
        assert_opt_json_eq(store->mq.pop(), json{{"a",1}}, "Pop should still return the same earliest message after peek.");

        // Clear
        store->mq.push(json{{"x",1}});
        store->mq.push(json{{"y",2}});
        store->mq.clear();
        assert_eq_json(store->mq.queue_size(), 0, "Clear should remove all items from the queue.");
        assert_is_none(store->mq.pop(), "After clear, popping should return None.");

        // Capture normal event flow (we'll just ensure callbacks are invoked)
        std::vector<json> events;
        auto capture = [&events](const json& payload){ events.push_back(payload); };
        store->mq.add_listener("default", capture, "pushed");
        store->mq.add_listener("default", capture, "popped");
        store->mq.add_listener("default", capture, "empty");
        store->mq.add_listener("default", capture, "cleared");
        store->mq.push(json{{"m",1}});
        store->mq.push(json{{"m",2}});
        auto a = store->mq.pop();
        auto b = store->mq.pop();
        store->mq.clear();
        // (Python had specific order asserts commented out; we mirror that.)

        // Listener failure should not break queue ops (isolated queue)
        std::string queue = std::string("t_listener_fail_") + uuid_v4().substr(0,8);
        auto bad = [](const json&) { throw std::runtime_error("boom"); };
        store->mq.add_listener(queue, bad, "pushed");

        store->mq.push(json{{"ok", true}}, queue);
        assert_eq_json(store->mq.queue_size(queue), 1, "ops should succeed even if a listener fails.");
        assert_opt_json_eq(store->mq.pop(queue), json{{"ok", true}}, "pop returns pushed message (listener threw).");

        // Multiple queues are isolated
        store->mq.push(json{{"a",1}}, "q1");
        store->mq.push(json{{"b",2}}, "q2");
        assert_eq_json(store->mq.queue_size("q1"), 1, "q1 should have one item.");
        assert_eq_json(store->mq.queue_size("q2"), 1, "q2 should have one item.");
        assert_opt_json_eq(store->mq.pop("q1"), json{{"a",1}}, "Popping q1 should return its own item.");
        assert_eq_json(store->mq.queue_size("q2"), 1, "Popping q1 should not affect q2.");
    }

    void test_all_cases() {
        std::cout << "start : self.test_set_and_get()\n";      test_set_and_get();
        std::cout << "start : self.test_exists()\n";           test_exists();
        std::cout << "start : self.test_delete()\n";           test_delete();
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

    void test_delete() {
        store->set("test3", nlohmann::json{{"data",789}});
        store->erase("test3");
        assert_false(store->exists("test3"), "Key should not exist after being deleted.");
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
        store->load_file("test.json");
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
        store->set_event("delete", [store2](const nlohmann::json& p){
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
        auto v1 = store->current_version();

        store->set("abeta", json{{"info","second"}});
        auto v2 = store->current_version();
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
        store->verc.limit_memory_MB = 0.2; // 0.2 MB
        auto& lvc2 = store->verc;

        for (int i=0;i<3;++i) {
            std::string small_payload = make_big_payload(62); // ~0.062 MB
            auto res = lvc2.add_operation(json::array({"write", "small_" + std::to_string(i), small_payload}),
                                          json::array({"delete","small_" + std::to_string(i)}));
            assert_true(!res.has_value(), "Should not return any warning message for small payloads.");
        }

        std::string big_payload = make_big_payload(600); // ~0.6 MB
        auto res = lvc2.add_operation(json::array({"write", "too_big", big_payload}),
                                      json::array({"delete", "too_big"}));
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
