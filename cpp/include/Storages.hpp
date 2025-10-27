
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
#include <utils.hpp>

using json = nlohmann::json;
using String = std::string;
namespace fs = std::filesystem;
// ===================== Utilities =====================

inline String uuid_v4() {
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
inline const String& b64_table() {
    static const String tbl =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    return tbl;
}
inline String base64_encode(const String& in) {
    String out;
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
inline String base64_decode(const String& in) {
    std::vector<int> T(256, -1);
    for (int i = 0; i < 64; i++) T[b64_table()[i]] = i;
    String out;
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
inline String b64url_encode(const String& s) {
    String b64 = base64_encode(s);
    // url-safe
    for (char& c : b64) {
        if (c == '+') c = '-';
        else if (c == '/') c = '_';
    }
    // strip padding
    while (!b64.empty() && b64.back() == '=') b64.pop_back();
    return b64;
}
inline String b64url_decode(const String& s) {
    String x = s;
    for (char& c : x) {
        if (c == '-') c = '+';
        else if (c == '_') c = '/';
    }
    while (x.size() % 4) x.push_back('=');
    return base64_decode(x);
}
inline bool is_b64url(const String& s) {
    try {
        return b64url_encode(b64url_decode(s)) == s;
    } catch (...) {
        return false;
    }
}

inline size_t deep_size_of_json(const json& j);

inline size_t deep_size_of_string(const String& s) {
    return sizeof(String) + s.size();
}
inline size_t deep_size_of_json(const json& j) {
    switch (j.type()) {
        case json::value_t::null:    return 0;
        case json::value_t::boolean: return sizeof(bool);
        case json::value_t::number_integer: 
        case json::value_t::number_unsigned:
        case json::value_t::number_float: return sizeof(double);
        case json::value_t::string:  return deep_size_of_string(j.get_ref<const String&>());
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
inline String humanize_bytes(size_t n) {
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
inline bool wildcard_match(const String& pattern, const String& s) {
    return wildcard_match(pattern.c_str(), s.c_str());
}

// ===================== Abstract Storage =====================

struct AbstractStorage {
    String uuid = uuid_v4();
    bool is_singleton = false;
    virtual ~AbstractStorage() = default;
    virtual size_t bytes_used(bool deep=true) const = 0; // approx
};

// CppDictStorage: map<string, json>
struct CppDictStorage : public AbstractStorage {
    using Store = std::unordered_map<String, json>;
    std::shared_ptr<Store> store;

    // shared singleton backing
    static std::shared_ptr<Store>& singleton_store() {
        static std::shared_ptr<Store> s = std::make_shared<Store>();
        return s;
    }

    CppDictStorage(std::shared_ptr<Store> st = nullptr, bool singleton=false) {
        store = st ? st : std::make_shared<Store>();
        is_singleton = singleton;
    }

    CppDictStorage get_singleton() const {
        CppDictStorage s(CppDictStorage::singleton_store(), true);
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
    virtual bool exists(const String& key) const = 0;
    virtual void set(const String& key, const json& value) = 0;
    virtual std::optional<json> get(const String& key) const = 0;
    virtual bool erase(const String& key) = 0;

    virtual std::vector<String> keys(const String& pattern="*") const = 0;

    virtual void clean() {
        auto ks = keys("*");
        for (auto& k : ks) erase(k);
    }

    virtual String dumps() const {
        json root = json::object();
        for (auto& k : keys("*")) {
            auto v = get(k);
            if (v) root[k] = *v;
        }
        return root.dump();
    }
    virtual void loads(const String& s) {
        json root = json::parse(s);
        for (auto it = root.begin(); it != root.end(); ++it) set(it.key(), it.value());
    }
    virtual void dump_file(const String& path) const {
        std::ofstream ofs(path);
        ofs << dumps();
    }
    virtual void load_file(const String& path) {
        std::ifstream ifs(path);
        Stringstream buf; buf << ifs.rdbuf();
        loads(buf.str());
    }

    // ---- Optional RSA wrappers: plug your own encryptor if needed
    struct SimpleRSAChunkEncryptor {
        virtual ~SimpleRSAChunkEncryptor() = default;
        virtual String encrypt_string(const String& s) = 0;
        virtual String decrypt_string(const String& s) = 0;
    };

    virtual void dump_RSA(const String& path, SimpleRSAChunkEncryptor& enc) const {
        std::ofstream ofs(path);
        ofs << enc.encrypt_string(dumps());
    }
    virtual void load_RSA(const String& path, SimpleRSAChunkEncryptor& enc) {
        std::ifstream ifs(path);
        Stringstream buf; buf << ifs.rdbuf();
        loads(enc.decrypt_string(buf.str()));
    }

    // Approximate memory
    virtual size_t bytes_used(bool deep=true) const = 0;
};

struct CppDictStorageController : public AbstractStorageController {
    CppDictStorage model;

    explicit CppDictStorageController(const CppDictStorage& m) : model(m) {}

    bool is_singleton() const override { return model.is_singleton; }

    bool exists(const String& key) const override {
        return model.store->find(key) != model.store->end();
    }

    void set(const String& key, const json& value) override {
        (*model.store)[key] = value;
    }

    std::optional<json> get(const String& key) const override {
        auto it = model.store->find(key);
        if (it == model.store->end()) return std::nullopt;
        return it->second;
    }

    bool erase(const String& key) override {
        auto it = model.store->find(key);
        if (it == model.store->end()) return false;
        model.store->erase(it);
        return true;
    }

    std::vector<String> keys(const String& pattern="*") const override {
        std::vector<String> out;
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
    static CppDictStorageController build_tmp() {
        return CppDictStorageController(CppDictStorage{});
    }
    static CppDictStorageController build() {
        CppDictStorage tmp;
        return CppDictStorageController(tmp.get_singleton());
    }
};

// ---- Memory-limited dict with LRU/FIFO eviction ----

struct PythonMemoryLimitedDictStorageController : public CppDictStorageController {
    enum class Policy { LRU, FIFO };

    size_t max_bytes;
    Policy policy;
    std::function<void(const String&, const json&)> on_evict;
    std::set<String> pinned;

    std::unordered_map<String, size_t> sizes;
    std::list<String> order; // front = oldest
    std::unordered_map<String, std::list<String>::iterator> where;
    size_t current_bytes = 0;

    PythonMemoryLimitedDictStorageController(
        const CppDictStorage& model,
        double max_memory_mb = 1024.0,
        const String& pol = "lru",
        std::function<void(const String&, const json&)> onEvict = [](auto, auto){},
        std::set<String> pinnedKeys = {}
    )
    : CppDictStorageController(model),
      max_bytes(static_cast<size_t>(std::max(0.0, max_memory_mb) * 1024.0 * 1024.0)),
      policy( (pol == "fifo" || pol == "FIFO") ? Policy::FIFO : Policy::LRU ),
      on_evict(std::move(onEvict)),
      pinned(std::move(pinnedKeys))
    {}

    size_t entry_size(const String& k, const json& v) const {
        return deep_size_of_string(k) + deep_size_of_json(v);
    }

    void reduce_key(const String& key) {
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
            const String victim = *it;

            auto val = CppDictStorageController::get(victim);
            reduce_key(victim);
            CppDictStorageController::erase(victim);
            if (val) on_evict(victim, *val);
        }
    }

    void set(const String& key, const json& value) override {
        if (exists(key)) reduce_key(key);
        CppDictStorageController::set(key, value);

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

    std::optional<json> get(const String& key) const override {
        auto v = CppDictStorageController::get(key);
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

    bool erase(const String& key) override {
        if (!exists(key)) return false;
        reduce_key(key);
        return CppDictStorageController::erase(key);
    }

    void clean() override {
        auto ks = keys("*");
        for (auto& k : ks) CppDictStorageController::erase(k);
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

    std::unordered_map<String, Callback> callbacks; // key -> cb
    mutable std::unordered_map<String, String> b64_cache{{"*","*"}};

    String event_glob(const String& event_name="*", const String& event_id="*") const {
        auto& cache = const_cast<std::unordered_map<String,String>&>(b64_cache);
        if (!cache.count(event_name)) cache[event_name] = b64url_encode(event_name);
        return String(ROOT_KEY) + ":" + cache[event_name] + ":" + event_id;
    }

    std::vector<std::pair<String, Callback>> events() const {
        std::vector<std::pair<String, Callback>> out;
        out.reserve(callbacks.size());
        for (auto& kv : callbacks) out.emplace_back(kv.first, kv.second);
        return out;
    }

    std::vector<Callback> get_event(const String& event_id) const {
        // find keys matching "*:<event_id>"
        std::vector<Callback> out;
        for (auto& kv : callbacks) {
            auto& k = kv.first;
            auto pos1 = k.find(':');
            auto pos2 = k.find(':', pos1 == String::npos ? 0 : pos1 + 1);
            if (pos2 != String::npos) {
                String eid = k.substr(pos2 + 1);
                if (eid == event_id) out.push_back(kv.second);
            }
        }
        return out;
    }

    int delete_event(const String& id) {
        return callbacks.erase(id);
    }

    String set_event(const String& event_name, Callback cb, const std::optional<String>& event_id = std::nullopt) {
        String eid = event_id.value_or(uuid_v4());
        auto& cache = b64_cache;
        if (!cache.count(event_name)) cache[event_name] = b64url_encode(event_name);
        String key = String(ROOT_KEY) + ":" + cache[event_name] + ":" + eid;
        callbacks[key] = std::move(cb);
        return eid;
    }

    void dispatch_event(const String& event_name, const json& payload=json::object()) {
        auto& cache = b64_cache;
        if (!cache.count(event_name)) cache[event_name] = b64url_encode(event_name);
        const String prefix = String(ROOT_KEY) + ":" + cache[event_name] + ":";
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
    mutable std::unordered_map<String,String> b64_cache{{"*","*"}};

    MessageQueueController(const CppDictStorage& model,
                           double max_memory_mb = 1024.0,
                           const String& pol = "lru",
                           std::function<void(const String&, const json&)> onEvict = [](auto, auto){},
                           std::set<String> pinnedKeys = {},
                           std::optional<EventDispatcherController> disp = std::nullopt)
    : PythonMemoryLimitedDictStorageController(model, max_memory_mb, pol, onEvict, std::move(pinnedKeys)),
      dispatcher(disp.value_or(EventDispatcherController{}))
    {}

    String qname(const String& q) const {
        auto& cache = const_cast<std::unordered_map<String,String>&>(b64_cache);
        if (!cache.count(q)) {
            String enc = b64url_encode(q);
            cache[q] = enc;
            cache[enc] = q;
        }
        return cache[q];
    }
    String qkey(const String& q, std::optional<String> idx = std::nullopt) const {
        String k = String(ROOT_KEY) + ":" + qname(q);
        if (idx) k += ":" + *idx;
        return k;
    }
    String event_name(const String& q, const String& kind) const {
        return String(ROOT_KEY_EVENT) + ":" + qname(q) + ":" + kind;
    }

    json load_meta(const String& q) {
        auto m = CppDictStorageController::get(qkey(q));
        if (!m || !m->is_object()) {
            json nm = {{"head",0}, {"tail",0}};
            CppDictStorageController::set(qkey(q), nm);
            return nm;
        }
        json meta = *m;
        if (!meta.contains("head") || !meta.contains("tail") ||
            !meta["head"].is_number_integer() || !meta["tail"].is_number_integer() ||
            meta["head"].get<int64_t>() < 0 || meta["tail"].get<int64_t>() < meta["head"].get<int64_t>()) {
            meta = {{"head",0}, {"tail",0}};
            CppDictStorageController::set(qkey(q), meta);
        }
        return meta;
    }
    void save_meta(const String& q, const json& meta) {
        CppDictStorageController::set(qkey(q), meta);
    }
    int size_from_meta(const json& meta) const {
        return std::max<int64_t>(0, meta["tail"].get<int64_t>() - meta["head"].get<int64_t>());
    }
    void try_dispatch(const String& q, const String& kind, const std::optional<String>& key, const std::optional<json>& msg) {
        try {
            json payload = json::object();
            if (msg) payload["message"] = *msg;
            dispatcher.dispatch_event(event_name(q, kind), payload);
        } catch (...) {}
    }

    String add_listener(const String& queue_name,
                             EventDispatcherController::Callback cb,
                             const String& event_kind = "pushed",
                             const std::optional<String>& listener_id = std::nullopt) {
        return dispatcher.set_event(event_name(queue_name, event_kind), std::move(cb), listener_id);
    }
    int remove_listener(const String& listener_id) {
        return dispatcher.delete_event(listener_id);
    }

    String push(const json& message, const String& q="default") {
        json meta = load_meta(q);
        int64_t idx = meta["tail"].get<int64_t>();
        String key = qkey(q, std::to_string(idx));
        CppDictStorageController::set(key, message);
        meta["tail"] = idx + 1;
        save_meta(q, meta);
        try_dispatch(q, "pushed", key, message);
        return key;
    }

    std::pair<std::optional<String>, std::optional<json>> pop_item(const String& q="default", bool peek=false) {
        json meta = load_meta(q);
        // advance head past holes
        while (meta["head"] < meta["tail"]) {
            String k = qkey(q, std::to_string((int64_t)meta["head"]));
            if (CppDictStorageController::get(k)) break;
            meta["head"] = (int64_t)meta["head"] + 1;
        }
        if (meta["head"] >= meta["tail"]) return {std::nullopt, std::nullopt};

        String key = qkey(q, std::to_string((int64_t)meta["head"]));
        auto msg = CppDictStorageController::get(key);
        if (!msg) {
            meta["head"] = (int64_t)meta["head"] + 1;
            save_meta(q, meta);
            // try again or return empty if at end
            while (meta["head"] < meta["tail"]) {
                String k = qkey(q, std::to_string((int64_t)meta["head"]));
                if (CppDictStorageController::get(k)) break;
                meta["head"] = (int64_t)meta["head"] + 1;
            }
            if (meta["head"] >= meta["tail"]) return {std::nullopt, std::nullopt};
            return pop_item(q, peek);
        }

        if (peek) return {key, msg};

        CppDictStorageController::erase(key);
        meta["head"] = (int64_t)meta["head"] + 1;
        save_meta(q, meta);

        try_dispatch(q, "popped", key, msg);
        if (size_from_meta(meta) == 0) try_dispatch(q, "empty", std::nullopt, std::nullopt);
        return {key, msg};
    }

    std::optional<json> pop(const String& q="default") { return pop_item(q).second; }
    std::optional<json> peek(const String& q="default") { return pop_item(q, true).second; }
    int queue_size(const String& q="default") { return size_from_meta(load_meta(q)); }

    void clear(const String& q="default") {
        auto ks = keys(String(ROOT_KEY) + ":" + qname(q) + ":*");
        for (auto& k : ks) CppDictStorageController::erase(k);
        CppDictStorageController::erase(qkey(q));
        try_dispatch(q, "cleared", std::nullopt, std::nullopt);
    }

    std::vector<String> list_queues() const {
        std::set<String> qs;
        for (auto& k : keys(String(ROOT_KEY) + ":*")) {
            auto parts = std::vector<String>{};
            String tmp = k;
            size_t pos = 0;
            while (true) {
                auto p = tmp.find(':', pos);
                if (p == String::npos) { parts.push_back(tmp.substr(pos)); break; }
                parts.push_back(tmp.substr(pos, p - pos));
                pos = p + 1;
            }
            if (parts.size() >= 2 && parts[0] == ROOT_KEY) {
                const String& enc = parts[1];
                auto it = b64_cache.find(enc);
                if (it != b64_cache.end()) qs.insert(it->second);
                else qs.insert(enc); // best effort
            }
        }
        return std::vector<String>(qs.begin(), qs.end());
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
    std::optional<String> current_version;

    explicit LocalVersionController(
        std::unique_ptr<PythonMemoryLimitedDictStorageController> client_ = nullptr,
        double limitMB = 128.0,
        const String& eviction_policy = "fifo"
    )
    : limit_memory_MB(limitMB)
    {
        if (!client_) {
            CppDictStorage model;
            client = std::make_unique<PythonMemoryLimitedDictStorageController>(
                model, limitMB, eviction_policy,
                [this](const String& key, const json& /*v*/) { this->on_evict(key); },
                std::set<String>{TABLENAME}
            );
        } else {
            client = std::move(client_);
        }
        auto table = client->get(TABLENAME).value_or(json::object());
        if (!table.contains(KEY)) client->set(TABLENAME, json{{KEY, json::array()}});
    }

    void on_evict(const String& key) {
        const String prefix = String(TABLENAME) + ":";
        if (key.rfind(prefix, 0) != 0) return;
        const String op_id = key.substr(prefix.size());
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

    std::vector<String> get_versions() const {
        auto t = client->get(TABLENAME).value_or(json::object());
        if (!t.contains(KEY)) return {};
        std::vector<String> out;
        for (auto& v : t[KEY]) out.push_back(v.get<String>());
        return out;
    }
    void set_versions(const std::vector<String>& ops) {
        client->set(TABLENAME, json{{KEY, ops}});
    }

    std::tuple<std::vector<String>, int, std::optional<int>, std::optional<json>>
    find_version(const std::optional<String>& version_uuid) const {
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
                auto opj = client->get(String(TABLENAME) + ":" + *version_uuid);
                if (opj) op = *opj;
            }
        }
        return {versions, current_idx, target_idx, op.is_null() ? std::optional<json>{} : std::optional<json>{op}};
    }

    double estimate_memory_MB() const {
        return double(client->bytes_used(true)) / (1024.0 * 1024.0);
    }

    // operation format: ["set", key, value] etc.
    std::optional<String> add_operation(const json& operation, const std::optional<json>& revert = std::nullopt, bool verbose=false) {
        const String opuuid = uuid_v4();
        client->set(String(TABLENAME) + ":" + opuuid, json{{FORWARD, operation}, {REVERT, revert ? *revert : json()}});
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

    std::vector<std::pair<String, json>> pop_operation(int n=1) {
        if (n <= 0) return {};
        auto ops = get_versions();
        if (ops.empty()) return {};
        std::vector<std::pair<String, json>> popped;
        for (int i=0; i<std::min<int>(n, (int)ops.size()); ++i) {
            int pop_idx = (!ops.empty() && (!current_version || ops[0] != *current_version)) ? 0 : (int)ops.size()-1;
            String op_id = ops[pop_idx];
            String op_key = String(TABLENAME) + ":" + op_id;
            auto op_record = client->get(op_key).value_or(json::object());
            popped.emplace_back(op_id, op_record);
            ops.erase(ops.begin() + pop_idx);
            client->erase(op_key);
        }
        set_versions(ops);
        if (!current_version || std::find(ops.begin(), ops.end(), *current_version) == ops.end()) {
            current_version = ops.empty() ? std::optional<String>{} : std::optional<String>{ops.back()};
        }
        return popped;
    }

    template<class ForwardCB>
    void forward_one_operation(ForwardCB cb) {
        auto [versions, cur_idx, _t, _o] = find_version(current_version);
        int next_idx = cur_idx + 1;
        if (next_idx >= (int)versions.size()) return;
        auto op = client->get(String(TABLENAME) + ":" + versions[next_idx]);
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
    void to_version(const String& version_uuid, VersionCB cb) {
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
        virtual String encrypt_string(const String&) = 0;
        virtual String decrypt_string(const String&) = 0;
    };
    Encryptor* encryptor = nullptr;

    // active backend
    std::unique_ptr<AbstractStorageController> conn;
    EventDispatcherController event_disp;
    LocalVersionController verc;
    MessageQueueController mq;

    SingletonKeyValueStorage(bool version_control_=false, Encryptor* enc=nullptr)
    : version_control(version_control_), encryptor(enc),
      conn(std::make_unique<CppDictStorageController>(CppDictStorageController::build())),
      event_disp(),
      verc(),
      mq(CppDictStorageController::build_tmp().model)
    {}

    SingletonKeyValueStorage& switch_backend(std::unique_ptr<AbstractStorageController> controller) {
        event_disp = EventDispatcherController{};
        verc = LocalVersionController{};
        mq = MessageQueueController(CppDictStorageController::build_tmp().model);
        conn = std::move(controller);
        return *this;
    }

    // --- helpers
    bool exists(const String& key) {
        try { return conn->exists(key); } catch (...) { return false; }
    }
    std::vector<String> keys(const String& pattern="*") {
        try { return conn->keys(pattern); } catch (...) { return {}; }
    }

    std::optional<json> get(const String& key) {
        try {
            auto v = conn->get(key);
            if (!v) return std::nullopt;
            if (encryptor && v->is_object() && v->contains("rjson")) {
                auto s = (*v)["rjson"].get<String>();
                return json::parse(encryptor->decrypt_string(s));
            }
            return v;
        } catch (...) {
            return std::nullopt;
        }
    }

    String dumps() {
        try {
            json root = json::object();
            for (auto& k : keys("*")) {
                auto v = get(k);
                if (v) root[k] = *v;
            }
            return root.dump();
        } catch (...) { return "{}"; }
    }

    bool set(const String& key, json value) {
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

    bool erase(const String& key) {
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

    bool load_file(const String& path) {
        auto args = json::array({"load", path});
        if (version_control) {
            json revert = json::array({"loads", dumps()});
            verc.add_operation(args, revert);
        }
        try { conn->load_file(path); event_disp.dispatch_event("load"); return true; }
        catch (...) { return false; }
    }
    bool loads(const String& s) {
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
            String f = rev[0].get<String>();
            if (f == "set")      this->conn->set(rev[1].get<String>(), rev[2]);
            else if (f == "delete") this->conn->erase(rev[1].get<String>());
            else if (f == "clean")  this->conn->clean();
            else if (f == "load")   this->conn->load_file(rev[1].get<String>());
            else if (f == "loads")  this->conn->loads(rev[1].is_string()? rev[1].get<String>() : rev[1].dump());
        });
    }
    void forward_one_operation() {
        verc.forward_one_operation([this](const json& fwd){
            if (!fwd.is_array() || fwd.empty()) return;
            String f = fwd[0].get<String>();
            if (f == "set")      this->conn->set(fwd[1].get<String>(), fwd[2]);
            else if (f == "delete") this->conn->erase(fwd[1].get<String>());
            else if (f == "clean")  this->conn->clean();
            else if (f == "load")   this->conn->load_file(fwd[1].get<String>());
            else if (f == "loads")  this->conn->loads(fwd[1].is_string()? fwd[1].get<String>() : fwd[1].dump());
        });
    }

    std::optional<String> current_version() const { return verc.current_version; }
    void local_to_version(const String& opuuid) {
        verc.to_version(opuuid, [this](const json& op){
            if (!op.is_array() || op.empty()) return;
            String f = op[0].get<String>();
            if (f == "set")      this->conn->set(op[1].get<String>(), op[2]);
            else if (f == "delete") this->conn->erase(op[1].get<String>());
            else if (f == "clean")  this->conn->clean();
            else if (f == "load")   this->conn->load_file(op[1].get<String>());
            else if (f == "loads")  this->conn->loads(op[1].is_string()? op[1].get<String>() : op[1].dump());
        });
    }

    // Events facade
    std::vector<std::pair<String, EventDispatcherController::Callback>> events() { return event_disp.events(); }
    std::vector<EventDispatcherController::Callback> get_event(const String& id) { return event_disp.get_event(id); }
    int delete_event(const String& id) { return event_disp.delete_event(id); }
    String set_event(const String& name, EventDispatcherController::Callback cb, const std::optional<String>& id=std::nullopt) {
        return event_disp.set_event(name, std::move(cb), id);
    }
    void dispatch_event(const String& name, const json& payload=json::object()) { event_disp.dispatch_event(name, payload); }
    void clean_events() { /* not stored -> nothing to wipe aside from replacing controller */ event_disp = EventDispatcherController{}; }
};
