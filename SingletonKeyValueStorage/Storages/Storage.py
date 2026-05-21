# Annotated with HEAVY_LEVEL comments.
# Levels used: Light, Medium, Heavy, Critical, or conditional combinations.
# These comments are performance estimates based on the visible code paths and backend/callback behavior.

# from https://github.com/qinhy/singleton-key-value-storage.git
import base64
import sys
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union
import uuid
import fnmatch
import json
from pathlib import Path
from collections import OrderedDict

try:
    from .rjson import SimpleRSAChunkEncryptor, PEMFileReader
except Exception as e:
    from rjson import SimpleRSAChunkEncryptor, PEMFileReader

# HEAVY_LEVEL: Light
# Reason: Base64-url encodes one string; work grows with input length but is usually cheap.
# Complexity: O(n), n = length of s.
def b64url_encode(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")

# HEAVY_LEVEL: Light
# Reason: Adds padding and base64-url decodes one string.
# Complexity: O(n), n = length of s.
def b64url_decode(s: str) -> str:
    # add back missing padding
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii")).decode("utf-8")

# HEAVY_LEVEL: Light
# Reason: Performs one decode and one encode to validate the string.
# Complexity: O(n), n = length of s.
def is_b64url(s: str) -> bool:
    try:
        return b64url_encode(b64url_decode(s)) == s
    except Exception:
        return False
        
# HEAVY_LEVEL: Heavy
# Reason: Recursively walks the reachable object graph, including dicts, containers, __dict__, and __slots__.
# Complexity: O(total reachable objects/items); expensive on large or deeply nested data.
def get_deep_bytes_size(obj, seen=None):
    obj_id = id(obj)
    if seen is None:
        seen = set()
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        for k, v in obj.items():
            size += get_deep_bytes_size(k, seen) + get_deep_bytes_size(v, seen)
        return size
    if isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(get_deep_bytes_size(i, seen) for i in obj)
        return size
    if hasattr(obj, "__dict__"):
        size += get_deep_bytes_size(vars(obj), seen)
    if hasattr(obj, "__slots__"):
        for slot in obj.__slots__:
            try:
                size += get_deep_bytes_size(getattr(obj, slot), seen)
            except AttributeError:
                pass
    return size

# HEAVY_LEVEL: Light
# Reason: Fixed-size loop over a small list of units.
# Complexity: O(1).
def humanize_bytes(n):
    size = float(n)
    for unit in ['B','KB','MB','GB','TB']:
        if size < 1024.0:
            return f"{size:3.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"

class AbstractStorage:
    _uuid = uuid.uuid4()
    _store = None
    _is_singleton = True
    _meta = {}
        
    # HEAVY_LEVEL: Light
    # Reason: Assigns a few attributes and may generate one UUID.
    # Complexity: O(1).
    def __init__(self,id=None,store=None,is_singleton=None):
        self.uuid = uuid.uuid4() if id is None else id
        self.store = None if store is None else store
        self.is_singleton = False if is_singleton is None else is_singleton
    
    # HEAVY_LEVEL: Light
    # Reason: Constructs one instance using class-level singleton fields.
    # Complexity: O(1), assuming subclass initialization stays lightweight.
    def get_singleton(self):
        return self.__class__(self._uuid,self._store,self._is_singleton)    
    
    # HEAVY_LEVEL: Light
    # Reason: Abstract placeholder that only raises NotImplementedError.
    # Complexity: O(1).
    def bytes_used(self, deep=True, human_readable=True):
        raise NotImplementedError("Subclasses must implement memory_usage method")
    
class DictStorage(AbstractStorage):
    _uuid = uuid.uuid4()
    _store = OrderedDict()
        
    # HEAVY_LEVEL: Light
    # Reason: Calls parent initializer and creates or assigns an OrderedDict.
    # Complexity: O(1) for an empty OrderedDict.
    def __init__(self,id=None,store=None,is_singleton=None):
        super().__init__(id,store,is_singleton)
        self.store = OrderedDict() if store is None else store

    # HEAVY_LEVEL: Heavy when deep=True; Light when deep=False
    # Reason: deep=True calls get_deep_bytes_size(self), which recursively scans reachable objects.
    # Complexity: O(total reachable objects/items) if deep=True; O(1) if deep=False.
    def bytes_used(self, deep=True, human_readable=True):
        size = get_deep_bytes_size(self) if deep else sys.getsizeof(self)
        return humanize_bytes(size) if human_readable else size
    
    @staticmethod
    # HEAVY_LEVEL: Light
    # Reason: Factory method creating an empty DictStorage and lightweight controller.
    # Complexity: O(1).
    def build_tmp(): return DictStorageController(DictStorage())

    @staticmethod
    # HEAVY_LEVEL: Light
    # Reason: Factory method creating a singleton-backed DictStorage controller.
    # Complexity: O(1).
    def build(): return DictStorageController(DictStorage().get_singleton())
    
class AbstractStorageController:
    # HEAVY_LEVEL: Light
    # Reason: Stores the model reference.
    # Complexity: O(1).
    def __init__(self, model): self.model:AbstractStorage = model
    # HEAVY_LEVEL: Light
    # Reason: Intended as a simple boolean check on model state.
    # Complexity: O(1).
    def is_singleton(self)->bool: return self.model.is_singleton if 'is_singleton' in self.model else False
    # HEAVY_LEVEL: Light
    # Reason: Placeholder method that only prints a message.
    # Complexity: O(1).
    def exists(self, key: str)->bool: print(f'[{self.__class__.__name__}]: not implement')
    # HEAVY_LEVEL: Light
    # Reason: Placeholder method that only prints a message.
    # Complexity: O(1).
    def set(self, key: str, value: dict): print(f'[{self.__class__.__name__}]: not implement')
    # HEAVY_LEVEL: Light
    # Reason: Placeholder method that only prints a message.
    # Complexity: O(1).
    def get(self, key: str)->dict: print(f'[{self.__class__.__name__}]: not implement')
    # HEAVY_LEVEL: Light
    # Reason: Placeholder method that only prints a message.
    # Complexity: O(1).
    def delete(self, key: str): print(f'[{self.__class__.__name__}]: not implement')
    # HEAVY_LEVEL: Light
    # Reason: Placeholder method that only prints a message.
    # Complexity: O(1).
    def keys(self, pattern: str='*')->list[str]: print(f'[{self.__class__.__name__}]: not implement')
    # HEAVY_LEVEL: Medium
    # Reason: Iterates all keys and deletes each one through the backend.
    # Complexity: O(k), k = number of keys; backend delete cost may add more.
    def clean(self): [self.delete(k) for k in self.keys('*')]
    # HEAVY_LEVEL: Heavy
    # Reason: Reads every key/value and serializes the full store to JSON.
    # Complexity: O(total stored data size).
    def dumps(self): return json.dumps({k:self.get(k) for k in self.keys('*')})
    # HEAVY_LEVEL: Heavy
    # Reason: Parses a JSON string and writes every item to the backend.
    # Complexity: O(JSON size + number of items * backend set cost).
    def loads(self, json_string=r'{}'): [ self.set(k,v) for k,v in json.loads(json_string).items()]
    # HEAVY_LEVEL: Heavy
    # Reason: Serializes the full store and writes it to disk.
    # Complexity: O(total stored data size + file I/O).
    def dump(self, path: str):return Path(path).write_text(self.dumps())
    # HEAVY_LEVEL: Heavy
    # Reason: Reads a file, parses JSON, and writes all entries to the backend.
    # Complexity: O(file size + number of items * backend set cost).
    def load(self, path: str):return self.loads(Path(path).read_text())

    # HEAVY_LEVEL: Critical
    # Reason: Serializes the full store, loads an RSA key, encrypts the JSON, and writes to disk.
    # Complexity: O(total stored data size + encryption + file I/O).
    def dump_RSA(self,path,public_pkcs8_key_path):
        encryptor = SimpleRSAChunkEncryptor(
            PEMFileReader(public_pkcs8_key_path).load_public_pkcs8_key(), None)
        return Path(path).write_text(encryptor.encrypt_string(self.dumps()))

    # HEAVY_LEVEL: Critical
    # Reason: Reads encrypted data, loads an RSA key, decrypts, parses JSON, and writes all entries.
    # Complexity: O(file size + decryption + JSON parse + backend writes).
    def load_RSA(self,path,private_pkcs8_key_path):
        encryptor = SimpleRSAChunkEncryptor(
            None, PEMFileReader(private_pkcs8_key_path).load_private_pkcs8_key())
        return self.loads(encryptor.decrypt_string(Path(path).read_text()))

class DictStorageController(AbstractStorageController):
    # HEAVY_LEVEL: Light
    # Reason: Stores references to model and model.store.
    # Complexity: O(1).
    def __init__(self, model:DictStorage):
        self.model:DictStorage = model
        self.store = self.model.store
    # HEAVY_LEVEL: Light
    # Reason: OrderedDict membership check.
    # Complexity: Average O(1).
    def exists(self, key: str)->bool: return key in self.store
    # HEAVY_LEVEL: Light
    # Reason: Single OrderedDict assignment.
    # Complexity: Average O(1), excluding object size.
    def set(self, key: str, value: dict): self.store[key] = value
    # HEAVY_LEVEL: Light
    # Reason: Single OrderedDict lookup.
    # Complexity: Average O(1).
    def get(self, key: str)->dict: return self.store.get(key,None)
    # HEAVY_LEVEL: Light
    # Reason: Single OrderedDict pop.
    # Complexity: Average O(1).
    def delete(self, key: str): return self.store.pop(key)
    # HEAVY_LEVEL: Medium
    # Reason: fnmatch.filter scans all keys to match the pattern.
    # Complexity: O(k * p), k = number of keys, p = pattern/key match cost.
    def keys(self, pattern: str='*'): return fnmatch.filter(self.store.keys(), pattern)

class MemoryLimitedDictStorageController(DictStorageController):
    # HEAVY_LEVEL: Medium
    # Reason: Initializes memory-limited controller state and eviction configuration.
    # Complexity: O(1), but future set/delete costs are higher due to tracking.
    def __init__(self,model: DictStorage, 
                 max_memory_mb: float = 1024.0, policy: str = 'lru',
                on_evict: Optional[Callable[[str, dict], None]] = lambda x:x,
                pinned: Optional[set[str]] = None,):
        super().__init__(model)
        self.max_bytes = int(max(0, max_memory_mb) * 1024 * 1024)
        self.policy = policy.lower().strip()
        if self.policy not in ('lru', 'fifo'):
            raise ValueError("policy must be 'lru' or 'fifo'")
        self.on_evict = on_evict
        self.pinned = pinned or set()
        self.init_size_manage()

    # HEAVY_LEVEL: Light
    # Reason: Initializes bookkeeping dictionaries and counters.
    # Complexity: O(1).
    def init_size_manage(self):
        self._sizes: dict[str, int] = {}  # key -> bytes (approx)
        self._order: OrderedDict[str, None] = self.model.store
        self._current_bytes: int = 0

    # HEAVY_LEVEL: Heavy
    # Reason: Calls get_deep_bytes_size on both key and value.
    # Complexity: O(total reachable objects/items in key and value).
    def _entry_size(self, key: str, value: dict) -> int:
        return get_deep_bytes_size(key) + get_deep_bytes_size(value)

    # HEAVY_LEVEL: Light
    # Reason: Returns the tracked byte counter and optionally formats it.
    # Complexity: O(1).
    def bytes_used(self, deep=True, human_readable=False):
        size = self._current_bytes
        return humanize_bytes(size) if human_readable else size

    # HEAVY_LEVEL: Medium
    # Reason: Scans order until it finds a non-pinned key.
    # Complexity: O(k), worst case when many keys are pinned.
    def pick_victim(self):
        return next((k for k in self._order if k not in self.pinned), None)
    
    # HEAVY_LEVEL: Heavy
    # Reason: May loop through many evictions and run user-provided on_evict callbacks.
    # Complexity: O(number of evictions * victim scan/delete/callback cost).
    def _maybe_evict(self):
        if self.max_bytes <= 0: return []
        while self._current_bytes > self.max_bytes and self._order:
            victim = self.pick_victim()
            if victim is None: break   # only pinned keys remain
            val = self.get(victim,False)  # avoid LRU touch
            self.delete(victim)
            self.on_evict(victim, val)

    # HEAVY_LEVEL: Light
    # Reason: Conditional OrderedDict move for LRU policy.
    # Complexity: Average O(1).
    def move_to_end(self,key):
        if self.policy == 'lru' and self.exists(key):
            self._order.move_to_end(key, last=True)

    # HEAVY_LEVEL: Heavy
    # Reason: Stores value, deeply estimates entry size, updates counters, and may evict entries.
    # Complexity: O(entry object graph size + eviction cost).
    def set(self, key: str, value: dict):
        super().set(key, value)

        # Track size and order
        old_sz = self._sizes.pop(key, 0)
        sz = self._entry_size(key, value)
        self._sizes[key] = sz
        self._current_bytes += sz - old_sz

        self.move_to_end(key)
        self._maybe_evict()

    # HEAVY_LEVEL: Light
    # Reason: Gets one value and optionally updates LRU order.
    # Complexity: Average O(1).
    def get(self, key: str, move_to_end=True) -> dict:
        value = super().get(key)
        if move_to_end: self.move_to_end(key)
        return value

    # HEAVY_LEVEL: Light
    # Reason: Updates tracked size and removes one key.
    # Complexity: Average O(1).
    def delete(self, key: str):
        self._current_bytes -= self._sizes.pop(key, 0)
        return super().delete(key)

    # HEAVY_LEVEL: Heavy
    # Reason: Deletes all keys through the backend and resets memory tracking.
    # Complexity: O(k), k = number of keys; callbacks/backend behavior may add cost.
    def clean(self):
        super().clean()
        self.init_size_manage()

class EventDispatcherController(DictStorageController):
    ROOT_KEY = '_Event'
    _b64_cache_:Dict[str,str] = {'*':'*'}

    # HEAVY_LEVEL: Light
    # Reason: Encodes/caches event name and formats one key pattern.
    # Complexity: O(len(event_name)).
    def _event_glob(self, event_name: str = '*', event_id: str = '*') -> str:
        self._b64_cache_[event_name] = self._b64_cache_.get(event_name,b64url_encode(event_name))
        return f'{self.ROOT_KEY}:{self._b64_cache_[event_name]}:{event_id}'

    # HEAVY_LEVEL: Medium
    # Reason: Builds a glob and scans storage keys through keys().
    # Complexity: O(k), k = number of stored keys.
    def _find_event_keys(self, event_id: str) -> List[str]:
        return self.keys(self._event_glob('*', event_id))
    
    # HEAVY_LEVEL: Medium
    # Reason: Scans matching event keys and reads each callback.
    # Complexity: O(k + e), e = matched events.
    def events(self) -> List[Tuple[str, Callable[..., None]]]:
        keys = self.keys(self._event_glob())
        return [(k, self.get(k)) for k in keys]

    # HEAVY_LEVEL: Medium
    # Reason: Finds matching event keys and reads each event callback.
    # Complexity: O(k + e).
    def get_event(self, event_id: str) -> List[Any]:
        return [self.get(k) for k in self._find_event_keys(event_id)]

    # HEAVY_LEVEL: Medium
    # Reason: Finds all matching event keys and deletes them.
    # Complexity: O(k + e).
    def delete_event(self, event_id: str) -> int:
        keys = self._find_event_keys(event_id)
        for k in keys: self.delete(k)
        return len(keys)

    # HEAVY_LEVEL: Light
    # Reason: Generates or uses one ID and stores one callback.
    # Complexity: Average O(1), excluding callback object size.
    def set_event(self, event_name: str, callback: Callable[..., Any], event_id: Optional[str] = None) -> str:
        eid = event_id or str(uuid.uuid4())
        self.set(self._event_glob(event_name, eid), callback)
        return eid

    # HEAVY_LEVEL: Heavy
    # Reason: Scans listeners and invokes arbitrary callback functions.
    # Complexity: O(k + e * callback cost).
    def dispatch_event(self, event_name: str, *args, **kwargs):
        for k in list(self.keys(self._event_glob(event_name, '*'))):
            cb = self.get(k) or (lambda x:x)
            cb(*args, **kwargs)

class MessageQueueController(MemoryLimitedDictStorageController):
    ROOT_KEY = "_MessageQueue"
    ROOT_KEY_EVENT = "MQE"
    _b64_cache_:Dict[str,str] = {'*':'*'}

    # HEAVY_LEVEL: Medium
    # Reason: Initializes memory-limited queue storage and an event dispatcher.
    # Complexity: O(1), excluding external dispatcher/model setup.
    def __init__(self,
                 model: DictStorage,
                 max_memory_mb: float = 1024.0,
                 policy: str = 'lru',
                 on_evict: Optional[Callable[[str, dict], None]] = lambda key, val: None,
                 pinned: Optional[set[str]] = None,
                 dispatcher: Optional[EventDispatcherController] = None):
        super().__init__(model, max_memory_mb, policy, on_evict, pinned)
        self.dispatcher = dispatcher or EventDispatcherController(model)

    # HEAVY_LEVEL: Light
    # Reason: Base64-url encodes/caches a queue name.
    # Complexity: O(len(queue_name)) on cache miss; O(1) on hit.
    def _qname(self, queue_name) -> str:
        self._b64_cache_[queue_name] = q = self._b64_cache_.get(queue_name,b64url_encode(queue_name))
        self._b64_cache_[q] = queue_name
        return q

    # HEAVY_LEVEL: Light
    # Reason: Builds one queue storage key string.
    # Complexity: O(len(queue_name) + len(index)).
    def _qkey(self, queue_name: str, index: Optional[Union[int, str]] = None) -> str:
        return ':'.join([i for i in [self.ROOT_KEY, self._qname(queue_name), (None if index is None else str(index))] if i is not None])

    # HEAVY_LEVEL: Light
    # Reason: Builds one event name string.
    # Complexity: O(len(queue_name) + len(kind)).
    def _event_name(self, queue_name: str, kind: str) -> str:
        return f"{self.ROOT_KEY_EVENT}:{self._qname(queue_name)}:{kind}"

    # HEAVY_LEVEL: Medium
    # Reason: Reads queue metadata and may write/reset it.
    # Complexity: Average O(1), but set() can trigger deep sizing and eviction.
    def _load_meta(self, queue_name: str) -> dict:
        # meta: {'head': int, 'tail': int}
        m = self.get(self._qkey(queue_name))
        if not m:
            m = {'head': 0, 'tail': 0}
            self.set(self._qkey(queue_name), m)
        # guard against bad states
        if m['head'] < 0 or m['tail'] < m['head']:
            m = {'head': 0, 'tail': 0}
            self.set(self._qkey(queue_name), m)
        return m

    # HEAVY_LEVEL: Medium
    # Reason: Writes queue metadata through memory-limited set().
    # Complexity: O(metadata size + possible eviction cost).
    def _save_meta(self, queue_name: str, meta: dict) -> None:
        self.set(self._qkey(queue_name), meta)

    # HEAVY_LEVEL: Light
    # Reason: Simple arithmetic on metadata counters.
    # Complexity: O(1).
    def _size_from_meta(self, meta: dict) -> int:
        return max(0, meta['tail'] - meta['head'])

    # HEAVY_LEVEL: Heavy
    # Reason: Dispatches to arbitrary event callbacks and suppresses their errors.
    # Complexity: O(listener scan + callback cost).
    def _try_dispatch_event(self, queue_name: str, kind: str,
                            key: Optional[str], message: Optional[dict]) -> None:
        try:
            self.dispatcher.dispatch_event(
                self._event_name(queue_name, kind),message=message)
            #     queue=queue_name, key=key, message=message, op=kind) # include helpful context
        except Exception:
            pass
        
    # HEAVY_LEVEL: Light
    # Reason: Registers one callback in the dispatcher.
    # Complexity: Average O(1).
    def add_listener(self, queue_name: str, callback: Callable[..., None],
                     event_kind: Literal["pushed", "popped", "empty", "cleared"] = "pushed",
                     listener_id: Optional[str] = None) -> str:
        # def on_any_event(message: Any):
        #     # op is one of: "push", "pop", "empty", "clear"
        #     print(f"msg={message}")
        return self.dispatcher.set_event(self._event_name(queue_name, event_kind), callback, listener_id)

    # HEAVY_LEVEL: Medium
    # Reason: Dispatcher deletion searches matching event keys before deleting.
    # Complexity: O(k + matched listeners).
    def remove_listener(self, listener_id: str) -> int:
        return self.dispatcher.delete_event(listener_id)

    # HEAVY_LEVEL: Medium
    # Reason: Reads all dispatcher events and filters them.
    # Complexity: O(e), e = number of listeners/events.
    def list_listeners(self, queue_name: Optional[str] = None, event_kind: Optional[str] = None):
        evts = self.dispatcher.events()  # -> [(key, cb), ...]
        if queue_name is None and event_kind is None: return evts
        queue_name = self._qname(queue_name)

        out: List[Tuple[str, Callable[..., None]]] = []
        for k, cb in evts:
            # expect "Event:MQE:<queue>:<kind>:<id>"
            parts = k.split(':')
            if len(parts) < 5: continue
            _, rk, qn, kind, *_ = parts
            if rk != self.ROOT_KEY_EVENT: continue
            if (queue_name is None or qn == queue_name) and (event_kind is None or kind == event_kind):
                out.append((k, cb))
        return out

    # HEAVY_LEVEL: Heavy
    # Reason: Loads/saves metadata, stores the message with deep size tracking, may evict, and dispatches callbacks.
    # Complexity: O(message object graph size + eviction cost + callback cost).
    def push(self, message: dict, queue_name: str = "default") -> str:
        meta = self._load_meta(queue_name)
        idx = meta['tail']
        key = self._qkey(queue_name, idx)
        self.set(key, message)
        meta['tail'] = idx + 1
        self._save_meta(queue_name, meta)
        self._try_dispatch_event(queue_name, "pushed", key, message)
        return key

    # HEAVY_LEVEL: Medium
    # Reason: Advances across missing/evicted queue entries one by one.
    # Complexity: O(h), h = number of holes skipped.
    def _advance_head_past_holes(self, queue_name: str, meta: dict) -> dict:
        # Skip missing/evicted entries at the head so pops don't get stuck
        while meta['head'] < meta['tail']:
            k = self._qkey(queue_name, meta['head'])
            if self.get(k) is not None: break
            meta['head'] += 1
        return meta

    # HEAVY_LEVEL: Heavy
    # Reason: Loads metadata, skips holes, may delete an item, saves metadata, and dispatches callbacks.
    # Complexity: O(h + delete/save cost + callback cost).
    def pop_item(self, queue_name: str = "default", peek: bool = False) -> Tuple[Optional[str], Optional[dict]]:
        meta = self._load_meta(queue_name)
        meta = self._advance_head_past_holes(queue_name, meta)

        if meta['head'] >= meta['tail']: return None, None

        key = self._qkey(queue_name, meta['head'])
        msg = self.get(key)
        if msg is None:
            # Should be rare due to advance; treat as empty this turn
            meta['head'] += 1
            self._save_meta(queue_name, meta)
            meta = self._advance_head_past_holes(queue_name, meta)
            return (None, None) if meta['head'] >= meta['tail'] else self.pop_item(queue_name, peek)

        if peek: return key, msg

        self.delete(key)
        meta['head'] += 1
        self._save_meta(queue_name, meta)

        self._try_dispatch_event(queue_name, "popped", key, msg)
        if self._size_from_meta(meta) == 0:
            self._try_dispatch_event(queue_name, "empty", None, None)
        return key, msg

    # HEAVY_LEVEL: Heavy
    # Reason: Delegates to pop_item(), which may scan holes and dispatch callbacks.
    # Complexity: Same as pop_item().
    def pop(self, queue_name: str = "default") -> Optional[dict]:
        return self.pop_item(queue_name)[1]

    # HEAVY_LEVEL: Medium
    # Reason: Delegates to pop_item(peek=True), which may scan holes but does not delete.
    # Complexity: O(h + metadata load cost).
    def peek(self, queue_name: str = "default") -> Optional[dict]:
        return self.pop_item(queue_name, True)[1]

    # HEAVY_LEVEL: Medium
    # Reason: Loads queue metadata, creating/resetting it if missing or invalid.
    # Complexity: Average O(1), but metadata write can trigger memory tracking.
    def queue_size(self, queue_name: str = "default") -> int:
        return self._size_from_meta(self._load_meta(queue_name))

    # HEAVY_LEVEL: Heavy
    # Reason: Scans queue keys, deletes every queue entry, deletes metadata, and dispatches a callback.
    # Complexity: O(k + q + callback cost), q = queue entries.
    def clear(self, queue_name: str = "default") -> None:
        for key in list(self.keys(f"{self.ROOT_KEY}:{self._qname(queue_name)}:*")):
            self.delete(key)
        self.delete(self._qkey(queue_name))
        self._try_dispatch_event(queue_name, "cleared", None, None)

    # HEAVY_LEVEL: Medium
    # Reason: Scans all message queue keys and sorts discovered queue names.
    # Complexity: O(k + q log q).
    def list_queues(self) -> List[str]:
        queues: set[str] = set()
        for k in self.keys(f"{self.ROOT_KEY}:*"):
            parts = k.split(':')
            # accept both meta ("ROOT:queue") and entries ("ROOT:queue:index")
            if len(parts) >= 2 and parts[0] == self.ROOT_KEY:
                queues.add(self._b64_cache_(parts[1]))                
        return sorted(queues)

class LocalVersionController:
    TABLENAME = '_Operation'
    KEY = 'ops'
    FORWARD = 'forward'
    REVERT = 'revert'

    # HEAVY_LEVEL: Medium
    # Reason: May create a memory-limited operation-log backend and initialize the version index.
    # Complexity: O(1), excluding backend setup side effects.
    def __init__(
        self,
        client: Optional[AbstractStorageController] = None,
        limit_memory_MB: float = 128.0,
        eviction_policy: str = 'fifo',  # FIFO fits "oldest ops fall off" best; 'lru' also works
    ):
        self.limit_memory_MB = float(limit_memory_MB)
        self.client = client
        if client is None:
            # Build a private, memory-capped op-log store
            self.client = MemoryLimitedDictStorageController(
                DictStorage(),
                max_memory_mb=self.limit_memory_MB,
                policy=eviction_policy,
                on_evict=self._on_evict,
                pinned={LocalVersionController.TABLENAME},  # never evict the index row
            )
            
        table = self.client.get(LocalVersionController.TABLENAME) or {}
        if LocalVersionController.KEY not in table:
            self.client.set(LocalVersionController.TABLENAME, {LocalVersionController.KEY: []})
        self._current_version: Optional[str] = None        

    # HEAVY_LEVEL: Medium
    # Reason: On operation eviction, copies/searches version list and may rewrite it.
    # Complexity: O(v), v = number of versions.
    def _on_evict(self, key: str, value: dict) -> None:
        # We only care about per-op rows like "_Operation:<uuid>"
        prefix = f'{LocalVersionController.TABLENAME}:'
        if not key.startswith(prefix): return

        op_id = key[len(prefix):]
        ops = self.get_versions()
        if op_id in ops:
            ops.remove(op_id)
            self._set_versions(ops)

        # If we evicted the current pointer, move it to the tail (latest) or None
        if self._current_version == op_id:raise ValueError('auto removed current_version')

    # HEAVY_LEVEL: Medium
    # Reason: Reads the version index and copies it to a new list.
    # Complexity: O(v), v = number of versions.
    def get_versions(self) -> List[str]:
        return list(self.client.get(self.TABLENAME).get(self.KEY, []))

    # HEAVY_LEVEL: Medium
    # Reason: Copies and writes the full version list; backend set may do deep size tracking.
    # Complexity: O(v + backend set cost).
    def _set_versions(self, ops: List[str]) -> Any:
        """Persist the ordered list of version UUIDs."""
        return self.client.set(self.TABLENAME, {self.KEY: list(ops)})

    # HEAVY_LEVEL: Medium
    # Reason: Uses list membership/index lookups over versions and may fetch one op record.
    # Complexity: O(v).
    def find_version(self, version_uuid: Optional[str]):
        versions = self.get_versions()
        current_version_idx = versions.index(self._current_version) if self._current_version in versions else -1
        target_version_idx = versions.index(version_uuid) if (version_uuid in versions) else None

        op = None
        if target_version_idx is not None:
            op_id = versions[target_version_idx]
            op = self.client.get(f'{self.TABLENAME}:{op_id}')

        return versions, current_version_idx, target_version_idx, op

    # Prefer the backend’s byte counter if available; otherwise fall back to deep measurement.
    # HEAVY_LEVEL: Light usually; Heavy with deep-counting backends
    # Reason: MemoryLimitedDictStorageController is O(1), but other backends may perform deep memory scans.
    # Complexity: O(1) with tracked bytes; otherwise can be O(total object graph size).
    def estimate_memory_MB(self) -> float:
        return float(self.client.bytes_used(True,False)) / (1024 * 1024)

    # HEAVY_LEVEL: Heavy
    # Reason: Stores an operation, may deeply size/evict, rewrites version index, and checks memory.
    # Complexity: O(operation payload size + v + eviction cost).
    def add_operation(self, operation: Tuple[Any, ...], revert: Optional[Tuple[Any, ...]] = None, verbose=False):
        opuuid = str(uuid.uuid4())

        # Store op payload (may trigger eviction of oldest ops in the backend)
        self.client.set( f'{self.TABLENAME}:{opuuid}',
                           {self.FORWARD: operation, self.REVERT: revert},)

        # Update ordered index (append)
        ops = self.get_versions()
        # If we are in the middle (after a manual revert), drop redo tail
        if self._current_version is not None and self._current_version in ops:
            opidx = ops.index(self._current_version)
            ops = ops[: opidx + 1]
        ops.append(opuuid)
        self._set_versions(ops)
        self._current_version = opuuid

        # Optional: warn if we still exceed cap (can happen if only pinned keys remain)
        if self.estimate_memory_MB() > self.limit_memory_MB:
            res = f"[LocalVersionController] Warning: memory usage {self.estimate_memory_MB():.1f} MB exceeds limit of {self.limit_memory_MB} MB"
            if verbose:print(res)
            return res
        return None

    # HEAVY_LEVEL: Heavy
    # Reason: Pops up to n operation records, deletes them, and rewrites the version index.
    # Complexity: O(min(n, v) + v + backend delete/set cost).
    def pop_operation(self, n: int = 1) -> List[Tuple[str, dict]]:
        if n <= 0: return []

        ops = self.get_versions()
        if not ops: return []

        popped: List[Tuple[str, dict]] = []
        for _ in range(min(n, len(ops))):
            pop_idx = 0 if ops and ops[0] != self._current_version else -1
            op_id = ops[pop_idx]
            op_key = f"{self.TABLENAME}:{op_id}"
            op_record = self.client.get(op_key)
            popped.append((op_id, op_record))

            # Remove from index and store
            ops.pop(pop_idx)
            self.client.delete(op_key)

        self._set_versions(ops)

        # Fix current pointer if it pointed to a removed op (or list is now empty)
        if self._current_version not in ops:
            self._current_version = ops[-1] if ops else None

        return popped

    # HEAVY_LEVEL: Medium
    # Reason: Finds next version, reads one op, and calls a user-provided callback.
    # Complexity: O(v + callback cost).
    def forward_one_operation(self, forward_callback: Callable[[Tuple[Any, ...]], None]) -> None:
        versions, current_version_idx, _, _ = self.find_version(self._current_version)
        next_idx = current_version_idx + 1
        if next_idx >= len(versions): return

        op = self.client.get(f'{self.TABLENAME}:{versions[next_idx]}')
        if not op or self.FORWARD not in op: return

        forward_callback(op[self.FORWARD])
        self._current_version = versions[next_idx]

    # HEAVY_LEVEL: Medium
    # Reason: Finds current version, reads revert op, and calls a user-provided callback.
    # Complexity: O(v + callback cost).
    def revert_one_operation(self, revert_callback: Callable[[Optional[Tuple[Any, ...]]], None]) -> None:
        versions, current_version_idx, _, op = self.find_version(self._current_version)
        if current_version_idx is None or current_version_idx <= 0: return
        if not op or self.REVERT not in op: return

        revert_callback(op[self.REVERT])
        self._current_version = versions[current_version_idx - 1]

    # HEAVY_LEVEL: Heavy
    # Reason: Walks version-by-version to target, repeatedly calling forward/revert callbacks.
    # Complexity: O(distance * (version lookup + callback cost)).
    def to_version(self, version_uuid: str, version_callback: Callable[[Tuple[Any, ...]], None]) -> None:
        _, current_idx, target_idx, _ = self.find_version(version_uuid)
        if target_idx is None:
            raise ValueError(f'no such version of {version_uuid}')

        if current_idx is None: current_idx = -1

        while current_idx != target_idx:
            if current_idx < target_idx:
                self.forward_one_operation(version_callback)
                current_idx += 1
            else:
                self.revert_one_operation(version_callback)
                current_idx -= 1

class SingletonKeyValueStorage(AbstractStorageController):
    # HEAVY_LEVEL: Medium
    # Reason: Initializes controllers and switches to the default DictStorage backend.
    # Complexity: O(1), excluding controller setup side effects.
    def __init__(self,version_controll=False,
                 encryptor:SimpleRSAChunkEncryptor=None)->None:
        self.version_controll = version_controll
        self.encryptor = encryptor
        self.conn:AbstractStorageController = None
        self.switch_backend(DictStorage.build())
    
    # HEAVY_LEVEL: Medium
    # Reason: Rebuilds event dispatcher, version controller, message queue, and backend reference.
    # Complexity: O(1), but creates several controller objects.
    def switch_backend(self,controller:AbstractStorageController):
        self._event_dispa = EventDispatcherController(DictStorage())
        self._verc = LocalVersionController()
        self.message_queue = MessageQueueController(DictStorage.build_tmp())
        self.conn = controller
        return self

    # HEAVY_LEVEL: Light
    # Reason: Prints one formatted message.
    # Complexity: O(len(msg)).
    def _print(self,msg): print(f'[{self.__class__.__name__}]: {msg}')
       
    # HEAVY_LEVEL: Medium
    # Reason: Delegates to delete_event(), which scans event keys.
    # Complexity: O(k + matched events).
    def delete_slave(self, slave:object)->bool: self.delete_event(getattr(slave,'uuid',None))

    # HEAVY_LEVEL: Medium
    # Reason: May assign a UUID and register callbacks for each requested event name.
    # Complexity: O(number of event_names).
    def add_slave(self, slave:object, event_names=['set','delete'])->bool:
        if getattr(slave,'uuid',None) is None:
            try:
                setattr(slave,'uuid',uuid.uuid4())
            except Exception:
                return self._print(f'can not set uuid to {slave}. Skip this slave.')
        for m in event_names:
            if hasattr(slave, m):
                self.set_event(m,getattr(slave,m),getattr(slave,'uuid'))
            else:
                self._print(f'no func of "{m}" in {slave}. Skip it.')         

    # HEAVY_LEVEL: Heavy for clean/load/loads; Light/Medium for set/delete
    # Reason: Delegates to backend mutation methods; bulk operations can scan or load full storage.
    # Complexity: Depends on selected backend method.
    def _edit_local(self,func_name:str, key:str=None, value:dict=None):
        if func_name not in ['set','delete','clean','load','loads']:
            return self._print(f'no func of "{func_name}". return.')
        func = getattr(self.conn, func_name)
        args = [i for i in [key,value] if i is not None]
        return func(*args)
    
    # HEAVY_LEVEL: Heavy when encrypting or dispatching many callbacks
    # Reason: May RSA-encrypt JSON, mutate backend, and dispatch events.
    # Complexity: O(value size + backend edit cost + callback cost).
    def _edit(self,func_name:str, key:str=None, value:dict=None):
        args = [i for i in [key,value] if i is not None]        
        
        if self.encryptor and func_name=='set':
            value = {'rjson':self.encryptor.encrypt_string(json.dumps(value))}

        res = self._edit_local(func_name,key,value)
        self.dispatch_event(func_name,*args)
        return res
    
    # HEAVY_LEVEL: Heavy
    # Reason: Version control may read old values or dump all data before edits; then performs the edit.
    # Complexity: O(versioning cost + backend edit cost); clean/load/loads may serialize full store.
    def _try_edit_error(self,args):
        if self.version_controll:
            # do local version controll
            func = args[0]
            if func == 'set':
                func,key,_ =args
                revert = None
                if self.exists(key):
                    revert = (func,key,self.get(key))
                else:
                    revert = ('delete',key)
                self._verc.add_operation(args,revert)
                
            elif func == 'delete':
                func,key = args
                revert = ('set',key,self.get(key))
                self._verc.add_operation(args,revert)

            elif func in ['clean','load','loads']:
                revert = ('loads',self.dumps())
                self._verc.add_operation(args,revert)

        try:
            self._edit(*args)
            return True
        except Exception as e:
            self._print(e)
            return False
    
    # HEAVY_LEVEL: Medium
    # Reason: Delegates one revert operation through LocalVersionController and local edit callback.
    # Complexity: O(version lookup + backend edit cost).
    def revert_one_operation(self):
        self._verc.revert_one_operation(lambda revert:self._edit_local(*revert))
    
    # HEAVY_LEVEL: Medium
    # Reason: Delegates one forward operation through LocalVersionController and local edit callback.
    # Complexity: O(version lookup + backend edit cost).
    def forward_one_operation(self):
        self._verc.forward_one_operation(lambda forward:self._edit_local(*forward))

    # HEAVY_LEVEL: Light
    # Reason: Returns one stored pointer.
    # Complexity: O(1).
    def get_current_version(self):
        return self._verc._current_version

    # HEAVY_LEVEL: Heavy
    # Reason: Moves version state step-by-step until the target version.
    # Complexity: O(distance * version/edit callback cost).
    def local_to_version(self,opuuid:str):
        self._verc.to_version(opuuid,lambda revert:self._edit_local(*revert))

    # True False(in error)
    # HEAVY_LEVEL: Heavy when version control/encryption/events are enabled; otherwise Medium
    # Reason: Wraps _try_edit_error(), which may version, encrypt, write, and dispatch callbacks.
    # Complexity: Depends on value size and enabled features.
    def set(self, key: str, value: dict):     return self._try_edit_error(('set',key,value))
    # HEAVY_LEVEL: Medium
    # Reason: Wraps _try_edit_error(); version control may read old value and store revert info.
    # Complexity: O(backend delete cost + versioning cost).
    def delete(self, key: str):               return self._try_edit_error(('delete',key))
    # HEAVY_LEVEL: Heavy
    # Reason: May snapshot full store for version control and delete all backend data.
    # Complexity: O(total stored data size).
    def clean(self):                          return self._try_edit_error(('clean',))
    # HEAVY_LEVEL: Heavy
    # Reason: May snapshot current store, read file, parse JSON, and load many entries.
    # Complexity: O(current store size + file size + backend write cost).
    def load(self,json_path):                 return self._try_edit_error(('load', json_path))
    # HEAVY_LEVEL: Heavy
    # Reason: May snapshot current store, parse JSON string, and load many entries.
    # Complexity: O(current store size + JSON size + backend write cost).
    def loads(self,json_str):                 return self._try_edit_error(('loads',json_str))
    
    # HEAVY_LEVEL: Light wrapper
    # Reason: Only calls a supplied function and catches exceptions; called function may be heavy.
    # Complexity: O(called function cost).
    def _try_load_error(self,func):
        try:
            return func()
        except Exception as e:
            self._print(e)
            return None
    # Object, None(in error)    
    # HEAVY_LEVEL: Light
    # Reason: Delegates one existence check to backend.
    # Complexity: Backend-dependent; typically O(1).
    def exists(self, key: str)->bool:         return self._try_load_error(lambda:self.conn.exists(key))
    # HEAVY_LEVEL: Medium
    # Reason: Delegates to backend keys(), which commonly scans all keys for pattern matching.
    # Complexity: O(k), k = number of keys.
    def keys(self, regx: str='*')->list[str]: return self._try_load_error(lambda:self.conn.keys(regx))

    # HEAVY_LEVEL: Heavy when decrypting; otherwise Light/Medium
    # Reason: Reads one value and may decrypt/JSON-parse encrypted payload.
    # Complexity: O(value size + decryption cost) when encrypted.
    def get(self, key: str)->dict:            
        value = self._try_load_error(lambda:self.conn.get(key))
        if value and self.encryptor and 'rjson' in value:
            value = self._try_load_error(
                lambda:json.loads(self.encryptor.decrypt_string(value['rjson'])))
        return value
    
    # HEAVY_LEVEL: Heavy
    # Reason: Iterates all keys, gets every value, decrypts if needed, and serializes to JSON.
    # Complexity: O(total stored data size + possible decryption cost).
    def dumps(self)->str:                  
        return self._try_load_error(lambda:json.dumps({k:self.get(k) for k in self.keys('*')}))
    
    # HEAVY_LEVEL: Heavy
    # Reason: Delegates full-store dump to backend, including serialization and file I/O.
    # Complexity: O(total stored data size + file I/O).
    def dump(self,json_path)->None:           return self._try_load_error(lambda:self.conn.dump(json_path))

    # events 
    # HEAVY_LEVEL: Medium
    # Reason: Lists stored event callbacks through dispatcher.
    # Complexity: O(k + e).
    def events(self): return self._event_dispa.events()
    # HEAVY_LEVEL: Medium
    # Reason: Finds event keys and returns matching callbacks.
    # Complexity: O(k + e).
    def get_event(self, uuid: str): return self._event_dispa.get_event(uuid)
    # HEAVY_LEVEL: Medium
    # Reason: Finds and deletes matching event callback entries.
    # Complexity: O(k + e).
    def delete_event(self, uuid: str): return self._event_dispa.delete_event(uuid)
    # HEAVY_LEVEL: Light
    # Reason: Stores one event callback through dispatcher.
    # Complexity: Average O(1).
    def set_event(self, event_name: str, callback, id:str=None): return self._event_dispa.set_event(event_name, callback, id)
    # HEAVY_LEVEL: Heavy
    # Reason: Dispatches to arbitrary registered callbacks.
    # Complexity: O(listener scan + callback cost).
    def dispatch_event(self, event_name, *args, **kwargs): return self._event_dispa.dispatch_event(event_name, *args, **kwargs)
    # HEAVY_LEVEL: Heavy
    # Reason: Deletes all event entries through dispatcher clean().
    # Complexity: O(number of event keys).
    def clean_events(self): return self._event_dispa.clean()
