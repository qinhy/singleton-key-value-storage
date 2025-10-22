# from https://github.com/qinhy/singleton-key-value-storage.git
import sys
from typing import Any, Callable, List, Optional, Tuple
import uuid
import fnmatch
import json
from pathlib import Path
from collections import OrderedDict

try:
    from .utils import SimpleRSAChunkEncryptor, PEMFileReader
except Exception as e:
    from utils import SimpleRSAChunkEncryptor, PEMFileReader

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
        
    def __init__(self,id=None,store=None,is_singleton=None):
        self.uuid = uuid.uuid4() if id is None else id
        self.store = None if store is None else store
        self.is_singleton = False if is_singleton is None else is_singleton
    
    def get_singleton(self):
        return self.__class__(self._uuid,self._store,self._is_singleton)    
    
    def bytes_used(self, deep=True, human_readable=True):
        raise NotImplementedError("Subclasses must implement memory_usage method")
    
class PythonDictStorage(AbstractStorage):
    _uuid = uuid.uuid4()
    _store = {}
        
    def __init__(self,id=None,store=None,is_singleton=None):
        super().__init__(id,store,is_singleton)
        self.store = {} if store is None else store

    def bytes_used(self, deep=True, human_readable=True):
        """Return memory usage (deep or shallow)."""
        size = get_deep_bytes_size(self) if deep else sys.getsizeof(self)
        return humanize_bytes(size) if human_readable else size
    
    @staticmethod
    def build_tmp():
        return PythonDictStorageController(PythonDictStorage())

    @staticmethod
    def build():
        return PythonDictStorageController(PythonDictStorage().get_singleton())
    
class AbstractStorageController:
    def __init__(self, model): self.model:AbstractStorage = model
    
    def is_singleton(self)->bool: return self.model.is_singleton if 'is_singleton' in self.model else False

    def exists(self, key: str)->bool: print(f'[{self.__class__.__name__}]: not implement')

    def set(self, key: str, value: dict): print(f'[{self.__class__.__name__}]: not implement')

    def get(self, key: str)->dict: print(f'[{self.__class__.__name__}]: not implement')

    def delete(self, key: str): print(f'[{self.__class__.__name__}]: not implement')

    def keys(self, pattern: str='*')->list[str]: print(f'[{self.__class__.__name__}]: not implement')
    
    def clean(self): [self.delete(k) for k in self.keys('*')]

    def dumps(self): return json.dumps({k:self.get(k) for k in self.keys('*')})
    
    def loads(self, json_string=r'{}'): [ self.set(k,v) for k,v in json.loads(json_string).items()]

    def dump(self, path: str):return Path(path).write_text(self.dumps())

    def load(self, path: str):return self.loads(Path(path).read_text())

    def dump_RSA(self,path,public_pkcs8_key_path):
        encryptor = SimpleRSAChunkEncryptor(
            PEMFileReader(public_pkcs8_key_path).load_public_pkcs8_key(), None)
        return Path(path).write_text(encryptor.encrypt_string(self.dumps()))

    def load_RSA(self,path,private_pkcs8_key_path):
        encryptor = SimpleRSAChunkEncryptor(
            None, PEMFileReader(private_pkcs8_key_path).load_private_pkcs8_key())
        return self.loads(encryptor.decrypt_string(Path(path).read_text()))

class PythonDictStorageController(AbstractStorageController):
    def __init__(self, model:PythonDictStorage):
        self.model:PythonDictStorage = model
        self.store = self.model.store

    def exists(self, key: str)->bool: return key in self.store

    def set(self, key: str, value: dict): self.store[key] = value

    def get(self, key: str)->dict: return self.store.get(key,None)

    def delete(self, key: str): return self.store.pop(key)

    def keys(self, pattern: str='*'): return fnmatch.filter(self.store.keys(), pattern)

class EventDispatcherController(PythonDictStorageController):
    ROOT_KEY = 'Event'
    
    def _find_event(self, uuid: str):
        es = self.keys(f'*:{uuid}')
        return [None] if len(es)==0 else es
    
    def events(self):
        return list(zip(self.keys('*'),[self.get(k) for k in self.keys('*')]))

    def get_event(self, uuid: str):
        return [self.get(k) for k in self._find_event(uuid)]
    
    def delete_event(self, uuid: str):
        return [self.delete(k) for k in self._find_event(uuid)]
    
    def set_event(self, event_name: str, callback, id:str=None):
        if id is None:id = uuid.uuid4()
        self.set(f'{EventDispatcherController.ROOT_KEY}:{event_name}:{id}', callback)
        return id
    
    def dispatch_event(self, event_name, *args, **kwargs):
        for event_full_uuid in self.keys(f'{EventDispatcherController.ROOT_KEY}:{event_name}:*'):
            self.get(event_full_uuid)(*args, **kwargs)

class MessageQueueController(PythonDictStorageController):
    ROOT_KEY = '_MessageQueue'
    
    def __init__(self, model: PythonDictStorage):
        super().__init__(model)
        self.counters = {}  # Track counters for each queue

    def _get_queue_key(self, queue_name: str, index: int) -> str:
        return f'{MessageQueueController.ROOT_KEY}:{queue_name}:{index}'

    def _get_queue_counter(self, queue_name: str) -> int:
        if queue_name not in self.counters:
            self.counters[queue_name] = 0
        return self.counters[queue_name]

    def _increment_queue_counter(self, queue_name: str):
        self.counters[queue_name] = self._get_queue_counter(queue_name) + 1

    def push(self, message: dict, queue_name: str = 'default'):
        counter = self._get_queue_counter(queue_name)
        key = self._get_queue_key(queue_name, counter)
        self.set(key, message)
        self._increment_queue_counter(queue_name)
        return key

    def pop(self, queue_name: str = 'default') -> dict:
        keys = self.keys(f'{MessageQueueController.ROOT_KEY}:{queue_name}:*')
        if not keys: return None  # Queue is empty
        earliest_key = keys[0]
        message = self.get(earliest_key)
        self.delete(earliest_key)
        return message

    def peek(self, queue_name: str = 'default') -> dict:
        keys = self.keys(f'{MessageQueueController.ROOT_KEY}:{queue_name}:*')
        if not keys: return None  # Queue is empty
        return self.get(keys[0])

    def size(self, queue_name: str = 'default') -> int:
        return len(self.keys(f'{MessageQueueController.ROOT_KEY}:{queue_name}:*'))

    def clear(self, queue_name: str = 'default'):
        for key in self.keys(f'{MessageQueueController.ROOT_KEY}:{queue_name}:*'):
            self.delete(key)
        if queue_name in self.counters:
            del self.counters[queue_name]
     
class PythonMemoryLimitedDictStorageController(PythonDictStorageController):
    def __init__(self,model: PythonDictStorage, 
                 max_memory_mb: float = 1024.0, policy: str = 'lru',
                on_evict: Optional[Callable[[str, dict], None]] = None,
                pinned: Optional[set[str]] = None,):
        super().__init__(model)
        self.max_bytes = int(max(0, max_memory_mb) * 1024 * 1024)
        self.policy = policy.lower().strip()
        if self.policy not in ('lru', 'fifo'):
            raise ValueError("policy must be 'lru' or 'fifo'")
        self.on_evict = on_evict
        self.pinned = pinned or set()
        self.init_size_manage()

    def init_size_manage(self):
        self._sizes: dict[str, int] = {}                     # key -> bytes (approx)
        self._order: OrderedDict[str, None] = OrderedDict()  # access/insertion order
        self._current_bytes: int = 0

    def _entry_size(self, key: str, value: dict) -> int:
        return get_deep_bytes_size(key) + get_deep_bytes_size(value)

    def bytes_used(self, deep=True, human_readable=False):
        size = self._current_bytes
        return humanize_bytes(size) if human_readable else size

    def _reduce(self,key):
        self._order.pop(key, None)
        self._current_bytes -= self._sizes.pop(key, 0)

    def _maybe_evict(self):
        if self.max_bytes <= 0: return []
        evicted = []
        def pick_victim():
            return next((k for k in self._order if k not in self.pinned), None)

        while self._current_bytes > self.max_bytes and self._order:
            victim = pick_victim()
            if victim is None: break   # only pinned keys remain
            val = super().get(victim)  # avoid LRU touch
            self._reduce(victim)
            super().delete(victim)
            evicted.append(victim)
            if self.on_evict:
                self.on_evict(victim, val)
        return evicted

    def set(self, key: str, value: dict):
        # If replacing existing key: remove its size first
        if self.exists(key): self._reduce(key)

        super().set(key, value)

        # Track size and order
        sz = self._entry_size(key, value)
        self._sizes[key] = sz
        self._current_bytes += sz

        # Order update
        self._order[key] = None
        if self.policy == 'lru':
            # For LRU we always treat 'set' as most recent use
            self._order.move_to_end(key, last=True)

        # Enforce limit
        self._maybe_evict()

    def get(self, key: str) -> dict:
        val = super().get(key)
        if val is not None and self.policy == 'lru' and key in self._order:
            # Mark as most recently used
            self._order.move_to_end(key, last=True)
        return val

    def delete(self, key: str):
        if self.exists(key): self._reduce(key)
        return super().delete(key)

    def clean(self):
        [super().delete(k) for k in list(self._order.keys())]
        self.init_size_manage()

class LocalVersionController:
    TABLENAME = '_Operation'
    KEY = 'ops'
    FORWARD = 'forward'
    REVERT = 'revert'

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
            model = PythonDictStorage()
            self.client = PythonMemoryLimitedDictStorageController(
                model,
                max_memory_mb=self.limit_memory_MB,
                policy=eviction_policy,
                on_evict=self._on_evict,
                pinned={LocalVersionController.TABLENAME},  # never evict the index row
            )
            
        table = self.client.get(LocalVersionController.TABLENAME) or {}
        if LocalVersionController.KEY not in table:
            self.client.set(LocalVersionController.TABLENAME, {LocalVersionController.KEY: []})
        self._current_version: Optional[str] = None        

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

    def get_versions(self) -> List[str]:
        return list(self.client.get(self.TABLENAME).get(self.KEY, []))

    def _set_versions(self, ops: List[str]) -> Any:
        """Persist the ordered list of version UUIDs."""
        return self.client.set( self.TABLENAME, {self.KEY: list(ops)})

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
    def estimate_memory_MB(self) -> float:
        return float(self.client.bytes_used(True,False)) / (1024 * 1024)

    def add_operation(self, operation: Tuple[Any, ...], revert: Optional[Tuple[Any, ...]] = None):
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
            print(res)
            return res
        return None

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

    def forward_one_operation(self, forward_callback: Callable[[Tuple[Any, ...]], None]) -> None:
        versions, current_version_idx, _, _ = self.find_version(self._current_version)
        next_idx = current_version_idx + 1
        if next_idx >= len(versions): return

        op = self.client.get(f'{self.TABLENAME}:{versions[next_idx]}')
        if not op or self.FORWARD not in op: return

        forward_callback(op[self.FORWARD])
        self._current_version = versions[next_idx]

    def revert_one_operation(self, revert_callback: Callable[[Optional[Tuple[Any, ...]]], None]) -> None:
        versions, current_version_idx, _, op = self.find_version(self._current_version)
        if current_version_idx is None or current_version_idx <= 0: return
        if not op or self.REVERT not in op: return

        revert_callback(op[self.REVERT])
        self._current_version = versions[current_version_idx - 1]

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
    def __init__(self,version_controll=False,
                 encryptor:SimpleRSAChunkEncryptor=None)->None:
        self.version_controll = version_controll
        self.encryptor = encryptor
        self.conn:AbstractStorageController = None
        self.switch_backend(PythonDictStorage.build())
    
    def switch_backend(self,controller:AbstractStorageController):
        self._event_dispa = EventDispatcherController(PythonDictStorage())
        self._verc = LocalVersionController()
        self.conn = controller
        return self

    def _print(self,msg): print(f'[{self.__class__.__name__}]: {msg}')
       
    def delete_slave(self, slave:object)->bool: self.delete_event(getattr(slave,'uuid',None))

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

    def _edit_local(self,func_name:str, key:str=None, value:dict=None):
        if func_name not in ['set','delete','clean','load','loads']:
            return self._print(f'no func of "{func_name}". return.')
        func = getattr(self.conn, func_name)
        args = [i for i in [key,value] if i is not None]
        return func(*args)
    
    def _edit(self,func_name:str, key:str=None, value:dict=None):
        args = [i for i in [key,value] if i is not None]        
        
        if self.encryptor and func_name=='set':
            value = {'rjson':self.encryptor.encrypt_string(json.dumps(value))}

        res = self._edit_local(func_name,key,value)
        self.dispatch_event(func_name,*args)
        return res
    
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
    
    def revert_one_operation(self):
        self._verc.revert_one_operation(lambda revert:self._edit_local(*revert))
    
    def forward_one_operation(self):
        self._verc.forward_one_operation(lambda forward:self._edit_local(*forward))

    def get_current_version(self):
        return self._verc._current_version

    def local_to_version(self,opuuid:str):
        self._verc.to_version(opuuid,lambda revert:self._edit_local(*revert))

    # True False(in error)
    def set(self, key: str, value: dict):     return self._try_edit_error(('set',key,value))
    def delete(self, key: str):               return self._try_edit_error(('delete',key))
    def clean(self):                          return self._try_edit_error(('clean',))
    def load(self,json_path):                 return self._try_edit_error(('load', json_path))
    def loads(self,json_str):                 return self._try_edit_error(('loads',json_str))
    
    def _try_load_error(self,func):
        try:
            return func()
        except Exception as e:
            self._print(e)
            return None
    # Object, None(in error)    
    def exists(self, key: str)->bool:         return self._try_load_error(lambda:self.conn.exists(key))
    def keys(self, regx: str='*')->list[str]: return self._try_load_error(lambda:self.conn.keys(regx))

    def get(self, key: str)->dict:            
        value = self._try_load_error(lambda:self.conn.get(key))
        if value and self.encryptor and 'rjson' in value:
            value = self._try_load_error(
                lambda:json.loads(self.encryptor.decrypt_string(value['rjson'])))
        return value
    
    def dumps(self)->str:                  
        return self._try_load_error(lambda:json.dumps({k:self.get(k) for k in self.keys('*')}))
    
    def dump(self,json_path)->None:           return self._try_load_error(lambda:self.conn.dump(json_path))

    # events 
    def events(self): return self._event_dispa.events()
    def get_event(self, uuid: str): return self._event_dispa.get_event(uuid)
    def delete_event(self, uuid: str): return self._event_dispa.delete_event(uuid)
    def set_event(self, event_name: str, callback, id:str=None): return self._event_dispa.set_event(event_name, callback, id)
    def dispatch_event(self, event_name, *args, **kwargs): return self._event_dispa.dispatch_event(event_name, *args, **kwargs)
    def clean_events(self): return self._event_dispa.clean()