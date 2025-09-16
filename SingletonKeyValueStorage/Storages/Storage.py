# from https://github.com/qinhy/singleton-key-value-storage.git
import sys
from typing import Any, Callable, List, Optional, Tuple
import uuid
import fnmatch
import json
from pathlib import Path

try:
    from .utils import SimpleRSAChunkEncryptor, PEMFileReader
except Exception as e:
    from utils import SimpleRSAChunkEncryptor, PEMFileReader

def get_deep_size(obj, seen=None):
    obj_id = id(obj)
    if seen is None:
        seen = set()
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    size = sys.getsizeof(obj)

    if isinstance(obj, dict):
        for k, v in obj.items():
            size += get_deep_size(k, seen) + get_deep_size(v, seen)
        return size
    if isinstance(obj, (list, tuple, set, frozenset)):
        size += sum(get_deep_size(i, seen) for i in obj)
        return size
    if hasattr(obj, "__dict__"):
        size += get_deep_size(vars(obj), seen)
    if hasattr(obj, "__slots__"):
        for slot in obj.__slots__:
            try:
                size += get_deep_size(getattr(obj, slot), seen)
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
    
    def memory_usage(self, deep=True):
        raise NotImplementedError("Subclasses must implement memory_usage method")
    
class PythonDictStorage(AbstractStorage):
    _uuid = uuid.uuid4()
    _store = {}
        
    def __init__(self,id=None,store=None,is_singleton=None):
        super().__init__(id,store,is_singleton)
        self.store = {} if store is None else store

    def memory_usage(self, deep=True, human_readable=True):
        """Return memory usage (deep or shallow)."""
        size = get_deep_size(self) if deep else sys.getsizeof(self)
        return humanize_bytes(size) if human_readable else size
    

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
            
class LocalVersionController:
    TABLENAME = '_Operation'
    KEY = 'ops'
    FORWARD = 'forward'
    REVERT = 'revert'

    def __init__(self, client=None, limit_memory_MB: float = 128):
        self.limit_memory_MB = limit_memory_MB
        if client is None:
            client = PythonDictStorageController(PythonDictStorage())
        self.client: AbstractStorageController = client

        # Ensure the ops list exists without clobbering existing data
        try:
            table = self.client.get(LocalVersionController.TABLENAME) or {}
        except Exception:
            table = {}
        if LocalVersionController.KEY not in table:
            self.client.set(LocalVersionController.TABLENAME, {LocalVersionController.KEY: []})

        self._current_version: Optional[str] = None

    def get_versions(self) -> List[str]:
        """Return the ordered list of version UUIDs (empty list if none)."""
        try:
            table = self.client.get(LocalVersionController.TABLENAME) or {}
        except Exception:
            table = {}
        return list(table.get(LocalVersionController.KEY, []))

    def _set_versions(self, ops: List[str]) -> Any:
        """Persist the ordered list of version UUIDs."""
        return self.client.set(LocalVersionController.TABLENAME, {LocalVersionController.KEY: list(ops)})

    def find_version(self, version_uuid: Optional[str]):
        versions = self.get_versions()
        current_version_idx = versions.index(self._current_version) if self._current_version in versions else -1
        target_version_idx = versions.index(version_uuid) if (version_uuid in versions) else None

        op = None
        if target_version_idx is not None:
            op_id = versions[target_version_idx]
            op = self.client.get(f'{LocalVersionController.TABLENAME}:{op_id}')

        return versions, current_version_idx, target_version_idx, op

    def estimate_memory_MB(self):
        return self.client.model.memory_usage(deep=True, human_readable=False) / (1024 * 1024)
    
    def add_operation(self, operation: Tuple[Any, ...], revert: Optional[Tuple[Any, ...]] = None):
        """Append a new operation after the current pointer, truncating any redo tail."""
        opuuid = str(uuid.uuid4())

        tmp = {f'{LocalVersionController.TABLENAME}:{opuuid}':
                 {LocalVersionController.FORWARD: operation, LocalVersionController.REVERT: revert}}        
        will_use_MB = get_deep_size(tmp) / (1024 * 1024)

        while will_use_MB+self.estimate_memory_MB() > self.limit_memory_MB:
            popped = self.pop_operation(1)
            if not popped: break

        self.client.set(
            f'{LocalVersionController.TABLENAME}:{opuuid}',
            {LocalVersionController.FORWARD: operation, LocalVersionController.REVERT: revert},
        )

        ops = self.get_versions()
        if self._current_version is not None and self._current_version in ops:
            opidx = ops.index(self._current_version)
            ops = ops[: opidx + 1]  # drop any redo branch
        ops.append(opuuid)
        self._set_versions(ops)
        self._current_version = opuuid

        if self.estimate_memory_MB() > self.limit_memory_MB:
            res = f"[LocalVersionController] Warning: memory usage {self.estimate_memory_MB():.1f} MB exceeds limit of {self.limit_memory_MB} MB"
            print(res)
            return res
        return None

    def pop_operation(self, n: int = 1) -> List[Tuple[str, dict]]:
        if n <= 0: return []

        # Load current list
        ops = self.get_versions()
        if not ops: return []

        popped = []
        for _ in range(min(n, len(ops))):
            pop_idx = 0 if ops[0] != self._current_version else -1
            op_id = ops[pop_idx]
            op_record = self.client.get(f"{self.TABLENAME}:{op_id}")
            popped.append((op_id, op_record))
            
            # Remove from list
            ops.pop(pop_idx)
            self.client.delete(f"{self.TABLENAME}:{op_id}")

        # Persist trimmed list
        self._set_versions(ops)

        # Fix current pointer if it pointed to a removed op (or now out-of-range)
        if self._current_version not in ops:
            self._current_version = ops[-1] if ops else None

        return popped
    
    def forward_one_operation(self, forward_callback: Callable[[Tuple[Any, ...]], None]) -> None:
        versions, current_version_idx, _, _ = self.find_version(self._current_version)
        next_idx = current_version_idx + 1
        if next_idx >= len(versions):
            return

        op = self.client.get(f'{LocalVersionController.TABLENAME}:{versions[next_idx]}')
        if not op or LocalVersionController.FORWARD not in op:
            return

        # Only advance the pointer if the callback succeeds
        forward_callback(op[LocalVersionController.FORWARD])
        self._current_version = versions[next_idx]

    def revert_one_operation(self, revert_callback: Callable[[Optional[Tuple[Any, ...]]], None]) -> None:
        versions, current_version_idx, _, op = self.find_version(self._current_version)
        if current_version_idx is None or current_version_idx <= 0:
            return
        if not op or LocalVersionController.REVERT not in op:
            return

        revert_callback(op[LocalVersionController.REVERT])
        self._current_version = versions[current_version_idx - 1]

    def to_version(self, version_uuid: str, version_callback: Callable[[Tuple[Any, ...]], None]) -> None:
        versions, current_idx, target_idx, _ = self.find_version(version_uuid)
        if target_idx is None:
            raise ValueError(f'no such version of {version_uuid}')

        # Normalize 'no current' to -1 so we can walk forward cleanly
        if current_idx is None:
            current_idx = -1

        while current_idx != target_idx:
            if current_idx < target_idx:
                # move forward by one
                self.forward_one_operation(version_callback)
                current_idx += 1
            else:
                # move backward by one
                self.revert_one_operation(version_callback)
                current_idx -= 1

class SingletonKeyValueStorage(AbstractStorageController):    
    backs={
        'temp_python':lambda *args,**kwargs:PythonDictStorageController(PythonDictStorage(*args,**kwargs)),
        'python':lambda *args,**kwargs:PythonDictStorageController(PythonDictStorage(*args,**kwargs).get_singleton()),
    }

    def __init__(self,version_controll=False,
                 encryptor:SimpleRSAChunkEncryptor=None)->None:
        self.version_controll = version_controll
        self.encryptor = encryptor
        self.conn:AbstractStorageController = None
        self.python_backend()
    
    def _switch_backend(self,name:str='python',*args,**kwargs):
        self._event_dispa = EventDispatcherController(PythonDictStorage())
        self._verc = LocalVersionController()
        back=self.backs.get(name.lower(),None)
        if back is None:raise ValueError(f'no back end of {name}, has {list(self.backs.items())}')
        return back
    
    def s3_backend(self,bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = '/SingletonS3Storage'):
        self.conn = self._switch_backend('s3',bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = s3_storage_prefix_path)
        
    def file_backend(self,storage_dir='./SingletonKeyValueStorage', ext='.json'):
        self.conn = self._switch_backend('file')(storage_dir=storage_dir,ext=ext)

    def temp_python_backend(self):
        self.conn = self._switch_backend('temp_python')()
    
    def python_backend(self):
        self.conn = self._switch_backend('python')()
    
    def sqlite_pymix_backend(self,mode='sqlite.db'):
        self.conn = self._switch_backend('sqlite_pymix')(mode=mode)
    
    def sqlite_backend(self):             
        self.conn = self._switch_backend('sqlite')()

    def firestore_backend(self,google_project_id:str=None,google_firestore_collection:str=None):
        self.conn = self._switch_backend('firestore')(google_project_id,google_firestore_collection)

    def redis_backend(self,redis_URL:str='redis://127.0.0.1:6379'):
        self.conn = self._switch_backend('redis')(redis_URL)

    def mongo_backend(self,mongo_URL:str="mongodb://127.0.0.1:27017/",
                        db_name:str="SingletonDB", collection_name:str="store"):
        self.conn = self._switch_backend('mongodb')(mongo_URL,db_name,collection_name)

    def couch_backend(self,username:str, password:str,
                      couchdb_URL:str="couchdb://localhost:5984/", dbname="singleton_db"):
        self.conn = self._switch_backend('couch')(couchdb_URL, username, password, dbname)

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