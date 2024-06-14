
import fnmatch
import json
from urllib.parse import urlparse

def get_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

# self checking
firestore_back = get_error(lambda:__import__('google.cloud.firestore')) is None
redis_back = get_error(lambda:__import__('redis')) is None

class SingletonStorageController:

    def add_slave(self, slave):
        self.model.slaves.append(slave)
        
    def _set_slaves(self, key: str, value: dict):
        [s.set(key, value) for s in self.model.slaves if hasattr(s, 'set')]
    
    def _delete_slaves(self, key: str):
        [s.delete(key) for s in self.model.slaves if hasattr(s, 'delete')]

    def exists(self, key: str): print('not implement')

    def set(self, key: str, value: dict): print('not implement')

    def get(self, key: str) -> dict: print('not implement')

    def delete(self, key: str): print('not implement')

    def keys(self, pattern: str): print('not implement')

    def dump(self, json_path=None): print('not implement')

    def load(self, json_path=None): print('not implement')

    def dumps(self): print('not implement')

    def loads(self, json_string=None): print('not implement')


if firestore_back:
    from google.cloud import firestore
    class SingletonFirestoreStorage:
        _instance = None
        _meta = {}

        @staticmethod
        def _set_instance(cls,google_project_id,google_firestore_collection):
            cls._instance = super(SingletonFirestoreStorage, cls).__new__(cls)
            cls._instance.client = firestore.Client(project=google_project_id)
            cls._instance.collection = cls._instance.client.collection(google_firestore_collection)
            cls._instance.slaves = []
            cls._meta['google_project_id']=google_project_id
            cls._meta['google_firestore_collection']=google_firestore_collection
        
        def __new__(cls,google_project_id:str=None,google_firestore_collection:str=None):
            if cls._instance is None:
                if google_project_id is None or google_firestore_collection is None:
                    raise ValueError('google_project_id and google_firestore_collection must not be None at first time')
                SingletonFirestoreStorage._set_instance(cls,google_project_id,google_firestore_collection)
            
            elif ( google_project_id is not None or google_firestore_collection is not None 
                  ) and (
                 cls._meta.get('google_project_id',None)!=google_project_id or cls._meta.get('google_firestore_collection',None)!=google_firestore_collection ):
                print(f'warnning: instance changed to {google_project_id} , {google_firestore_collection}')
                SingletonFirestoreStorage._set_instance(cls,google_project_id,google_firestore_collection)
            return cls._instance
        
        def __init__(self,google_project_id:str=None,google_firestore_collection:str=None):
            self.slaves:list = self.slaves
            self.client:firestore.Client = self.client    
            self.collection:firestore.CollectionReference = self.collection
    class SingletonFirestoreStorageController(SingletonStorageController):
        def __init__(self, model: SingletonFirestoreStorage):
            self.model = model

        def exists(self, key: str):
            doc = self.model.collection.document(key).get()
            return doc.exists
        
        def set(self, key: str, value: dict):
            self.model.collection.document(key).set(value)
            self._set_slaves(key,value)

        def get(self, key: str) -> dict:
            doc = self.model.collection.document(key).get()
            return doc.to_dict() if doc.exists else None
        
        def delete(self, key: str):
            self.model.collection.document(key).delete()
            self._delete_slaves(key)

        def keys(self, pattern: str):
            docs = self.model.collection.stream()
            keys = [doc.id for doc in docs]
            return fnmatch.filter(keys, pattern)

if redis_back:
    import redis
    class SingletonRedisStorage:
        _instance = None
        _meta = {}

        @staticmethod
        def _set_instance(cls,redis_URL):# 'redis://127.0.0.1:6379'
            url = urlparse(redis_URL)
            cls._instance = super(SingletonRedisStorage, cls).__new__(cls)                                
            cls._instance.client = redis.Redis(host=url.hostname, port=url.port, db=0, decode_responses=True)
            cls._instance.slaves = []
            cls._meta['redis_URL'] = redis_URL

        def __new__(cls, redis_URL=None):
            if cls._instance is None:
                if redis_URL is None: raise ValueError('redis_URL must not be None at first time (redis://127.0.0.1:6379)')
                SingletonRedisStorage._set_instance(cls,redis_URL)
            elif redis_URL is not None and cls._meta.get('redis_URL',None)!=redis_URL:
                print(f'warnning: instance changed to url {redis_URL}')
                SingletonRedisStorage._set_instance(cls,redis_URL)
            return cls._instance

        def __init__(self, redis_URL=None):
            self.slaves:list = self.slaves
            self.client:redis.Redis = self.client
    class SingletonRedisStorageController(SingletonStorageController):
        def __init__(self, model: SingletonRedisStorage):
            self.model = model

        def exists(self, key: str):
            return self.model.client.exists(key)

        def set(self, key: str, value: dict):
            self.model.client.set(key, json.dumps(value))
            self._set_slaves(key,value)

        def get(self, key: str) -> dict:
            return json.loads(self.model.client.get(key))

        def delete(self, key: str):
            self.model.client.delete(key)
            self._delete_slaves(key)


        def keys(self, pattern: str = "*"):
            return self.model.client.keys(pattern)

        def dump(self, path="RedisStorage.json"):
            all_keys = self.model.client.keys()
            all_data = {key: self.model.client.get(key) for key in all_keys}
            with open(path, "w") as tf:
                json.dump(all_data, tf)

        def load(self, path="RedisStorage.json"):
            with open(path, "r") as tf:
                data:dict = json.load(tf)
            for key, value in data.items():
                self.model.client.set(key, value)

class SingletonPythonDictStorage:
    _instance = None
    _meta = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SingletonPythonDictStorage, cls).__new__(cls)
            cls._instance.store = {}
            cls._instance.slaves = []
        return cls._instance
    
    def __init__(self):
        self.slaves:list = self.slaves
        self.store:dict = self.store

class SingletonPythonDictStorageController(SingletonStorageController):
    def __init__(self, model:SingletonPythonDictStorage):
        self.model = model

    def exists(self, key: str):
        return key in self.model.store

    def set(self, key: str, value: dict):
        self.model.store[key] = value
        self._set_slaves(key,value)

    def get(self, key: str) -> dict:
        return self.model.store[key]

    def delete(self, key: str):
        if key in self.model.store:
            del self.model.store[key]
        self._delete_slaves(key)

    def keys(self, pattern: str):
        return fnmatch.filter(self.model.store.keys(), pattern)

    def dumps(self):
        return json.dumps(self.model.store)
    
    def loads(self, json_string=None):
       self.model.store = json.loads(json_string)

    def dump(self,path="PythonDictStorage.json"):
        with open(path, "w") as tf: json.dump(self.model.store, tf)

    def load(self,path="PythonDictStorage.json"):
        with open(path, "r") as tf: self.model.store = json.load(tf)

class SingletonKeyValueStorage(SingletonStorageController):

    def __init__(self) -> None:
        self.python_backend()
    
    def python_backend(self):
        self.client = SingletonPythonDictStorageController(SingletonPythonDictStorage())
        return self
    
    if firestore_back:
        def firestore_backend(self,google_project_id=None,google_firestore_collection=None):
            self.client = SingletonFirestoreStorageController(SingletonFirestoreStorage(google_project_id,google_firestore_collection))
            return self

    if redis_back:
        def redis_backend(self,redis_URL=None):# 'redis://127.0.0.1:6379'
            self.client = SingletonRedisStorageController(SingletonRedisStorage(redis_URL))
            return self

    def exists(self, key: str): return self.client.exists(key)

    def set(self, key: str, value: dict): self.client.set( key, value)

    def get(self, key: str) -> dict: return self.client.get( key)

    def delete(self, key: str): self.client.delete(key)

    def keys(self, pattern: str): return self.client.keys(pattern)

    def dump(self,json_path): self.client.dump(json_path)

    def load(self,json_path): self.client.load(json_path)