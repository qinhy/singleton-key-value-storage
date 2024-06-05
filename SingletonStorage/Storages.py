
import fnmatch
import json
import os
from urllib.parse import urlparse

try:
    from google.cloud import firestore
    os.environ['GOOGLE_PROJECT_ID']
    os.environ['GOOGLE_FIRESTORE_COLLECTION']

    class SingletonFirestoreStorage:
        _instance = None
        client = firestore.Client(project=os.environ['GOOGLE_PROJECT_ID'])
        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(SingletonFirestoreStorage, cls).__new__(cls)
            return cls._instance
        def __init__(self):
            self.slaves = []
            self.collection = self.client.collection(os.environ['GOOGLE_FIRESTORE_COLLECTION'])
    
    class SingletonFirestoreStorageController:
        def __init__(self, model: SingletonFirestoreStorage):
            self.model = model
        def add_slave(self, s):
            self.model.slaves.append(s)
        def exists(self, key: str):
            doc = self.model.collection.document(key).get()
            return doc.exists
        def set(self, key: str, value: dict):
            self.model.collection.document(key).set(json.loads(value))
            for s in self.model.slaves:
                if hasattr(s, 'set'):
                    s.set(key, value)
        def get(self, key: str) -> dict:
            doc = self.model.collection.document(key).get()
            return doc.to_dict() if doc.exists else None
        def delete(self, key: str):
            self.model.collection.document(key).delete()
            for s in self.model.slaves:
                if hasattr(s, 'delete'):
                    s.delete(key)
        def keys(self, pattern: str):
            docs = self.model.collection.stream()
            keys = [doc.id for doc in docs]
            return fnmatch.filter(keys, pattern)
        
except Exception as e:
    print('no google firestore support',e)



try:
    import redis
    os.environ['REDIS_URL'] # 'redis://127.0.0.1:6379'

    class SingletonRedisStorage:
        _instance = None

        def __new__(cls, *args, **kwargs):
            if cls._instance is None:
                cls._instance = super(SingletonRedisStorage, cls).__new__(cls)                
                url = urlparse(os.environ['REDIS_URL'])
                cls._instance.redis = redis.Redis(host=url.hostname, port=url.port, db=0, decode_responses=True)
            return cls._instance

        def __init__(self):
            self.slaves = []

    class SingletonRedisStorageController:
        def __init__(self, model: SingletonRedisStorage):
            self.model = model

        def add_slave(self, s):
            self.model.slaves.append(s)

        def exists(self, key: str):
            return self.model.redis.exists(key)

        def set(self, key: str, value: dict):
            self.model.redis.set(key, json.dumps(value))
            for s in self.model.slaves:
                if hasattr(s, 'set'):
                    s.set(key, value)

        def get(self, key: str) -> dict:
            return json.loads(self.model.redis.get(key))

        def delete(self, key: str):
            self.model.redis.delete(key)
            for s in self.model.slaves:
                if hasattr(s, 'delete'):
                    s.delete(key)

        def keys(self, pattern: str = "*"):
            return self.model.redis.keys(pattern)

        def dump(self, path="RedisStorage.json"):
            all_keys = self.model.redis.keys()
            all_data = {key: self.model.redis.get(key) for key in all_keys}
            with open(path, "w") as tf:
                json.dump(all_data, tf)

        def load(self, path="RedisStorage.json"):
            with open(path, "r") as tf:
                data = json.load(tf)
            for key, value in data.items():
                self.model.redis.set(key, value)

except Exception as e:
    print('no redis support',e)


class SingletonPythonDictStorage:
    _instance = None
    store = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SingletonPythonDictStorage, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.slaves = []

class SingletonPythonDictStorageController:
    def __init__(self, model:SingletonPythonDictStorage):
        self.model = model

    def _get_store(self):
        return self.model.store

    def add_slave(self, s):
        self.model.slaves.append(s)

    def exists(self, key: str):
        return key in self.model.store

    def set(self, key: str, value: dict):
        self.model.store[key] = value
        for s in self.model.slaves:
            if hasattr(s, 'set'):
                s.set(key, value)

    def get(self, key: str) -> dict:
        return self.model.store[key]

    def delete(self, key: str):
        if key in self.model.store:
            del self.model.store[key]
        for s in self.model.slaves:
            if hasattr(s, 'delete'):
                s.delete(key)

    def keys(self, pattern: str):
        return fnmatch.filter(self.model.store.keys(), pattern)

    def dump(self,path="PythonDictStorage.json"):
        with open(path, "w") as tf:
            json.dump(self.model.store, tf)

    def load(self,path="PythonDictStorage.json"):
        with open(path, "r") as tf:
            self.model.store = json.load(tf)


class SingletonKeyValueStorage:

    def __init__(self) -> None:
        self.python_backend()
    
    def python_backend(self):
        self.client = SingletonPythonDictStorageController(SingletonPythonDictStorage())
        
    def redis_backend(self):
        self.client = SingletonRedisStorageController(SingletonRedisStorage())

    def firestore_backend(self):
        self.client = SingletonFirestoreStorageController(SingletonFirestoreStorage())

        
    def add_slave(self, s):
        self.client.add_slave(s)

    def exists(self, key: str):
        return self.client.exists(key)

    def set(self, key: str, value: dict):
        self.client.set( key, value)

    def get(self, key: str) -> dict:
        return self.client.get( key)

    def delete(self, key: str):
        self.client.delete(key)

    def keys(self, pattern: str):
        return self.client.keys(pattern)
