
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
sqlite_back = True

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

if sqlite_back:
    import sqlite3
    import threading
    import queue
    import time
    import uuid
    class SingletonSqliteStorage:
        _instance = None
        _meta = {}

        @staticmethod
        def _set_instance(cls):
            cls._instance = super(SingletonSqliteStorage, cls).__new__(cls)                                
            cls._instance.client = None
            cls._instance.slaves = []

        def __new__(cls):
            if cls._instance is None:
                SingletonSqliteStorage._set_instance(cls)
            return cls._instance

        def __init__(self):
            self.client = sqlite3.connect(':memory:')
            self.query_queue = queue.Queue()
            self.result_dict = {}
            self.lock = threading.Lock()
            self.worker_thread = threading.Thread(target=self._process_queries)
            self.worker_thread.daemon = True
            self.should_stop = threading.Event()  # Use an event to signal the thread to stop
            self.worker_thread.start()

        def _process_queries(self):
            self.client = sqlite3.connect(':memory:')

            while not self.should_stop.is_set():  # Check if the thread should stop
                try:
                    # Use get with timeout to allow periodic checks for should_stop
                    query_info = self.query_queue.get(timeout=1)
                    # print('query_info',query_info)
                except queue.Empty:
                    continue  # If the queue is empty, continue to check should_stop

                query, query_id = query_info['query'], query_info['id']
                query,val = query
                if 'dump_file' == query[:len('dump_file')]:
                    disk_conn = sqlite3.connect(query.split()[1])
                    self._clone(self.client,disk_conn)
                    disk_conn.close()
                
                if 'load_file' == query[:len('load_file')]:     
                    disk_conn = sqlite3.connect(query.split()[1])
                    self.client.close()
                    self.client = sqlite3.connect(':memory:')
                    self._clone(disk_conn,self.client)
                    disk_conn.close()

                try:
                    cursor = self.client.cursor()
                    if val is None:
                        cursor.execute(query)
                    else:
                        cursor.execute(query, val)

                    if cursor.description is None:
                        self._store_result(query_id, query, True)
                        continue
                    columns = [description[0] for description in cursor.description]
                    result = cursor.fetchall()
                    if len(columns) > 1:
                        result = [dict(zip(columns, row)) for row in result]
                    else:
                        result = [str(row[0]) for row in result]
                    self._store_result(query_id, query, result)
                except sqlite3.Error as e:
                    self._store_result(query_id, query, f"SQLite error: {e}")
                finally:
                    self.query_queue.task_done()
                    self.client.commit()

        def _store_result(self, query_id, query, result):
            with self.lock:
                self.result_dict["query"] = query
                self.result_dict[query_id] = result
                # if "INSERT" or "DELETE" or "UPDATE":
                #     self.execute_query_toKafka(query)
        
        def _clone(self,a,b):
            query = "".join(line for line in a.iterdump())
            # print(query)
            b.executescript(query)
            b.commit()

        # def dump_file(self, file_path, overwrite=True):
        #     if os.path.isfile(file_path) and not overwrite:
        #         raise ValueError(f'{file_path} is exists!')
        #     if os.path.isfile(file_path) and overwrite:
        #         os.remove(file_path)
        #     self.execute_query(f'dump2file {file_path}')

        def _execute_query(self, query, val=None):
            if self.should_stop.is_set():
                raise ValueError('the DB thread is stopped!')
            query_id = str(uuid.uuid4())
            self.query_queue.put({'query': (query,val), 'id': query_id})
            return query_id

        def _pop_result(self, query_id, timeout=1):
            start_time = time.time()
            while True:
                with self.lock:
                    if query_id in self.result_dict:
                        return self.result_dict.pop(query_id)
                if time.time() - start_time > timeout:
                    return None  # or return a custom timeout message
                time.sleep(0.1)  # Wait a short time before checking again to reduce CPU usage

        def _clean_result(self):
            while True:
                with self.lock:
                    self.result_dict = {}
                return True

        def _stop_thread(self):
            while not self.query_queue.empty():
                time.sleep(0.1)
            self.should_stop.set()  # Signal the thread to stop
            self.worker_thread.join()  # Wait for the thread to finish

    class SingletonSqliteStorageController(SingletonStorageController):
        def __init__(self, model: SingletonSqliteStorage):
            self.model = model
            query = "CREATE TABLE KeyValueStore (key TEXT PRIMARY KEY, value JSON)"
            query_id = self.model._execute_query(query)
            print(f"Create query submitted with ID: {query_id}")
            result = self.model._pop_result(query_id)
            print(f"Result for {query_id}: {result}")

        def _execute_query_with_res(self,query):
            query_id = self.model._execute_query(query)
            print(f"Create query submitted with ID: {query_id}")
            result = self.model._pop_result(query_id)
            print(f"Result for {query_id}: {result}")
            return result

        def exists(self, key: str):
            query = f"SELECT EXISTS(SELECT 1 FROM KeyValueStore WHERE key = '{key}');"
            result = self._execute_query_with_res(query)
            return result

        def set(self, key: str, value: dict):
            query = f"INSERT INTO KeyValueStore (key, value) VALUES ('{key}', json('{json.dumps(value)}'))"
            result = self._execute_query_with_res(query)

        def get(self, key: str) -> dict:
            query = f"SELECT value FROM KeyValueStore WHERE key = '{key}'"
            result = self._execute_query_with_res(query)
            if result is None:return {}
            return result
        
        def delete(self, key: str):
            query = f"DELETE FROM KeyValueStore WHERE key = '{key}'"
            result = self._execute_query_with_res(query)
            self._delete_slaves(key)

        def keys(self, pattern: str):
            pattern = pattern.replace('*', '%').replace('?', '_')  # Translate fnmatch pattern to SQL LIKE pattern
            query = f"SELECT key FROM KeyValueStore WHERE key LIKE '{pattern}'"
            result = self._execute_query_with_res(query)
            return result

        # def dump(self,path="PythonDictStorage.json"):
        #     with open(path, "w") as tf: json.dump(self.model.store, tf)

        # def load(self,path="PythonDictStorage.json"):
        #     with open(path, "r") as tf: self.model.store = json.load(tf)

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

    def dump(self,path="PythonDictStorage.json"):
        with open(path, "w") as tf: json.dump(self.model.store, tf)

    def load(self,path="PythonDictStorage.json"):
        with open(path, "r") as tf: self.model.store = json.load(tf)

class SingletonKeyValueStorage(SingletonStorageController):

    def __init__(self) -> None:
        self.python_backend()
    
    def python_backend(self):
        self.client = SingletonPythonDictStorageController(SingletonPythonDictStorage())
    
    if firestore_back:
        def firestore_backend(self,google_project_id=None,google_firestore_collection=None):
            self.client = SingletonFirestoreStorageController(SingletonFirestoreStorage(google_project_id,google_firestore_collection))

    if redis_back:
        def redis_backend(self,redis_URL=None):# 'redis://127.0.0.1:6379'
            self.client = SingletonRedisStorageController(SingletonRedisStorage(redis_URL))

    def exists(self, key: str): return self.client.exists(key)

    def set(self, key: str, value: dict): self.client.set( key, value)

    def get(self, key: str) -> dict: return self.client.get( key)

    def delete(self, key: str): self.client.delete(key)

    def keys(self, pattern: str): return self.client.keys(pattern)

    def dump(self,json_path): self.client.dump(json_path)

    def load(self,json_path): self.client.load(json_path)