
import fnmatch
import json
import unittest
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
    def __init__(self, model):
        self.model:object = model

    def slaves(self) -> list:
        return self.model.slaves

    def add_slave(self, slave):
        self.slaves().append(slave)
        
    def _set_slaves(self, key: str, value: dict):
        [s.set(key, value) for s in self.slaves() if hasattr(s, 'set')]
    
    def _delete_slaves(self, key: str):
        [s.delete(key) for s in self.slaves() if hasattr(s, 'delete')]

    def exists(self, key: str) -> bool: print(f'[{self.__class__.__name__}]: not implement')

    def set(self, key: str, value: dict): print(f'[{self.__class__.__name__}]: not implement')

    def get(self, key: str) -> dict: print(f'[{self.__class__.__name__}]: not implement')

    def delete(self, key: str): print(f'[{self.__class__.__name__}]: not implement')

    def keys(self, pattern: str='*') -> list[str]: print(f'[{self.__class__.__name__}]: not implement')
    
    def clean(self): [self.delete(k) for k in self.keys('*')]

    def dumps(self): return json.dumps({k:self.get(k) for k in self.keys('*')})
    
    def loads(self, json_string=r'{}'): [ self.set(k,v) for k,v in json.loads(json_string).items()]

    def dump(self,path):
        with open(path, "w") as tf: tf.write(self.dumps())

    def load(self,path):
        with open(path, "r") as tf: self.loads(tf.read())

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
            self.model:SingletonFirestoreStorage = model

        def exists(self, key: str) -> bool:
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

        def keys(self, pattern: str='*') -> list[str]:
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
            self.model:SingletonRedisStorage = model

        def exists(self, key: str) -> bool:
            return self.model.client.exists(key)

        def set(self, key: str, value: dict):
            self.model.client.set(key, json.dumps(value))
            self._set_slaves(key,value)

        def get(self, key: str) -> dict:
            res = self.model.client.get(key)
            if res: res = json.loads(res)
            return res

        def delete(self, key: str):
            self.model.client.delete(key)
            self._delete_slaves(key)

        def keys(self, pattern: str='*') -> list[str]:            
            try:
                res = self.model.client.keys(pattern)
            except Exception as e:
                res = []
            return res

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
            
            cls._instance.query_queue = queue.Queue()
            cls._instance.result_dict = {}
            cls._instance.lock = threading.Lock()
            cls._instance.worker_thread = threading.Thread(target=cls._instance._process_queries)
            cls._instance.worker_thread.daemon = True
            cls._instance.should_stop = threading.Event()  # Use an event to signal the thread to stop
            cls._instance.worker_thread.start()
            cls._instance._execute_query("CREATE TABLE KeyValueStore (key TEXT PRIMARY KEY, value JSON)")

        def __new__(cls):
            if cls._instance is None:
                SingletonSqliteStorage._set_instance(cls)
            return cls._instance

        def __init__(self):
            self.slaves:list = self.slaves
            self.client:sqlite3.Connection = self.client
            self.query_queue:queue.Queue = self.query_queue 
            self.result_dict:dict = self.result_dict 
            self.lock:threading.Lock = self.lock 
            self.worker_thread:threading.Thread = self.worker_thread 
            self.should_stop:threading.Event = self.should_stop
            
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
                    try:
                        disk_conn = sqlite3.connect(query.split()[1])
                        self._clone(self.client,disk_conn)
                    except sqlite3.Error as e:
                        self._store_result(query_id, query, f"SQLite error: {e}")
                    finally:
                        disk_conn.close()
                        self.query_queue.task_done()
                        self.client.commit()   
                
                elif 'load_file' == query[:len('load_file')]:     
                    try:
                        disk_conn = sqlite3.connect(query.split()[1])
                        self.client.close()
                        self.client = sqlite3.connect(':memory:')
                        self._clone(disk_conn,self.client)
                    except sqlite3.Error as e:
                        self._store_result(query_id, query, f"SQLite error: {e}")
                    finally:
                        disk_conn.close()  
                        self.query_queue.task_done()
                        self.client.commit()                    
                else:
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
                self.result_dict[query_id] = {'result':result,'query':query,'time':time.time()}
                # if "INSERT" or "DELETE" or "UPDATE":
                #     self.execute_query_toKafka(query)
        
        def _clone(self,a:sqlite3.Connection,b:sqlite3.Connection):
            query = "".join(line for line in a.iterdump())
            # print(query)
            b.executescript(query)
            b.commit()

        def _execute_query(self, query, val=None):
            if self.should_stop.is_set():
                raise ValueError('the DB thread is stopped!')
            query_id = str(uuid.uuid4())
            self.query_queue.put({'query': (query,val), 'id': query_id, 'time':time.time()})
            return query_id

        def _pop_result(self, query_id, timeout=1, wait=0.01):
            start_time = time.time()
            while True:
                with self.lock:
                    if query_id in self.result_dict:
                        return self.result_dict.pop(query_id)
                if time.time() - start_time > timeout:
                    return None  # or return a custom timeout message
                time.sleep(wait)  # Wait a short time before checking again to reduce CPU usage

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
            self.model:SingletonSqliteStorage = model

        def _execute_query_with_res(self,query):
            query_id = self.model._execute_query(query)
            # print({'time':time.time()})
            # print(f"Create query submitted with ID: {query_id}")
            result = self.model._pop_result(query_id)
            # print(f"Result for {query_id}: {result}")
            return result['result']

        def exists(self, key: str) -> bool:
            query = f"SELECT EXISTS(SELECT * FROM KeyValueStore WHERE key = '{key}');"
            result = self._execute_query_with_res(query)
            return result[0]!='0'

        def set(self, key: str, value: dict):
            if self.exists(key):
                query = f"UPDATE KeyValueStore SET value = json('{json.dumps(value)}') WHERE key = '{key}'"
                result = self._execute_query_with_res(query)
            else:
                query = f"INSERT INTO KeyValueStore (key, value) VALUES ('{key}', json('{json.dumps(value)}'))"
                result = self._execute_query_with_res(query)
            self._set_slaves(key,value)


        def get(self, key: str) -> dict:
            query = f"SELECT value FROM KeyValueStore WHERE key = '{key}'"
            result = self._execute_query_with_res(query)
            if result is None:return None
            if len(result)==0:return None
            return json.loads(result[0])
        
        def delete(self, key: str):
            query = f"DELETE FROM KeyValueStore WHERE key = '{key}'"
            result = self._execute_query_with_res(query)
            self._delete_slaves(key)

        def keys(self, pattern: str='*') -> list[str]:
            pattern = pattern.replace('*', '%').replace('?', '_')  # Translate fnmatch pattern to SQL LIKE pattern
            query = f"SELECT key FROM KeyValueStore WHERE key LIKE '{pattern}'"
            result = self._execute_query_with_res(query)
            return result

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
        self.model:SingletonPythonDictStorage = model

    def exists(self, key: str) -> bool:
        return key in self.model.store

    def set(self, key: str, value: dict):
        self.model.store[key] = value
        self._set_slaves(key,value)

    def get(self, key: str) -> dict:
        return self.model.store.get(key,None)

    def delete(self, key: str):
        if key in self.model.store:
            del self.model.store[key]
        self._delete_slaves(key)

    def keys(self, pattern: str='*') -> list[str]:
        return fnmatch.filter(self.model.store.keys(), pattern)

class SingletonKeyValueStorage(SingletonStorageController):

    def __init__(self) -> None:
        self.python_backend()
    
    def python_backend(self):
        self.client = SingletonPythonDictStorageController(SingletonPythonDictStorage())
    
    if firestore_back:
        def firestore_backend(self,google_project_id=None,google_firestore_collection=None):
            self.client = SingletonFirestoreStorageController(SingletonFirestoreStorage(google_project_id,google_firestore_collection))

    if redis_back:
        def redis_backend(self,redis_URL='redis://127.0.0.1:6379'):
            self.client = SingletonRedisStorageController(SingletonRedisStorage(redis_URL))

    if sqlite_back:
        def sqlite_backend(self):
            self.client = SingletonSqliteStorageController(SingletonSqliteStorage())

    def exists(self, key: str) -> bool: return self.client.exists(key)

    def set(self, key: str, value: dict): self.client.set( key, value)

    def get(self, key: str) -> dict: return self.client.get( key)

    def delete(self, key: str): self.client.delete(key)

    def keys(self, pattern: str='*') -> list[str]: return self.client.keys(pattern)

    def clean(self): self.client.clean()

    def dump(self,json_path): self.client.dump(json_path)

    def load(self,json_path): self.client.load(json_path)

    def dumps(self,): return self.client.dumps()

    def loads(self,json_str): self.client.loads(json_str)

class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs) -> None:
        super().__init__(*args,**kwargs)
        self.store = SingletonKeyValueStorage()

    def test_all(self,num=1):
        self.test_python(num)
        self.test_sqlite(num)
        self.test_redis(num)
        # self.test_firestore(num)

    def test_python(self,num=1):
        self.store.python_backend()
        for i in range(num):self.test_all_cases()

    def test_redis(self,num=1):
        self.store.redis_backend()
        for i in range(num):self.test_all_cases()

    def test_sqlite(self,num=1):
        self.store.sqlite_backend()
        for i in range(num):self.test_all_cases()

    def test_firestore(self,num=1):
        self.store.firestore_backend()
        for i in range(num):self.test_all_cases()

    def test_all_cases(self):
        self.test_set_and_get()
        self.test_exists()
        self.test_delete()
        self.test_keys()
        self.test_get_nonexistent()
        self.test_dump_and_load()

    def test_set_and_get(self):
        self.store.set('test1', {'data': 123})
        self.assertEqual(self.store.get('test1'), {'data': 123}, "The retrieved value should match the set value.")

    def test_exists(self):
        self.store.set('test2', {'data': 456})
        self.assertTrue(self.store.exists('test2'), "Key should exist after being set.")

    def test_delete(self):
        self.store.set('test3', {'data': 789})
        self.store.delete('test3')
        self.assertFalse(self.store.exists('test3'), "Key should not exist after being deleted.")

    def test_keys(self):
        self.store.set('alpha', {'info': 'first'})
        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        expected_keys = ['alpha', 'abeta']
        self.assertEqual(sorted(self.store.keys('a*')), sorted(expected_keys), "Should return the correct keys matching the pattern.")

    def test_get_nonexistent(self):
        self.assertEqual(self.store.get('nonexistent'), None, "Getting a non-existent key should return None.")
        
    def test_dump_and_load(self):
        raw = {"test1": {"data": 123}, "test2": {"data": 456}, "alpha": {"info": "first"}, "abeta": {"info": "second"}, "gamma": {"info": "third"}}
        self.store.dump('test.json')

        self.store.clean()        
        self.assertEqual(self.store.dumps(),'{}', "Should return the correct keys and values.")

        self.store.load('test.json')
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")
        import os
        
        self.store.clean()
        self.store.loads(json.dumps(raw))
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")