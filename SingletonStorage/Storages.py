
import base64
import re
import sqlite3
import threading
import queue
import time
import uuid
import fnmatch
import json
import unittest
import urllib
import urllib.parse
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
aws_dynamo = get_error(lambda:__import__('boto3')) is None
mongo_back = get_error(lambda:__import__('pymongo')) is None

class SingletonStorageController:
    def __init__(self, model):
        self.model:object = model

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

class SingletonPythonDictStorage:
    _instance = None
    _meta = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SingletonPythonDictStorage, cls).__new__(cls)
            cls._instance.uuid = uuid.uuid4()
            cls._instance.store = {}
        return cls._instance
    
    def __init__(self):
        self.uuid:str = self.uuid
        self.store:dict = self.store

class SingletonPythonDictStorageController(SingletonStorageController):
    def __init__(self, model:SingletonPythonDictStorage):
        self.model:SingletonPythonDictStorage = model

    def exists(self, key: str) -> bool:
        return key in self.model.store

    def set(self, key: str, value: dict):
        self.model.store[key] = value

    def get(self, key: str) -> dict:
        return self.model.store.get(key,None)

    def delete(self, key: str):
        if key in self.model.store:
            del self.model.store[key]

    def keys(self, pattern: str='*') -> list[str]:
        return fnmatch.filter(self.model.store.keys(), pattern)


if firestore_back:
    from google.cloud import firestore
    class SingletonFirestoreStorage:
        _instance = None
        _meta = {}
        
        def __new__(cls,google_project_id:str=None,google_firestore_collection:str=None):
            same_proj = cls._meta.get('google_project_id',None)==google_project_id
            same_coll = cls._meta.get('google_firestore_collection',None)==google_firestore_collection

            if cls._instance is not None and same_proj and same_coll:
                return cls._instance
            
            if google_project_id is None or google_firestore_collection is None:
                raise ValueError('google_project_id or google_firestore_collection must not be None at first time')
            
            if cls._instance is not None and (not same_proj or not same_coll):
                cls._instance.model.close()
                print(f'warnning: instance changed to {google_project_id} , {google_firestore_collection}')

            cls._instance = super(SingletonFirestoreStorage, cls).__new__(cls)
            cls._instance.uuid = uuid.uuid4()
            cls._instance.model = firestore.Client(project=google_project_id)
            cls._instance.collection = cls._instance.model.collection(google_firestore_collection)

            cls._meta['google_project_id']=google_project_id
            cls._meta['google_firestore_collection']=google_firestore_collection

            return cls._instance
        
        def __init__(self,google_project_id:str=None,google_firestore_collection:str=None):
            self.uuid:str = self.uuid
            self.model:firestore.Client = self.model    
            self.collection:firestore.CollectionReference = self.collection
    class SingletonFirestoreStorageController(SingletonStorageController):
        def __init__(self, model: SingletonFirestoreStorage):
            self.model:SingletonFirestoreStorage = model

        def exists(self, key: str) -> bool:
            doc = self.model.collection.document(key).get()
            return doc.exists
        
        def set(self, key: str, value: dict):
            self.model.collection.document(key).set(value)

        def get(self, key: str) -> dict:
            doc = self.model.collection.document(key).get()
            return doc.to_dict() if doc.exists else None
        
        def delete(self, key: str):
            self.model.collection.document(key).delete()

        def keys(self, pattern: str='*') -> list[str]:
            docs = self.model.collection.stream()
            keys = [doc.id for doc in docs]
            return fnmatch.filter(keys, pattern)      

if redis_back:
    import redis
    class SingletonRedisStorage:
        _instance = None
        _meta = {}

        def __new__(cls, redis_URL=None):# redis://127.0.0.1:6379
            if cls._instance is not None and cls._meta.get('redis_URL',None)==redis_URL:
                return cls._instance
            
            if redis_URL is None: raise ValueError('redis_URL must not be None at first time (redis://127.0.0.1:6379)')
            
            if cls._instance is not None and cls._meta.get('redis_URL',None)!=redis_URL:
                cls._instance.client.close()
                print(f'warnning: instance changed to url {redis_URL}')

            url:urllib.parse.ParseResult = urlparse(redis_URL)
            cls._instance = super(SingletonRedisStorage, cls).__new__(cls)                        
            cls._instance.uuid = uuid.uuid4()
            cls._instance.client = redis.Redis(host=url.hostname, port=url.port, db=0, decode_responses=True)
            cls._meta['redis_URL'] = redis_URL

            return cls._instance

        def __init__(self, redis_URL=None):#redis://127.0.0.1:6379
            self.uuid:str = self.uuid
            self.client:redis.Redis = self.client

    class SingletonRedisStorageController(SingletonStorageController):
        def __init__(self, model: SingletonRedisStorage):
            self.model:SingletonRedisStorage = model

        def exists(self, key: str) -> bool:
            return self.model.client.exists(key)

        def set(self, key: str, value: dict):
            self.model.client.set(key, json.dumps(value))

        def get(self, key: str) -> dict:
            res = self.model.client.get(key)
            if res: res = json.loads(res)
            return res

        def delete(self, key: str):
            self.model.client.delete(key)

        def keys(self, pattern: str='*') -> list[str]:            
            try:
                res = self.model.client.keys(pattern)
            except Exception as e:
                res = []
            return res

if sqlite_back:
    class SingletonSqliteStorage:
        _instance = None
        _meta = {}

        DUMP_FILE='dump_db_file'
        LOAD_FILE='load_db_file'
               
        def __new__(cls):
            if cls._instance is None:
                cls._instance = super(SingletonSqliteStorage, cls).__new__(cls)                        
                cls._instance.uuid = uuid.uuid4()
                cls._instance.client = None
                
                cls._instance.query_queue = queue.Queue()
                cls._instance.result_dict = {}
                cls._instance.lock = threading.Lock()
                cls._instance.worker_thread = threading.Thread(target=cls._instance._process_queries)
                cls._instance.worker_thread.daemon = True
                cls._instance.should_stop = threading.Event()  # Use an event to signal the thread to stop
                cls._instance.worker_thread.start()
                cls._instance._execute_query("CREATE TABLE KeyValueStore (key TEXT PRIMARY KEY, value JSON)")
            return cls._instance

        def __init__(self):
            self.uuid:str = self.uuid
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
                query:str = query
                if SingletonSqliteStorage.DUMP_FILE == query[:len(SingletonSqliteStorage.DUMP_FILE)]:
                    try:
                        disk_conn = sqlite3.connect(query.split()[1])
                        self._clone(self.client,disk_conn)
                    except sqlite3.Error as e:
                        self._store_result(query_id, query, f"SQLite error: {e}")
                    finally:
                        disk_conn.close()
                        self.query_queue.task_done()
                        self.client.commit()   
                
                elif SingletonSqliteStorage.LOAD_FILE == query[:len(SingletonSqliteStorage.LOAD_FILE)]:     
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
            result = self.model._pop_result(query_id)
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
            return result


        def get(self, key: str) -> dict:
            query = f"SELECT value FROM KeyValueStore WHERE key = '{key}'"
            result = self._execute_query_with_res(query)
            if result is None:return None
            if len(result)==0:return None
            return json.loads(result[0])
        
        def delete(self, key: str):
            query = f"DELETE FROM KeyValueStore WHERE key = '{key}'"
            return self._execute_query_with_res(query)

        def keys(self, pattern: str='*') -> list[str]:
            pattern = pattern.replace('*', '%').replace('?', '_')  # Translate fnmatch pattern to SQL LIKE pattern
            query = f"SELECT key FROM KeyValueStore WHERE key LIKE '{pattern}'"
            result = self._execute_query_with_res(query)
            return result

if aws_dynamo:
    import boto3
    from botocore.exceptions import ClientError

    class SingletonDynamoDBStorage:
        _instance = None
        
        def __new__(cls,your_table_name):
            if cls._instance is None:
                cls._instance = super(SingletonDynamoDBStorage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                cls._instance.client = boto3.resource('dynamodb')
                cls._instance.table = cls._instance.client.Table(your_table_name)
            return cls._instance

        def __init__(self,your_table_name):
            self.uuid = self.uuid
            self.client = self.client
            self.table = self.table

    class SingletonDynamoDBStorageController(SingletonStorageController):
        def __init__(self, model:SingletonDynamoDBStorage):
            self.model:SingletonDynamoDBStorage = model
        
        def exists(self, key: str) -> bool:
            try:
                response = self.model.table.get_item(Key={'key': key})
                return 'Item' in response
            except ClientError as e:
                print(f'Error checking existence: {e}')
                return False

        def set(self, key: str, value: dict):
            try:
                self.model.table.put_item(Item={'key': key, 'value': json.dumps(value)})
            except ClientError as e:
                print(f'Error setting value: {e}')

        def get(self, key: str) -> dict:
            try:
                response = self.model.table.get_item(Key={'key': key})
                if 'Item' in response:
                    return json.loads(response['Item']['value'])
                return None
            except ClientError as e:
                print(f'Error getting value: {e}')
                return None

        def delete(self, key: str):
            try:
                self.model.table.delete_item(Key={'key': key})
            except ClientError as e:
                print(f'Error deleting value: {e}')

        def keys(self, pattern: str='*') -> list[str]:
            # Convert simple wildcard patterns to regular expressions for filtering
            regex = fnmatch.translate(pattern)
            compiled_regex = re.compile(regex)

            matched_keys = []
            try:
                # Scan operation with no filters - potentially very costly
                scan_kwargs = {
                    'ProjectionExpression': "key",
                    'FilterExpression': "attribute_exists(key)"
                }
                done = False
                start_key = None

                while not done:
                    if start_key:
                        scan_kwargs['ExclusiveStartKey'] = start_key
                    response = self.model.table.scan(**scan_kwargs)
                    items = response.get('Items', [])
                    matched_keys.extend([item['key'] for item in items if compiled_regex.match(item['key'])])

                    start_key = response.get('LastEvaluatedKey', None)
                    done = start_key is None
            except ClientError as e:
                print(f'Error scanning keys: {e}')

            return matched_keys

# from azure.data.tables import TableServiceClient

# class SingletonAzureTableStorage:
#     _instance = None
#     _meta = {}

#     def __new__(cls):
#         if cls._instance is None:
#             cls._instance = super(SingletonAzureTableStorage, cls).__new__(cls)
#             # Setup Azure Table connection here (adjust as necessary)
#             cls._instance.table_service_client = TableServiceClient.from_connection_string("Your_Connection_String")
#             cls._instance.table_client = cls._instance.table_service_client.get_table_client(table_name="Your_Table_Name")
#         return cls._instance
    
#     def __init__(self):
#         self.table_client = self.table_client

# class SingletonAzureTableStorageController(SingletonStorageController):
#     def __init__(self, model: SingletonAzureTableStorage):
#         self.model:SingletonAzureTableStorage = model

#     def exists(self, key: str) -> bool:
#         try:
#             entity = self.model.table_client.get_entity(partition_key="default", row_key=key)
#             return True
#         except Exception as e:
#             return False

#     def set(self, key: str, value: dict):
#         entity = {"partition_key": "default", "row_key": key, **value}
#         self.model.table_client.upsert_entity(entity)

#     def get(self, key: str) -> dict:
#         try:
#             entity = self.model.table_client.get_entity(partition_key="default", row_key=key)
#             return {k: v for k, v in entity.items() if k not in ["partition_key", "row_key", "Timestamp", "etag"]}
#         except Exception as e:
#             return None

#     def delete(self, key: str):
#         self.model.table_client.delete_entity(partition_key="default", row_key=key)

#     def keys(self, pattern: str = '*') -> list[str]:
#         entities = self.model.table_client.list_entities()
#         return [entity['row_key'] for entity in entities if fnmatch.fnmatch(entity['row_key'], pattern)]

if mongo_back:
    from pymongo import MongoClient, database, collection
    
    class SingletonMongoDBStorage:
        _instance = None
        
        def __new__(cls, connection_string: str = "mongodb://localhost:27017/", db_name: str = "SingletonDB", collection_name: str = "store"):
            if cls._instance is None:
                cls._instance = super(SingletonMongoDBStorage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                client = MongoClient(connection_string)
                cls._instance.db = client.get_database(db_name)
                cls._instance.collection = cls._instance.db.get_collection(collection_name)
            return cls._instance

        def __init__(self, connection_string: str = "mongodb://localhost:27017/", db_name: str = "SingletonDB", collection_name: str = "store"):
            self.uuid: str = self.uuid
            self.db:database.Database = self.db
            self.collection:collection.Collection = self.collection

    class SingletonMongoDBStorageController(SingletonStorageController):
        
        def __init__(self, model: SingletonMongoDBStorage):
            self.model: SingletonMongoDBStorage = model

        def _ID_KEY(self):return '_id'

        def exists(self, key: str) -> bool:
            return self.model.collection.find_one({self._ID_KEY(): key}) is not None

        def set(self, key: str, value: dict):
            self.model.collection.update_one({self._ID_KEY(): key}, {"$set": value}, upsert=True)

        def get(self, key: str) -> dict:
            res = self.model.collection.find_one({self._ID_KEY(): key})            
            if res: del res['_id']
            return res

        def delete(self, key: str):
            self.model.collection.delete_one({self._ID_KEY(): key})

        def keys(self, pattern: str = '*') -> list[str]:
            regex = '^'+pattern.replace('*', '.*')
            return [doc['_id'] for doc in self.model.collection.find({self._ID_KEY(): {"$regex": regex}})]


class PythonDictStorage:
    def __init__(self):
        self.uuid = uuid.uuid4()
        self.store = {}
class EventDispatcherController:
    ROOT_KEY = 'Event'

    def __init__(self, client=None):
        if client is None:
            client = SingletonPythonDictStorageController(PythonDictStorage())
        self.client:SingletonStorageController = client
    
    def events(self):
        return list(zip(self.client.keys('*'),[self.client.get(k) for k in self.client.keys('*')]))

    def _find_event(self, uuid: str):
        es = self.client.keys(f'*:{uuid}')
        return [None] if len(es)==0 else es
    
    def get_event(self, uuid: str):
        return [self.client.get(k) for k in self._find_event(uuid)]
    
    def delete_event(self, uuid: str):
        return [self.client.delete(k) for k in self._find_event(uuid)]
    
    def set_event(self, event_name: str, callback, id:str=None):
        if id is None:id = uuid.uuid4()
        self.client.set(f'{EventDispatcherController.ROOT_KEY}:{event_name}:{id}', callback)
        return id
    
    def dispatch(self, event_name, *args, **kwargs):
        for event_full_uuid in self.client.keys(f'{EventDispatcherController.ROOT_KEY}:{event_name}:*'):
            self.client.get(event_full_uuid)(*args, **kwargs)

    def clean(self):
        return self.client.clean()
    

class KeysHistoryController:
    def __init__(self, client=None):
        if client is None:
            client = SingletonPythonDictStorageController(PythonDictStorage())
        self.client:SingletonStorageController = client

    def _str2base64(self,key: str):
        return base64.b64encode(key.encode())
    def reset(self):
        self.client.set('_History:',{})
    def set_history(self,key: str, result:dict):
        self.client.set(f'_History:{self._str2base64(key)}',{'result':result})
    def get_history(self,key: str):
        res = self.client.get(f'_History:{self._str2base64(key)}')
        return res.get('result',None) if res else None

class SingletonKeyValueStorage(SingletonStorageController):

    def __init__(self) -> None:
        self.python_backend()
    
    def init(self):        
        self.event_dispa = EventDispatcherController()
        self._hist_con = KeysHistoryController()
    
    def python_backend(self):
        self.init()
        self.client = SingletonPythonDictStorageController(SingletonPythonDictStorage())
    
    if firestore_back:
        def firestore_backend(self,google_project_id=None,google_firestore_collection=None):            
            self.init()
            self.client = SingletonFirestoreStorageController(SingletonFirestoreStorage(
                                                    google_project_id,google_firestore_collection))

    if redis_back:
        def redis_backend(self,redis_URL='redis://127.0.0.1:6379'):            
            self.init()
            self.client = SingletonRedisStorageController(SingletonRedisStorage(redis_URL))

    if sqlite_back:
        def sqlite_backend(self):            
            self.init()
            self.client = SingletonSqliteStorageController(SingletonSqliteStorage())

    if mongo_back:
        def mongo_backend(self):            
            self.init()
            self.client = SingletonMongoDBStorageController(SingletonMongoDBStorage())

    def add_slave(self, slave:object, event_names=['set','delete']) -> bool:
        if slave.__dict__.get('uuid',None) is None: slave.__dict__['uuid'] = uuid.uuid4()
        for m in event_names:
            if hasattr(slave, m):
                self.event_dispa.set_event(m,getattr(slave,m),slave.__dict__.get('uuid',None))
                
    def delete_slave(self, slave:object) -> bool:
        self.event_dispa.delete_event(slave.__dict__.get('uuid',None))

    def set(self, key: str, value: dict):
        self._hist_con.reset()
        res = self.client.set(key,value)
        self.event_dispa.dispatch('set',key,value)
        return res
    
    def delete(self, key: str):
        self._hist_con.reset()
        res = self.client.delete(key)
        self.event_dispa.dispatch('delete',key)
        return res
    
    def exists(self, key: str) -> bool: 
        res = self._hist_con.get_history(key)
        return res if res else self.client.exists(key)
    
    def keys(self, pattern: str='*') -> list[str]:
        res = self._hist_con.get_history(pattern)
        return res if res else self.client.keys(pattern)

    def get(self, key: str) -> dict:  return self.client.get( key)
    def clean(self):                  return self.client.clean()
    def dump(self,json_path):         return self.client.dump(json_path)
    def load(self,json_path):         return self.client.load(json_path)
    def dumps(self,):                 return self.client.dumps()
    def loads(self,json_str):         return self.client.loads(json_str)

class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs) -> None:
        super().__init__(*args,**kwargs)
        self.store = SingletonKeyValueStorage()

    def test_all(self,num=1):
        self.test_python(num)
        self.test_sqlite(num)
        self.test_mongo(num)
        # self.test_redis(num)
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

    def test_mongo(self,num=1):
        self.store.mongo_backend()
        for i in range(num):self.test_all_cases()

    def test_all_cases(self):
        self.test_set_and_get()
        self.test_exists()
        self.test_delete()
        self.test_keys()
        self.test_get_nonexistent()
        self.test_dump_and_load()
        self.test_slaves()

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
        self.assertEqual(sorted(self.store.keys('a*')), sorted(expected_keys), 
                         "Should return the correct keys matching the pattern.")

    def test_get_nonexistent(self):
        self.assertEqual(self.store.get('nonexistent'), None, "Getting a non-existent key should return None.")
        
    def test_dump_and_load(self):
        raw = {"test1": {"data": 123}, "test2": {"data": 456}, "alpha": {"info": "first"}, 
               "abeta": {"info": "second"}, "gamma": {"info": "third"}}
        self.store.dump('test.json')

        self.store.clean()
        self.assertEqual(self.store.dumps(),'{}', "Should return the correct keys and values.")

        self.store.load('test.json')
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")
        
        self.store.clean()
        self.store.loads(json.dumps(raw))
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")

    def test_slaves(self):
        if self.store.client.__class__.__name__=='SingletonPythonDictStorageController':return
        store2 = SingletonKeyValueStorage()
        self.store.add_slave(store2)
        self.store.set('alpha', {'info': 'first'})
        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        self.store.delete('abeta')
        self.assertEqual(json.loads(self.store.dumps()),json.loads(store2.dumps()), "Should return the correct keys and values.")
