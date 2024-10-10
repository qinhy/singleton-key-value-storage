# from https://github.com/qinhy/singleton-key-value-storage.git
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

try:
    from .Storage import SingletonKeyValueStorage,AbstractStorageController,PythonDictStorageController,PythonDictStorage
except Exception as e:
    from Storage import SingletonKeyValueStorage,AbstractStorageController,PythonDictStorageController,PythonDictStorage


def try_if_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

# self checking
sqlite_back    = True

if sqlite_back:
    class SingletonSqliteStorage:
        _instance = None
        _meta = {}

        DUMP_FILE='dump_db_file'
        LOAD_FILE='load_db_file'
               
        def __new__(cls,mode='sqlite.db'):
            if cls._instance is None:
                cls._instance = super(SingletonSqliteStorage, cls).__new__(cls)                        
                cls._instance.uuid = uuid.uuid4()
                cls._instance.client = None
                
                cls._instance.query_queue = queue.Queue()
                cls._instance.result_dict = {}
                cls._instance.lock = threading.Lock()
                cls._instance.worker_thread = threading.Thread(target=cls._instance._process_queries,args=(mode,))
                cls._instance.worker_thread.daemon = True
                cls._instance.should_stop = threading.Event()  # Use an event to signal the thread to stop
                cls._instance.worker_thread.start()
                cls._instance._execute_query("CREATE TABLE IF NOT EXISTS KeyValueStore (key TEXT PRIMARY KEY, value JSON)")
            return cls._instance

        def __init__(self,mode='sqlite.db'):
            self.uuid:str = self.uuid
            self.client:sqlite3.Connection = self.client
            self.query_queue:queue.Queue = self.query_queue 
            self.result_dict:dict = self.result_dict 
            self.lock:threading.Lock = self.lock 
            self.worker_thread:threading.Thread = self.worker_thread 
            self.should_stop:threading.Event = self.should_stop
            
        def _process_queries(self,mode=':memory:'):
            self.client = sqlite3.connect(mode)

            while not self.should_stop.is_set():  # Check if the thread should stop
                query_infos = []
                # Use get with timeout to allow periodic checks for should_stop
                # query_info = self.query_queue.get(timeout=1)
                while True:                        
                    try:
                        query_info = self.query_queue.get(timeout=1)
                        query, query_id = query_info['query'], query_info['id']                        
                        query_infos.append(query_info)
                        if query[:len('SELECT')] == 'SELECT':
                            break
                    except queue.Empty:
                        break
                if len(query_infos)==0:continue

                # print(self.result_dict)

                for query_info in query_infos:
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

        def _pop_result(self, query_id, timeout=2, wait=0.01):
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

    class SingletonSqliteStorageController(AbstractStorageController):
        def __init__(self, model: SingletonSqliteStorage):
            self.model:SingletonSqliteStorage = model

        def _execute_query(self,query):
            query_id = self.model._execute_query(query)
            return query_id

        def _execute_query_with_res(self,query):
            result = self.model._pop_result(self._execute_query(query))
            return result['result']

        def exists(self, key: str)->bool:
            query = f"SELECT EXISTS(SELECT * FROM KeyValueStore WHERE key = '{key}');"
            result = self._execute_query_with_res(query)
            return result[0]!='0'

        def set(self, key: str, value: dict)->str:
            query = f"INSERT OR REPLACE INTO KeyValueStore (key, value) VALUES ('{key}', json('{json.dumps(value)}'))"
            return self._execute_query(query)

        def get(self, key: str)->dict:
            query = f"SELECT value FROM KeyValueStore WHERE key = '{key}'"
            result = self._execute_query_with_res(query)
            if result is None:return None
            if len(result)==0:return None
            return json.loads(result[0])
        
        def delete(self, key: str)->str:
            query = f"DELETE FROM KeyValueStore WHERE key = '{key}'"
            return self._execute_query(query)

        def keys(self, pattern: str='*')->list[str]:
            pattern = pattern.replace('*', '%').replace('?', '_')  # Translate fnmatch pattern to SQL LIKE pattern
            query = f"SELECT key FROM KeyValueStore WHERE key LIKE '{pattern}'"
            result = self._execute_query_with_res(query)
            return result
                
        def is_working(self): return not self.model.query_queue.empty()

    class SingletonSqlitePythonMixStorageController(AbstractStorageController):
        def __init__(self, model: SingletonSqliteStorage):
            self.disk = SingletonSqliteStorageController(model)
            self.memory = PythonDictStorageController(PythonDictStorage())
            for k in self.disk.keys():self.memory.set(k,{})

        def exists(self, key: str)->bool:
            return self.memory.exists(key)

        def set(self, key: str, value: dict):
            self.disk.set(key,value)
            self.memory.set(key,value)

        def get(self, key: str) -> dict:
            if not self.memory.exists(key):
                return None
            
            value = self.memory.get(key)
            if len(value)==0:
                value = self.disk.get(key)
                if value:
                    self.memory.set(key,value)
                return value
            else:
                return value

        def delete(self, key: str):
            self.disk.delete(key)
            self.memory.delete(key)

        def keys(self, pattern: str='*')->list[str]:
            return self.memory.keys(pattern)
        
        def is_working(self): return self.disk.is_working()

SingletonKeyValueStorage.backs['sqlite']=lambda *args,**kwargs:SingletonSqliteStorageController(SingletonSqliteStorage(*args,**kwargs)) if sqlite_back else None
SingletonKeyValueStorage.backs['sqlite_pymix']=lambda *args,**kwargs:SingletonSqlitePythonMixStorageController(SingletonSqliteStorage(*args,**kwargs)) if sqlite_back else None

