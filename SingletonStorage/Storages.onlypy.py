
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

class SingletonStorageController:
    def __init__(self, model):
        self.model:object = model

    def exists(self, key: str)->bool: print(f'[{self.__class__.__name__}]: not implement')

    def set(self, key: str, value: dict): print(f'[{self.__class__.__name__}]: not implement')

    def get(self, key: str)->dict: print(f'[{self.__class__.__name__}]: not implement')

    def delete(self, key: str): print(f'[{self.__class__.__name__}]: not implement')

    def keys(self, pattern: str='*')->list[str]: print(f'[{self.__class__.__name__}]: not implement')
    
    def clean(self): [self.delete(k) for k in self.keys('*')]

    def dumps(self): return json.dumps({k:self.get(k) for k in self.keys('*')})
    
    def loads(self, json_string=r'{}'): [ self.set(k,v) for k,v in json.loads(json_string).items()]

    def dump(self,path):
        with open(path, "w") as tf: tf.write(self.dumps())

    def load(self,path):
        with open(path, "r") as tf: self.loads(tf.read())

class PythonDictStorage:
    def __init__(self):
        self.uuid = uuid.uuid4()
        self.store = {}

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

    def exists(self, key: str)->bool: return key in self.model.store

    def set(self, key: str, value: dict): self.model.store[key] = value

    def get(self, key: str)->dict: return self.model.store.get(key,None)

    def delete(self, key: str):
        if key in self.model.store:     
            del self.model.store[key]

    def keys(self, pattern: str='*')->list[str]:
        return fnmatch.filter(self.model.store.keys(), pattern)

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
        return base64.b64encode(key.encode()).decode()
    def reset(self):
        self.client.set('_History:',{})
    def set_history(self,key: str, result:dict):
        if result:
            self.client.set(f'_History:{self._str2base64(key)}',{'result':result})
        return result
    
    def get_history(self,key: str):
        res = self.client.get(f'_History:{self._str2base64(key)}')
        return res.get('result',None) if res else None

    def try_history(self,key: str, result_func=lambda :None):
        res = self.get_history(key)
        if res is None:
            res = result_func()
            if res : self.set_history(key,res)
        return res

class SingletonKeyValueStorage(SingletonStorageController):

    def __init__(self)->None:
        self.conn:SingletonStorageController = None
        self.python_backend()
    
    def init(self):        
        self.event_dispa = EventDispatcherController()
        self._hist = KeysHistoryController()
    
    def python_backend(self):
        self.init()
        self.conn = SingletonPythonDictStorageController(SingletonPythonDictStorage())
    
    def add_slave(self, slave:object, event_names=['set','delete'])->bool:
        if slave.__dict__.get('uuid',None) is None: slave.__dict__['uuid'] = uuid.uuid4()
        for m in event_names:
            if hasattr(slave, m):
                self.event_dispa.set_event(m,getattr(slave,m),slave.__dict__.get('uuid',None))
                
    def delete_slave(self, slave:object)->bool:
        self.event_dispa.delete_event(slave.__dict__.get('uuid',None))

    def _edit(self,func_name:str, key:str, value:dict=None):
        self._hist.reset()
        func = getattr(self.conn,func_name)
        args = [key,value] if value else [key] 
        res = func(*args)
        self.event_dispa.dispatch(func_name,*args)
        return res

    def set(self, key: str, value: dict): return self._edit('set',key,value)
    
    def delete(self, key: str): return self._edit('delete',key)
    
    def exists(self, key: str)->bool: return self._hist.try_history(key, lambda:self.conn.exists(key))
    
    def keys(self, regx: str='*')->list[str]: return self._hist.try_history(regx, lambda:self.conn.keys(regx))

    def get(self, key: str)->dict:    return self.conn.get( key)
    def clean(self):                  return self.conn.clean()
    def dump(self,json_path):         return self.conn.dump(json_path)
    def load(self,json_path):         return self.conn.load(json_path)
    def dumps(self,):                 return self.conn.dumps()
    def loads(self,json_str):         return self.conn.loads(json_str)

class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs)->None:
        super().__init__(*args,**kwargs)
        self.store = SingletonKeyValueStorage()

    def test_all(self,num=1):
        self.test_python(num)

    def test_python(self,num=1):
        self.store.python_backend()
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
        if self.store.conn.__class__.__name__=='SingletonPythonDictStorageController':return
        store2 = SingletonKeyValueStorage()
        self.store.add_slave(store2)
        self.store.set('alpha', {'info': 'first'})
        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        self.store.delete('abeta')
        self.assertEqual(json.loads(self.store.dumps()),json.loads(store2.dumps()), "Should return the correct keys and values.")
