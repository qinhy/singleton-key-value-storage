# from https://github.com/qinhy/singleton-key-value-storage.git
import base64
import uuid
import fnmatch
import json
import unittest

def try_if_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

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
        data = self.dumps()
        with open(path, "w") as tf: tf.write(data)
        return data

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
        self.client = SingletonPythonDictStorageController(PythonDictStorage())        
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

class LocalVersionController:
    def __init__(self,client=None):
        if client is None:
            client = SingletonPythonDictStorageController(PythonDictStorage())
        self.client:SingletonStorageController = client
        self.client.set(f'_Operations',{'ops':[]})
    
    def add_operation(self,operation:tuple,revert:tuple=None):
        opuuid = str(uuid.uuid4())
        self.client.set(f'_Operation:{opuuid}',{'forward':operation,'revert':revert})
        ops = self.client.get(f'_Operations')
        ops['ops'].append(opuuid)
        self.client.set(f'_Operations',ops)
    
    def revert_one_operation(self,revert_callback:lambda revert:None):
        ops:list = self.client.get(f'_Operations')['ops']
        opuuid = ops[-1]
        op = self.client.get(f'_Operation:{opuuid}')
        revert = op['revert']        
        # do revert
        revert_callback(revert)
        ops.pop()
        self.client.set(f'_Operations',{'ops':ops})
    
    def get_versions(self):
        return self.client.get(f'_Operations')['ops']

    def revert_operations_untill(self,opuuid:str,revert_callback:lambda revert:None):
        ops = [i for i in self.client.get(f'_Operations')['ops']]
        if opuuid in ops:
            for i in ops[::-1]:
                if i==opuuid:break
                self.revert_one_operation(revert_callback)
        else:
            raise ValueError(f'no such version of {opuuid}')

class SingletonKeyValueStorage(SingletonStorageController):

    def __init__(self)->None:
        self.conn:SingletonStorageController = None
        self.python_backend()
    
    def _switch_backend(self,name:str='python',*args,**kwargs):
        self.event_dispa = EventDispatcherController()
        self._hist = KeysHistoryController()
        self._verc = LocalVersionController()
        backs={
            'python':lambda:SingletonPythonDictStorageController(SingletonPythonDictStorage(*args,**kwargs)),
        }
        back=backs.get(name.lower(),lambda:None)()
        if back is None:raise ValueError(f'no back end of {name}, has {list(backs.items())}')
        return back
    
    def python_backend(self):
        self.conn = self._switch_backend('python')
    
    def _print(self,msg):
        print(f'[{self.__class__.__name__}]: {msg}')

    def add_slave(self, slave:object, event_names=['set','delete'])->bool:
        if getattr(slave,'uuid',None) is None:
            try:
                setattr(slave,'uuid',uuid.uuid4())
            except Exception:
                self._print(f'can not set uuid to {slave}. Skip this slave.')
                return
        for m in event_names:
            if hasattr(slave, m):
                self.event_dispa.set_event(m,getattr(slave,m),getattr(slave,'uuid'))
            else:
                self._print(f'no func of "{m}" in {slave}. Skip it.')
                
    def delete_slave(self, slave:object)->bool:
        self.event_dispa.delete_event(getattr(slave,'uuid',None))

    def _edit(self,func_name:str, key:str=None, value:dict=None):
        if func_name not in ['set','delete','clean','load','loads']:
            self._print(f'no func of "{func_name}". return.')
            return
        self._hist.reset()
        func = getattr(self.conn, func_name)
        args = list(filter(lambda x:x is not None, [key,value]))
        res = func(*args)
        self.event_dispa.dispatch(func_name,*args)
        return res
    
    def _try_edit_error(self,args):

        # do local version controll
        func = args[0]
        if func == 'set':
            func,key,value =args
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
        self._verc.revert_one_operation(lambda revert:self._edit(*revert))

    def get_current_version(self):
        vs = self._verc.get_versions()
        if len(vs)==0:
            return None
        return vs[-1]

    def revert_operations_untill(self,opuuid:str):
        self._verc.revert_operations_untill(opuuid,lambda revert:self._edit(*revert))

    # True False(in error)
    def set(self, key: str, value: dict):     return self._try_edit_error(('set',key,value))
    def delete(self, key: str):               return self._try_edit_error(('delete',key))
    def clean(self):                          return self._try_edit_error(('clean',))
    def load(self,json_path):                 return self._try_edit_error(('load', json_path))
    def loads(self,json_str):                 return self._try_edit_error(('loads',json_str))
    
    def _try_obj_error(self,func):
        try:
            return func()
        except Exception as e:
            self._print(e)
            return None
    # Object, None(in error)
    
    def exists(self, key: str)->bool:         return self._try_obj_error(lambda:self.conn.exists(key))
    def keys(self, regx: str='*')->list[str]: return self._try_obj_error(lambda:self.conn.keys(regx))
    def get(self, key: str)->dict:            return self._try_obj_error(lambda:self.conn.get(key))
    def dumps(self)->str:                     return self._try_obj_error(lambda:self.conn.dumps())
    def dump(self,json_path)->str:            return self._try_obj_error(lambda:self.conn.dump(json_path))

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
        self.test_version()
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

    def test_version(self):
        self.store.clean()
        self.store.set('alpha', {'info': 'first'})
        data = self.store.dumps()
        version = self.store.get_current_version()

        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        self.store.revert_operations_untill(version)

        self.assertEqual(json.loads(self.store.dumps()),json.loads(data), "Should return the same keys and values.")