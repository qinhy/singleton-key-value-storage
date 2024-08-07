
import base64
from datetime import datetime
import io
import json
import os
import unittest
from PIL import Image
from typing import Any, List
from uuid import uuid4
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field

from Storages import SingletonKeyValueStorage

def now_utc():
    return datetime.now().replace(tzinfo=ZoneInfo("UTC"))

class Controller4Basic:
    class AbstractObjController:
        def __init__(self, store, model):
            self.model:Model4Basic.AbstractObj = model
            self._store:BasicStore = store

        def update(self, **kwargs):
            assert  self.model is not None, 'controller has null model!'
            for key, value in kwargs.items():
                if hasattr(self.model, key):
                    setattr(self.model, key, value)
            self._update_timestamp()
            self.store()

        def _update_timestamp(self):
            assert  self.model is not None, 'controller has null model!'
            self.model.update_time = now_utc()
            
        def store(self):
            assert self.model._id is not None
            self._store.set(self.model._id,self.model.model_dump_json_dict())
            return self

        def delete(self):
            self._store.delete(self.model.get_id())
            self.model._controller = None

        def update_metadata(self, key, value):
            updated_metadata = {**self.model.metadata, key: value}
            self.update(metadata = updated_metadata)
            return self
        
class Model4Basic:
    class AbstractObj(BaseModel):
        _id: str=None
        rank: list = [0]
        create_time: datetime = Field(default_factory=now_utc)
        update_time: datetime = Field(default_factory=now_utc)
        status: str = ""
        metadata: dict = {}

        def model_dump_json_dict(self):
            return json.loads(self.model_dump_json())

        def class_name(self): return self.__class__.__name__

        def set_id(self,id:str):
            assert self._id is None, 'this obj is been setted! can not set again!'
            self._id = id
            return self
        
        def gen_new_id(self): return f"{self.class_name()}:{uuid4()}"

        def get_id(self):
            assert self._id is not None, 'this obj is not setted!'
            return self._id
        
        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4Basic.AbstractObjController = None
        def get_controller(self)->Controller4Basic.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4Basic.AbstractObjController(store,self)


class BasicStore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        self.python_backend()

    def _get_class(self, id: str, modelclass=Model4Basic):
        class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in modelclass.__dict__.items() if '_' not in k]}
        res = res.get(class_type, None)
        if res is None: raise ValueError(f'No such class of {class_type}')
        return res
    
    def _get_as_obj(self,id,data_dict)->Model4Basic.AbstractObj:
        obj:Model4Basic.AbstractObj = self._get_class(id)(**data_dict)
        obj.set_id(id).init_controller(self)
        return obj
    
    
    def _add_new_obj(self, obj:Model4Basic.AbstractObj, id:str=None):
        id,d = obj.gen_new_id() if id is None else id, obj.model_dump_json_dict()
        self.set(id,d)
        return self._get_as_obj(id,d)
    
    def add_new_obj(self, obj:Model4Basic.AbstractObj, id:str=None):        
        if obj._id is not None: raise ValueError(f'obj._id is {obj._id}, must be none')
        return self._add_new_obj(obj,id)
    
    # available for regx?
    def find(self,id:str) -> Model4Basic.AbstractObj:
        raw = self.get(id)
        if raw is None:return None
        return self._get_as_obj(id,raw)
    
    def find_all(self,id:str=f'AbstractObj:*')->list[Model4Basic.AbstractObj]:
        return [self.find(k) for k in self.keys(id)]


class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs)->None:
        super().__init__(*args,**kwargs)
        self.store = BasicStore()

    def test_all(self,num=1):
        self.test_python(num)

    def test_python(self,num=1):
        self.store.python_backend()
        for i in range(num):self.test_all_cases()
        self.store.clean()

    def test_all_cases(self):
        self.store.clean()
        self.test_add_and_get()
        self.test_find_all()
        # self.test_exists()
        self.test_delete()
        self.test_get_nonexistent()
        self.test_dump_and_load()
        # self.test_slaves()

    def test_get_nonexistent(self):
        self.assertEqual(self.store.find('nonexistent'), None, "Getting a non-existent key should return None.")
        
    def test_add_and_get(self):
        obj = self.store.add_new_obj(Model4Basic.AbstractObj())
        objr = self.store.find(obj.get_id())
        self.assertEqual(obj.model_dump_json_dict(),
                        objr.model_dump_json_dict(),
                         "The retrieved value should match the set value.")
    def test_find_all(self):
        self.store.add_new_obj(Model4Basic.AbstractObj())
        self.assertEqual(len(self.store.find_all()),2,
                         "The retrieved value should match number of objs.")

    def test_dump_and_load(self):
        a = self.store.find_all()
        js = self.store.dumps()
        self.store.clean()
        self.store.loads(js)
        b = self.store.find_all()
        self.assertTrue(all([x.model_dump_json_dict()==y.model_dump_json_dict() for x,y in zip(a,b)]),
                         "The same before dumps and loads.")

    def test_delete(self):
        obj = self.store.find_all()[0]
        obj.get_controller().delete()
        self.assertFalse(self.store.exists(obj.get_id()), "Key should not exist after being deleted.")