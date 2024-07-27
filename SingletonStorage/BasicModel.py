
import base64
from datetime import datetime
import io
import json
import os
from PIL import Image
from typing import Any, List
from uuid import uuid4
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field

from Storages import SingletonKeyValueStorage

def get_current_datetime_with_utc():
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
            self.model.update_time = get_current_datetime_with_utc()
            
        def store(self):
            assert self.model._id is not None
            self._store.set(self.model._id,
                            json.loads(self.model.model_dump_json()))
            return self

        def delete(self):
            # self._store.delete_obj(self.model)    
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
        create_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        update_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        status: str = ""
        metadata: dict = {}

        def class_name(self): return self.__class__.__name__

        def set_id(self,id:str):
            self._id = id
            return self
        
        def gen_new_id(self): return self.set_id(f"{self.class_name()}:{uuid4()}")

        def get_id(self): return self._id
        
        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4Basic.AbstractObjController = None
        def get_controller(self)->Controller4Basic.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4Basic.AbstractObjController(store,self)


class BasicStore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        self.python_backend()

    def _get_class(self, id: str, modelclass=Model4Basic):
        class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in modelclass.__dict__.items() if '_' not in k]}.get(class_type, None)
        if res is None: raise ValueError(f'No such class of {class_type}')
        return res
       
    def _store_new_obj(self, obj:Model4Basic.AbstractObj):
        id = obj.gen_new_id()
        self.set(id,json.loads(obj.model_dump_json()))
        return self._get_as_obj(id,json.loads(obj.model_dump_json()))
        
    # def add_new_obj(self,name, role, rank:list=[0], metadata={}) -> Model4Basic.Author:
    #     auther = self._store_new_obj(Model4Basic.Author(name=name, role=role, rank=rank, metadata=metadata))
    #     auther.init_controller(self,auther)
    #     return auther
    
    def _get_as_obj(self,id,data_dict)->Model4Basic.AbstractObj:
        obj:Model4Basic.AbstractObj = self._get_class(id)(**data_dict)
        obj.set_id(id).init_controller(self)
        return obj
        
    # available for regx?
    def find(self,id:str) -> Model4Basic.AbstractObj:
        return self._get_as_obj(id,self.get(id))
    
    def find_all(self,id:str=f'AbstractObj:*')->list[Model4Basic.AbstractObj]:
        results = [self.find(k) for k in self.keys(id)]
        return results