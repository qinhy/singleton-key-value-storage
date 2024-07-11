
import base64
from datetime import datetime
import io
import json
import os
from typing import Any, List
from uuid import uuid4
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field

from .Storages import SingletonKeyValueStorage

def get_current_datetime_with_utc():
    return datetime.now().replace(tzinfo=ZoneInfo("UTC"))

class Controller4User:
    class AbstractObjController:
        def __init__(self, store, model):
            self.model:Model4User.AbstractObj = model
            self._store:SingletonKeyValueStorage = store

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
            self._store._store_obj(self.model)
            return self

        def delete(self):
            # self._store.delete_obj(self.model)    
            self._store.delete(self.model.id)
            self.model._controller = None

        def update_metadata(self, key, value):
            updated_metadata = {**self.model.metadata, key: value}
            self.update(metadata = updated_metadata)
            return self
        
class Model4User:
    class AbstractObj(BaseModel):
        id: str
        rank: list = [0]
        create_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        update_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        status: str = ""
        metadata: dict = {}


        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4User.AbstractObjController = None
        def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)

    class User(AbstractObj):
        id: str = Field(default_factory=lambda :f"User:{uuid4()}")
        email = None
        
        # _controller: Controller4LLM.ContentGroupController = None
        # def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        # def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)

    class App(AbstractObj):
        id: str = Field(default_factory=lambda :f"App:{uuid4()}")
        parent_App_id:int = 'auto increatment'
        running_cost = None
        major_name = None
        minor_name = None
        
        # _controller: Controller4LLM.ContentGroupController = None
        # def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        # def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)

    # class LicenseApp(AbstractObj):
    #     license_App_id:int = 'auto increatment'
    #     license_id:int = 'auto increatment'
    #     App_id:int = 'auto increatment'

    class License(AbstractObj):
        id: str = Field(default_factory=lambda :f"License:{uuid4()}")
        user_id:int = 'auto increatment'
        access_token = None
        bought_at = None
        expiration_date = None
        running_time = None
        max_running_time = None
        
        # _controller: Controller4LLM.ContentGroupController = None
        # def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        # def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)

    class AppUsage(AbstractObj):
        id: str = Field(default_factory=lambda :f"AppUsage:{uuid4()}")
        user_id:int = 'auto increatment'
        App_id:int = 'auto increatment'
        license_id:int = 'auto increatment'
        start_time = None
        end_time = None
        running_time_cost = None
        
        # _controller: Controller4LLM.ContentGroupController = None
        # def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        # def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)
