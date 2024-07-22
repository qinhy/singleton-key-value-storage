
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
            self._store:UsersStore = store

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
        name:str
        role:str
        email:str=None
        
        # _controller: Controller4LLM.ContentGroupController = None
        # def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        # def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)

    class App(AbstractObj):
        id: str = Field(default_factory=lambda :f"App:{uuid4()}")
        parent_App_id:int = 'auto increatment'
        running_cost:int = 0
        major_name:str = None
        minor_name:str = None
        
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
        access_token:str = None
        bought_at:datetime = None
        expiration_date:datetime = None
        running_time:int = 0
        max_running_time:int = 0
        
        # _controller: Controller4LLM.ContentGroupController = None
        # def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        # def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)

    class AppUsage(AbstractObj):
        id: str = Field(default_factory=lambda :f"AppUsage:{uuid4()}")
        user_id:int = 'auto increatment'
        App_id:int = 'auto increatment'
        license_id:int = 'auto increatment'
        start_time:datetime = None
        end_time:datetime = None
        running_time_cost:int = 0
        
        # _controller: Controller4LLM.ContentGroupController = None
        # def get_controller(self)->Controller4User.AbstractObjController: return self._controller
        # def init_controller(self,store):self._controller = Controller4User.AbstractObjController(store,self)

class UsersStore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        self.python_backend()
        
    def _client(self):
        return self.client
    
    def get_class(self, id: str):
        class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in Model4User.__dict__.items() if '_' not in k]}.get(class_type, None)
        if res is None:
            raise ValueError(f'No such class of {class_type}')
        return res
       
    def _store_obj(self, obj:Model4User.AbstractObj):
        self.set(obj.id,json.loads(obj.model_dump_json()))
        return obj
    
    def _init_controller(self,obj:Model4User.AbstractObj):
        obj.init_controller(self,obj)
        return obj

    def add_new_user(self, name, role, rank:list=[0], metadata={}) -> Model4User.User:
        return self._init_controller(
            self._store_obj(Model4User.User(name=name, role=role, rank=rank, metadata=metadata))
        )
    def add_new_app(self, major_name:str,minor_name:str,running_cost:int=0,parent_App_id:str=None) -> Model4User.App:
        return self._init_controller(
            self._store_obj(Model4User.App(major_name=major_name,minor_name=minor_name,
                                           running_cost=running_cost,parent_App_id=parent_App_id))
        )
    def add_new_license(self) -> Model4User.License:
        return self._init_controller(
            self._store_obj(Model4User.License())
        )
    def add_new_appUsage(self) -> Model4User.AppUsage:
        return self._init_controller(
            self._store_obj(Model4User.AppUsage())
        )

    # def add_new_root_group(self,metadata={},rank=[0]) -> Model4User.ContentGroup:
    #     group = self._store_obj( Model4User.ContentGroup(rank=rank, metadata=metadata) )         
    #     group.init_controller(self,group)
    #     return group
    
    # def _add_new_content_to_group(self,group:Model4User.ContentGroup,content:Model4User.AbstractContent,raw:str=None):
    #     group.children_id.append(content.id)
    #     self._store_obj(group)
    #     if raw is not None and 'ContentGroup' not in content.id:
    #         self._store_obj(content)
    #         self._store_obj(Model4User.CommonData(id=content.data_id(), raw=raw))
    #     else:
    #         self._store_obj(content)
    #     content.init_controller(self)
    #     return group,content    

    # def read_image(self, filepath):
    #     with open(filepath, "rb") as f:
    #         return f.read()
        
    # def b64encode(self, file_bytes):
    #     return base64.b64encode(file_bytes)
        
    # def encode_image(self, image_bytes):
    #     return self.b64encode(image_bytes)
    
    # def add_new_group_to_group(self,group:Model4User.ContentGroup,metadata={},rank=[0]):
    #     parent,child = self._add_new_content_to_group(group, Model4User.ContentGroup(rank=rank, metadata=metadata, parent_id=group.id))
    #     return parent,child

    # def add_new_text_to_group(self,group:Model4User.ContentGroup,user_id:str,text:str):
    #     parent,child = self._add_new_content_to_group(group,
    #                                                   Model4User.TextContent(user_id=user_id, group_id=group.id),
    #                                                   raw=text)
    #     return parent,child
    
    # def add_new_embedding_to_group(self,group:Model4User.ContentGroup, user_id:str, content_id:str, vec:list[float]):
    #     parent,child = self._add_new_content_to_group(group,
    #                                                   Model4User.EmbeddingContent(user_id=user_id, 
    #                                                                    group_id=group.id,target_id=content_id),
    #                                                   raw=str(vec))
    #     return parent,child
    
    # def add_new_image_to_group(self,group:Model4User.ContentGroup,user_id:str, filepath:str):
    #     raw_bytes = self.read_image(filepath)
    #     raw_base64 = self.encode_image(raw_bytes)
    #     parent,child = self._add_new_content_to_group(group,
    #                                                   Model4User.ImageContent(user_id=user_id,group_id=group.id),
    #                                                   raw=raw_base64)
    #     return parent,child
    
    # available for regx?
    def find(self,id:str) -> Model4User.AbstractObj:
        return self._init_controller(self.get_class(id)(**self.get(id)))
    
    def find_all(self,id:str=f'User:*')->list[Model4User.AbstractObj]:
        return [self.find(key) for key in self.keys(id)]
    
    def find_all_users(self)->list[Model4User.User]:
        return self.find_all('User:*')
    