
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

from SingletonStorage.Storages import SingletonKeyValueStorage

def get_current_datetime_with_utc():
    return datetime.now().replace(tzinfo=ZoneInfo("UTC"))

class Controller4LLM:
    class AbstractObjController:
        def __init__(self, store, model):
            self.model:Model4LLM.AbstractObj = model
            self._store:LLMstore = store

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

    class CommonDataController(AbstractObjController):
        def __init__(self, store, model):
            self.model: Model4LLM.CommonData = model
            self._store:LLMstore = store

    class AuthorController(AbstractObjController):
        def __init__(self, store ,model):
            self.model: Model4LLM.Author = model
            self._store:LLMstore = store

    class AbstractContentController(AbstractObjController):
        def __init__(self, store, model):
            self.model: Model4LLM.AbstractContent = model
            self._store:LLMstore = store

        def data_id(self):
            return f"CommonData:{self.model.id}"

        def delete(self):
            self.get_data()._controller.delete()        
            # self._store.delete_obj(self.model)        
            self._store.delete(self.model.id)
            self.model._controller = None

        def get_author(self):
            author:Model4LLM.Author = self._store.find(self.model.author_id)
            return author

        def get_group(self):
            res:Model4LLM.ContentGroup = self._store.find(self.model.group_id)
            return res
        
        def get_data(self):
            res:Model4LLM.CommonData = self._store.find(self.data_id())
            return res

        def get_data_raw(self):
            return self.get_data().raw    

        def update_data_raw(self, msg: str):
            self.get_data()._controller.update(raw = msg)
            return self

        def append_data_raw(self, msg: str):
            data = self.get_data()
            data._controller.update(raw = data.raw + msg)
            return self
        
    class AbstractGroupController(AbstractObjController):
        def __init__(self, store, model):
            self.model:Model4LLM.AbstractGroup = model
            self._store:LLMstore = store

        def yield_children_content_recursive(self, depth: int = 0):
            for child_id in self.model.children_id:
                if not self._store.exists(child_id):
                    continue
                content:Model4LLM.AbstractObj = self._store.find(child_id)
                yield content, depth
                if child_id.startswith('ContentGroup'):
                    group:Controller4LLM.AbstractGroupController = content._controller
                    for cc, d in group.yield_children_content_recursive(depth + 1):
                        yield cc, d

        def delete_recursive_from_keyValue_storage(self):
            for c, d in self.yield_children_content_recursive():
                c._controller.delete()
            self.delete()

        def get_children_content(self):
            # self.load()
            assert  self.model is not None, 'controller has null model!'
            results:List[Model4LLM.AbstractObj] = []
            for child_id in self.model.children_id:
                results.append(self._store.find(child_id))
            return results

        def get_child_content(self, child_id: str):
            res:Model4LLM.AbstractContent = self._store.find(child_id)
            return res

        def prints(self):
            res = '########################################################\n'
            for content, depth in self.yield_children_content_recursive():
                res += f"{'    ' * depth}{content.id}\n"
            res += '########################################################\n'
            print(res)
            return res

    class ContentGroupController(AbstractGroupController):
        def __init__(self, store, model):
            self.model:Model4LLM.ContentGroup = model
            self._store:LLMstore = store

        def add_new_child_group(self,metadata={},rank=[0]):
            parent,child = self._store.add_new_group_to_group(group=self.model,metadata=metadata,rank=rank)
            return child

        def add_new_text_content(self, author_id:str, text:str):
            parent,child = self._store.add_new_text_to_group(group=self.model,author_id=author_id,
                                                    text=text)                             
            return child
        
        def add_new_embeding_content(self, author_id:str, content_id:str, vec:list[float]):
            parent,child = self._store.add_new_embedding_to_group(group=self.model,author_id=author_id,
                                                        content_id=content_id, vec=vec)                                   
            return child
        
        def add_new_image_content(self,author_id:str, filepath:str):
            parent,child = self._store.add_new_image_to_group(group=self.model,author_id=author_id,
                                                    filepath=filepath)                              
            return child
            

        def remove_child(self, child_id:str):
            remaining_ids = [cid for cid in self.model.children_id if cid != child_id]
            for content in self.get_children_content():
                if content._controller.model.id == child_id:
                    if child_id.startswith('ContentGroup'):
                        group:Controller4LLM.ContentGroupController = content._controller
                        group.delete_recursive_from_keyValue_storage()
                    content._controller.delete()
                    break
            self.update(children_id = remaining_ids)
            return self

        def get_children_content_recursive(self):
            results:list[Model4LLM.AbstractContent] = []
            for c, d in self.yield_children_content_recursive():
                results.append(c)
            return results

    class TextContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model:Model4LLM.TextContent = model
            self._store:LLMstore = store


    class EmbeddingContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.EmbeddingContent = model
            self._store:LLMstore = store

        def get_data_raw(self):
            return list(map(float,super().get_data_raw()[1:-1].split(',')))
        
        def get_data_rLOD0(self):
            return self.get_data_raw()[::10**(0+1)]
        
        def get_data_rLOD1(self):
            return self.get_data_raw()[::10**(1+1)]
        
        def get_data_rLOD2(self):
            return self.get_data_raw()[::10**(2+1)]
        
        def get_target(self):
            assert  self.model is not None, 'controller has null model!'
            target_id = self.model.target_id
            res:Model4LLM.AbstractContent = self._store.find(target_id)
            return res
        
        def update_data_raw(self, embedding: list[float]):
            super().update_data_raw(str(embedding))
            return self

    class FileLinkContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.FileLinkContent = model
            self._store:LLMstore = store

    class BinaryFileContentController(AbstractContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.BinaryFileContent = model
            self._store:LLMstore = store
            
        def read_bytes(self, filepath):
            with open(filepath, "rb") as f:
                return f.read()
            
        def b64decode(self, file_base64):
            return base64.b64decode(file_base64)
            
        def get_data_rLOD0(self):
            raise ValueError('binary file has no LOD concept')
        
        def get_data_rLOD1(self):
            raise ValueError('binary file has no LOD concept')
        
        def get_data_rLOD2(self):
            raise ValueError('binary file has no LOD concept')
        
    class ImageContentController(BinaryFileContentController):
        def __init__(self, store, model):
            self.model: Model4LLM.ImageContent = model    
            self._store:LLMstore = store

        def decode_image(self, encoded_string):
            return Image.open(io.BytesIO(self.b64decode(encoded_string)))
        
        def get_image(self):
            encoded_image = self.get_data_raw()
            if encoded_image:
                image = self.decode_image(encoded_image)
                return image
            return None
                    
        def get_image_format(self):
            image = self.get_image()
            return image.format if image else None
        
        def get_data_rLOD(self,lod=0):
            image = self.get_image()
            ratio = 10**(lod+1)
            if image.size[0]//ratio==0 or image.size[1]//ratio ==0:
                raise ValueError(f'img size({image.size}) of LOD{lod} is smaller than 0')
            return image.resize((image.size[0]//ratio,image.size[1]//ratio)) if image else None

        def get_data_rLOD0(self):
            return self.get_data_rLOD(lod=0)
        
        def get_data_rLOD1(self):
            return self.get_data_rLOD(lod=1)
        
        def get_data_rLOD2(self):
            return self.get_data_rLOD(lod=2)
    
class Model4LLM:
    class AbstractObj(BaseModel):
        id: str
        rank: list = [0]
        create_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        update_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        status: str = ""
        metadata: dict = {}
        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4LLM.AbstractObjController = None

        def get_controller(self)->Controller4LLM.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AbstractObjController(store,self)
    class CommonData(AbstractObj):
        raw: str = ''
        rLOD0: str = ''
        rLOD1: str = ''
        rLOD2: str = ''
        _controller: Controller4LLM.CommonDataController = None
        
        def get_controller(self)->Controller4LLM.CommonDataController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.CommonDataController(store,self)
    class Author(AbstractObj):
        id: str = Field(default_factory=lambda :f"Author:{uuid4()}")
        name: str = ''
        role: str = ''
        _controller: Controller4LLM.AuthorController = None
        
        def get_controller(self)->Controller4LLM.AuthorController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AuthorController(store,self)
    class AbstractContent(AbstractObj):
        author_id: str=''
        group_id: str=''
        _controller: Controller4LLM.AbstractContentController = None
        
        def get_controller(self)->Controller4LLM.AbstractContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AbstractContentController(store,self)

        def data_id(self):return f"CommonData:{self.id}"
    class AbstractGroup(AbstractObj):
        author_id: str=''
        parent_id: str = ''
        children_id: List[str] = []
        _controller: Controller4LLM.AbstractGroupController = None
        
        def get_controller(self)->Controller4LLM.AbstractGroupController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.AbstractGroupController(store,self)
    class ContentGroup(AbstractGroup):
        id: str = Field(default_factory=lambda :f"ContentGroup:{uuid4()}")
        _controller: Controller4LLM.ContentGroupController = None
        
        def get_controller(self)->Controller4LLM.ContentGroupController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.ContentGroupController(store,self)
    class TextContent(AbstractContent):
        id: str = Field(default_factory=lambda :f"TextContent:{uuid4()}")
        _controller: Controller4LLM.TextContentController = None
        
        def get_controller(self)->Controller4LLM.TextContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.TextContentController(store,self)

    class EmbeddingContent(AbstractContent):
        id: str = Field(default_factory=lambda :f"EmbeddingContent:{uuid4()}")
        _controller: Controller4LLM.EmbeddingContentController = None
        
        def get_controller(self)->Controller4LLM.EmbeddingContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.EmbeddingContentController(store,self)
        target_id: str
    class FileLinkContent(AbstractContent):
        id: str = Field(default_factory=lambda :f"FileLinkContent:{uuid4()}")
        _controller: Controller4LLM.FileLinkContentController = None
        
        def get_controller(self)->Controller4LLM.FileLinkContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.FileLinkContentController(store,self)
    class BinaryFileContent(AbstractContent):
        id: str = Field(default_factory=lambda :f"BinaryFileContent:{uuid4()}")
        _controller: Controller4LLM.BinaryFileContentController = None
        
        def get_controller(self)->Controller4LLM.BinaryFileContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.BinaryFileContentController(store,self)
            
    class ImageContent(BinaryFileContent):
        id: str = Field(default_factory=lambda :f"ImageContent:{uuid4()}")
        _controller: Controller4LLM.ImageContentController = None
        
        def get_controller(self)->Controller4LLM.ImageContentController: return self._controller
        def init_controller(self,store):self._controller = Controller4LLM.ImageContentController(store,self)

class LLMstore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        self.python_backend()
        
    def _client(self):
        return self.client
    
    def get_class(self, id: str):
        class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in Model4LLM.__dict__.items() if '_' not in k]}.get(class_type, None)
        if res is None:
            raise ValueError(f'No such class of {class_type}')
        return res
       
    def _store_obj(self, obj:Model4LLM.AbstractObj):
        self.set(obj.id,json.loads(obj.model_dump_json()))
        return obj
        
    def add_new_author(self,name, role, rank:list=[0], metadata={}) -> Model4LLM.Author:
        auther = self._store_obj(Model4LLM.Author(name=name, role=role, rank=rank, metadata=metadata))
        auther.init_controller(self,auther)
        return auther
    
    def add_new_root_group(self,metadata={},rank=[0]) -> Model4LLM.ContentGroup:
        group = self._store_obj( Model4LLM.ContentGroup(rank=rank, metadata=metadata) )         
        group.init_controller(self,group)
        return group
    
    def _add_new_content_to_group(self,group:Model4LLM.ContentGroup,content:Model4LLM.AbstractContent,raw:str=None):
        group.children_id.append(content.id)
        self._store_obj(group)
        if raw is not None and 'ContentGroup' not in content.id:
            self._store_obj(content)
            self._store_obj(Model4LLM.CommonData(id=content.data_id(), raw=raw))
        else:
            self._store_obj(content)
        content.init_controller(self)
        return group,content    

    def read_image(self, filepath):
        with open(filepath, "rb") as f:
            return f.read()
        
    def b64encode(self, file_bytes):
        return base64.b64encode(file_bytes)
        
    def encode_image(self, image_bytes):
        return self.b64encode(image_bytes)
    
    def add_new_group_to_group(self,group:Model4LLM.ContentGroup,metadata={},rank=[0]):
        parent,child = self._add_new_content_to_group(group, Model4LLM.ContentGroup(rank=rank, metadata=metadata, parent_id=group.id))
        return parent,child

    def add_new_text_to_group(self,group:Model4LLM.ContentGroup,author_id:str,text:str):
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.TextContent(author_id=author_id, group_id=group.id),
                                                      raw=text)
        return parent,child
    
    def add_new_embedding_to_group(self,group:Model4LLM.ContentGroup, author_id:str, content_id:str, vec:list[float]):
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.EmbeddingContent(author_id=author_id, 
                                                                       group_id=group.id,target_id=content_id),
                                                      raw=str(vec))
        return parent,child
    
    def add_new_image_to_group(self,group:Model4LLM.ContentGroup,author_id:str, filepath:str):
        raw_bytes = self.read_image(filepath)
        raw_base64 = self.encode_image(raw_bytes)
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.ImageContent(author_id=author_id,group_id=group.id),
                                                      raw=raw_base64)
        return parent,child
    
    # available for regx?
    def find(self,id:str) -> Model4LLM.AbstractObj:
        data_dict = self.get(id)
        obj:Model4LLM.AbstractObj = self.get_class(id)(**data_dict)
        obj.init_controller(self)
        return obj
    
    def find_group(self,id:str) -> Model4LLM.ContentGroup:
        assert 'Group:' in id, 'this id is not a id for goup'
        data_dict = self.get(id)
        obj:Model4LLM.ContentGroup = self.get_class(id)(**data_dict)
        obj.init_controller(self)
        return obj
    
    def find_all(self,id:str=f'Author:*'):
        keys = [key for key in self.keys(id)]
        results:list[Model4LLM.AbstractObj] = []
        for key in keys:
            obj = self.find(key)
            results.append(obj)
        return results
    
    def find_all_authors(self):
        results:list[Model4LLM.Author] = self.find_all('Author:*')
        return results