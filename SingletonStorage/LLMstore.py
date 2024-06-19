
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

class AbstractObjController:
    def __init__(self, store, model):
        self.model:AbstractObj = model
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
        self._store.delete_obj(self.model)

    def update_metadata(self, key, value):
        updated_metadata = {**self.model.metadata, key: value}
        self.update(metadata = updated_metadata)
        return self

class CommonDataController(AbstractObjController):
    def __init__(self, store, model):
        self.model: CommonData = model
        self._store:LLMstore = store

class AuthorController(AbstractObjController):
    def __init__(self, store ,model):
        self.model: Author = model
        self._store:LLMstore = store

class AbstractContentController(AbstractObjController):
    def __init__(self, store, model):
        self.model: AbstractContent = model
        self._store:LLMstore = store

    def data_id(self):
        return f"CommonData:{self.model.id}"

    def delete(self):
        self.get_data().controller.delete()        
        self._store.delete_obj(self.model)

    def get_author(self):
        author:Author = self._store.find(self.model.author_id)
        return author

    def get_group(self):
        res:ContentGroup = self._store.find(self.model.group_id)
        return res
    
    def get_data(self):
        res:CommonData = self._store.find(self.data_id())
        return res

    def get_data_raw(self):
        return self.get_data().raw    

    def update_data_raw(self, msg: str):
        self.get_data().controller.update(raw = msg)
        return self

    def append_data_raw(self, msg: str):
        data = self.get_data()
        data.controller.update(raw = data.raw + msg)
        return self
    
class AbstractGroupController(AbstractObjController):
    def __init__(self, store, model):
        self.model:AbstractGroup = model
        self._store:LLMstore = store

    def yield_children_content_recursive(self, depth: int = 0):
        for child_id in self.model.children_id:
            if not self._store.exists(child_id):
                continue
            content:AbstractObj = self._store.find(child_id)
            yield content, depth
            if child_id.startswith('ContentGroup'):
                group:AbstractGroupController = content.controller
                for cc, d in group.yield_children_content_recursive(depth + 1):
                    yield cc, d

    def delete_recursive_from_keyValue_storage(self):
        for c, d in self.yield_children_content_recursive():
            c.controller.delete()
        self.delete()

    def get_children_content(self):
        # self.load()
        assert  self.model is not None, 'controller has null model!'
        results:List[AbstractObj] = []
        for child_id in self.model.children_id:
            results.append(self._store.find(child_id))
        return results

    def get_child_content(self, child_id: str):
        res:AbstractContent = self._store.find(child_id)
        return res

    def prints(self):
        print('########################################################')
        for content, depth in self.yield_children_content_recursive():
            print(f"{'    ' * depth}{content.id}")
        print('########################################################')

class ContentGroupController(AbstractGroupController):
    def __init__(self, store, model):
        self.model:ContentGroup = model
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
            if content.controller.model.id == child_id:
                if child_id.startswith('ContentGroup'):
                    group:ContentGroupController = content.controller
                    group.delete_recursive_from_keyValue_storage()
                content.controller.delete()
                break
        self.update(children_id = remaining_ids)
        return self

    def get_children_content_recursive(self):
        results:list[AbstractContent] = []
        for c, d in self.yield_children_content_recursive():
            results.append(c)
        return results

class TextContentController(AbstractContentController):
    def __init__(self, store, model):
        self.model:TextContent = model
        self._store:LLMstore = store


class EmbeddingContentController(AbstractContentController):
    def __init__(self, store, model):
        self.model: EmbeddingContent = model
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
        res:AbstractContent = self._store.find(target_id)
        return res
    
    def update_data_raw(self, embedding: list[float]):
        super().update_data_raw(str(embedding))
        return self

class FileLinkContentController(AbstractContentController):
    def __init__(self, store, model):
        self.model: FileLinkContent = model
        self._store:LLMstore = store

class BinaryFileContentController(AbstractContentController):
    def __init__(self, store, model):
        self.model: BinaryFileContent = model
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
        self.model: ImageContent = model    
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
    
class AbstractObj(BaseModel):
    id: str
    rank: list = [0]
    create_time: datetime = Field(default_factory=get_current_datetime_with_utc)
    update_time: datetime = Field(default_factory=get_current_datetime_with_utc)
    status: str = ""
    metadata: dict = {}
    controller: AbstractObjController = None
    model_config = ConfigDict(arbitrary_types_allowed=True)

class CommonData(AbstractObj):
    raw: str = ''
    rLOD0: str = ''
    rLOD1: str = ''
    rLOD2: str = ''
    controller: CommonDataController = None

    def __init__(self, **data):
        if 'id' not in data:
            raise ValueError(f"[{self.__class__.__name__}]: The object has no id!")
        super().__init__(**data)

class Author(AbstractObj):
    name: str = ''
    role: str = ''
    controller: AuthorController = None

    def __init__(self, **data):
        has_id = 'id' in data
        if not has_id:
            data['id'] = f"Author:{uuid4()}"
        super().__init__(**data)

class AbstractContent(AbstractObj):
    author_id: str=''
    group_id: str=''
    controller: AbstractContentController = None

class AbstractGroup(AbstractObj):
    author_id: str=''
    parent_id: str = ''
    children_id: List[str] = []
    controller: AbstractGroupController = None

class ContentGroup(AbstractGroup):
    controller: ContentGroupController = None
    def __init__(self, **data):
        if 'id' not in data:
            data['id'] = f"ContentGroup:{uuid4()}"
        super().__init__(**data)

class TextContent(AbstractContent):
    controller: TextContentController = None
    def __init__(self, **data):
        if 'id' not in data:
            data['id'] = f"TextContent:{uuid4()}"
        super().__init__(**data)

class EmbeddingContent(AbstractContent):
    controller: EmbeddingContentController = None
    target_id: str
    def __init__(self, **data):
        if 'id' not in data:
            data['id'] = f"EmbeddingContent:{uuid4()}"
        elif 'id' in data and 'target_id' not in data:
            data['target_id'] = ''
        super().__init__(**data)

class FileLinkContent(AbstractContent):
    controller: FileLinkContentController = None
    def __init__(self, **data):
        if 'id' not in data:
            data['id'] = f"FileLinkContent:{uuid4()}"
        super().__init__(**data)        

class BinaryFileContent(AbstractContent):
    controller: BinaryFileContentController = None
    def __init__(self, **data):
        if 'id' not in data:
            data['id'] = f"BinaryFileContent:{uuid4()}"
        super().__init__(**data)
        
class ImageContent(BinaryFileContent):
    controller: ImageContentController = None
    def __init__(self, **data):
        if 'id' not in data:
            data['id'] = f"ImageContent:{uuid4()}"
        super().__init__(**data)

class LLMstore(SingletonKeyValueStorage):
    client = SingletonKeyValueStorage().python_backend()

    def _client(self):
        return self.client
    
    def _get_all_object_names(self):
        return ['AbstractObj',
                'CommonData',
                'Author',
                'AbstractContent',
                'AbstractGroup',
                'ContentGroup',
                'TextContent',
                'EmbeddingContent',
                'FileLinkContent',
                'BinaryFileContent',
                'ImageContent',]
    
    def _get_all_object_classes(self):
        return [AbstractObj,
                CommonData,
                Author,
                AbstractContent,
                AbstractGroup,
                ContentGroup,
                TextContent,
                EmbeddingContent,
                FileLinkContent,
                BinaryFileContent,
                ImageContent,]    
    
    def _get_all_object_controller_classes(self):
        return [AbstractObjController,
                CommonDataController,
                AuthorController,
                AbstractContentController,
                AbstractGroupController,
                ContentGroupController,
                TextContentController,
                EmbeddingContentController,
                FileLinkContentController,
                BinaryFileContentController,
                ImageContentController,]
    
    def get_class(self, id: str):
        class_type = id.split(':')[0]
        res = {a:b for a,b in zip(self._get_all_object_names(),self._get_all_object_classes())}.get(class_type, None)
        if res is None:
            raise ValueError(f'No such class of {class_type}')
        return res
    
    def get_controller(self, id: str):
        class_type = id.split(':')[0]
        res = {a:b for a,b in zip(self._get_all_object_names(),self._get_all_object_controller_classes())}.get(class_type, None)
        if res is None:
            raise ValueError(f'No such controller of {class_type}')
        return res
   
    def _store_obj(self, obj:AbstractObj):
        self.client.set(obj.id,json.loads(obj.model_dump_json(exclude=['controller'])))
        return obj
        
    def add_new_author(self,name, role, rank:list=[0], metadata={}) -> Author:
        auther = self._store_obj(Author(name=name, role=role, rank=rank, metadata=metadata))
        auther.controller = self.get_controller(auther.id)(self,auther)
        return auther
    
    def add_new_root_group(self,metadata={},rank=[0]) -> ContentGroup:
        group = self._store_obj( ContentGroup(rank=rank, metadata=metadata) )         
        group.controller = self.get_controller(group.id)(self,group)
        return group
    
    def _add_new_content_to_group(self,group:ContentGroup,content:AbstractObj,raw:str=None):
        group.children_id.append(content.id)
        self._store_obj(group)
        if raw is not None and 'ContentGroup' not in content.id:
            self._store_obj(content)
            self._store_obj(CommonData(id=f"CommonData:{content.id}", raw=raw))        
        else:
            self._store_obj(content)
            
        content.controller = self.get_controller(content.id)(self,content)
        return group,content
    
    def _add_new_content_to_group_byID(self,group_id:str,content:AbstractObj,raw:str=None):
        g:AbstractGroup = self.find(group_id)
        return self._add_new_content_to_group(g,content,raw)
    
    def add_new_group_to_group_byID(self,group_id:str,metadata={},rank=[0]):
        parent,child = self._add_new_content_to_group_byID(group_id, ContentGroup(rank=rank, metadata=metadata, parent_id=group_id))
        return parent,child

    def add_new_text_to_group_byID(self,group_id:str,author_id:str,text:str):
        parent,child = self._add_new_content_to_group_byID(group_id,
                                                      TextContent(author_id=author_id, group_id=group_id),
                                                      raw=text)
        return parent,child
    
    def add_new_embedding_to_group_byID(self,group_id:str, author_id:str, content_id:str, vec:list[float]):
        parent,child = self._add_new_content_to_group_byID(group_id,
                                                      EmbeddingContent(author_id=author_id, 
                                                                       group_id=group_id,target_id=content_id),
                                                      raw=str(vec))
        return parent,child
    
    def add_new_image_to_group_byID(self,group_id:str,author_id:str, filepath:str):
        raw_bytes = self.read_image(filepath)
        raw_base64 = self.encode_image(raw_bytes)
        parent,child = self._add_new_content_to_group_byID(group_id,
                                                      ImageContent(author_id=author_id,group_id=group_id),
                                                      raw=raw_base64)
        return parent,child
    

    def read_image(self, filepath):
        with open(filepath, "rb") as f:
            return f.read()
        
    def b64encode(self, file_bytes):
        return base64.b64encode(file_bytes)
        
    def encode_image(self, image_bytes):
        return self.b64encode(image_bytes)
    
    def add_new_group_to_group(self,group:ContentGroup,metadata={},rank=[0]):
        parent,child = self._add_new_content_to_group(group, ContentGroup(rank=rank, metadata=metadata, parent_id=group.id))
        return parent,child

    def add_new_text_to_group(self,group:ContentGroup,author_id:str,text:str):
        parent,child = self._add_new_content_to_group(group,
                                                      TextContent(author_id=author_id, group_id=group.id),
                                                      raw=text)
        return parent,child
    
    def add_new_embedding_to_group(self,group:ContentGroup, author_id:str, content_id:str, vec:list[float]):
        parent,child = self._add_new_content_to_group(group,
                                                      EmbeddingContent(author_id=author_id, 
                                                                       group_id=group.id,target_id=content_id),
                                                      raw=str(vec))
        return parent,child
    
    def add_new_image_to_group(self,group:ContentGroup,author_id:str, filepath:str):
        raw_bytes = self.read_image(filepath)
        raw_base64 = self.encode_image(raw_bytes)
        parent,child = self._add_new_content_to_group(group,
                                                      ImageContent(author_id=author_id,group_id=group.id),
                                                      raw=raw_base64)
        return parent,child
    
    # available for regx?
    def find(self,id:str) -> AbstractObj:
        data_dict = self.client.get(id)
        obj:AbstractObj = self.get_class(id)(**data_dict)
        obj.controller = self.get_controller(id)(self,obj)
        return obj
    
    def find_all(self,id:str=f'Author:*'):
        keys = [key for key in self.client.keys(id)]
        results:list[AbstractObj] = []
        for key in keys:
            obj = self.find(key)
            results.append(obj)
        return results

    def delete_obj(self, obj:AbstractObj):
        self.client.delete(obj.id)
        obj.controller = None
    
    def update_obj(self, obj:AbstractObj, **kwargs) -> AbstractObj:
        for k,v in kwargs.items():
            if v is not None and k in obj.model_fields:
                obj.__dict__[k] = v
                obj.update_time = get_current_datetime_with_utc()        
        self._store_obj(obj)
        return obj
