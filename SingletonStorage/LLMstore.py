
import base64
from datetime import datetime
import io
from PIL import Image
from typing import List
from zoneinfo import ZoneInfo
from .BasicModel import BasicStore, Controller4Basic, Model4Basic

def get_current_datetime_with_utc():
    return datetime.now().replace(tzinfo=ZoneInfo("UTC"))

class Controller4LLM:
    class AbstractObjController(Controller4Basic.AbstractObjController):
        pass

    class CommonDataController(AbstractObjController):
        pass

    class AuthorController(AbstractObjController):
        pass

    class AbstractContentController(AbstractObjController):
        def __init__(self, store, model):
            self.model: Model4LLM.AbstractContent = model
            self._store:LLMstore = store

        def delete(self):
            self.get_data().get_controller().delete()
            # self.storage().delete_obj(self.model)
            super().delete()

        def get_author(self):
            author:Model4LLM.Author = self.storage().find(self.model.author_id)
            return author

        def get_group(self):
            res:Model4LLM.ContentGroup = self.storage().find(self.model.group_id)
            return res
        
        def get_data(self):
            res:Model4LLM.CommonData = self.storage().find(self.model.data_id())
            return res

        def get_data_raw(self):
            return self.get_data().raw    

        def update_data_raw(self, msg: str):
            self.get_data().get_controller().update(raw = msg)
            return self

        def append_data_raw(self, msg: str):
            data = self.get_data()
            return self.update_data_raw(data.raw + msg)
        
    class AbstractGroupController(Controller4Basic.AbstractGroupController):
        # def __init__(self, store, model):
        #     self.model:Model4LLM.AbstractGroup = model
        #     self._store:LLMstore = store

        # def yield_children_recursive(self, depth: int = 0):
        #     for child_id in self.model.children_id:
        #         if not self.storage().exists(child_id):
        #             continue
        #         content:Model4LLM.AbstractObj = self.storage().find(child_id)
        #         yield content, depth
        #         if child_id.startswith('ContentGroup'):
        #             group:Controller4LLM.AbstractGroupController = content._controller
        #             for cc, d in group.yield_children_recursive(depth + 1):
        #                 yield cc, d

        # def delete_recursive(self):
        #     for c, d in self.yield_children_recursive():
        #         c.get_controller().delete()
        #     self.delete()

        # def get_children(self):
        #     # self.load()
        #     assert  self.model is not None, 'controller has null model!'
        #     results:List[Model4LLM.AbstractObj] = []
        #     for child_id in self.model.children_id:
        #         results.append(self.storage().find(child_id))
        #     return results

        # def get_child(self, child_id: str):
        #     res:Model4LLM.AbstractContent = self.storage().find(child_id)
        #     return res

        def prints(self):
            res = '########################################################\n'
            for content, depth in self.yield_children_recursive():
                res += f"{'    ' * depth}{content.get_id()}\n"
            res += '########################################################\n'
            print(res)
            return res

    class ContentGroupController(AbstractGroupController):
        def __init__(self, store, model):
            self.model:Model4LLM.ContentGroup = model
            self._store:LLMstore = store
        
        def storage(self):return self._store

        def add_new_child_group(self,metadata={},rank=[0]):
            parent,child = self.storage().add_new_group_to_group(group=self.model,metadata=metadata,rank=rank)
            return child

        def add_new_text_content(self, author_id:str, text:str):
            parent,child = self.storage().add_new_text_to_group(group=self.model,author_id=author_id,
                                                    text=text)                             
            return child
        
        def add_new_embeding_content(self, author_id:str, content_id:str, vec:list[float]):
            parent,child = self.storage().add_new_embedding_to_group(group=self.model,author_id=author_id,
                                                        content_id=content_id, vec=vec)                                   
            return child
        
        def add_new_image_content(self,author_id:str, filepath:str):
            parent,child = self.storage().add_new_image_to_group(group=self.model,author_id=author_id,
                                                    filepath=filepath)                              
            return child
            

        # def delete_child(self, child_id:str):
        #     remaining_ids = [cid for cid in self.model.children_id if cid != child_id]
        #     for content in self.get_children():
        #         if content.get_controller().model.get_id() == child_id:
        #             if child_id.startswith('ContentGroup'):
        #                 group:Controller4LLM.ContentGroupController = content.get_controller()
        #                 group.delete_recursive()
        #             else:
        #                 content.get_controller().delete()
        #             break
        #     self.update(children_id = remaining_ids)
        #     return self

        # def get_children_recursive(self):
        #     results:list[Model4LLM.AbstractContent] = []
        #     for c, d in self.yield_children_recursive():
        #         results.append(c)
        #     return results

    class TextContentController(AbstractContentController):
        pass

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
            res:Model4LLM.AbstractContent = self.storage().find(target_id)
            return res
        
        def update_data_raw(self, embedding: list[float]):
            super().update_data_raw(str(embedding))
            return self

    class FileLinkContentController(AbstractContentController):
        pass

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
    class AbstractObj(Model4Basic.AbstractObj):
        def _get_controller_class(self,modelclass=Controller4LLM):
            class_type = self.__class__.__name__+'Controller'
            res = {c.__name__:c for c in [i for k,i in modelclass.__dict__.items() if '_' not in k]}
            res = res.get(class_type, None)
            if res is None: raise ValueError(f'No such class of {class_type}')
            return res

    class AbstractGroup(Model4Basic.AbstractGroup):
        _controller: Controller4LLM.AbstractGroupController = None
        def get_controller(self):return self._controller

        def _get_controller_class(self,modelclass=Controller4LLM):
            class_type = self.__class__.__name__+'Controller'
            res = {c.__name__:c for c in [i for k,i in modelclass.__dict__.items() if '_' not in k]}
            res = res.get(class_type, None)
            if res is None: raise ValueError(f'No such class of {class_type}')
            return res
        
    class CommonData(AbstractObj):
        raw: str = ''
        rLOD0: str = ''
        rLOD1: str = ''
        rLOD2: str = ''
        _controller: Controller4LLM.CommonDataController = None
        def get_controller(self):return self._controller

    class Author(AbstractObj):
        name: str = ''
        role: str = ''
        _controller: Controller4LLM.AuthorController = None
        def get_controller(self):return self._controller

    class AbstractContent(AbstractObj):
        author_id: str=''
        group_id: str=''
        _controller: Controller4LLM.AbstractContentController = None
        def get_controller(self):return self._controller
        def data_id(self):return f"CommonData:{self.get_id()}"
        
    class ContentGroup(AbstractGroup):
        _controller: Controller4LLM.ContentGroupController = None
        def get_controller(self):return self._controller

    class TextContent(AbstractContent):
        _controller: Controller4LLM.TextContentController = None
        def get_controller(self):return self._controller

    class EmbeddingContent(AbstractContent):
        target_id: str
        _controller: Controller4LLM.EmbeddingContentController = None
        def get_controller(self):return self._controller
        
    class FileLinkContent(AbstractContent):
        _controller: Controller4LLM.FileLinkContentController = None
        def get_controller(self):return self._controller

    class BinaryFileContent(AbstractContent):
        _controller: Controller4LLM.BinaryFileContentController = None
        def get_controller(self):return self._controller

    class ImageContent(BinaryFileContent):
        _controller: Controller4LLM.ImageContentController = None
        def get_controller(self):return self._controller


class LLMstore(BasicStore):    
    
    def __init__(self, version_controll=False) -> None:
        super().__init__(version_controll)
        self.python_backend()

    def _get_class(self, id: str, modelclass=Model4LLM):
        return super()._get_class(id, modelclass)
    
    def add_new_author(self,name, role, rank:list=[0], metadata={}) -> Model4LLM.Author:
        return self.add_new_obj(Model4LLM.Author(name=name, role=role, rank=rank, metadata=metadata))
    
    def add_new_root_group(self,metadata={},rank=[0]) -> Model4LLM.ContentGroup:
        return self.add_new_obj( Model4LLM.ContentGroup(rank=rank, metadata=metadata) )
    
    def _add_new_content_to_group(self,group:Model4LLM.ContentGroup,content:Model4LLM.AbstractContent,raw:str=None):
        if group._id is None:raise ValueError('the group is no exits!')
        content = self.add_new_obj(content)
        group.get_controller().update(children_id=group.children_id+[content.get_id()])
        if raw is not None and 'ContentGroup' not in content.get_id():
            self.add_new_obj(Model4LLM.CommonData(raw=raw),id=content.data_id())
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
        parent,child = self._add_new_content_to_group(group, Model4LLM.ContentGroup(rank=rank, metadata=metadata, parent_id=group.get_id()))
        return parent,child

    def add_new_text_to_group(self,group:Model4LLM.ContentGroup,author_id:str,text:str):
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.TextContent(author_id=author_id, group_id=group.get_id()),
                                                      raw=text)
        return parent,child
    
    def add_new_embedding_to_group(self,group:Model4LLM.ContentGroup, author_id:str, content_id:str, vec:list[float]):
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.EmbeddingContent(author_id=author_id, 
                                                                       group_id=group.get_id(),target_id=content_id),
                                                      raw=str(vec))
        return parent,child
    
    def add_new_image_to_group(self,group:Model4LLM.ContentGroup,author_id:str, filepath:str):
        raw_bytes = self.read_image(filepath)
        raw_base64 = self.encode_image(raw_bytes)
        parent,child = self._add_new_content_to_group(group,
                                                      Model4LLM.ImageContent(author_id=author_id,group_id=group.get_id()),
                                                      raw=raw_base64)
        return parent,child
    
    
    def find_group(self,id:str) -> Model4LLM.ContentGroup:
        assert 'Group:' in id, 'this id is not a id for goup'
        return self.find(id)
    
    def find_all_authors(self)->list[Model4LLM.Author]:
        return self.find_all('Author:*')