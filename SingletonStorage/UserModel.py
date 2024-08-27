
from datetime import datetime

from BasicModel import BasicStore,Controller4Basic,Model4Basic

import base64
import uuid,hashlib

import numpy as np

def text2hash2base64Str(text:str,salt:bytes = b'',ite:int = 10**6):
    return base64.b64encode(hashlib.pbkdf2_hmac('sha256', text.encode(), salt, ite, dklen=16)).decode()

def text2hash(text:str,salt:bytes = b'',ite:int = 10**6):
    return hashlib.pbkdf2_hmac('sha256', text.encode(), salt, ite, dklen=16)

def text2hash2uuid(text:str,salt:bytes = b'',ite:int = 10**6):
    return str(uuid.UUID(bytes=text2hash(text,salt,ite)))

def remove_hyphen(uuid:str):
    return uuid.replace('-', '')

def restore_hyphen(uuid:str):
    if len(uuid) != 32:
        raise ValueError("Invalid UUID format")
    return f'{uuid[:8]}-{uuid[8:12]}-{uuid[12:16]}-{uuid[16:20]}-{uuid[20:]}'

def list2base64Str(l:list):
    if l is None:
        l = []
    t = np.asarray(l)
    return base64.b64encode(t).decode()

def base64Str2list(bs:str):
    r = base64.decodebytes(bs.encode())
    return np.frombuffer(r).tolist()


class Controller4User:
    class AbstractObjController(Controller4Basic.AbstractObjController):
        pass
    class UserController(AbstractObjController):
        def __init__(self, store, model):
            self.model:Model4User.User = model
            self._store:UsersStore = store

        def mail2user(self,message):
            pass

        def set_password(self,password):
            self.update(hashed_password=text2hash2base64Str(password))

        def check_password(self,password):
            return self.model.hashed_password==text2hash2base64Str(password)

        def set_name(self,):
            pass

        def set_role(self,):
            pass

        def get_licenses(self,):
            pass

        def add_license(self,):
            pass

        def delete_license(self,):
            pass

        def get_appusages(self,):
            pass

        def add_appusage(self,):
            pass

        def delete_appusage(self,):
            pass
        
    class AppController(AbstractObjController):
        def __init__(self, store, model):
            self.model:Model4User.App = model
            self._store:UsersStore = store

        def delete(self):
            pass
    class LicenseController(AbstractObjController):
        def __init__(self, store, model):
            self.model:Model4User.License = model
            self._store:UsersStore = store

        def delete(self):
            pass

    class AppUsageController(AbstractObjController):
        def __init__(self, store, model):
            self.model:Model4User.AppUsage = model
            self._store:UsersStore = store

        def delete(self):
            pass

class Model4User:
    class AbstractObj(Model4Basic.AbstractObj):
        pass
    class User(AbstractObj):
        name:str
        full_name: str
        role:str
        hashed_password:str # text2hash2base64Str(password),
        email:str
        disabled: bool=False
        
        @staticmethod
        def static_gen_new_id(email): return f"User:{text2hash2uuid(email)}"

        def gen_new_id(self): return f"{self.class_name()}:{text2hash2uuid(self.email)}"
        
        _controller: Controller4User.UserController = None
        def get_controller(self)->Controller4User.UserController: return self._controller
        def init_controller(self,store):self._controller = Controller4User.UserController(store,self)

    class App(AbstractObj):
        parent_App_id:str
        running_cost:int = 0
        major_name:str = None
        minor_name:str = None
        
        _controller: Controller4User.AppController = None
        def get_controller(self)->Controller4User.AppController: return self._controller
        def init_controller(self,store):self._controller = Controller4User.AppController(store,self)

    class License(AbstractObj):
        user_id:str
        access_token:str = None
        bought_at:datetime = None
        expiration_date:datetime = None
        running_time:int = 0
        max_running_time:int = 0
        
        _controller: Controller4User.LicenseController = None
        def get_controller(self)->Controller4User.LicenseController: return self._controller
        def init_controller(self,store):self._controller = Controller4User.LicenseController(store,self)

    class AppUsage(AbstractObj):
        user_id:str
        App_id:str
        license_id:str
        start_time:datetime = None
        end_time:datetime = None
        running_time_cost:int = 0
        
        _controller: Controller4User.AppUsageController = None
        def get_controller(self)->Controller4User.AppUsageController: return self._controller
        def init_controller(self,store):self._controller = Controller4User.AppUsageController(store,self)

class UsersStore(BasicStore):

    def __init__(self) -> None:
        super().__init__()
    
    def _get_class(self, id: str, modelclass=Model4User):
        return super()._get_class(id, modelclass)

    def add_new_user(self, name:str,role:str,hashed_password:str,full_name:str,email:str, rank:list=[0], metadata={}) -> Model4User.User:
        tmp = Model4User.User(name=name, role=role,full_name=full_name,hashed_password=hashed_password,
                                            email=email,rank=rank, metadata=metadata)
        if self.exists(tmp.gen_new_id()) : raise ValueError('user already exists!')
        return self.add_new_obj(tmp)
    
    def add_new_app(self, major_name:str,minor_name:str,running_cost:int=0,parent_App_id:str=None) -> Model4User.App:
        return self.add_new_obj(Model4User.App(major_name=major_name,minor_name=minor_name,
                                           running_cost=running_cost,parent_App_id=parent_App_id))
        
    def find_all_users(self)->list[Model4User.User]:
        return self.find_all('User:*')
    
    def find_user_by_email(self,email):
        user_uuid =  Model4User.User.static_gen_new_id(email)
        return self.find(user_uuid)
    

def test():
    us = UsersStore()
    us.add_new_user('John','admin','123','John anna','123@123.com')
    return us
