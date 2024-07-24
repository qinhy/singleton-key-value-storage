
from datetime import datetime
import json
from uuid import uuid4
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field

from .Storages import SingletonKeyValueStorage

def get_current_datetime_with_utc():
    return datetime.now().replace(tzinfo=ZoneInfo("UTC"))

class Controller4Task:
    class AbstractObjController:
        def __init__(self, store, model):
            self.model:Model4Task.AbstractObj = model
            self._store:TaskStore = store

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
    
    class TaskController(AbstractObjController):
        PENDING='pending'
        PROCESSING='processing'
        SUCCESS='success'
        FAILURE='failure'

        def __init__(self, store, model):
            self.model:Model4Task.Task = model
            self._store:TaskStore = store

        def revoke(self,):
            pass

        def set_status(self,status:str=PENDING):
            self.update({'status':status})

        def set_pending(self):
            self.set_status(Controller4Task.TaskController.PENDING)

        def set_result(self,result:dict):
            self.update({'result':result})

        def set_error(self,error:dict):
            self.update({'error':error})

        
class Model4Task:
    class AbstractObj(BaseModel):
        id: str
        rank: list = [0]
        create_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        update_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        status: str = ""
        metadata: dict = {}


        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4Task.AbstractObjController = None
        def get_controller(self)->Controller4Task.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.AbstractObjController(store,self)

    class Task(AbstractObj):
        id: str = Field(default_factory=lambda :f"Task:{uuid4()}")
        status: str = "pending"
        name:str
        args:dict
        result:dict = None
        error:dict = None
        
        _controller: Controller4Task.TaskController = None
        def get_controller(self)->Controller4Task.TaskController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.TaskController(store,self)


class TaskStore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        self.python_backend()
        
    def _client(self):
        return self.client
    
    def _get_class(self, id: str):
        class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in Model4Task.__dict__.items() if '_' not in k]}.get(class_type, None)
        if res is None:
            raise ValueError(f'No such class of {class_type}')
        return res
    
    def _init_controller(self,obj:Model4Task.AbstractObj):
        obj.init_controller(self,obj)
        return obj
       
    def _store_obj(self, obj:Model4Task.AbstractObj):
        self.set(obj.id,json.loads(obj.model_dump_json()))
        return self._init_controller(obj)

    def add_new_task(self, name, args={}, rank:list=[0], metadata={}) -> Model4Task.Task:
        return self._store_obj(Model4Task.Task(name=name, args=args, rank=rank, metadata=metadata))
    
    # available for regx?
    def find(self,id:str) -> Model4Task.AbstractObj:
        return self._init_controller(self._get_class(id)(**self.get(id)))
    
    def find_task(self,id:str) -> Model4Task.Task:
        return self.find(id)
    
    def find_all(self,id:str=f'Task:*')->list[Model4Task.AbstractObj]:
        return [self.find(key) for key in self.keys(id)]
    
    def find_all_tasks(self)->list[Model4Task.Task]:
        return self.find_all('Task:*')
    
# - get task status (something like : success , failed , pending , processing) by task_id.
# - get task result ( when success ) by task_id.
# - get task error ( when failed ) by task_id.
# - get task args by task_id.
# - get task function name by task_id.
# - get runnable task list by task_id.
# - get all tasks id list.

    def task_runnable_list():
        pass

    def task_id_list():
        pass

    def task_status(self, id):
        t = self.find_task(id)
        return t.status

    def task_result(self, id):
        t = self.find_task(id)
        return t.result
    
    def task_error(self, id):
        t = self.find_task(id)
        return t.error
    
    def task_args(self, id):
        t = self.find_task(id)
        return t.args

    def task_function_name(self, id):
        t = self.find_task(id)
        return t.name
