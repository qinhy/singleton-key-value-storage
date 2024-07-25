
from datetime import datetime
import json
import queue
from threading import Thread
import time
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field

from Storages import EventDispatcherController, SingletonKeyValueStorage

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
            self.update(status=status)

        def set_pending(self):
            self.set_status(Controller4Task.TaskController.PENDING)
        def set_processing(self):
            self.set_status(Controller4Task.TaskController.PROCESSING)
        def set_success(self):
            self.set_status(Controller4Task.TaskController.SUCCESS)
        def set_failure(self):
            self.set_status(Controller4Task.TaskController.FAILURE)

        def set_result(self,result):
            self.update(result=str(result))

        def set_error(self,error):
            self.update(error=str(error))

    class WorkerController(AbstractObjController):
        FAILURE='failure'
        SLEEPING='sleeping'
        PROCESSING='processing'
        STOPPED='stopped'
        
        def __init__(self, store, model):
            self.model:Model4Task.Worker = model
            self._store:TaskStore = store
            if self.model._thread is None:
                self.model._thread = Thread(target=self.task_worker)
            
        def task_worker(self):
            while not self._store.find(self.model.id).stop:
                if not self._store.get_task_queue().empty():
                    res = 'NULL'
                    task:Model4Task.Task = self._store.get_task_queue().get()
                    try:
                        task.get_controller().set_pending()
                        res = self._store.get(f'_Runnable:{task.name}')(*task.args)
                    except Exception as e:
                        task.get_controller().set_failure()
                        task.get_controller().set_error(e)
                    task.get_controller().set_success()
                    if res != 'NULL':
                        task.get_controller().set_result(res)
                    self._store.get_task_queue().task_done()
                time.sleep(1)
        
        def delete(self, wait=True):
            self.model._thread.stop = True
            if wait : self.model._thread.join()
            self._store.delete(self.model.id)

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
        args:list
        result:Any = None
        error:Any = None
        
        _controller: Controller4Task.TaskController = None
        def get_controller(self)->Controller4Task.TaskController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.TaskController(store,self)

    class Worker(AbstractObj):
        id: str = Field(default_factory=lambda :f"Worker:{uuid4()}")
        stop:bool = False
        
        _thread:Thread = None
        _controller: Controller4Task.WorkerController = None
        def get_controller(self)->Controller4Task.WorkerController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.WorkerController(store,self)

class TaskStore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        super.__init__()
        self.python_backend() # only python backend works
        self._task_queue=queue.Queue()
    
    def get_task_queue(self)->queue.Queue:
        return self._task_queue
    
    def _get_class(self, id: str):
        class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in Model4Task.__dict__.items() if '_' not in k]}.get(class_type, None)
        if res is None:
            raise ValueError(f'No such class of {class_type}')
        return res
    
    def _init_controller(self,obj:Model4Task.AbstractObj):
        obj.init_controller(self)
        return obj
       
    def _store_obj(self, obj:Model4Task.AbstractObj):
        self.set(obj.id,json.loads(obj.model_dump_json()))
        return self._init_controller(obj)

    def add_new_task(self, name, args=[], rank:list=[0], metadata={}) -> Model4Task.Task:
        task = self._store_obj(Model4Task.Task(name=name, args=args, rank=rank, metadata=metadata))
        self.get_task_queue().put(task)
        return task
    
    def add_new_worker(self, name, args=[], rank:list=[0], metadata={}) -> Model4Task.Task:
        task = self._store_obj(Model4Task.Worker(_thread=Thread()))
        thread = Thread(target=self.task_worker, args=(id,))
        thread.stop = False
        self.set(id,thread)
        thread.start()
        return task
    
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

    def add_runnable(self, runnable_name, runnable):
        self.set(f'_Runnable:{runnable_name}',runnable)

    def runnable_list(self):
        return self.keys('_Runnable:*')

    def get_workers(self)->list[Thread]:
        return self.find_all('Worker:*')
    
    def stop_workers(self):
        [self.delete_worker(k.id) for k in self.find_all('Worker:*')]

    def add_worker(self):

        thread = Thread(target=self.task_worker, args=(id,))
        thread.stop = False
        self.set(id,thread)
        thread.start()
        return id
    
    def delete_worker(self,id):
        c:Controller4Task.WorkerController = self.find(id).get_controller()
        c.delete()

    def task_list(self)->list[Model4Task.Task]:
        return self.find_all('Task:*')

    def get_task(self, id)->Model4Task.Task:
        return self.find_task(id)



##### testing
ts = TaskStore()
w = ts.add_worker()
ts.add_runnable('print',print)
t = ts.add_new_task('print',[1,2,3,4])
# 1 2 3 4
ts.task_list()
ts.stop_workers()