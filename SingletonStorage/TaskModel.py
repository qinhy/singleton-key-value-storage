
import ctypes
from datetime import datetime
import inspect
import json
import queue
from threading import Thread
import time
from typing import Any, Dict
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
            self.set_failure()

    class WorkerController(AbstractObjController):
        FAILURE='failure'
        SLEEPING='sleeping'
        PROCESSING='processing'
        
        def set_status(self,status:str=SLEEPING):
            self.update(status=status)
        def set_sleeping(self):
            self.set_status(Controller4Task.WorkerController.SLEEPING)
        def set_processing(self):
            self.set_status(Controller4Task.WorkerController.PROCESSING)
        def set_failure(self):
            self.set_status(Controller4Task.WorkerController.FAILURE)

        def __init__(self, store, model):
            self.model:Model4Task.Worker = model
            self._store:TaskStore = store
            if not self.model.stop and self.model.thread_memo_id<0:
                thread = Thread(target=self.task_worker)
                self.set_memo_id(id(thread))
                thread.start()
                
        def set_memo_id(self,memo_id=-1):
            self.update(thread_memo_id=memo_id)
                
        def get_memo_id(self):
            return self._store.find_worker(self.model.id).thread_memo_id
        
        def _get_thread(self)->Thread:
            thread_memo_id = self.get_memo_id()
            if thread_memo_id>0:
                return ctypes.cast(thread_memo_id, ctypes.py_object).value
            return None

        def start(self):
            if self.is_stop():
                thread = Thread(target=self.task_worker)
                self.set_memo_id(id(thread))
                self.update(stop=False)
                thread.start()

        def revoke(self,wait=True):
            self.update(stop=True)
            thread = self._get_thread()
            if thread is not None and wait : self._get_thread().join()
            self.set_memo_id(-1)

        def is_stop(self):
            return self._store.find_worker(self.model.id).stop

        def task_worker(self):
            try:
                while not self.is_stop():
                    self.set_sleeping()
                    if not self._store._get_task_queue().empty():
                        self.set_processing()
                        res = 'NULL'
                        task:Model4Task.Task = self._store._get_task_queue().get()
                        try:
                            task.get_controller().set_pending()
                            res = self._store.find_function(f'Function:{task.name}')(*task.args)
                            task.get_controller().set_success()
                        except Exception as e:
                            task.get_controller().set_error(e)                        
                        if res != 'NULL':
                            task.get_controller().set_result(res)
                        self._store._get_task_queue().task_done()
                    
                    self.set_sleeping()
                    time.sleep(1)
            except Exception as e:
                print(e)
                self.set_failure()
                self.set_memo_id(-1)
        
        def delete(self, wait=True):
            self.revoke(wait)

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
        thread_memo_id:int = -1

        _controller: Controller4Task.WorkerController = None
        def get_controller(self)->Controller4Task.WorkerController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.WorkerController(store,self)

    class Function(AbstractObj):
        class Parameter(BaseModel):
            type: str
            description: str            

        id: str = Field(default_factory=lambda :f"Function:{uuid4()}")
        name: str = 'NULL'
        description: str = None
        # arguments: Dict[str, Any] = None
        _properties: Dict[str, Parameter] = {}
        parameters: Dict[str, Any] = {"type": "object",'properties':_properties}
        required: list[str] = []        
        _parameters_description: Dict[str, str] = {}
        _string_arguments: str='\{\}'

        def _extract_signature(self):
            self.name=self.__class__.__name__
            self.id = f"Function:{self.name}"
            sig = inspect.signature(self.__call__)
            # Map Python types to more generic strings
            type_map = {
                int: "integer",float: "number",
                str: "string",bool: "boolean",
                list: "array",dict: "object"
                # ... add more mappings if needed
            }
            self.required = []
            for name, param in sig.parameters.items():
                param_type = type_map.get(param.annotation, "object")
                self._properties[name] = Model4Task.Function.Parameter(
                    type=param_type, description=self._parameters_description.get(name,''))
                if param.default is inspect._empty:
                    self.required.append(name)
            self.parameters['properties']=self._properties

        def __call__(self):
            print('this is root class , not implement')
        
        def get_description(self):
            return self.model_dump()#exclude=['arguments'])

        _controller: Controller4Task.AbstractObjController = None
        def get_controller(self)->Controller4Task.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.AbstractObjController(store,self)


class TaskStore(SingletonKeyValueStorage):
    
    def __init__(self) -> None:
        super().__init__()
        self.python_backend() # only python backend works
        self._task_queue=queue.Queue()
    
    def _get_task_queue(self)->queue.Queue:
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
    
    def dumps(self) -> str:
        self.stop_workers()
        res = super().dumps()
        self.start_workers()
        return res
    
    def clean(self):
        self.stop_workers()
        return super().clean()

    def add_new_task(self, name, args=[], rank:list=[0], metadata={}) -> Model4Task.Task:
        task = self._store_obj(Model4Task.Task(name=name, args=args, rank=rank, metadata=metadata))
        self._get_task_queue().put(task)
        if len([w for w in self.get_workers() if not w.stop]):
            self.start_workers()
        return task
    
    def add_new_worker(self, metadata={})->Model4Task.Task:
        return self._store_obj(Model4Task.Worker(stop=False, metadata=metadata))    

    def add_new_function(self, function_obj:Model4Task.Function)->Model4Task.Function:    
        setattr(Model4Task,function_obj.__class__.__name__,function_obj.__class__)
        return self._store_obj(function_obj)
    
    # available for regx?
    def find(self,id:str) -> Model4Task.AbstractObj:
        return self._init_controller(self._get_class(id)(**self.get(id)))
    
    def find_task(self,id:str) -> Model4Task.Task: return self.find(id)

    def find_worker(self,id:str) -> Model4Task.Worker: return self.find(id)
    
    def find_function(self,id:str) -> Model4Task.Function:
        return self._init_controller(self._get_class(id.replace('Function:',''))(**self.get(id)))
    
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

    def function_list(self):
        return [self.find_function(k) for k in self.keys('Function:*')]

    def get_workers(self)->list[Model4Task.Worker]:
        return self.find_all('Worker:*')
    
    def stop_workers(self):
        [w.get_controller().revoke() for w in self.get_workers()]

    def start_workers(self):
        [w.get_controller().start() for w in self.get_workers()]

    def task_list(self)->list[Model4Task.Task]:
        return self.find_all('Task:*')

##### testing
class ExmpalePrintFunction(Model4Task.Function):
    description: str = 'just print some thing'
    _parameters_description = dict(
        msg='string to print',
    )

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self._extract_signature()

    def __call__(self,msg:str):
        print(msg)

class ExmpalePowerFunction(Model4Task.Function):
    description: str = 'just power a number'
    _parameters_description = dict(
        a='base number',
        b='exponent number',
    )

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self._extract_signature()

    def __call__(self,a:int,b:int):
        return a**b

### tests 
# ts = TaskStore()
# w = ts.add_new_worker()
# ts.add_new_function(ExmpalePowerFunction())
# ts.add_new_task('ExmpalePowerFunction',[3,4])
# ts.add_new_function(ExmpalePrintFunction())
# ts.add_new_task('ExmpalePrintFunction',['3,4'])
# ts.stop_workers()
# print(ts.task_list())

# w = ts.get_workers()[0]
# w.get_controller().start()
# ts.add_new_task('ExmpalePrintFunction',['3,4'])
# print(ts.task_list())
# ts.stop_workers()