
import ctypes
from datetime import datetime
import inspect
import json
import queue
import sys
from threading import Event, Thread
import time
from typing import Any, Dict
from uuid import uuid4
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field
from Storages import SingletonKeyValueStorage

def get_current_datetime_with_utc():
    return datetime.now().replace(tzinfo=ZoneInfo("UTC"))


class TraceableThread(Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exit_thread = False
    
    def revoke(self):
        self.exit_thread=True

    def trace_lines(self, frame, event, arg):
        if event == "line":
            if self.exit_thread:
                raise SystemExit("Stopping thread as exit_thread is set to True")
        return self.trace_lines

    def run(self):
        sys.settrace(self.trace_lines)
        try:
            super().run()
        finally:
            sys.settrace(None)


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

        def revoke(self):
            pass

        def set_status(self,status:str=PENDING):
            self.update(status=status)

        def task_pending(self):
            self.set_status(Controller4Task.TaskController.PENDING)
        def task_processing(self):
            self.set_status(Controller4Task.TaskController.PROCESSING)
        def task_success(self):
            self.set_status(Controller4Task.TaskController.SUCCESS)
        def task_failure(self):
            self.set_status(Controller4Task.TaskController.FAILURE)

        def set_result(self,result):
            self.update(result=str(result))

        def set_error(self,error):
            self.update(error=str(error))
            self.task_failure()

    class WorkerController(AbstractObjController):
        FAILURE='failure'
        SLEEPING='sleeping'
        PROCESSING='processing'
        
        def set_status(self,status:str=SLEEPING):
            self.update(status=status)

        def worker_sleeping(self):
            self.set_status(Controller4Task.WorkerController.SLEEPING)

        def worker_processing(self):
            self.set_status(Controller4Task.WorkerController.PROCESSING)

        def worker_failure(self):
            self.set_status(Controller4Task.WorkerController.FAILURE)

        def __init__(self, store, model):
            self.model:Model4Task.Worker = model
            self._store:TaskStore = store
            if not self.model.stop and self.model.thread_memo_id<0:
                thread = TraceableThread(target=self.do_task)
                self._set_start(thread)
                thread.start()
                
        def set_memo_id(self,memo_id=-1):
            self.update(thread_memo_id=memo_id)
        
        def is_stop(self):
            return self._store.find_worker(self.model.id).stop
        
        def get_memo_id(self):
            return self._store.find_worker(self.model.id).thread_memo_id
        
        def _get_thread(self)->TraceableThread:
            thread_memo_id = self.get_memo_id()
            if thread_memo_id>0:
                return ctypes.cast(thread_memo_id, ctypes.py_object).value
            return None

        def start(self):
            if self.is_stop():
                thread = TraceableThread(target=self.do_task)
                self._set_start(thread)
                thread.start()

        def revoke(self,wait=True):
            thread = self._get_thread()
            if thread is not None:
                thread.revoke() 
                if wait: thread.join()
            self._set_stop()
        
        def _set_start(self,thread):
            self.set_memo_id(id(thread))
            self.update(stop=False)
        
        def _set_stop(self):
            self.update(stop=True)
            self.set_memo_id(-1)
            self.worker_sleeping()

        def do_task(self):
            try:
                while not self._store._get_task_queue().empty():
                    self.worker_processing()
                    res = 'NULL'
                    task:Model4Task.Task = self._store._get_task_queue().get()
                    task.get_controller().task_pending()
                    func = self._store.find_function(task.name)
                    kwargs = task.kwargs
                    
                    try:
                        res = func(**kwargs)
                        task.get_controller().task_success()
                    except Exception as e:
                        task.get_controller().set_error(e)

                    if res != 'NULL':
                        task.get_controller().set_result(res)
                    self._store._get_task_queue().task_done()

                    time.sleep(1)
            except Exception as e:
                print(e)
                self.worker_failure()
            finally:                
                self._set_stop()
        
        def delete(self, wait=True):
            self.revoke(wait)

class Model4Task:
    class AbstractObj(BaseModel):
        _id: str=None
        rank: list = [0]
        create_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        update_time: datetime = Field(default_factory=get_current_datetime_with_utc)
        status: str = ""
        metadata: dict = {}

        def set_id(self,id:str):
            self._id = id
            return self
        
        def get_id(self,create=False):
            if self._id is None and create:
                self.set_id(f"{self.__class__.__name__}:{uuid4()}")
            return self._id
        
        model_config = ConfigDict(arbitrary_types_allowed=True)    
        _controller: Controller4Task.AbstractObjController = None
        def get_controller(self)->Controller4Task.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.AbstractObjController(store,self)

    class Task(AbstractObj):
        id: str = Field(default_factory=lambda :f"Task:{uuid4()}")
        status: str = "pending"
        name:str
        kwargs:dict = {}
        result:Any = None
        error:Any = None

        def is_pending(self):
            return self.status=="pending"

        _controller: Controller4Task.TaskController = None
        def get_controller(self)->Controller4Task.TaskController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.TaskController(store,self)

    class Worker(AbstractObj):
        id: str = Field(default_factory=lambda :f"Worker:{uuid4()}")
        stop:bool = False
        thread_memo_id:int = -1
        
        def is_sleeping(self):
            return self.status==Controller4Task.WorkerController.SLEEPING

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
    
    def loads(self, json_str):
        res = super().loads(json_str)
        for t in [i for i in self.task_list() if i.is_pending()]:
            self._get_task_queue().put(t)
            self.start_workers()
        return res
    
    def dumps(self) -> str:
        self.stop_workers()
        res = super().dumps()
        self.start_workers()
        return res
    
    def clean(self):
        self.stop_workers()
        return super().clean()

    def add_new_task(self, name, kwargs={}, rank=[0], metadata={}) -> Model4Task.Task:
        task = self._store_obj(Model4Task.Task(name=name, kwargs=kwargs, rank=rank, metadata=metadata))
        self._get_task_queue().put(task)
        self.start_workers()
        return task
    
    def add_new_worker(self, metadata={})->Model4Task.Task:
        return self._store_obj(Model4Task.Worker(stop=False, metadata=metadata))    

    def add_new_function(self, function_obj:Model4Task.Function)->Model4Task.Function:    
        setattr(Model4Task,function_obj.__class__.__name__,function_obj.__class__)
        return self._store_obj(function_obj)
    
    def _get_as_obj(self,data_dict)->Model4Task.AbstractObj:
        obj:Model4Task.AbstractObj = self._get_class(id)(**data_dict)
        obj.set_id(id).init_controller(self)
        return obj
    # available for regx?
    def find(self,id:str) -> Model4Task.AbstractObj:
        return self._init_controller(self._get_class(id)(**self.get(id)))
    
    def find_task(self,id:str) -> Model4Task.Task: return self.find(id)

    def find_worker(self,id:str) -> Model4Task.Worker: return self.find(id)
    
    def find_function(self,function_name:str) -> Model4Task.Function:
        id = f'Function:{function_name}'
        return self._init_controller(self._get_class(function_name)(**self.get(id)))
    
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
        return [self.find_function(k.replace('Function:','')) for k in self.keys('Function:*')]

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

class ExmpaleInfinityLoopFunction(Model4Task.Function):
    description: str = 'infinity loop'
    
    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self._extract_signature()

    def __call__(self):
        cnt=0
        while True:
            print(f'infinity loop {cnt}')
            time.sleep(0.5)
            cnt+=1

def test():
    ### tests 
    ts = TaskStore()
    w = ts.add_new_worker()
    ts.add_new_function(ExmpalePowerFunction())
    ts.add_new_task('ExmpalePowerFunction',dict(a=3,b=4))
    ts.add_new_function(ExmpalePrintFunction())
    ts.add_new_task('ExmpalePrintFunction',dict(msg='hello!'))
    # ts.add_new_function(ExmpaleInfinityLoopFunction())
    # ts.add_new_task('ExmpaleInfinityLoopFunction',dict())
    # ts.stop_workers()
    # print(ts.task_list())

    # w = ts.get_workers()[0]
    # w.get_controller().start()
    # ts.add_new_task('ExmpalePrintFunction',dict(msg='hello!'))
    # print(ts.task_list())
    # ts.stop_workers()
    return ts