
import ctypes
from datetime import datetime
import inspect
import queue
import sys
from threading import Thread
import time
from typing import Any, Dict
from zoneinfo import ZoneInfo
from pydantic import BaseModel
from .BasicModel import BasicStore, Controller4Basic, Model4Basic

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
    class AbstractObjController(Controller4Basic.AbstractObjController):
        pass
    
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
            self.update_metadata('task',None)

        def worker_processing(self,task):
            task:Model4Task.Task=task
            self.set_status(Controller4Task.WorkerController.PROCESSING)            
            task.get_controller().update(worker=self.model.get_id())
            task.get_controller().task_processing()
            self.update_metadata('task',task.model_dump())

        def worker_failure(self):
            self.set_status(Controller4Task.WorkerController.FAILURE)
            self.update_metadata('task',None)

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
            return self._store.find_worker(self.model.get_id()).stop
        
        def get_memo_id(self):
            return self._store.find_worker(self.model.get_id()).thread_memo_id
        
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
                    res = 'null'
                    task:Model4Task.Task = self._store._get_task_queue().get()
                    task.get_controller().task_pending()
                    func = self._store.find_function(task.name)
                    kwargs = task.kwargs
                    
                    try:
                        self.worker_processing(task)
                        res = func(**kwargs)
                        task.get_controller().task_success()
                    except Exception as e:
                        task.get_controller().set_error(e)

                    if res != 'null':
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
    class AbstractObj(Model4Basic.AbstractObj):        
        _controller: Controller4Task.AbstractObjController = None
        def _get_controller_class(self,modelclass=Controller4Task):
            class_type = self.__class__.__name__+'Controller'
            res = {c.__name__:c for c in [i for k,i in modelclass.__dict__.items() if '_' not in k]}
            res = res.get(class_type, None)
            if res is None: raise ValueError(f'No such class of {class_type}')
            return res

    class Task(AbstractObj):
        status: str = "pending"
        worker:str = 'null'
        name:str
        kwargs:dict = {}
        result:Any = None
        error:Any = None

        def is_pending(self):
            return self.status=="pending"

        _controller: Controller4Task.TaskController = None
        def get_controller(self)->Controller4Task.TaskController: return self._controller

    class Worker(AbstractObj):
        stop:bool = False
        thread_memo_id:int = -1
        
        def is_sleeping(self):
            return self.status==Controller4Task.WorkerController.SLEEPING

        _controller: Controller4Task.WorkerController = None
        def get_controller(self)->Controller4Task.WorkerController: return self._controller

    class Function(AbstractObj):

        def param_descriptions(description,**descriptions):
            def decorator(func):
                func:Model4Task.Function = func
                func._parameters_description = descriptions
                func._description = description
                return func
            return decorator

        class Parameter(BaseModel):
            type: str
            description: str            

        name: str = 'null'
        description: str = 'null'
        _description: str = 'null'
        # arguments: Dict[str, Any] = None
        _properties: Dict[str, Parameter] = {}
        parameters: Dict[str, Any] = {"type": "object",'properties':_properties}
        required: list[str] = []        
        _parameters_description: Dict[str, str] = {}
        _string_arguments: str='\{\}'

        def __init__(self, *args, **kwargs):
            # super(self.__class__, self).__init__(*args, **kwargs)
            super().__init__(*args, **kwargs)
            self._extract_signature()

        def _extract_signature(self):
            self.name=self.__class__.__name__
            sig = inspect.signature(self.__call__)
            try:
                self.__call__()
            except Exception as e:
                pass
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
            self.description = self._description

        def __call__(self):
            print('this is root class , not implement')
        
        def get_description(self):
            return self.model_dump()#exclude=['arguments'])

        _controller: Controller4Task.AbstractObjController = None
        def get_controller(self)->Controller4Task.AbstractObjController: return self._controller
        def init_controller(self,store):self._controller = Controller4Task.AbstractObjController(store,self)


class TaskStore(BasicStore):
    
    def __init__(self, version_controll=False) -> None:
        super().__init__(version_controll)
        self.python_backend() # only python backend works
        self._task_queue=queue.Queue()
        
    def _get_class(self, id: str, modelclass=Model4Task):
        if 'Function:'in id:
            class_type = id.split(':')[1]
        else:
            class_type = id.split(':')[0]
        res = {c.__name__:c for c in [i for k,i in modelclass.__dict__.items() if '_' not in k]}
        res = res.get(class_type, None)
        if res is None: raise ValueError(f'No such class of {class_type}')
        return res
    
    def _get_task_queue(self)->queue.Queue:
        return self._task_queue
    
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
    
    def _ignite(self,task):
        self._get_task_queue().put(task)
        self.start_workers()
        return task
   
    def restart_task(self, id:str) -> Model4Task.Task:
        task = self.find(id)
        if task is None:raise ValueError('No such task of {id}')
        return self._ignite(task)

    def add_new_task(self, function_obj, kwargs={}, rank=[0], metadata={}, id:str=None) -> Model4Task.Task:
        if type(function_obj) is not str:
            function_name = function_obj.__class__.__name__
            if not self.exists(function_name):self.add_new_function(function_obj)
        else:
            function_name = function_obj
        task = self._add_new_obj(Model4Task.Task(name=function_name, 
                                                 kwargs=kwargs, rank=rank, metadata=metadata),id)
        return self._ignite(task)
    
    def add_new_worker(self, metadata={}, id:str=None)->Model4Task.Task:
        return self._add_new_obj(Model4Task.Worker(stop=False, metadata=metadata),id) 

    def add_new_function(self, function_obj:Model4Task.Function)->Model4Task.Function:  
        function_name = function_obj.__class__.__name__
        setattr(Model4Task,function_name,function_obj.__class__)
        return self._add_new_obj(function_obj,f'Function:{function_name}')
        
    def find_task(self,id:str) -> Model4Task.Task: return self.find(id)

    def find_worker(self,id:str) -> Model4Task.Worker: return self.find(id)
    
    def find_function(self,function_name:str) -> Model4Task.Function:
        return self.find(f'Function:{function_name}')
    
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

    def worker_list(self)->list[Model4Task.Worker]:
        return self.find_all('Worker:*')
    
    def print_workers(self):
        ws = self.worker_list()
        print('#######################')
        for w in ws:print(f'{w.get_id()},stop:{w.stop},status:{w.status}')
        print('#######################')
    
    def stop_workers(self):
        [w.get_controller().revoke() for w in self.worker_list()]

    def start_workers(self):
        [w.get_controller().start() for w in self.worker_list()]

    def task_list(self)->list[Model4Task.Task]:
        return self.find_all('Task:*')

    def print_tasks(self):
        ts = self.task_list()
        print('#######################')
        for t in ts:print(f'{t.get_id()},name:{t.name},status:{t.status}')
        print('#######################')