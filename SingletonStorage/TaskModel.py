
import ctypes
from datetime import datetime
import inspect
import queue
import random
import sys
from threading import Thread
import time
from typing import Any, Dict
from zoneinfo import ZoneInfo
from pydantic import BaseModel
from .BasicModel import BasicStore, Controller4Basic, Model4Basic, now_utc

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
        def __init__(self, store, model):
            super().__init__(store, model)
            self._store:TaskStore = store

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

        def set_task(self,task=None):
            self.update_metadata('task',task)

        def worker_sleeping(self):
            self.set_status(Controller4Task.WorkerController.SLEEPING)
            self.set_task()

        def worker_processing(self,task):
            task:Model4Task.Task=task
            self.set_status(Controller4Task.WorkerController.PROCESSING)            
            task.get_controller().update(worker=self.model.get_id())
            task.get_controller().task_processing()
            self.set_task(task.model_dump())

        def worker_failure(self):
            self.set_status(Controller4Task.WorkerController.FAILURE)
            self.set_task()

        def __init__(self, store, model):
            self.model:Model4Task.Worker = model
            self._store:TaskStore = store
            
        def fresh_model(self):            
            self.model = self._store.find(self.model.get_id())

        def put_task(self,task_id:str):
            task = self._store.find_task(task_id)
            if task is None:raise ValueError(f'no such task of {task_id}')
            self.update(task_id_queue=self.model.task_id_queue + [task_id])
            return task

        def get_task(self):
            self.fresh_model()
            if self.model is None or len(self.model.task_id_queue)==0:return None
            task_id = self.model.task_id_queue.pop()
            self.update(task_id_queue=self.model.task_id_queue)
            return self._store.find_task(task_id)

        def serve(self):
            def _serve():
                while self._store.exists(self.model.get_id()):
                    self._update_timestamp()
                    self.store()
                    time.sleep(1)
                    self.fresh_model()
                    if self.model is None:break
                    if self.model.status!=self.PROCESSING:
                        task = self.get_task()
                        if task is not None:
                            self.start(task)
            Thread(target=_serve).start()
                
        def set_memo_id(self,memo_id=-1):
            self.update(thread_memo_id=memo_id)
        
        def get_memo_id(self):
            return self._store.find_worker(self.model.get_id()).thread_memo_id
        
        def _get_thread(self)->TraceableThread:
            thread_memo_id = self.get_memo_id()
            if thread_memo_id>0:
                return ctypes.cast(thread_memo_id, ctypes.py_object).value
            return None

        def start(self,task):
            thread = TraceableThread(target=self.do_task, args=(task,))
            self.set_memo_id(id(thread))
            thread.start()

        def revoke(self,wait=True):
            thread = self._get_thread()
            if thread is not None:
                thread.revoke() 
                if wait: thread.join()
            self._set_task_end()
                
        def _set_task_end(self):
            self.set_memo_id(-1)
            self.worker_sleeping()

        def do_task(self,task):
            try:
                res = 'null'
                task:Model4Task.Task = task
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
                    
            except Exception as e:
                print(e)
                self.worker_failure()
            finally:                
                self._set_task_end()
        
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
        # stop:bool = False
        task_id_queue:list[str] = []
        thread_memo_id:int = -1
        
        def is_sleeping(self):
            return self.status==Controller4Task.WorkerController.SLEEPING

        _controller: Controller4Task.WorkerController = None
        def get_controller(self)->Controller4Task.WorkerController: return self._controller

    class Function(AbstractObj):
        class Parameter(BaseModel):
            type: str
            description: str            

        name: str = 'null'
        description: str = None
        # arguments: Dict[str, Any] = None
        _properties: Dict[str, Parameter] = {}
        parameters: Dict[str, Any] = {"type": "object",'properties':_properties}
        required: list[str] = []        
        _parameters_description: Dict[str, str] = {}
        _string_arguments: str='\{\}'

        def _extract_signature(self):
            self.name=self.__class__.__name__
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
    
    def _get_rand_worker(self):
        # return self._task_queue
        ws = self.worker_list()
        if len(ws)==0:return None
        worker = random.choice(self.worker_list())
        return worker.get_controller()
    
    def loads(self, json_str):
        res = super().loads(json_str)
        for task in [i for i in self.task_list() if i.is_pending()]:
            w = self._get_rand_worker()
            if w:w.put_task(task.get_id())
        return res
    
    def dumps(self) -> str:
        self.stop_workers()
        res = super().dumps()
        return res
    
    def clean(self):
        self.stop_workers()
        return super().clean()
       
    def restart_task(self, id:str) -> Model4Task.Task:
        task = self.find(id)
        if task is None:raise ValueError('No such task of {id}')
        w = self._get_rand_worker()
        if w is None:raise ValueError('No active worker')
        return w.put_task(task.get_id())

    def add_new_task(self, function_obj, kwargs={}, rank=[0], metadata={}, id:str=None) -> Model4Task.Task:
        if type(function_obj) is not str:
            function_name = function_obj.__class__.__name__
            if not self.exists(function_name):self.add_new_function(function_obj)
        else:
            function_name = function_obj
        task = self._add_new_obj(Model4Task.Task(name=function_name, 
                                                 kwargs=kwargs, rank=rank, metadata=metadata),id)
        w = self._get_rand_worker()
        if w is None:raise ValueError('No active worker')
        return w.put_task(task.get_id())
    
    def add_new_worker(self, metadata={}, id:str=None)->Model4Task.Worker:
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

    def worker_list(self,timeout=10)->list[Model4Task.Worker]:
        for w in self.find_all('Worker:*'):
            if (now_utc() - w.update_time).seconds>timeout:
                w.get_controller().delete()
        return self.find_all('Worker:*')
        
    def print_workers(self):
        ws = self.worker_list()
        print('#######################')
        for w in ws:print(f'{w.get_id()},status:{w.status}')
        print('#######################')
    
    def stop_workers(self):
        [w.get_controller().revoke() for w in self.worker_list()]

    # def start_workers(self):
    #     [w.get_controller().start() for w in self.worker_list()]

    def task_list(self)->list[Model4Task.Task]:
        return self.find_all('Task:*')

    def print_tasks(self):
        ts = self.task_list()
        print('#######################')
        for t in ts:print(f'{t.get_id()},name:{t.name},status:{t.status}')
        print('#######################')