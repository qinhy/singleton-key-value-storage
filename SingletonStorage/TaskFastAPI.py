from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from TaskModel import ExmpalePowerFunction, ExmpalePrintFunction, TaskStore, Model4Task
# from TaskModel

app = FastAPI()
task_manager = TaskStore()
task_manager.add_new_function(ExmpalePowerFunction())
task_manager.add_new_function(ExmpalePrintFunction())

#/tasks/ExmpalePrintFunction/['3,4']
#/tasks/ExmpalePowerFunction/[3,4]
@app.get("/tasks/{name}/{args}",description="you can try /tasks/ExmpalePrintFunction/['3,4'] or /tasks/ExmpalePowerFunction/[3,4]")
def add_new_task(name:str,args:str):
    return task_manager.add_new_task(name, eval(args))

@app.get("/workers/new")
def add_new_worker():
    task_manager.add_new_worker()
    return task_manager.task_list()

# @app.post("/functions/")
# def add_new_function(function: Model4Task.Function):
#     return task_manager.add_new_function(function.function_obj)

@app.get("/tasks/{id}")
def find_task(id: str):
    return task_manager.find_task(id)

@app.get("/workers/{id}")
def find_worker(id: str):
    return task_manager.find_worker(id)

# @app.post("/workers/start/{id}")
# def stop_workers():
#     return task_manager.stop_workers()

# @app.post("/workers/delete/{id}")
# def stop_workers():
#     return task_manager.stop_workers()

@app.get("/functions/{id}")
def find_function(id: str):
    return task_manager.find_function(id)

@app.get("/functions/")
def function_list():
    return task_manager.function_list()

@app.get("/workers/")
def get_workers():
    return task_manager.get_workers()

@app.get("/workers/stop/")
def stop_workers():
    return task_manager.stop_workers()

@app.get("/tasks/")
def task_list():
    return task_manager.task_list()
