
from datetime import datetime
import json
import math
import random
from uuid import uuid4
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, Field

from BasicModel import BasicStore,Controller4Basic,Model4Basic

class Controller4Machine:
    class AbstractObjController(Controller4Basic.AbstractObjController):
        pass
    class MachineController(AbstractObjController):
        def __init__(self, store, model):
            self.model:Model4Machine.Machine = model
            self._store:MachinesStore = store

        def turn_left(self, degrees):
            # Turn the machine left by a certain degree
            self.orientation = (self.orientation + degrees) % 360
            print(f"Turned left {degrees} degrees. New orientation: {self.orientation}")
            return self.random_crash()

        def turn_right(self, degrees):
            # Turn the machine right by a certain degree
            self.orientation = (self.orientation - degrees) % 360
            print(f"Turned right {degrees} degrees. New orientation: {self.orientation}")
            return self.random_crash()

        def move_forward(self, distance):
            # Move the machine forward in the direction of the current orientation
            radian = math.radians(self.orientation)
            self.position[0] += distance * math.cos(radian)  # x position changes
            self.position[1] += distance * math.sin(radian)  # y position changes
            print(f"Moved forward {distance} distance. New position: {self.position}")
            return self.random_crash()

        def move_backward(self, distance):
            # Move the machine backward opposite to the current orientation
            radian = math.radians(self.orientation)
            self.position[0] -= distance * math.cos(radian)  # x position changes
            self.position[1] -= distance * math.sin(radian)  # y position changes
            print(f"Reversed {distance} distance. New position: {self.position}")
            return self.random_crash()

        def get_machine_state(self):
            return self.model.orientation,self.model.position
        
        def random_crash(self):
            if random.random() > 0.6:
                print('This machine crashed!')
                self.reset()
                False
            else:
                return True

        def reset(self):
            # Reset the machine to the initial state
            self.update(orientation = 0)
            self.update(position = [0, 0])
            print("Machine has been reset to the 0,(0,0) state.")
        

class Model4Machine:
    class AbstractObj(Model4Basic.AbstractObj):
        pass

    class Machine(AbstractObj):    
        orientation:float = 0.0  # In degrees, 0 pointing east( x+ )
        position:list[float] = [0.0,0.0]  # Position as a list [x, y]


        _controller: Controller4Machine.MachineController = None
        def get_controller(self)->Controller4Machine.MachineController: return self._controller
        def init_controller(self,store):self._controller = Controller4Machine.MachineController(store,self)


class MachinesStore(BasicStore):

    def __init__(self) -> None:
        super().__init__()
    
    def _get_class(self, id: str, modelclass=Model4Machine):
        return super()._get_class(id, modelclass)
    
    def add_new_machine(self, major_name:str,minor_name:str,running_cost:int=0,parent_App_id:str=None) -> Model4Machine.App:
        return self.add_new_obj(Model4Machine.App(major_name=major_name,minor_name=minor_name,
                                           running_cost=running_cost,parent_App_id=parent_App_id))
        
    def find_all_machines(self)->list[Model4Machine.Machine]:
        return self.find_all('Machine:*')
    

def test():
    us = MachinesStore()
    us.add_new_machine('John','admin','123','John anna','123@123.com')
    return us
