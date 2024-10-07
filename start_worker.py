import time
from SingletonStorage import Model4Task,TaskStore

descriptions = Model4Task.Function.param_descriptions

##### testing
@descriptions('just print some thing',
              msg='string to print')
class ExamplePrintFunction(Model4Task.Function):
    def __call__(self,msg:str):
        print(msg)

@descriptions('just power a number',
              a='base number',b='exponent number')
class ExamplePowerFunction(Model4Task.Function):
    def __call__(self,a:int,b:int):
        return a**b

@descriptions('infinity loop')
class ExampleInfinityLoopFunction(Model4Task.Function):
    def __call__(self):
        cnt=0
        while True:
            print(f'infinity loop {cnt}')
            time.sleep(0.5)
            cnt+=1

@descriptions('Raise error')
class ExampleRaiseError(Model4Task.Function):
    def __call__(self):
        raise ValueError('Something is wrong!!!')

@descriptions('generate Fibonacci sequence up to n-th number',
              n='the position in the Fibonacci sequence to compute')
class ExampleFibonacciFunction(Model4Task.Function):
    def __call__(self, n: int):
        def fibonacci(n):
            if n <= 1 : return n
            return fibonacci(n-1)+fibonacci(n-2)
        return fibonacci(n)


ts = TaskStore()
ts.redis_backend()

ts.add_new_function(ExamplePrintFunction())
ts.add_new_function(ExamplePowerFunction())
ts.add_new_function(ExampleInfinityLoopFunction())
ts.add_new_function(ExampleRaiseError())
ts.add_new_function(ExampleFibonacciFunction())

w = ts.add_new_worker()
w.get_controller().serve()
