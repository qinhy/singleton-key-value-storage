import time
from SingletonStorage import Model4Task,TaskStore

##### testing
class ExamplePrintFunction(Model4Task.Function):
    description: str = 'just print some thing'
    _parameters_description = dict(
        msg='string to print',
    )

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self._extract_signature()

    def __call__(self,msg:str):
        print(msg)

class ExamplePowerFunction(Model4Task.Function):
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

class ExampleInfinityLoopFunction(Model4Task.Function):
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

class ExampleRaiseError(Model4Task.Function):
    description: str = 'Raise error'

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self._extract_signature()

    def __call__(self):
        raise ValueError('Something is wrong!!!')

class ExampleFibonacciFunction(Model4Task.Function):
    description: str = 'generate Fibonacci sequence up to n-th number'
    _parameters_description = dict(
        n='the position in the Fibonacci sequence to compute'
    )

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self._extract_signature()

    def __call__(self, n: int):
        def fibonacci(n):
            if n <= 1 : return n
            return fibonacci(n-1)+fibonacci(n-2)
        return fibonacci(n)

ts = TaskStore()
ts.redis_backend()
print(ts.worker_list())
w = ts.worker_list()[0]
t = ts.add_new_task(ExamplePrintFunction(),dict(msg='hello!'))
ts.add_new_task(ExampleFibonacciFunction(),dict(n=36))
