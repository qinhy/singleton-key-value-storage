from SingletonKeyValueStorage import SingletonKeyValueStorage

#################################### basic
store = SingletonKeyValueStorage()
store.python_backend()
store.set('test1', {'data': 123})
print(store.get('test1'))

# it is singleton
store = SingletonKeyValueStorage()
store.python_backend()
print(store.dumps())