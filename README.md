# Python Singleton Key-Value Storage System

## Overview
This project implements a key-value storage system in Python with a singleton design pattern. It allows for different backends such as Python dictionaries, Redis, and Firestore. The system supports basic CRUD operations and can handle multiple slaves for synchronization.

## Components

### SingletonPythonDictStorage and SingletonPythonDictStorageController
- A singleton class that maintains a single instance of the dictionary storage.
- Supports adding slaves that can be notified when the dictionary is modified.
- Manages interactions with the `SingletonPythonDictStorage`.
- Implements methods to perform CRUD operations on the storage and manage slaves.

### SingletonFirestoreStorage and SingletonFirestoreStorageController
- require
    from google.cloud import firestore
    os.environ['GOOGLE_PROJECT_ID']
    os.environ['GOOGLE_FIRESTORE_COLLECTION']

### SingletonRedisStorage and SingletonRedisStorageController
- require
    import redis
    os.environ['REDIS_URL'] # 'redis://127.0.0.1:6379'


### SingletonKeyValueStorage
- Acts as a facade for various storage backends.
- Facilitates switching between different storage implementations such as Python dictionary, Redis, and Firestore.

## Usage

### Initialization
```python
storage = SingletonKeyValueStorage()
storage.python_backend()  # Initialize with Python dictionary as the backend
```

### CRUD Operations
```python
# Add a key-value pair
storage.set('key1', {'data': 'value1'})

# Retrieve a value
print(storage.get('key1'))

# Check if a key exists
print(storage.exists('key1'))

# Delete a key
storage.delete('key1')

# List keys with a specific pattern
print(storage.keys('*key*'))
```

### Managing Slaves
Slaves can be added to the storage. These slaves are updated whenever changes occur in the main storage.

```python
class ExampleSlave:
    def set(self, key, value):
        print(f"Slave setting: {key} = {value}")

    def delete(self, key):
        print(f"Slave deleting: {key}")

slave = ExampleSlave()
storage.add_slave(slave)

# Now operations on `storage` will also notify `slave`.
storage.set('key2', {'data': 'value2'})
```

### Serialization ( Firestore is not support )
The Python dictionary storage can be dumped to a JSON file and loaded from it.

```python
# Dump to file
storage.state.dump('storage_dump.json')

# Load from file
storage.state.load('storage_dump.json')
```

## Additional Features
- Extensible to other backends like Redis and Firestore by implementing the corresponding storage controller classes.

## Limitations
- Current implementation does not provide the Redis and Firestore backend implementations.
- Error handling and data validation are minimal and should be enhanced for production use.