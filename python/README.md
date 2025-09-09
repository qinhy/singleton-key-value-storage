# Python Singleton Key-Value Storage System

## Overview
This project implements a flexible, extensible key-value storage system in Python using the singleton design pattern. It supports multiple backends, including Python dictionaries, Redis, Firestore, AWS DynamoDB, MongoDB, SQLite, CouchDB, and the local file system. The system provides basic CRUD operations and supports slave synchronization.

## Features
- Singleton pattern for all storage backends
- Pluggable backends: Python dict, Redis, Firestore, AWS DynamoDB, MongoDB, SQLite, CouchDB, FileSystem
- Unified CRUD API
- Optional encryption support
- Easy backend switching

## Installation
Install directly from GitHub using pip:
```bash
pip install "git+https://github.com/qinhy/singleton-key-value-storage.git"
```
To install with specific backend dependencies (e.g., Redis and MongoDB):
```bash
pip install "git+https://github.com/qinhy/singleton-key-value-storage.git#egg=singleton-key-value-storage[redis,mongo]"
```
Available extras: `redis`, `firestore`, `aws`, `mongo`, `couch`, `pydantic`, `all`

## Usage
### Basic Initialization
```python
from SingletonKeyValueStorage import SingletonKeyValueStorage
storage = SingletonKeyValueStorage()
storage.python_backend()  # Use Python dict backend
```
### CRUD Operations
```python
storage.set('key1', {'data': 'value1'})
print(storage.get('key1'))
print(storage.exists('key1'))
storage.delete('key1')
print(storage.keys('*key*'))
```
### Switching Backends
```python
storage.redis_backend(redis_URL="redis://127.0.0.1:6379")
storage.firestore_backend(project_id="your-gcp-project", collection="your-collection")
storage.aws_backend(table_name="your-dynamodb-table")
storage.mongo_backend(mongo_URL="mongodb://127.0.0.1:27017/", db_name="SingletonDB", collection_name="store")
storage.sqlite_backend(mode="sqlite.db")
storage.file_backend(storage_dir="./data")
storage.couch_backend(couchdb_URL="http://127.0.0.1:5984", username="user", password="pass")
```
### Managing Slaves
You can add slave storages for synchronization. See the code for details.

## Optional Dependencies
- `redis` for Redis backend
- `google-cloud-firestore` for Firestore backend
- `boto3` for AWS DynamoDB backend
- `pymongo` for MongoDB backend
- `requests` for CouchDB backend
- `pydantic` for advanced model support

## License
MIT License

## Links
- [GitHub Repository](https://github.com/qinhy/singleton-key-value-storage)