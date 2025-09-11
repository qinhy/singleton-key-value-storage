# Singleton Key-Value Storage System

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
  - [Python](#python)
  - [JavaScript/TypeScript](#javascripttypescript)
- [Usage](#usage)
  - [Python](#python-1)
  - [JavaScript/TypeScript](#javascripttypescript-1)
  - [Managing Slaves](#managing-slaves)
- [Optional Dependencies](#optional-dependencies)
- [Advanced Features](#advanced-features)
  - [Encryption Support](#encryption-support)
  - [Basic Model Support](#basic-model-support)
- [Contributing](#contributing)
- [License](#license)
- [Links](#links)

## Overview
This project implements a flexible, extensible key-value storage system using the singleton design pattern. It supports multiple backends, including Python dictionaries, Redis, Firestore, AWS DynamoDB, MongoDB, SQLite, CouchDB, and the local file system. The system provides basic CRUD operations and supports slave synchronization.

Available in multiple languages:
- Python
- JavaScript
- TypeScript

## Features
- **Singleton pattern** for all storage backends
- **Cross-language support**: Python, JavaScript, and TypeScript implementations
- **Pluggable backends**: Python dict, Redis, Firestore, AWS DynamoDB, MongoDB, SQLite, CouchDB, FileSystem
- **Unified CRUD API** across all backends
- **Optional encryption support** for sensitive data
- **Easy backend switching** without code changes
- **Slave synchronization** for data replication

## Installation

### Python
Install directly from GitHub using pip:
```bash
pip install "git+https://github.com/qinhy/singleton-key-value-storage.git"
```
To install with specific backend dependencies (e.g., Redis and MongoDB):
```bash
pip install "git+https://github.com/qinhy/singleton-key-value-storage.git#egg=singleton-key-value-storage[redis,mongo]"
```
Available extras: `redis`, `firestore`, `aws`, `mongo`, `couch`, `pydantic`, `all`

### JavaScript/TypeScript
Clone the repository and use the files directly in your project:
```bash
git clone https://github.com/qinhy/singleton-key-value-storage.git
```
Or install via npm/yarn (if published):
```bash
npm install singleton-key-value-storage
# or
yarn add singleton-key-value-storage
```

## Usage

### Python

#### Basic Initialization
```python
from SingletonKeyValueStorage import SingletonKeyValueStorage
storage = SingletonKeyValueStorage()
storage.python_backend()  # Use Python dict backend
```

#### CRUD Operations
```python
storage.set('key1', {'data': 'value1'})
print(storage.get('key1'))  # {'data': 'value1'}
print(storage.exists('key1'))  # True
storage.delete('key1')
print(storage.keys('*key*'))  # []
```

#### Switching Backends
```python
# Redis backend
storage.redis_backend(redis_URL="redis://127.0.0.1:6379")

# Firestore backend
storage.firestore_backend(project_id="your-gcp-project", collection="your-collection")

# AWS DynamoDB backend
storage.aws_backend(table_name="your-dynamodb-table")

# MongoDB backend
storage.mongo_backend(
    mongo_URL="mongodb://127.0.0.1:27017/", 
    db_name="SingletonDB", 
    collection_name="store"
)

# SQLite backend
storage.sqlite_backend(mode="sqlite.db")

# File system backend
storage.file_backend(storage_dir="./data")

# CouchDB backend
storage.couch_backend(
    couchdb_URL="http://127.0.0.1:5984", 
    username="user", 
    password="pass"
)
```

### JavaScript/TypeScript

#### Basic Initialization
```javascript
// JavaScript
import { SingletonKeyValueStorage } from './Storage.js';

const storage = SingletonKeyValueStorage.getInstance();
storage.pythonBackend();  // Use JavaScript object backend
```

```typescript
// TypeScript
import { SingletonKeyValueStorage } from './Storage';

const storage = SingletonKeyValueStorage.getInstance();
storage.pythonBackend();  // Use TypeScript object backend
```

#### CRUD Operations
```javascript
storage.set('key1', {data: 'value1'});
console.log(storage.get('key1'));  // {data: 'value1'}
console.log(storage.exists('key1'));  // true
storage.delete('key1');
console.log(storage.keys('*key*'));  // []
```

### Managing Slaves
You can add slave storages for synchronization across different backends:

```python
# Python example
main_storage = SingletonKeyValueStorage()
main_storage.python_backend()

slave_storage = SingletonKeyValueStorage()
slave_storage.file_backend(storage_dir="./data")

main_storage.add_slave(slave_storage)

# Now when you set a value in main_storage, it will be synchronized to slave_storage
main_storage.set('key1', {'data': 'value1'})
```

## Optional Dependencies

### Python
- `redis` for Redis backend
- `google-cloud-firestore` for Firestore backend
- `boto3` for AWS DynamoDB backend
- `pymongo` for MongoDB backend
- `requests` for CouchDB backend
- `pydantic` for advanced model support

## Advanced Features

### Encryption Support
The library supports encryption for sensitive data:

```python
# Python example with encryption
from SingletonKeyValueStorage import SingletonKeyValueStorage

storage = SingletonKeyValueStorage()
storage.python_backend()

# Enable encryption with a public key
storage.enable_encryption(public_key_path="./public_key.pem")

# Data will be automatically encrypted when stored
storage.set('sensitive_key', {'secret': 'confidential_data'})
```

```javascript
// JavaScript/TypeScript example with encryption
import { SingletonKeyValueStorage } from './Storage';

const storage = SingletonKeyValueStorage.getInstance();
storage.pythonBackend();

// Enable encryption with a public key
storage.enableEncryption("./public_key.pem");

// Data will be automatically encrypted when stored
storage.set('sensitive_key', {secret: 'confidential_data'});
```

### Basic Model Support
The library includes a BasicModel class for structured data handling:

```python
from SingletonKeyValueStorage import BasicModel, SingletonKeyValueStorage

class User(BasicModel):
    def __init__(self, name, email):
        self.name = name
        self.email = email

# Store model instances
storage = SingletonKeyValueStorage()
storage.python_backend()

user = User("John Doe", "john@example.com")
storage.set('user1', user)

# Retrieve model instance
retrieved_user = storage.get('user1')
print(retrieved_user.name)  # John Doe
```

## Contributing
Contributions are welcome! Here's how you can help:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

Please make sure to update tests as appropriate and follow the existing code style.

## License
MIT License

## Links
- [GitHub Repository](https://github.com/qinhy/singleton-key-value-storage)
- [Report Issues](https://github.com/qinhy/singleton-key-value-storage/issues)