# from https://github.com/qinhy/singleton-key-value-storage.git
import base64
import hashlib
import math
import os
import re
import sqlite3
import threading
import queue
import time
import uuid
import fnmatch
import json
import unittest
import urllib
import urllib.parse
from urllib.parse import urlparse

try:
    from .Storage import SingletonKeyValueStorage,AbstractStorageController
except Exception as e:
    from Storage import SingletonKeyValueStorage,AbstractStorageController


def try_if_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

# self checking
firestore_back = try_if_error(lambda:__import__('google.cloud.firestore')) is None

if firestore_back:
    from google.cloud import firestore
    class SingletonFirestoreStorage:
        _instance = None
        _meta = {}
        
        def __new__(cls,google_project_id:str=None,google_firestore_collection:str=None):
            same_proj = cls._meta.get('google_project_id',None)==google_project_id
            same_coll = cls._meta.get('google_firestore_collection',None)==google_firestore_collection

            if cls._instance is not None and same_proj and same_coll:
                return cls._instance
            
            if google_project_id is None or google_firestore_collection is None:
                raise ValueError('google_project_id or google_firestore_collection must not be None at first time')
            
            if cls._instance is not None and (not same_proj or not same_coll):
                cls._instance.model.close()
                print(f'warnning: instance changed to {google_project_id} , {google_firestore_collection}')

            cls._instance = super(SingletonFirestoreStorage, cls).__new__(cls)
            cls._instance.uuid = uuid.uuid4()
            cls._instance.model = firestore.Client(project=google_project_id)
            cls._instance.collection = cls._instance.model.collection(google_firestore_collection)

            cls._meta['google_project_id']=google_project_id
            cls._meta['google_firestore_collection']=google_firestore_collection

            return cls._instance
        
        def __init__(self,google_project_id:str=None,google_firestore_collection:str=None):
            self.uuid:str = self.uuid
            self.model:firestore.Client = self.model    
            self.collection:firestore.CollectionReference = self.collection
        
        @staticmethod
        def build(google_project_id:str=None,google_firestore_collection:str=None):
            return SingletonFirestoreStorageController(SingletonFirestoreStorage(google_project_id,google_firestore_collection))

    class SingletonFirestoreStorageController(AbstractStorageController):
        def __init__(self, model: SingletonFirestoreStorage):
            self.model:SingletonFirestoreStorage = model

        def exists(self, key: str)->bool:
            doc = self.model.collection.document(key).get()
            return doc.exists
        
        def set(self, key: str, value: dict):
            self.model.collection.document(key).set(value)

        def get(self, key: str)->dict:
            doc = self.model.collection.document(key).get()
            return doc.to_dict() if doc.exists else None
        
        def delete(self, key: str):
            self.model.collection.document(key).delete()

        def keys(self, pattern: str='*')->list[str]:
            docs = self.model.collection.stream()
            keys = [doc.id for doc in docs]
            return fnmatch.filter(keys, pattern)      
