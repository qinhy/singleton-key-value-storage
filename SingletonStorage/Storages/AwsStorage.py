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
    from .Storage import SingletonKeyValueStorage,SingletonStorageController
except Exception as e:
    from Storage import SingletonKeyValueStorage,SingletonStorageController

def try_if_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

# self checking
aws_dynamo     = try_if_error(lambda:__import__('boto3')) is None
aws_s3         = try_if_error(lambda:__import__('boto3')) is None

if aws_dynamo:
    import boto3
    from botocore.exceptions import ClientError

    class SingletonDynamoDBStorage:
        _instance = None
        
        def __new__(cls,your_table_name):
            if cls._instance is None:
                cls._instance = super(SingletonDynamoDBStorage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                cls._instance.client = boto3.resource('dynamodb')
                cls._instance.table = cls._instance.client.Table(your_table_name)
            return cls._instance

        def __init__(self,your_table_name):
            self.uuid = self.uuid
            self.client = self.client
            self.table = self.table

    class SingletonDynamoDBStorageController(SingletonStorageController):
        def __init__(self, model:SingletonDynamoDBStorage):
            self.model:SingletonDynamoDBStorage = model
        
        def exists(self, key: str)->bool:
            try:
                response = self.model.table.get_item(Key={'key': key})
                return 'Item' in response
            except ClientError as e:
                print(f'Error checking existence: {e}')
                return False

        def set(self, key: str, value: dict):
            try:
                self.model.table.put_item(Item={'key': key, 'value': json.dumps(value)})
            except ClientError as e:
                print(f'Error setting value: {e}')

        def get(self, key: str)->dict:
            try:
                response = self.model.table.get_item(Key={'key': key})
                if 'Item' in response:
                    return json.loads(response['Item']['value'])
                return None
            except ClientError as e:
                print(f'Error getting value: {e}')
                return None

        def delete(self, key: str):
            try:
                self.model.table.delete_item(Key={'key': key})
            except ClientError as e:
                print(f'Error deleting value: {e}')

        def keys(self, pattern: str='*')->list[str]:
            # Convert simple wildcard patterns to regular expressions for filtering
            regex = fnmatch.translate(pattern)
            compiled_regex = re.compile(regex)

            matched_keys = []
            try:
                # Scan operation with no filters - potentially very costly
                scan_kwargs = {
                    'ProjectionExpression': "key",
                    'FilterExpression': "attribute_exists(key)"
                }
                done = False
                start_key = None

                while not done:
                    if start_key:
                        scan_kwargs['ExclusiveStartKey'] = start_key
                    response = self.model.table.scan(**scan_kwargs)
                    items = response.get('Items', [])
                    matched_keys.extend([item['key'] for item in items if compiled_regex.match(item['key'])])

                    start_key = response.get('LastEvaluatedKey', None)
                    done = start_key is None
            except ClientError as e:
                print(f'Error scanning keys: {e}')

            return matched_keys

if aws_s3:
    import boto3
    from mypy_boto3_s3 import S3Client
    from botocore.exceptions import ClientError
    class SingletonS3Storage:
        _instance = None
        _meta = {}
        
        def __new__(cls,bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = '/SingletonS3Storage'):
            meta = {                
                'bucket_name':bucket_name,
                'aws_access_key_id':aws_access_key_id,
                'aws_secret_access_key':aws_secret_access_key,
                'region_name':region_name,
            }
            def init():                
                cls._instance = super(SingletonS3Storage, cls).__new__(cls)
                cls._instance.uuid = uuid.uuid4()
                cls._instance.s3 = boto3.client('s3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    region_name=region_name
                )
                cls._instance.bucket_name = bucket_name
                cls._instance._meta = meta
            if cls._instance is None:
                init()
            elif cls._meta!=meta:                
                print(f'warnning: instance changed to new one')
                init()

            return cls._instance

        def __init__(self,bucket_name,
                    aws_access_key_id,aws_secret_access_key,region_name,
                    s3_storage_prefix_path = '/SingletonS3Storage'):
            self.uuid = self.uuid
            self.s3:S3Client = self.s3
            self.bucket_name = self.bucket_name
            self.s3_storage_prefix_path = '/SingletonS3Storage'
    class SingletonS3StorageController(SingletonStorageController):
        def __init__(self, model:SingletonS3Storage):
            self.model:SingletonS3Storage = model
            self.bucket_name = self.model.bucket_name

        def _s3_path(self,key:str):
            return f'{self.model.s3_storage_prefix_path}/{key}.json'
            
        def _de_s3_path(self,path:str):
            return path.replace(f'{self.model.s3_storage_prefix_path}/',''
                         ).replace(f'.json','')
        
        def exists(self, key: str)->bool:
            try:
                self.model.s3.head_object(Bucket=self.bucket_name,
                                          Key=self._s3_path(key))
                return True
            except self.model.s3.exceptions.NoSuchKey:
                return False
            
        def set(self, key: str, value: dict):
            json_data = json.dumps(value)
            self.model.s3.put_object(Bucket=self.bucket_name,
                                        Key=self._s3_path(key), Body=json_data)
        
        def get(self, key: str)->dict:
            obj = self.model.s3.get_object(Bucket=self.bucket_name, Key=self._s3_path(key))
            return json.loads(obj['Body'].read().decode('utf-8'))
                
        def delete(self, key):
            self.model.s3.delete_object(Bucket=self.bucket_name, Key=self._s3_path(key))
            
        def keys(self, pattern='*')->list[str]:
            keys = []
            paginator = self.model.s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name,
                                           Prefix=self.model.s3_storage_prefix_path):
                for obj in page.get('Contents', []):
                    keys.append(self._de_s3_path(obj['Key']))
                    
            return fnmatch.filter(keys, pattern)

SingletonKeyValueStorage.backs['s3']=lambda *args,**kwargs:SingletonS3StorageController(SingletonS3Storage(*args,**kwargs)) if aws_s3 else None