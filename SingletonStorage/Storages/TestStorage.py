# from https://github.com/qinhy/singleton-key-value-storage.git
import os
import json
import unittest

try:
    from .Storage import SingletonKeyValueStorage
    from .RedisStorage import *
    from .AwsStorage import *
    from .FirestoreStorage import *
    from .SqliteStorage import *
    from .MongoStorage import *
except Exception as e:
    from Storage import SingletonKeyValueStorage
    from RedisStorage import *
    from AwsStorage import *
    from FirestoreStorage import *
    from SqliteStorage import *
    from MongoStorage import *

class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs)->None:
        super().__init__(*args,**kwargs)
        self.store = SingletonKeyValueStorage()

    def test_all(self,num=1):
        self.test_python(num)
        self.test_sqlite(num)
        self.test_sqlite_pymix(num)
        # self.test_mongo(num)
        # self.test_redis(num)
        # self.test_firestore(num)

    def test_python(self,num=1):
        self.store.python_backend()
        for i in range(num):self.test_all_cases()

    def test_redis(self,num=1):
        self.store.redis_backend()
        for i in range(num):self.test_all_cases()

    def test_sqlite(self,num=1):
        self.store.sqlite_backend()
        for i in range(num):self.test_all_cases()

    def test_sqlite_pymix(self,num=1):
        self.store.sqlite_pymix_backend()
        for i in range(num):self.test_all_cases()

    def test_firestore(self,num=1):
        self.store.firestore_backend()
        for i in range(num):self.test_all_cases()

    def test_mongo(self,num=1):
        self.store.mongo_backend()
        for i in range(num):self.test_all_cases()

    def test_s3(self,num=1):
        self.store.s3_backend(
                    bucket_name = os.environ['AWS_S3_BUCKET_NAME'],
                    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                    region_name=os.environ['AWS_DEFAULT_REGION'])
        for i in range(num):self.test_all_cases()

    def test_all_cases(self):
        self.test_set_and_get()
        self.test_exists()
        self.test_delete()
        self.test_keys()
        self.test_get_nonexistent()
        self.test_dump_and_load()
        self.test_version()
        self.test_slaves()
        self.store.clean()

    def test_set_and_get(self):
        self.store.set('test1', {'data': 123})
        self.assertEqual(self.store.get('test1'), {'data': 123}, "The retrieved value should match the set value.")

    def test_exists(self):
        self.store.set('test2', {'data': 456})
        self.assertTrue(self.store.exists('test2'), "Key should exist after being set.")

    def test_delete(self):
        self.store.set('test3', {'data': 789})
        self.store.delete('test3')
        self.assertFalse(self.store.exists('test3'), "Key should not exist after being deleted.")

    def test_keys(self):
        self.store.set('alpha', {'info': 'first'})
        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        expected_keys = ['alpha', 'abeta']
        self.assertEqual(sorted(self.store.keys('a*')), sorted(expected_keys), 
                         "Should return the correct keys matching the pattern.")

    def test_get_nonexistent(self):
        self.assertEqual(self.store.get('nonexistent'), None, "Getting a non-existent key should return None.")
        
    def test_dump_and_load(self):
        raw = {"test1": {"data": 123}, "test2": {"data": 456}, "alpha": {"info": "first"}, 
               "abeta": {"info": "second"}, "gamma": {"info": "third"}}
        self.store.dump('test.json')

        self.store.clean()
        self.assertEqual(self.store.dumps(),'{}', "Should return the correct keys and values.")

        self.store.load('test.json')
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")
        
        self.store.clean()
        self.store.loads(json.dumps(raw))
        self.assertEqual(json.loads(self.store.dumps()),raw, "Should return the correct keys and values.")

    def test_slaves(self):
        if self.store.conn.__class__.__name__=='SingletonPythonDictStorageController':return
        store2 = SingletonKeyValueStorage()
        self.store.add_slave(store2)
        self.store.set('alpha', {'info': 'first'})
        self.store.set('abeta', {'info': 'second'})
        self.store.set('gamma', {'info': 'third'})
        self.store.delete('abeta')
        self.assertEqual(json.loads(self.store.dumps()),json.loads(store2.dumps()), "Should return the correct keys and values.")

    def test_version(self):
        self.store.clean()
        self.store.version_controll = True
        self.store.set('alpha', {'info': 'first'})
        data1 = self.store.dumps()
        v1 = self.store.get_current_version()

        self.store.set('abeta', {'info': 'second'})
        v2 = self.store.get_current_version()
        data2 = self.store.dumps()

        self.store.set('gamma', {'info': 'third'})
        self.store.local_to_version(v1)

        self.assertEqual(json.loads(self.store.dumps()),json.loads(data1), "Should return the same keys and values.")

        self.store.local_to_version(v2)
        self.assertEqual(json.loads(self.store.dumps()),json.loads(data2), "Should return the same keys and values.")