# from https://github.com/qinhy/singleton-key-value-storage.git
import unittest
from fastapi import FastAPI, HTTPException
from Storages import SingletonKeyValueStorage, SingletonStorageController

from dateutil import parser

######################################### connect to local key-value store
store = SingletonKeyValueStorage()
# store.mongo_backend()

class RESTapi:   
    api = FastAPI()
    @api.post("/store/set/{key}")
    async def set_item(key:str, value: dict):
        return store.set(key, value)

    @api.get("/store/get/{key}")
    async def get_item(key: str, timestamp: str = ''):
        result = store.get(key)
        if result is None:
            raise HTTPException(status_code=404, detail="Item not found")

        # Convert both timestamps to datetime objects
        if timestamp:
            try:
                provided_ts = parser.isoparse(timestamp)
                stored_ts = parser.isoparse(result.get('update_time',timestamp))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid timestamp format")

            # Compare the timestamps
            if provided_ts >= stored_ts:
                raise HTTPException(status_code=304, detail="No updates")

        return result

    @api.delete("/store/delete/{key}")
    async def delete_item(key:str):
        success = store.delete(key)
        if not success:
            raise HTTPException(status_code=404, detail="Item not found to delete")
        return {"deleted": key}

    @api.get("/store/exists/{key}")
    async def exists_item(key:str):
        return {"exists":store.exists(key)}

    @api.get("/store/keys/{pattern}")
    async def get_keys(pattern:str = '*'):
        return store.keys(pattern)

    @api.post("/store/loads/")
    async def load_items(item_json:str):
        store.loads(item_json)
        return {"loaded": True}

    @api.get("/store/dumps/")
    async def dump_items():
        return store.dumps()


#### test client 
import requests
import json

class RestApiStorageController(SingletonStorageController):
    def __init__(self, base_url: str, default_headers: dict = None):
        self.base_url = base_url
        self.default_headers = default_headers or {"Content-Type": "application/json"}

    def exists(self, key: str) -> bool:
        url = f"{self.base_url}/exists/{key}"
        response = requests.get(url, headers=self.default_headers)
        if response.status_code == 200:
            return response.json().get("exists", False)
        return False

    def set(self, key: str, value: dict):
        url = f"{self.base_url}/set/{key}"
        response = requests.post(url, data=json.dumps(value), headers=self.default_headers)
        if response.status_code != 200:
            print(f"Error: Unable to set key {key}")
        return response.json()

    def get(self, key: str, timestamp: str = '') -> dict:
        url = f"{self.base_url}/get/{key}"
        params = {}
        if timestamp:
            params['timestamp'] = timestamp
        
        response = requests.get(url, params=params, headers=self.default_headers)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 304:  # Assuming API returns 304 for no updates
            return {"message": "No updates"}
        return None

    def delete(self, key: str):
        url = f"{self.base_url}/delete/{key}"
        response = requests.delete(url, headers=self.default_headers)
        if response.status_code != 200:
            print(f"Error: Unable to delete key {key}")

    def keys(self, pattern: str = '*') -> list[str]:
        url = f"{self.base_url}/keys/{pattern}"
        response = requests.get(url, headers=self.default_headers)
        return response.json()

# Example Usage:
# api_controller = RestApiStorageController(base_url="http://yourapi.com", default_headers={"Authorization": "Bearer token"})
# api_controller.set("example_key", {"data": "example_value"})
# value = api_controller.get("example_key", timestamp="2024-09-19T09:31:43.084976Z")


class Tests(unittest.TestCase):
    def __init__(self,*args,**kwargs)->None:
        super().__init__(*args,**kwargs)
        self.store = RestApiStorageController('http://127.0.0.1:8000/store')

    def test_all(self,num=1):
        for i in range(num):self.test_all_cases()

    def test_all_cases(self):
        self.test_set_and_get()
        self.test_exists()
        self.test_delete()
        self.test_keys()
        self.test_get_nonexistent()
        self.test_dump_and_load()
        # self.test_version()
        # self.test_slaves()

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
