# from https://github.com/qinhy/singleton-key-value-storage.git
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from Storages import SingletonKeyValueStorage
from datetime import datetime
from dateutil import parser

######################################### connect to local key-value store
store = SingletonKeyValueStorage()
# store.mongo_backend()

class RESTapi:   
    api = FastAPI()
    class Item(BaseModel):
        key: str
        value: dict = None
        
    @api.post("/store/set/")
    async def set_item(item: Item):
        return store.set(item.key, item.value)

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
import fnmatch

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
        url = f"{self.base_url}/keys"
        response = requests.get(url, headers=self.default_headers)
        if response.status_code == 200:
            all_keys = response.json().get("keys", [])
            return fnmatch.filter(all_keys, pattern)
        return []

# Example Usage:
# api_controller = RestApiStorageController(base_url="http://yourapi.com", default_headers={"Authorization": "Bearer token"})
# api_controller.set("example_key", {"data": "example_value"})
# value = api_controller.get("example_key", timestamp="2024-09-19T09:31:43.084976Z")
