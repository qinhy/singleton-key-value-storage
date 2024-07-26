

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from Storages import SingletonKeyValueStorage

######################################### connect to local key-value store
store = SingletonKeyValueStorage()
# store.mongo_backend()

api = FastAPI()

class Item(BaseModel):
    key: str
    value: dict = None
    
@api.post("/store/set/")
async def set_item(item: Item):
    return store.set(item.key, item.value)

@api.get("/store/get/{key}")
async def get_item(key: str):
    result = store.get(key)
    if result is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return result

@api.delete("/store/delete/{key}")
async def delete_item(key: str):
    success = store.delete(key)
    if not success:
        raise HTTPException(status_code=404, detail="Item not found to delete")
    return {"deleted": key}

@api.get("/store/exists/{key}")
async def exists_item(key: str):
    return {"exists": store.exists(key)}

@api.get("/store/keys/{pattern}")
async def get_keys(pattern: str = '*'):
    return store.keys(pattern)

@api.post("/store/loads/")
async def load_items(item_json: str):
    store.loads(item_json)
    return {"loaded": True}

@api.get("/store/dumps/")
async def dump_items():
    return store.dumps()
