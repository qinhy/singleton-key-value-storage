# from https://github.com/qinhy/singleton-key-value-storage.git

import fnmatch
import re
import uuid
from urllib.parse import ParseResult, urlparse
import requests

try:
    from .Storage import SingletonKeyValueStorage, AbstractStorageController
except Exception as e:
    from Storage import SingletonKeyValueStorage, AbstractStorageController

class SingletonCouchDBStorage:
    _instance = None
    _meta = {}

    def __new__(cls, couchdb_URL=None, username=None, password=None, dbname="singleton_db"):
        if cls._instance is not None and cls._meta.get('couchdb_URL', None) == couchdb_URL:
            return cls._instance

        if couchdb_URL is None:
            raise ValueError('couchdb_URL must not be None at first time (e.g. http://127.0.0.1:5984)')

        url:ParseResult = urlparse(couchdb_URL)
        if not username or not password:
            if url.username and url.password:
                username = url.username
                password = url.password
            else:
                raise ValueError("Username and password required for CouchDB.")

        cls._instance = super(SingletonCouchDBStorage, cls).__new__(cls)
        cls._instance.uuid = uuid.uuid4()
        cls._instance.username = username
        cls._instance.password = password
        cls._instance.dbname = dbname

        # Determine scheme and host        
        couchdb_url = f'http://{url.hostname}:{url.port}/'
        response = requests.get(couchdb_url)
        if response.status_code == 200:
            # print("CouchDB is accessible using HTTP.")                
            couchdb_url = f'http://{url.hostname}:{url.port}/'
        else:
            # print("CouchDB might be configured with HTTPS or inaccessible.")
            couchdb_url = f'https://{url.hostname}:{url.port}/'

        cls._instance.base_url = couchdb_url.rstrip('/')

        # Create DB if not exist
        resp = requests.get(f"{cls._instance.base_url}/{dbname}", auth=(username, password))
        if resp.status_code == 404:
            resp = requests.put(f"{cls._instance.base_url}/{dbname}", auth=(username, password))
            if not resp.ok:
                raise Exception(f"Could not create database: {resp.text}")
        cls._meta['couchdb_URL'] = couchdb_URL

        return cls._instance

    def __init__(self, couchdb_URL=None, username=None, password=None, dbname="singleton_db"):
        # All attributes are set in __new__
        self.uuid:str = self.uuid
        self.username:str = self.username
        self.password:str = self.password
        self.dbname:str = self.dbname
        self.base_url:str = self.base_url

    # CouchDB REST helpers
    def _request(self, method, path, **kwargs):
        url = f"{self.base_url}/{self.dbname}{path}"
        resp = requests.request(method, url, auth=(self.username, self.password), **kwargs)
        return resp

class SingletonCouchDBStorageController(AbstractStorageController):
    def __init__(self, model: SingletonCouchDBStorage):
        self.model: SingletonCouchDBStorage = model
        
    def set(self, key, value: dict):
        value = dict(value)
        value['_id'] = key
        existing = self.get_full(key)
        if existing and '_rev' in existing:
            value['_rev'] = existing['_rev']
        resp = self.model._request("PUT", f"/{key}", json=value)
        if not resp.ok:
            raise Exception(f"Set failed: {resp.text}")

    def get_full(self, key):
        resp = self.model._request("GET", f"/{key}")
        if resp.status_code == 200:
            return resp.json()
        return None

    def get(self, key):
        doc:dict = self.get_full(key)
        if doc:
            doc.pop('_id', None)
            doc.pop('_rev', None)
            return doc
        return None

    def delete(self, key):
        doc = self.get_full(key)
        if not doc or '_rev' not in doc:
            return False
        resp = self.model._request("DELETE", f"/{key}?rev={doc['_rev']}")
        return resp.ok

    def exists(self, key):
        resp = self.model._request("HEAD", f"/{key}")
        return resp.status_code == 200

    def keys(self, pattern='*'):
        resp = self.model._request("GET", "/_all_docs")
        rows = resp.json().get("rows", []) if resp.ok else []
        all_keys = [row["id"] for row in rows]
        regex = re.compile(fnmatch.translate(pattern))
        matched_keys = [key for key in all_keys if regex.match(key)]
        return matched_keys

SingletonKeyValueStorage.backs['couch'] = lambda *args, **kwargs: SingletonCouchDBStorageController(
    SingletonCouchDBStorage(*args, **kwargs)
)
