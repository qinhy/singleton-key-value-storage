# from https://github.com/qinhy/singleton-key-value-storage.git
from pathlib import Path
import sqlite3
import threading
import queue
import time
from typing import Any, Dict, List, Optional
import uuid
import json

try:
    from .Storage import SingletonKeyValueStorage,MemoryLimitedDictStorageController,AbstractStorageController,DictStorage
except Exception as e:
    from Storage import SingletonKeyValueStorage,MemoryLimitedDictStorageController,AbstractStorageController,DictStorage


def try_if_error(func):
    try:
        func()
    except Exception as e:
        print(e)
        return e

# self checking
sqlite_back    = True

if sqlite_back:

    class SingletonSqliteStorage:
        _instance: "SingletonSqliteStorage" = None
        _meta: Dict[str, Any] = {}

        DUMP_FILE = "dump_db_file"
        LOAD_FILE = "load_db_file"

        def __new__(cls, sqlite_URL: str = "sqlite.db"):
            if cls._instance is None:
                inst = super(SingletonSqliteStorage, cls).__new__(cls)
                cls._instance = inst

                # Core attributes
                inst.uuid = uuid.uuid4()
                inst.sqlite_URL = sqlite_URL        # store DB path
                inst.client = None

                # Async infra
                inst.query_queue = queue.Queue()
                inst.result_dict = {} #: Dict[str, Dict[str, Any]]
                inst.lock = threading.Lock()
                inst.should_stop = threading.Event()

                # Worker thread
                inst.worker_thread = threading.Thread(
                    target=inst._process_queries,
                    args=(sqlite_URL,),
                )
                inst.worker_thread.daemon = True
                inst.worker_thread.start()

                # Ensure table exists (synchronously)
                qid = inst._execute_query(
                    "CREATE TABLE IF NOT EXISTS KeyValueStore (key TEXT PRIMARY KEY, value JSON)"
                )
                inst._pop_result(qid, timeout=5.0)

            return cls._instance

        def __init__(self, sqlite_URL: str = "sqlite.db"):
            self.sqlite_URL:str = self.sqlite_URL
            self.uuid:str = self.uuid
            self.client:sqlite3.Connection = self.client
            self.query_queue:queue.Queue = self.query_queue 
            self.result_dict:dict = self.result_dict 
            self.lock:threading.Lock = self.lock 
            self.worker_thread:threading.Thread = self.worker_thread 
            self.should_stop:threading.Event = self.should_stop            
        # --------------------------------------------------------------------- #
        # Worker thread
        # --------------------------------------------------------------------- #

        def _process_queries(self, sqlite_URL: str, timeout: float = 0.1) -> None:
            # Each worker has its own connection
            self.client = sqlite3.connect(sqlite_URL)

            while not self.should_stop.is_set():
                query_infos: List[Dict[str, Any]] = []

                # Batch writes; end batch when we hit the first SELECT
                while True:
                    try:
                        query_info = self.query_queue.get(timeout=timeout)
                        sql, params = query_info["query"]
                        sql:str = sql
                        query_infos.append(query_info)

                        # Normalize & detect SELECT
                        if sql.lstrip().upper().startswith("SELECT"):
                            break
                    except queue.Empty:
                        break

                if not query_infos:
                    continue

                for query_info in query_infos:
                    (sql, params) = query_info["query"]
                    sql:str = sql
                    query_id = query_info["id"]

                    try:
                        # --- Special commands: DUMP_FILE / LOAD_FILE ----------------
                        if sql.startswith(self.DUMP_FILE):
                            # Format: "dump_db_file /path/to/file.db"
                            parts = sql.split(maxsplit=1)
                            if len(parts) != 2:
                                self._store_result(query_id, sql, "Invalid dump command")
                            else:
                                dump_path = parts[1]
                                disk_conn = None
                                try:
                                    disk_conn = sqlite3.connect(dump_path)
                                    self._clone(self.client, disk_conn)
                                    self._store_result(query_id, sql, True)
                                except sqlite3.Error as e:
                                    self._store_result(query_id, sql, f"SQLite error: {e}")
                                finally:
                                    if disk_conn is not None:
                                        disk_conn.close()

                        elif sql.startswith(self.LOAD_FILE):
                            # Format: "load_db_file /path/to/file.db"
                            parts = sql.split(maxsplit=1)
                            if len(parts) != 2:
                                self._store_result(query_id, sql, "Invalid load command")
                            else:
                                load_path = parts[1]
                                disk_conn = None
                                try:
                                    disk_conn = sqlite3.connect(load_path)
                                    # Replace underlying connection
                                    self.client.close()
                                    self.client = sqlite3.connect(sqlite_URL)
                                    self._clone(disk_conn, self.client)
                                    self._store_result(query_id, sql, True)
                                except sqlite3.Error as e:
                                    self._store_result(query_id, sql, f"SQLite error: {e}")
                                finally:
                                    if disk_conn is not None:
                                        disk_conn.close()

                        # --- Normal SQL --------------------------------------------
                        else:
                            cursor = self.client.cursor()
                            if params is None:
                                cursor.execute(sql)
                            else:
                                cursor.execute(sql, params)

                            if cursor.description is None:
                                # Non-SELECT (INSERT/UPDATE/DELETE/etc.)
                                self._store_result(query_id, sql, True)
                            else:
                                columns = [d[0] for d in cursor.description]
                                rows = cursor.fetchall()

                                if len(columns) > 1:
                                    rows = [dict(zip(columns, row)) for row in rows]
                                else:
                                    # For single-column results, preserve previous behavior: list[str]
                                    rows = [str(row[0]) for row in rows]

                                self._store_result(query_id, sql, rows)

                    except sqlite3.Error as e:
                        self._store_result(query_id, sql, f"SQLite error: {e}")
                    finally:
                        # IMPORTANT: one task_done per get()
                        self.query_queue.task_done()

                # Commit after each batch
                self.client.commit()

        # --------------------------------------------------------------------- #
        # Internal helpers
        # --------------------------------------------------------------------- #
        def _store_result(self, query_id: str, query: str, result: Any) -> None:
            with self.lock:
                self.result_dict[query_id] = {
                    "result": result,
                    "query": query,
                    "time": time.time(),
                }

        def _clone(self, a: sqlite3.Connection, b: sqlite3.Connection) -> None:
            query = "".join(line for line in a.iterdump())
            b.executescript(query)
            b.commit()

        def _execute_query(self, query: str, val: Optional[tuple] = None) -> str:
            if self.should_stop.is_set():
                raise ValueError("The DB thread is stopped!")
            query_id = str(uuid.uuid4())
            self.query_queue.put(
                {"query": (query, val), "id": query_id, "time": time.time()}
            )
            return query_id

        def _pop_result(
            self, query_id: str, timeout: float = 2.0, wait: float = 0.01
        ) -> Optional[Dict[str, Any]]:
            start_time = time.time()
            while True:
                with self.lock:
                    if query_id in self.result_dict:
                        return self.result_dict.pop(query_id)
                if time.time() - start_time > timeout:
                    return None
                time.sleep(wait)

        def _clean_result(self) -> bool:
            with self.lock:
                self.result_dict = {}
            return True

        def _stop_thread(self, wait: float = 0.01) -> None:
            # Wait until queue is drained
            while not self.query_queue.empty():
                time.sleep(wait)
            self.should_stop.set()
            self.worker_thread.join()
            if self.client is not None:
                self.client.close()

        # --------------------------------------------------------------------- #
        # Builders (as in your original code)
        # --------------------------------------------------------------------- #
        @staticmethod
        def build(sqlite_URL: str = "sqlite.db"):
            return SingletonSqlitePythonMixStorageController(
                SingletonSqliteStorage(sqlite_URL))

        @staticmethod
        def build_pure(sqlite_URL: str = "sqlite.db"):
            return SingletonSqliteStorageController(SingletonSqliteStorage(sqlite_URL))

    class SingletonSqliteStorageController(AbstractStorageController):
        def __init__(self, model: SingletonSqliteStorage):
            self.model: SingletonSqliteStorage = model

        # Low-level helpers -------------------------------------------------- #
        def _execute_query(self, query: str, params: Optional[tuple] = None) -> str:
            return self.model._execute_query(query, params)

        def _execute_query_with_res(
            self,
            query: str,
            params: Optional[tuple] = None,
            timeout: float = 2.0,
        ):
            query_id = self.model._execute_query(query, params)
            result = self.model._pop_result(query_id, timeout=timeout)
            if result is None:
                raise TimeoutError("Timed out waiting for SQLite result")
            return result["result"]

        # Public API --------------------------------------------------------- #
        def exists(self, key: str) -> bool:
            sql = "SELECT EXISTS(SELECT 1 FROM KeyValueStore WHERE key = ?);"
            rows = self._execute_query_with_res(sql, (key,))
            # rows is like ['0'] or ['1']
            return bool(int(rows[0]))

        def set(self, key: str, value: dict) -> str:
            sql = "INSERT OR REPLACE INTO KeyValueStore (key, value) VALUES (?, json(?))"
            params = (key, json.dumps(value))
            # Fire-and-forget; if you want sync behavior, call _execute_query_with_res
            return self._execute_query(sql, params)

        def get(self, key: str) -> Optional[dict]:
            sql = "SELECT value FROM KeyValueStore WHERE key = ?"
            rows = self._execute_query_with_res(sql, (key,))
            if not rows:
                return None
            return json.loads(rows[0])

        def delete(self, key: str) -> str:
            sql = "DELETE FROM KeyValueStore WHERE key = ?"
            return self._execute_query(sql, (key,))

        def keys(self, pattern: str = "*") -> List[str]:
            # Translate shell-like wildcards to SQL LIKE
            sql_pattern = pattern.replace("*", "%").replace("?", "_")
            sql = "SELECT key FROM KeyValueStore WHERE key LIKE ?"
            rows = self._execute_query_with_res(sql, (sql_pattern,))
            # rows is already list[str] (single-column SELECT behavior)
            return rows

        def load(self, path: str):
            path_obj = Path(path)

            # Non-DB file: delegate to .loads(text) if your base class supports it
            if path_obj.suffix != ".db":
                return self.loads(path_obj.read_text())
            else:
                new_path = path_obj.absolute()
                current_path = Path(self.model.sqlite_URL).absolute()

                # No-op if same DB file
                if new_path == current_path:
                    return

                # Use LOAD_FILE meta command and wait for completion
                sql = f"{self.model.LOAD_FILE} {new_path}"
                self._execute_query_with_res(sql, None, timeout=30.0)

        def is_query_empty(self) -> bool:
            # True if queue is empty (fixed semantics)
            return self.model.query_queue.empty()
        
    class SingletonSqlitePythonMixStorageController(AbstractStorageController):
        def __init__(self, model: SingletonSqliteStorage, max_memory_mb=128.0):
            self.sqlite = SingletonSqliteStorageController(model)
            self.memory = MemoryLimitedDictStorageController(
                DictStorage(),max_memory_mb=max_memory_mb)

        def exists(self, key: str) -> bool:
            if self.memory.exists(key):
                return True
            return self.sqlite.exists(key)

        def set(self, key: str, value: dict):
            self.sqlite.set(key,value)
            self.memory.set(key,value)

        def get(self, key: str) -> dict:
            value = self.memory.get(key)
            if value is None:
                value = self.sqlite.get(key)
                if value is not None:
                    self.memory.set(key, value)
            return value

        def delete(self, key: str):
            self.sqlite.delete(key)
            self.memory.delete(key)

        def keys(self, pattern: str = "*") -> list[str]:
            return (set(self.memory.keys(pattern)) | set(self.sqlite.keys(pattern)))
            
        def is_query_empty(self): return self.sqlite.is_query_empty()
