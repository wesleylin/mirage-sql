import sqlite3
import weakref
from collections import UserList, UserDict
from dataclasses import is_dataclass, fields
from typing import Any, Iterable, Union, List, Dict

def get_sqlite_type(value: Any) -> str:
    if isinstance(value, bool): return "INTEGER"
    if isinstance(value, int): return "INTEGER"
    if isinstance(value, float): return "REAL"
    return "TEXT"

class MirageProxy:
    """Interceptors attribute changes to sync with the SQL index."""
    def __init__(self, target: Any, manager: 'MirageManager'):
        self.__dict__['_target'] = target
        self.__dict__['_manager'] = manager

    def __setattr__(self, name: str, value: Any):
        setattr(self._target, name, value)
        if not self._manager._in_transaction:
            self._manager.sync_object(self._target)

    def __getattr__(self, name: str):
        return getattr(self._target, name)
    
    def __repr__(self):
        return f"MirageProxy({repr(self._target)})"

class MirageManager:
    """Handles the SQLite connection and schema inference."""
    def __init__(self, sample_obj: Any):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._registry = weakref.WeakValueDictionary()
        self._in_transaction = False
        self.cols = self._infer_columns(sample_obj)
        self._create_table()

    def _infer_columns(self, obj: Any) -> List[str]:
        if is_dataclass(obj):
            return [f.name for f in fields(obj)]
        return [k for k in vars(obj).keys() if not k.startswith('_')]

    def _create_table(self):
        col_defs = [f"attr_{c} {get_sqlite_type(None)}" for c in self.cols] # Type mapping can be refined
        query = f"CREATE TABLE data (obj_ptr INTEGER PRIMARY KEY, key_val TEXT, {', '.join(col_defs)})"
        self.conn.execute(query)

    def sync_object(self, obj: Any, key_val: Any = None, is_new: bool = False):
        ptr = id(obj)
        self._registry[ptr] = obj
        vals = [ptr, str(key_val) if key_val else None] + [getattr(obj, c, None) for c in self.cols]
        
        if is_new:
            placeholders = ", ".join(["?"] * len(vals))
            self.conn.execute(f"INSERT OR REPLACE INTO data VALUES ({placeholders})", vals)
        else:
            set_clause = ", ".join([f"attr_{c} = ?" for c in self.cols])
            self.conn.execute(f"UPDATE data SET {set_clause} WHERE obj_ptr = ?", vals[2:] + [ptr])
        self.conn.commit()

    def remove_object(self, obj: Any):
        self.conn.execute("DELETE FROM data WHERE obj_ptr = ?", (id(obj),))
        self.conn.commit()

class MirageList(UserList):
    def __init__(self, initlist, manager):
        super().__init__([MirageProxy(obj, manager) for obj in initlist])
        self.manager = manager
        for obj in initlist:
            self.manager.sync_object(obj, is_new=True)

    def append(self, item):
        proxy = MirageProxy(item, self.manager)
        super().append(proxy)
        self.manager.sync_object(item, is_new=True)

    def query(self, where: str) -> List[Any]:
        cursor = self.manager.conn.execute(f"SELECT obj_ptr FROM data WHERE {where}")
        return [MirageProxy(self.manager._registry[row['obj_ptr']], self.manager) for row in cursor.fetchall()]
    
    def pop(self, index=-1):
        # 1. Get the proxy object at that index
        item_proxy = self.data[index]
        
        # 2. Tell the manager to delete it from SQL 
        # (We use ._target because the manager needs the real object ID)
        self.manager.remove_object(item_proxy._target)
        return super().pop(index)
    

class MirageDict(UserDict):
    def __init__(self, initdict, manager):
        self.manager = manager
        super().__init__({k: MirageProxy(v, manager) for k, v in initdict.items()})
        for k, v in initdict.items():
            self.manager.sync_object(v, key_val=k, is_new=True)

    def __setitem__(self, key, value):
        proxy = MirageProxy(value, self.manager)
        super().__setitem__(key, proxy)
        self.manager.sync_object(value, key_val=key, is_new=True)

    def query(self, where: str) -> List[Any]:
        # 'key_val' is the special column for dict keys
        cursor = self.manager.conn.execute(f"SELECT obj_ptr FROM data WHERE {where}")
        return [MirageProxy(self.manager._registry[row['obj_ptr']], self.manager) for row in cursor.fetchall()]

def mirror(collection: Union[List, Dict]):
    if not collection:
        raise ValueError("Collection cannot be empty for inference.")
    
    sample = list(collection.values())[0] if isinstance(collection, dict) else collection[0]
    manager = MirageManager(sample)
    
    if isinstance(collection, dict):
        return MirageDict(collection, manager)
    return MirageList(collection, manager)