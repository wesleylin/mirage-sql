import sqlite3
import weakref
from collections import UserList, UserDict
from dataclasses import is_dataclass, fields
from typing import Any, Iterable, Union, List, Dict, overload

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
    def __init__(self, sample_obj: Any=None):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._registry = weakref.WeakValueDictionary()
        self._in_transaction = False
        # self.cols = self._infer_columns(sample_obj)
        # self._create_table()
        self.tables = {} # Format: {"classname": ["col1", "col2", ...]}

    def _infer_columns(self, obj: Any) -> List[str]:
        if is_dataclass(obj):
            return [f.name for f in fields(obj)]
        return [k for k in vars(obj).keys() if not k.startswith('_')]

    def _create_table(self):
        col_defs = [f"\"{c}\" {get_sqlite_type(None)}" for c in self.cols] # Type mapping can be refined
        query = f"CREATE TABLE data (obj_ptr INTEGER PRIMARY KEY, key_val TEXT, {', '.join(col_defs)})"
        self.conn.execute(query)

    def _get_table_name(self, obj: Any) -> str:
        """Determines the table name (lowercase class name)."""
        # Safety: Reach through proxy if it exists
        real_obj = getattr(obj, '_target', obj)
        return real_obj.__class__.__name__.lower()

    def register_type(self, obj: Any):
        """Creates a table for the object's class if it doesn't exist."""

        real_obj = obj
        while hasattr(real_obj, '_target'):
            real_obj = real_obj._target
        table_name = self._get_table_name(real_obj)
        
        if table_name in self.tables:
            return table_name # Already exists
        
        # Infer columns (dataclass or standard object)
        print(real_obj)
        if is_dataclass(real_obj):
            cols = [f.name for f in fields(real_obj)]
        else:
            cols = [k for k in vars(real_obj).keys() if not k.startswith('_')]
        
        self.tables[table_name] = cols
        
        # Build the CREATE TABLE query
        col_defs = [f'"{c}" TEXT' for c in cols]
        if len(col_defs) == 0:
            raise Exception("incorrect col_defs")
        query = f'CREATE TABLE IF NOT EXISTS "{table_name}" (obj_ptr INTEGER PRIMARY KEY, key_val TEXT, {", ".join(col_defs)} )'
        print(f"query {query}")
        self.conn.execute(query)
        return table_name
    

    def sync_object(self, obj: Any, key_val: Any = None, is_new: bool = False):
        # fetch table
        table_name = self.register_type(obj)
        cols = self.tables[table_name]

        # fetch real_object if proxy, real id and data
        real_obj = getattr(obj, '_target', obj)
        ptr = id(obj)
        self._registry[ptr] = obj

        attr_values = [getattr(real_obj, c, None) for c in cols]
        all_values = [ptr, str(key_val) if key_val else None] + attr_values
        # vals = [ptr, str(key_val) if key_val else None] + [getattr(obj, c, None) for c in self.cols]
        
        placeholders = ", ".join(["?"] * len(all_values))
        col_names = ", ".join([f'"{c}"' for c in cols])
        query = f'INSERT OR REPLACE INTO "{table_name}" (obj_ptr, key_val, {col_names}) VALUES ({placeholders})'
        self.conn.execute(query, all_values)
        self.conn.commit()

    def remove_object(self, table_name:str, obj: Any):
        self.conn.execute(f"DELETE FROM {table_name} WHERE obj_ptr = ?", (id(obj),))
        self.conn.commit()

class MirageList(UserList):
    def __init__(self, initlist, manager):
        if not initlist:
            raise ValueError("MirageList requires at least one item for type inference.")

        super().__init__([MirageProxy(obj, manager) for obj in initlist])
        self.manager = manager
        for obj in initlist:
            self.manager.sync_object(obj, is_new=True)

        first_item = initlist[0]
        self.table_name = self.manager.register_type(first_item)

    def append(self, item):
        proxy = MirageProxy(item, self.manager)
        super().append(proxy)
        self.manager.sync_object(item, is_new=True)

    def query(self, where: str) -> List[Any]:
        cursor = self.manager.conn.execute(f"SELECT obj_ptr FROM {self.table_name} WHERE {where}")
        return [MirageProxy(self.manager._registry[row['obj_ptr']], self.manager) for row in cursor.fetchall()]
    
    def pop(self, index=-1):
        # 1. Get the proxy object at that index
        item_proxy = self.data[index]
        
        # 2. Tell the manager to delete it from SQL 
        # (We use ._target because the manager needs the real object ID)
        self.manager.remove_object(self.table_name, item_proxy._target)
        return super().pop(index)
    

class MirageDict(UserDict):
    def __init__(self, initdict:Dict, manager):
        if not initdict:
            raise ValueError("MirageDict requires at least one item for type inference.")
        self.manager = manager
        super().__init__({k: MirageProxy(v, manager) for k, v in initdict.items()})
        for k, v in initdict.items():
            self.manager.sync_object(v, key_val=k, is_new=True)

        _, first_val = next(iter(initdict.items()))
        self.table_name = self.manager.register_type(first_val)


    def __setitem__(self, key, value):
        proxy = MirageProxy(value, self.manager)
        super().__setitem__(key, proxy)
        self.manager.sync_object(value, key_val=key, is_new=True)

    def query(self, where: str) -> List[Any]:
        # 'key_val' is the special column for dict keys
        cursor = self.manager.conn.execute(f"SELECT obj_ptr FROM {self.table_name} WHERE {where}")
        return [MirageProxy(self.manager._registry[row['obj_ptr']], self.manager) for row in cursor.fetchall()]

@overload
def mirror(collection: List) -> MirageList: ...


@overload
def mirror(collection: Dict) -> MirageDict: ...

def mirror(collection: Union[List, Dict]):
    if not collection:
        raise ValueError("Collection cannot be empty for inference.")
    
    sample = list(collection.values())[0] if isinstance(collection, dict) else collection[0]
    manager = MirageManager(sample)
    
    if isinstance(collection, dict):
        return MirageDict(collection, manager)
    return MirageList(collection, manager)