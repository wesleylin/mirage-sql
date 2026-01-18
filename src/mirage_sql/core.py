import sqlite3
import weakref
from collections import UserList, UserDict
from dataclasses import is_dataclass, fields
from typing import Any, Iterable, Union, List, Dict, overload

from .proxy import MirageProxy

def get_sqlite_type(value: Any) -> str:
    if isinstance(value, bool): return "INTEGER"
    if isinstance(value, int): return "INTEGER"
    if isinstance(value, float): return "REAL"
    return "TEXT"



class MirageManager:
    """Handles the SQLite connection and schema inference."""
    def __init__(self, sample_obj: Any=None):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._registry = weakref.WeakValueDictionary()
        self._in_transaction = False
        self.tables = {} # Format: {"classname": ["col1", "col2", ...]}

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
        self.conn.execute(query)
        return table_name
    

    def sync_object(self, obj: Any, key_val: Any = None, is_new: bool = False):
        # fetch table
        table_name = self.register_type(obj)
        cols = self.tables[table_name]

        # fetch real_object if proxy, real id and data
        real_obj = getattr(obj, '_target', obj)
        ptr = id(real_obj)
        self._registry[ptr] = real_obj

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



    def join_query(self, select_cols: str, tables: List[str], where: str) -> List[tuple]:
        """
        Executes a JOIN and returns the actual Python objects.
        Example: select_cols="player.obj_ptr, item.obj_ptr"
        """
        query = f"SELECT {select_cols} FROM {tables[0]} "
        # Simple join logic for MVP (Inner Join)
        for table in tables[1:]:
            query += f" JOIN {table}" # You can expand this for 'ON' clauses
        
        query += f" WHERE {where}"
        
        cursor = self.conn.execute(query)
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            # Convert the row of ptrs into a tuple of Proxies
            result_tuple = tuple(
                MirageProxy(self._registry[ptr], self) for ptr in row
            )
            results.append(result_tuple)
            
        return results
    

    def resolve(self, sql: str, params: tuple = ()) -> List[Any]:
        cursor = self.conn.execute(sql, params)
        rows:List = cursor.fetchall()
        results = []

        for row in rows:
            # 1. Build a list of 'processed' values for this specific row
            processed_row = []
            
            for col_name in row.keys():
                val = row[col_name]
                
                # 2. Check if this specific column is a pointer
                if 'ptr' in col_name.lower() and val is not None:
                    raw_obj = self._registry.get(val)
                    # Wrap in proxy if found, otherwise keep the ID (or None)
                    processed_row.append(MirageProxy(raw_obj, self) if raw_obj else val)
                else:
                    # 3. It's regular data (int, string, float), keep it as is
                    processed_row.append(val)

            # 4. Shape the output
            if len(processed_row) == 1:
                # If only 1 column was selected, return the item directly
                results.append(processed_row[0])
            else:
                # If multiple columns, return a tuple for unpacking
                results.append(tuple(processed_row))
                
        return results