import sqlite3
from dataclasses import is_dataclass, fields
from typing import List, Any, Iterable

class MirageProxy:
    """Wraps an object to catch attribute changes and update the SQL index."""
    def __init__(self, target, parent):
        self.__dict__['_target'] = target
        self.__dict__['_parent'] = parent

    def __setattr__(self, name, value):
        # print(f"setattr {name}, {value}")
        setattr(self._target, name, value)
        self._parent._update_object_in_sql(self._target)

    def __getattr__(self, name):
        return getattr(self._target, name)

class Mirage:
    def __init__(self, data: Iterable[Any]):
        # We store proxies so the user's interactions are tracked
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        
        # Initial setup
        raw_items = list(data)
        self._proxies = [MirageProxy(obj, self) for obj in raw_items]
        self._registry = {id(obj): obj for obj in raw_items}
        
        self._create_table(raw_items[0])
        for obj in raw_items:
            self._update_object_in_sql(obj, insert=True)

    def _create_table(self, sample):
        # Infer column names from the object
        self.cols = [f.name for f in fields(sample)] if is_dataclass(sample) else list(vars(sample).keys())
        col_defs = ", ".join([f"{c} TEXT" for c in self.cols]) # Simplification: Treat all as TEXT for MVP
        self.conn.execute(f"CREATE TABLE data (obj_ptr INTEGER PRIMARY KEY, {col_defs})")

    def _update_object_in_sql(self, obj, insert=False):
        values = [id(obj)] + [getattr(obj, c) for c in self.cols]
        if insert:
            placeholders = ", ".join(["?"] * (len(self.cols) + 1))
            self.conn.execute(f"INSERT INTO data VALUES ({placeholders})", values)
        else:
            set_clause = ", ".join([f"{c} = ?" for c in self.cols])
            self.conn.execute(f"UPDATE data SET {set_clause} WHERE obj_ptr = ?", values[1:] + [values[0]])
        self.conn.commit()

    def query(self, sql_where: str) -> List[Any]:
        cursor = self.conn.execute(f"SELECT obj_ptr FROM data WHERE {sql_where}")
        return [MirageProxy(self._registry[row["obj_ptr"]], self) for row in cursor.fetchall()]

    @property
    def objects(self):
        return self._proxies