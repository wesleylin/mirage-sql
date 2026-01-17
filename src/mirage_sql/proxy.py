import sqlite3
import weakref
from collections import UserList, UserDict
from dataclasses import is_dataclass, fields
from typing import Any, Iterable, Union, List, Dict, overload


class MirageProxy:
    """Interceptors attribute changes to sync with the SQL index."""
    # 'MirageManager'
    def __init__(self, target: Any, manager: Any):
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