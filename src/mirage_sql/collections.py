
from collections import UserList, UserDict
from typing import Any, List, Dict

from .proxy import MirageProxy

class MirageList(UserList):
    def __init__(self, initlist:List, manager):
        if not initlist:
            raise ValueError("MirageList requires at least one item for type inference.")

        self.manager = manager 
        first_item = initlist[0]
        self.allowed_type:type = type(getattr(first_item, '_target', first_item))
        self.table_name:str = self.manager.register_type(first_item)


        # Keep a strong reference to the raw objects. otherwise it is cleaned up to early
        self._items = [getattr(obj, '_target', obj) for obj in initlist]

        super().__init__([MirageProxy(obj, manager) for obj in initlist])
        
        for obj in initlist:
            self.manager.sync_object(obj, is_new=True)



    def append(self, item):
        """
        append intercepts the native list.append() function
    
        Args:
            item to be added. potentially is a MirageProxy so need to check

        Returns:
            None
        """
        # 1. Type Enforcement (Optional but recommended)
        real_item = getattr(item, '_target', item)

        # Keep it alive
        self._items.append(real_item)

        if not isinstance(real_item, self.allowed_type):
            raise TypeError(f"Expected {self.allowed_type.__name__}, got {type(real_item).__name__}")

        # 2. Check if it's already a proxy
        if isinstance(item, MirageProxy):
            proxy = item
            # Optional: ensure it's using the current manager
            proxy.__dict__['_manager'] = self.manager 
        else:
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
    

    def join(self, other_list: 'MirageList', on: str, where: str = "1=1") -> List:
        """
        Join this list with another MirageList.
        Example: players.join(items, "players.id = items.owner_id")
        """
        # 1. Construct the SELECT to get the ptrs from both tables
        select_clause = f"{self.table_name}.obj_ptr as self_ptr, {other_list.table_name}.obj_ptr as right_ptr"
        
        # 2. Construct the FROM/JOIN clause
        # Note: We use double quotes for table names to be safe
        query = (f'SELECT {select_clause} FROM "{self.table_name}" '
                f'JOIN "{other_list.table_name}" ON {on} '
                f'WHERE {where}')
        
        cursor = self.manager.conn.execute(query)
        
        # 3. Re-materialize the Python objects
        final_results = []
        for row in cursor.fetchall():
            self_ptr, right_ptr = row['self_ptr'], row['right_ptr']
            right_obj = MirageProxy(self.manager._registry[right_ptr], self.manager)
            self_obj = MirageProxy(self.manager._registry[self_ptr], self.manager)
            final_results.append((self_obj, right_obj))
            
        return final_results
    

class MirageDict(UserDict):
    def __init__(self, initdict:Dict, manager):
        if not initdict:
            raise ValueError("MirageDict requires at least one item for type inference.")
        
        self.manager = manager

        # Keep a strong reference to the raw values
        self._items = {k: getattr(v, '_target', v) for k, v in initdict.items()}

        _, first_val = next(iter(initdict.items()))
        self.table_name = self.manager.register_type(first_val)
        self.allowed_type = type(getattr(first_val, '_target', first_val))

        super().__init__({k: MirageProxy(v, manager) for k, v in initdict.items()})
        for k, v in initdict.items():
            self.manager.sync_object(v, key_val=k, is_new=True)


    def __setitem__(self, key, value):
        proxy = MirageProxy(value, self.manager)
        super().__setitem__(key, proxy)
        self._items[key] = value
        self.manager.sync_object(value, key_val=key, is_new=True)

    def query(self, where: str) -> List[Any]:
        # 'key_val' is the special column for dict keys
        cursor = self.manager.conn.execute(f"SELECT obj_ptr FROM {self.table_name} WHERE {where}")
        return [MirageProxy(self.manager._registry[row['obj_ptr']], self.manager) for row in cursor.fetchall()]
