from typing import Union, List, Dict, overload
from .core import MirageManager
from .collections import MirageList, MirageDict

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

__all__ = ["mirror"]
