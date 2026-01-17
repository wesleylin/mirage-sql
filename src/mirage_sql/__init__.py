from typing import Union, List, Dict, Optional, overload
from .core import MirageManager
from .collections import MirageList, MirageDict

_GLOBAL_MANAGER = None

def get_global_manager() -> MirageManager:
    """
        get_global_manager fetch existing global manager 
            or create one if doesn't exist yet
        Params: None
        Returns: MirageManager
    """

    global _GLOBAL_MANAGER
    if _GLOBAL_MANAGER is None:
        _GLOBAL_MANAGER = MirageManager()
    return _GLOBAL_MANAGER

@overload
def mirror(collection: List) -> MirageList: ...

@overload
def mirror(collection: Dict) -> MirageDict: ...

def mirror(collection: Union[List, Dict], manager:Optional[MirageManager]=None):
    if not collection:
        raise ValueError("Collection cannot be empty for inference.")
    
    actual_manager = manager or get_global_manager()
    
    if isinstance(collection, dict):
        return MirageDict(collection, actual_manager)
    return MirageList(collection, actual_manager)

__all__ = ["mirror"]
