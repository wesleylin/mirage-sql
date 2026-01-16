import inspect
from dataclasses import is_dataclass, fields

def get_sql_schema(obj):
    cols = []
    # If it's a Dataclass, it's easy
    if is_dataclass(obj):
        for f in fields(obj):
            sql_type = "TEXT" if f.type == str else "INTEGER"
            cols.append(f"{f.name} {sql_type}")
    else:
        # Fallback to inspecting instance variables
        for name, value in vars(obj).items():
            sql_type = "TEXT" if isinstance(value, str) else "INTEGER"
            cols.append(f"{name} {sql_type}")
    
    return f"CREATE TABLE IF NOT EXISTS data (obj_id INTEGER, {', '.join(cols)})"