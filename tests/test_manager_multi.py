import pytest
from dataclasses import dataclass
from mirage_sql.core import MirageManager

@dataclass
class User:
    name: str
    age: int

@dataclass
class Product:
    title: str
    price: float

def test_manager_multiple_tables():
    # 1. Setup Manager
    mgr = MirageManager()
    
    # 2. Sync different types
    alice = User("Alice", 30)
    sword = Product("Iron Sword", 15.5)
    
    mgr.sync_object(alice)
    mgr.sync_object(sword)
    
    # 3. Verify tables exist in SQLite
    cursor = mgr.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    table_names = [row['name'] for row in cursor.fetchall()]
    
    assert "user" in table_names
    assert "product" in table_names
    
    # 4. Verify data is in the correct "bucket"
    user_data = mgr.conn.execute("SELECT name FROM user").fetchone()
    product_data = mgr.conn.execute("SELECT title FROM product").fetchone()
    
    assert user_data['name'] == "Alice"
    assert product_data['title'] == "Iron Sword"

def test_manager_updates_correct_table():
    mgr = MirageManager()
    bob = User("Bob", 25)
    
    # First sync (Insert)
    mgr.sync_object(bob)
    
    # Modify and sync again (Update)
    bob.age = 26
    mgr.sync_object(bob)
    
    res = mgr.conn.execute("SELECT age FROM user WHERE name='Bob'").fetchone()
    # TODO Fix this
    assert res['age'] == "26"