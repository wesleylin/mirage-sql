import pytest
from dataclasses import dataclass
from mirage_sql import mirror, get_global_manager


@dataclass
class Player:
    name: str
    score: int

@pytest.fixture
def players():
    return [
        Player("Alice", 100),
        Player("Bob", 200),
        Player("Charlie", 300)
    ]

@pytest.fixture(autouse=True)
def clean_db():
    """Reset the global manager before every single test."""

    manager = get_global_manager()
    # Drop all tables and clear the registry
    cursor = manager.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    for table in tables:
        manager.conn.execute(f'DROP TABLE "{table}"')
    manager._registry.clear()
    manager.tables.clear()
    yield

def test_list_identity(players):
    """Verify that results from query ARE the original objects."""
    db = mirror(players)
    results = db.query("score > 150")
    
    assert len(results) == 2

    results.sort(key=lambda x: x.name)

    # Identity check (is) vs Equality check (==)
    assert results[0]._target is players[1] 

def test_live_sync(players):
    """Verify that changing a Python attribute updates SQL results."""
    db = mirror(players)
    
    # Change Alice's score in Python
    db[0].score = 500 
    
    # Query SQL for the new score
    results = db.query("score > 400")
    assert len(results) == 1
    assert results[0].name == "Alice"

def test_list_mutation(players):
    """Verify that append/pop updates the SQL index."""
    db = mirror(players)
    
    # Append new
    db.append(Player("Dave", 400))
    assert len(db.query("name = 'Dave'")) == 1
    
    # Pop Bob
    result =  db.pop(1)
    assert result.name == 'Bob'
    assert len(db.query("name = 'Bob'")) == 0

def test_weakref_cleanup(players):
    """Ensure we aren't leaking memory (registry uses WeakValueDictionary)."""
    import gc
    db = mirror(players)
    ptr = id(players[0])
    
    # Delete local references
    del players
    gc.collect()
    
    # The registry should still have them because 'db' holds the Proxy list
    assert ptr in db.manager._registry


