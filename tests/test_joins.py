import pytest
from dataclasses import dataclass
from mirage_sql import mirror, get_global_manager

@dataclass
class Player:
    id: int
    name: str

@dataclass
class Item:
    name: str
    owner_id: int

@pytest.fixture(autouse=True)
def clear_database():
    """Wipe all data from the global manager between tests."""
    manager = get_global_manager()
    cursor = manager.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    for table in tables:
        # Delete all rows so the next test starts fresh
        manager.conn.execute(f'DELETE FROM "{table}"')
    
    # Also clear the registry so old objects don't haunt us
    manager._registry.clear()
    manager.tables.clear() # Reset schema inference too
    manager.conn.commit()

def test_cross_table_join():
    """Verify that joining two different mirrored collections returns paired proxies."""
    # 1. Setup data
    raw_players = [Player(1, "Alice"), Player(2, "Bob")]
    raw_items = [
        Item("Golden Sword", 1), 
        Item("Shield", 2), 
        Item("Rusty Dagger", 1)
    ]
    
    # 2. Mirror into the Global Manager
    players = mirror(raw_players)
    items = mirror(raw_items)
    
    # 3. Perform the Magic Join
    # We want Alice's items only
    matches = players.join(
        items, 
        on="player.id = item.owner_id", 
        where="player.name = 'Alice'"
    )
    
    # 4. Assertions
    assert len(matches) == 2  # Alice has Golden Sword and Rusty Dagger
    
    # Check first match (Alice + Golden Sword)
    # matches[0] is a tuple: (MirageProxy(Player), MirageProxy(Item))

    # Sort the results by the item name to ensure deterministic order
    matches.sort(key=lambda x: x[1].name) # x[1] is the Item proxy
    
    # Now matches[0] is guaranteed to be "Golden Sword" (G comes before R)
    p_match, i_match = matches[0]
    assert p_match.name == "Alice"
    assert i_match.name == "Golden Sword"
    
    p_match2, i_match2 = matches[1]
    assert p_match2.name == "Alice"
    assert i_match2.name == "Rusty Dagger"
    
    # Identity Check: The proxy's target must be the exact object from raw_players
    assert p_match._target is raw_players[0]
    assert i_match._target is raw_items[0]

def test_join_mutation_sync():
    """Verify that changing an attribute affects join results immediately."""
    players = mirror([Player(1, "Alice")])
    items = mirror([Item("Stick", 1)])
    
    # Initially 1 match
    result = players.join(items, "player.id = item.owner_id")
    assert len(result) == 1
    
    # Change the owner_id in Python via the proxy
    items[0].owner_id = 99  # No longer matches Alice (id=1)
    
    # Join should now be empty
    assert len(players.join(items, "player.id = item.owner_id")) == 0
