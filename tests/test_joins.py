import pytest
from dataclasses import dataclass
from mirage_sql import mirror, get_global_manager
from mirage_sql.proxy import MirageProxy

@dataclass
class Player:
    id: int
    name: str

@dataclass
class Item:
    name: str
    owner_id: int
    value: int = 10

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

def test_manager_direct_join_query():
    """Verify executing a raw JOIN via the manager returns correct objects."""
    manager = get_global_manager()
    
    # 1. Setup data
    p = Player(10, "Zelda")
    i = Item("Master Sword", 10)
    
    mirror([p])
    mirror([i])
    
    # 2. Execute a raw SQL join through the manager's connection
    # We are looking for the obj_ptr from both tables
    query = """
        SELECT player.obj_ptr AS p_ptr, item.obj_ptr AS i_ptr 
        FROM player 
        JOIN item ON player.id = item.owner_id 
        WHERE player.name = 'Zelda'
    """
    cursor = manager.conn.execute(query)
    row = cursor.fetchone()
    
    assert row is not None
    
    # 3. Manually resolve the pointers from the registry
    # This is exactly what MirageList.join does under the hood
    retrieved_player = manager._registry[row['p_ptr']]
    retrieved_item = manager._registry[row['i_ptr']]
    
    # 4. Assertions
    assert retrieved_player.name == "Zelda"
    assert retrieved_item.name == "Master Sword"
    assert retrieved_player is p  # Identity check
    assert retrieved_item is i    # Identity check


def test_manager_resolve_capabilities():
    """Verify the manager.resolve() method handles single, multi, and mixed results."""
    manager = get_global_manager()
    
    # 1. Setup Data
    alice = Player( 1,  "Alice")
    stick = Item("Stick", 1)
    mirror([alice])
    mirror([stick])

    # --- Scenario A: Single Object Resolution ---
    # Querying just the pointer should return the proxy directly
    res_a = manager.resolve("SELECT obj_ptr FROM player WHERE name = 'Alice'")
    assert len(res_a) == 1
    assert res_a[0].name == "Alice"
    assert isinstance(res_a[0], MirageProxy)

    # --- Scenario B: Multi-Object (Join) Resolution ---
    # Querying multiple pointers should return a list of tuples
    query_b = """
        SELECT p.obj_ptr AS p_ptr, i.obj_ptr AS i_ptr 
        FROM player p 
        JOIN item i ON p.id = i.owner_id
    """
    res_b = manager.resolve(query_b)
    assert len(res_b) == 1
    p_proxy, i_proxy = res_b[0] # Unpacking the tuple
    assert p_proxy.name == "Alice"
    assert i_proxy.name == "Stick"

    # --- Scenario C: Mixed Results (Data + Proxy) ---
    # Querying a raw value alongside a pointer
    query_c = "SELECT name, obj_ptr FROM player WHERE id = 1"
    res_c = manager.resolve(query_c)
    print(res_c)
    
    # res_c[0] should be a tuple: ("Alice", <MirageProxy>)
    name_val, proxy_obj = res_c[0]
    assert name_val == "Alice"
    assert proxy_obj.name == "Alice"
    
    # --- Scenario D: No Pointers (Raw SQL) ---
    # If no 'ptr' column is found, it should behave like a normal SQL fetch
    res_d = manager.resolve("SELECT count(*) FROM player")
    assert res_d[0] == 1


def test_manager_aggregation_resolution():
    """Verify that resolve() correctly handles SQL aggregations like SUM and mixed GROUP BY results."""
    manager = get_global_manager()
    
    # 1. Setup Data: Give Alice multiple items with different values
    alice = Player(1, "Alice")
    items = [
        Item("Gold", 1, 100),
        Item("Silver", 1, 50),
        Item("Bronze", 1, 10)
    ]
    mirror([alice])
    mirror(items)

    # --- Scenario E: Simple Scalar Aggregation ---
    # Should return a single number, not a list of proxies
    res_e = manager.resolve("SELECT SUM(value) FROM item")
    # res_e is a list of results, so res_e[0] is the sum
    assert res_e[0] == 160

    # --- Scenario F: Aggregate with Group By (Mixed Result) ---
    # Query: Total value per player, returning the Player proxy and the sum
    # This tests the 'tuple' return logic for mixed Data + Proxy
    query_f = """
        SELECT p.obj_ptr AS p_ptr, SUM(i.value) as total_wealth
        FROM player p
        JOIN item i ON p.id = i.owner_id
        GROUP BY p.obj_ptr
    """
    res_f = manager.resolve(query_f)
    
    assert len(res_f) == 1
    player_proxy, total_wealth = res_f[0]
    
    assert player_proxy.name == "Alice"
    assert total_wealth == 160
    assert isinstance(player_proxy, MirageProxy)

    # --- Scenario G: Multiple Aggregates ---
    # Should return a tuple of raw values
    res_g = manager.resolve("SELECT COUNT(*), AVG(value) FROM item")
    count, average = res_g[0]
    assert count == 3
    assert average == 160 / 3