from dataclasses import dataclass
from mirage_sql import mirror

@dataclass
class Player:
    id: int
    name: str

@dataclass
class Item:
    name: str
    owner_id: int

# 1. Setup our mirrored collections
players = mirror([
    Player(1, "Alice"),
    Player(2, "Bob")
])

items = mirror([
    Item("Excalibur", 1),
    Item("Wooden Shield", 2),
    Item("Phoenix Down", 1)
])

print("--- Initial Join ---")
# 2. Join players and items where player.id matches item.owner_id
# We expect to see (Alice, Excalibur) and (Alice, Phoenix Down)
alice_loot = players.join(
    items, 
    on="player.id = item.owner_id", 
    where="player.name = 'Alice'"
)


# Prints:
# Alice owns a Excalibur
# Alice owns a Phoenix Down
for p, i in alice_loot:
    print(f"{p.name} owns a {i.name}")



print("\n--- Live Mutation Sync ---")
# 3. Modify an item via the proxy. 
# Let's give Bob's shield to Alice by changing the owner_id to 1.
items[1].owner_id = 1 

# 4. Re-run the join. 
# The "Wooden Shield" will now automatically appear in Alice's results.
new_alice_loot = players.join(
    items, 
    on="player.id = item.owner_id", 
    where="player.name = 'Alice'"
)

print(f"Alice now has {len(new_alice_loot)} items:")
for p, i in new_alice_loot:
    print(f"- {i.name}")

# Verification: The identity remains the same
print(new_alice_loot)
print(f"\nIdentity check: {new_alice_loot[0][0] is players[0]}") # True