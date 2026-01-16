from dataclasses import dataclass
from mirage_sql import mirror

@dataclass
class Hero:
    name: str
    hp: int
    level: int

# --- List Demo ---
heroes = mirror([
    Hero("Arthur", 100, 5),
    Hero("Gwen", 80, 10),
    Hero("Merlin", 75, 7)
])

print("Querying List...")
strong_heroes = heroes.query("attr_hp > 50 AND attr_level >= 5")
print(f"Found: {strong_heroes}")

# Live Sync Test
heroes[0].hp = 10  # Arthur takes damage
still_strong = heroes.query("attr_hp > 50")
print(f"Still strong: {still_strong}") # Only Gwen remains

# --- Dict Demo ---
party = mirror({
    "tank": Hero("Arthur", 100, 5),
    "healer": Hero("Gwen", 80, 10)
})

print("\nQuerying Dict by Key...")
healer = party.query("key_val = 'healer'")
print(f"Healer Found: {healer[0].name}")