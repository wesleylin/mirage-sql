from dataclasses import dataclass
from mirage import Mirage

@dataclass
class User:
    name: str
    age: int
    city: str

# 1. Create some live Python objects
users = [
    User("Alice", 30, "New York"),
    User("Bob", 22, "London"),
    User("Charlie", 35, "New York"),
]

# 2. Mirror them into SQL
db = Mirage(users)

# 3. Query using standard SQL syntax
results = db.query("age > 25 AND city = 'New York'")

print(f"Found {len(results)} users:")
for user in results:
    print(f"- {user.name} (Object type: {type(user).__name__})")

# Verify they are the SAME objects, not copies
print(f"Is result Alice the same object? {results[0] is users[0]}")