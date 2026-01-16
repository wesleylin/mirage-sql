Mirage

mirage examples

```python
import mirage

# Wrap any collection (List, Dict, or Set)

users = mirage.mirror([User(name="Alice", age=30), User(name="Bob", age=25)])


# Standard mutations (SQL is updated automatically)
users.append(User(name="Charlie", age=35))
users[0].age = 31  # Proxy catches this, SQL UPDATES the row
users.pop(1)       # SQL DELETES Bob

# The Superpower
results = users.query("age > 30 AND name LIKE 'A%'")
# Returns: [User(name="Alice", age=31)]
```

Set example

```python
# Wrap a dictionary
registry = mirage.mirror({
    "id_101": User(name="Alice"),
    "id_102": User(name="Bob")
})

# Access by key (Standard Dict)
user = registry["id_101"]

# Query by value (Relational)
# Note the special 'key' column name for the dict keys
admins = registry.query("key = 'id_101' OR age > 50")
```

bulk updates and

```python
with users.transaction():
    for u in users:
        u.age += 1
# SQL is updated once at the end of the block in a single transaction.
```

Schema Control (Optional)
Mirage will auto infer indexes, but you can explicitly add them as well

```python
# Explicitly tell Mirage which fields to index for speed
users = mirage.mirror(my_list, index=["age", "city"])
```
