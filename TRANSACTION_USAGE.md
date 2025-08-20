# Transaction Support in Simplesingletable

SimplesingletTable now supports DynamoDB transactions for atomic operations across multiple resources. This feature enables you to perform multiple creates, updates, deletes, and other operations as a single atomic unit.

## Basic Usage

### Simple Transaction

```python
from simplesingletable import DynamoDbMemory

# Initialize memory
memory = DynamoDbMemory(...)

# Use transaction context manager
with memory.transaction() as txn:
    # Create new user
    user = txn.create(User(
        name="Alice",
        email="alice@example.com"
    ))
    
    # Create related profile
    profile = txn.create(Profile(
        user_id=user.resource_id,
        bio="Alice's profile"
    ))
    
    # Update another resource
    txn.update(Settings, resource_id="global", updates={"users_count": 1})
    
# All operations commit atomically on context exit
```

### Transaction Operations

The transaction context supports the following operations:

#### Create
```python
resource = txn.create(MyResource(field1="value1", field2="value2"))
```

#### Update
```python
# Update by instance
txn.update(resource, updates={"field1": "new_value"})

# Update by class and ID
txn.update(MyResource, resource_id="id123", updates={"field1": "new_value"})
```

#### Delete
```python
# Delete by instance
txn.delete(resource)

# Delete by class and ID
txn.delete(MyResource, resource_id="id123")
```

#### Increment
```python
# Increment a numeric field
txn.increment(Counter, field_name="count", amount=5, resource_id="counter1")
```

#### Append
```python
# Append to a list field
txn.append(User, field_name="tags", values=["tag1", "tag2"], resource_id="user1")
```

## Advanced Features

### Isolation Levels

Control how reads behave within a transaction:

```python
# Read Committed (default) - reads go to database
with memory.transaction(isolation_level="read_committed") as txn:
    user = txn.read(User, "user1")  # Reads from database
    
# Snapshot - reads are cached within transaction
with memory.transaction(isolation_level="snapshot") as txn:
    user1 = txn.read(User, "user1")  # First read from database
    user2 = txn.read(User, "user1")  # Returns cached version
```

### Automatic Retry

Handle version conflicts with automatic retry:

```python
with memory.transaction(auto_retry=True, max_retries=3) as txn:
    # Operations that might conflict
    txn.update(resource, updates={"counter": 10})
```

### Conditional Operations

Add conditions to your operations:

```python
with memory.transaction() as txn:
    # Only update if age < 30
    txn.update(
        User,
        resource_id="user1",
        updates={"status": "active"},
        condition="age < :max_age",
        condition_values={":max_age": 30}
    )
```

### Mixed Versioned and Non-Versioned Resources

Transactions work seamlessly with both versioned and non-versioned resources:

```python
with memory.transaction() as txn:
    # Non-versioned resource
    user = txn.create(User(name="Bob"))
    
    # Versioned resource (maintains version history)
    post = txn.create(BlogPost(
        title="First Post",
        author_id=user.resource_id
    ))
    
    # Both commit atomically
```

## Error Handling

Transactions automatically roll back on any error:

```python
try:
    with memory.transaction() as txn:
        txn.create(resource1)
        txn.update(resource2, updates={"field": "value"})
        
        # If this fails, all operations roll back
        txn.delete(resource3)
except TransactionError as e:
    print(f"Transaction failed: {e}")
    # No changes were made to the database
```

## Limitations

- Maximum 100 items per transaction (DynamoDB limit)
- Maximum 4MB total transaction size (DynamoDB limit)
- All items must be in the same AWS region
- Cannot include items from different AWS accounts
- S3 blob operations are not part of the transaction (eventual consistency)

## Implementation Notes

### Optimistic Locking

Non-versioned resources can use optimistic locking with version tokens:

```python
resource._version_token = str(uuid.uuid4())
# Version token is preserved through serialization/deserialization
```

### Transaction Building

Operations are queued and built into DynamoDB transaction items:
- CREATE → Put with existence check
- UPDATE → Update with optional conditions
- DELETE → Delete with optional conditions
- INCREMENT → Update with ADD operation
- APPEND → Update with list_append function

### Performance Considerations

- Transactions have higher latency than individual operations
- Use batch operations when atomicity is not required
- Consider eventual consistency for non-critical updates
- S3 blob operations happen outside the transaction

## Example: E-commerce Order Processing

```python
with memory.transaction() as txn:
    # Create order
    order = txn.create(Order(
        customer_id=customer_id,
        items=cart_items,
        total=total_amount
    ))
    
    # Update inventory
    for item in cart_items:
        txn.increment(
            Inventory,
            field_name="quantity",
            amount=-item.quantity,
            resource_id=item.product_id
        )
    
    # Update customer
    txn.update(
        Customer,
        resource_id=customer_id,
        updates={"last_order_id": order.resource_id}
    )
    
    # Create payment record
    payment = txn.create(Payment(
        order_id=order.resource_id,
        amount=total_amount,
        status="pending"
    ))
    
# All operations succeed or fail together
```

This ensures that orders, inventory, customer records, and payments remain consistent even if any single operation fails.