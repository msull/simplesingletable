# Simple Single Table

**Latest Version:** 5.1.0

**simplesingletable** is a Python library that offers an abstraction layer for AWS DynamoDB operations, particularly for
those tables using single-table design. It uses Pydantic to define the models, and tries to be as "batteries-included"
based on how I personally have come to use DynamoDb.

I've used and written variations of this same code many times, and finally decided to try and package it all up into a
single library I could pip install and use whenever I needed cheap, easy, fast storage with few access patterns. So far
this is working quite well for the way I use DynamoDb and the assumptions that are true for me, such as:

* My apps are generally pretty small scale -- a few hundred users at a time max
* My collection sizes are pretty small -- tens or hundreds of thousands, not millions or billions

In this scenario I've found DynamoDb to be an extremely valuable tool that can perform extremely fast and consistently,
and essentially for free or very little cost. I believe most of these tools and techniques would scale beyond that,
but cannot personally attest to it.

## Key Features

There are many ways to use DynamoDb. These are the things I've come to want:

* A single table where I can store different types of Objects (Pydantic Models)
* Automatic resource ID creation using a lexicographically sortable ID (via `ulid-py`); this is very useful for setting
  up alternative access patterns using the secondary indices and having them sort by creation date of the objects.
* Simple way to list, paginate, and filter by object type, sortable by last updated time, seamlessly blending DynamoDb
  filter expressions and server side filtering on the loaded Pydantic models using simple python functions
* Automatic versioning for created resources -- every update generates a new version, and the complete version history
  is maintained and easily accessed; additionally the code prevents simultaneous writes from the same version; updates
  can only be performed from the latest version of an object.
* Powerful query function for enabling secondary access patterns using GSIs as needed

## Installation:

```bash
pip install simplesingletable
```

# Usage

Docs, examples, and access patterns coming soon!

Here's a brief demo of how to utilize:

```python

# How to Use:
# 1. Define a Resource:
# Resources are essentially DynamoDB items. They can be defined by subclassing `DynamodbVersionedResource`:

from simplesingletable import DynamodbVersionedResource, DynamoDBMemory


class MyTestResource(DynamodbVersionedResource):
    some_field: str
    bool_field: bool
    list_of_things: list[str | int | bool | float]


# 2. CRUD Operations
from logzero import logger

dynamodb_memory = DynamoDBMemory(logger=logger, table_name="my-dynamodb-table")
resource = dynamodb_memory.create_new(MyTestResource, {})
retrieved = dynamodb_memory.read_existing(resource.resource_id, MyTestResource)
updated_resource = dynamodb_memory.update_existing(retrieved, {})
```

More coming soon...

# Testing:

The package includes a comprehensive test suite to ensure reliability and robustness. Use the test_simplesingletable.py
as a reference for the functionalities available until more documentation is available.
