from typing import Optional

from logzero import logger

from simplesingletable import DynamoDbMemory
from simplesingletable.extras.singleton import SingletonResource, SingletonVersionedResource

memory = DynamoDbMemory(
    logger=logger,
    table_name="standardexample",
    endpoint_url="http://localhost:8000",
    connection_params={
        "aws_access_key_id": "unused",
        "aws_secret_access_key": "unused",
        "region_name": "us-east-1",
    },
)


class MyAppConfig(SingletonResource):
    config_value: str = "example"
    allowed_users: set[str] = None

    def __str__(self):
        return f"{self.resource_id} / updated {self.updated_ago()}"


# creates the object if it doesn't exist, otherwise retrieves it
config = MyAppConfig.ensure_exists(memory)
logger.info(config)

# push item to the set;
if "user1" not in (config.allowed_users or []):
    logger.info("Adding user1 to allowed users")
    memory.add_to_set(config, "allowed_users", "user1")
    # re-reread config to get the updated listing; add_to_set doesn't automatically provide this

    # ensure exists uses a consistent read by default, so reading after write will return the new info
    config = MyAppConfig.ensure_exists(memory)
    logger.info(config)
else:
    logger.debug("User user1 already in allowed_users set")


# can do all the same with a Versioned singleton; the primary difference is you cannot use the "add_to_set" and
# increment methods on a versioned resource
class MyVersionedAppConfig(SingletonVersionedResource):
    config_value: str = "example"
    allowed_users: Optional[set[str]] = None

    def __str__(self):
        return f"{self.resource_id} (v{self.version}) / updated {self.updated_ago()}"


config = MyVersionedAppConfig.ensure_exists(memory)
logger.info(config)

# push item to the set;
allowed_users = config.allowed_users or set()
if "user1" not in allowed_users:
    logger.info("Adding user1 to allowed users")
    allowed_users.add("user1")
    config.allowed_users = allowed_users
    config = config.saved_updated_singleton(memory)
    logger.info(config)
else:
    logger.debug("User user1 already in allowed_users set")


logger.info(f"Config Object Database Size: {config.get_db_item_size()} ")
