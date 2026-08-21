from abc import ABC, abstractmethod

from typing_extensions import Self

from .. import DynamoDbMemory, DynamoDbResource, DynamoDbVersionedResource


class BaseSingleton(ABC):
    @classmethod
    def ensure_exists(cls, memory: "DynamoDbMemory", consistent_read=True) -> Self:
        if not (existing := memory.get_existing(cls.__name__, data_class=cls, consistent_read=consistent_read)):
            return memory.create_new(cls, {}, override_id=cls.__name__)
        return existing

    @abstractmethod
    def saved_updated_singleton(self, memory: "DynamoDbMemory"):
        pass


class SingletonResource(DynamoDbResource, BaseSingleton):
    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "SINGLETON"

    def saved_updated_singleton(self, memory: "DynamoDbMemory") -> Self:
        """Overwrites the existing Singleton with the current object"""
        existing = self.ensure_exists(memory)
        return memory.update_existing(existing, self)


class SingletonVersionedResource(DynamoDbVersionedResource, BaseSingleton):
    @classmethod
    def get_unique_key_prefix(cls) -> str:
        return "SINGLETON"

    def saved_updated_singleton(self, memory: "DynamoDbMemory") -> Self:
        """Overwrites the existing Singleton with the current object;
        the version number on this resource must match the latest existing version in the database."""
        existing = self.ensure_exists(memory)
        if existing.version != self.version:
            raise ValueError("Cannot update from non-latest version")
        return memory.update_existing(existing, self)
