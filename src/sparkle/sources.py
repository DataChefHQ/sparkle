from dataclasses import dataclass
from enum import Enum
from typing import Any
from sparkle.reader import Reader


class SourceType(Enum):
    """Enum for different types of sources."""

    hive_table = "hive_table"
    iceberg_table = "iceberg_table"
    kafka_topic = "kafka_topic"
    s3_bucket = "s3_bucket"


@dataclass
class Source:
    """Dataclass for Source."""

    name: str
    address: str
    type: SourceType
    reader_config: dict[str, Any]

    @classmethod
    def from_dict(cls, source_dict: dict) -> "Source":
        """Create a Source object from a dictionary.

        Args:
            source_dict (dict): Dictionary containing source information

        Returns:
            Source: Source object
        """
        # TODO Add validation for source_dict, and handle missing keys
        return Source(
            name=source_dict["name"],
            address=source_dict["address"],
            type=SourceType(source_dict["type"]),
            reader_config=source_dict["reader_config"],
        )

    @property
    def reader(self) -> Reader:
        """The reader method creates a reader object based on the source type.

        Returns:
            Reader: Reader object
        """
        match self.type:
            case SourceType.hive_table:
                # TODO Create a Hive reader
                raise NotImplementedError("Hive reader not implemented")
            case SourceType.iceberg_table:
                # TODO Create an Iceberg reader
                raise NotImplementedError("Iceberg reader not implemented")
            case SourceType.kafka_topic:
                # TODO Create a Kafka reader
                raise NotImplementedError("Kafka reader not implemented")
            case SourceType.s3_bucket:
                # TODO Create an S3 reader
                raise NotImplementedError("S3 reader not implemented")


@dataclass
class SourcesModel:
    """Dataclass model for source validation.

    This class is used to validate that all properties of a class are
    of type Source. To use this class, inherit from it and define the
    properties as Source objects. For example:

    @dataclass
    class Sources(SourcesModel):
        f1: Source
        f2: Source

    """

    def __post_init__(self):
        """Validate that all properties are of type Source."""
        for prop in self.__annotations__:
            type_ = type(getattr(self, prop))
            if not isinstance(type_, Source):
                raise TypeError(f"Expected Source, got {type_}")
