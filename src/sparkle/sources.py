from dataclasses import dataclass
from enum import Enum
from .reader import Reader


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
    reader_config: dict[str, any]

    def from_dict(self, source_dict: dict) -> "Source":
        """Create a Source object from a dictionary.

        Args:
            source_dict (dict): Dictionary containing source information

        Returns:
            Source: Source object
        """
        return Source(
            name=source_dict["name"],
            address=source_dict["address"],
            type=SourceType(source_dict["type"]),
            reader_config=source_dict["reader_config"],
        )


class Sources:
    """Class to simplify source access."""

    def __init__(self, sources: list[SourceType]) -> None:
        """Initialize the Sources object.

        After initialization, the Sources object will have attributes
        for each source. The properties will be named after the
        source, and their values are actual readers pre-configured to
        read from the target source type.

        Args:
            sources (list[SourceType]): List of sources

        """
        for source in sources:
            self._add_source(source)

    def _add_source(self, source: SourceType) -> None:
        """Add a source to the Sources object.

        Args:
            source (SourceType): Source to add
        """
        if not isinstance(source, SourceType):
            raise TypeError(f"Expected SourceType, got {type(source)}")

        setattr(self, source.name, self._dataframe_reader(source))

    def _dataframe_reader(self, source: Source) -> Reader:
        """Create a DataFrame reader for a source.

        Args:
            source (Source): Source object

        Returns:
            Reader: Reader object
        """
        if source.type == SourceType.hive_table:
            # TODO Read from Hive
            pass
        elif source.type == SourceType.iceberg_table:
            # TODO Read from Iceberg
            pass
        elif source.type == SourceType.kafka_topic:
            # TODO Read from Kafka
            pass
        elif source.type == SourceType.s3_bucket:
            # TODO Read from S3
            pass
