import os
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class TablePath:
    """A class representing the path details of a table.

    Attributes:
        url (str): The full URL path of the table.
        bucket (str): The bucket name if the table is stored in an S3 bucket; otherwise, it can be empty.
        prefix (str): The path prefix within the bucket or the filesystem path.
        is_s3 (bool): A flag indicating whether the table path is an S3 path.
    """

    url: str
    bucket: str
    prefix: str
    is_s3: bool

    @classmethod
    def from_database_table(cls, database_path: str, table_name: str) -> "TablePath":
        """Create a TablePath object from a database path and table name.

        This method constructs the full path to a table by joining the database path
        and the table name. It parses the constructed path to extract the bucket name,
        prefix, and determine if the path is an S3 path.

        Args:
            database_path (str): The base path to the database, typically a filesystem or S3 path.
            table_name (str): The name of the table whose path is being constructed.

        Returns:
            TablePath: An instance of TablePath containing the parsed path details.
        """
        path = os.path.join(database_path, table_name.strip("/"))
        parsed = urlparse(path)
        return cls(
            url=path,
            bucket=parsed.netloc,
            prefix=parsed.path.lstrip("/"),
            is_s3=parsed.scheme == "s3",
        )
