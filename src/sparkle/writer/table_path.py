import os

from urllib.parse import urlparse
from dataclasses import dataclass


@dataclass
class TablePath:
    url: str
    bucket: str
    prefix: str
    is_s3: bool

    @classmethod
    def from_database_table(cls, database_path: str, table_name: str) -> "TablePath":
        path = os.path.join(database_path, table_name.strip("/"))
        parsed = urlparse(path)
        return cls(
            url=path,
            bucket=parsed.netloc,
            prefix=parsed.path.lstrip("/"),
            is_s3=parsed.scheme == "s3",
        )
