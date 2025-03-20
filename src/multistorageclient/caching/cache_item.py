from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import os


@dataclass
class CacheItem:
    """
    A class representing a cached file and its metadata.

    :param file_path: The full path to the cached file.
    :param file_size: The size of the file in bytes.
    :param atime: The last access time of the file (timestamp).
    :param mtime: The last modification time of the file (timestamp).
    :param hashed_key: The hashed key used to identify this file in the cache.
    """

    def __init__(
        self,
        file_path: str,
        file_size: int,
        atime: float,
        mtime: float,
        hashed_key: str,
    ) -> None:
        """Initialize a CacheItem instance.

        :param file_path: The full path to the cached file.
        :param file_size: The size of the file in bytes.
        :param atime: The last access time of the file (timestamp).
        :param mtime: The last modification time of the file (timestamp).
        :param hashed_key: The hashed key used to identify this file in the cache.
        """
        self.file_path = file_path
        self.file_size = file_size
        self.atime = atime
        self.mtime = mtime
        self.hashed_key = hashed_key

    @staticmethod
    def from_path(file_path: str, hashed_key: str) -> Optional[CacheItem]:
        """
        Create a CacheItem instance from a file path.

        :param file_path: The path to the file.
        :param hashed_key: The hashed key used to identify this file in the cache.
        :return: CacheItem instance if the file exists and is accessible, None otherwise.
        """
        try:
            stat = os.stat(file_path)
            return CacheItem(
                file_path=file_path,
                file_size=stat.st_size,
                atime=stat.st_atime,
                mtime=stat.st_mtime,
                hashed_key=hashed_key,
            )
        except OSError:
            return None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CacheItem):
            return False
        return self.hashed_key == other.hashed_key

    def __hash__(self) -> int:
        return hash(self.hashed_key)
