# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import os
import stat
import tempfile
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Tuple, Union

from filelock import BaseFileLock, FileLock, Timeout

from .instrumentation.utils import CacheManagerMetricsHelper

DEFAULT_CACHE_SIZE_MB = 10_000  # 10 GB
DEFAULT_CACHE_REFRESH_INTERVAL = 300  # 5 minutes
DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes


@dataclass
class CacheConfig:
    """
    Configuration for the :py:class:`CacheManager`.
    """

    #: The directory where the cache is stored.
    location: str
    #: The maximum size of the cache in megabytes.
    size_mb: int
    #: Use etag to update the cached files.
    use_etag: bool

    def size_bytes(self) -> int:
        """
        Convert cache size from megabytes to bytes.

        :return: The size of the cache in bytes.
        """
        return self.size_mb * 1024 * 1024


class CacheManager:
    """
    A cache manager that stores files in a specified directory and evicts files based on the LRU policy.
    """

    def __init__(
        self, profile: str, cache_config: CacheConfig, cache_refresh_interval: int = DEFAULT_CACHE_REFRESH_INTERVAL
    ):
        self._profile = profile
        self._cache_config = cache_config
        self._max_cache_size = cache_config.size_bytes()
        self._cache_refresh_interval = cache_refresh_interval
        self._last_refresh_time = datetime.now()

        # Metrics
        self._metrics_helper = CacheManagerMetricsHelper()

        # Create cache directory
        self._cache_dir = cache_config.location
        os.makedirs(self._cache_dir, exist_ok=True)
        os.makedirs(os.path.join(self._cache_dir, self._profile), exist_ok=True)

        # Populate cache with existing files in the cache directory
        self._cache_refresh_lock_file = FileLock(
            os.path.join(self._cache_dir, ".cache_refresh.lock"), timeout=0, blocking=False
        )
        self.refresh_cache()

    def _get_file_size(self, file_path: str) -> Optional[int]:
        """
        Get the size of the file in bytes.

        :return: The file size in bytes.
        """
        try:
            return os.path.getsize(file_path)
        except OSError:
            return None

    def _delete(self, file_name: str) -> None:
        """
        Delete a file from the cache directory.
        """
        try:
            os.unlink(os.path.join(self._cache_dir, self._profile, file_name))
            os.unlink(os.path.join(self._cache_dir, self._profile, f".{file_name}.lock"))
        except OSError:
            pass

    def _get_cache_key(self, file_name: str) -> str:
        """
        Hash the file name using MD5.
        """
        return hashlib.md5(file_name.encode()).hexdigest()

    def _should_refresh_cache(self) -> bool:
        """
        Check if enough time has passed since the last refresh.
        """
        now = datetime.now()
        return (now - self._last_refresh_time).seconds > self._cache_refresh_interval

    def use_etag(self) -> bool:
        """
        Check if ``use_etag`` is set in the cache config.
        """
        return self._cache_config.use_etag

    def get_max_cache_size(self) -> int:
        """
        Return the cache size in bytes from the cache config.
        """
        return self._max_cache_size

    def get_cache_dir(self) -> str:
        """
        Return the path to the local cache directory.

        :return: The full path to the cache directory.
        """
        return os.path.join(self._cache_dir, self._profile)

    def get_cache_file_path(self, key: str) -> str:
        """
        Return the path to the local cache file for the given key.

        :return: The full path to the cached file.
        """
        hashed_name = self._get_cache_key(key)
        return os.path.join(self._cache_dir, self._profile, hashed_name)

    def read(self, key: str) -> Optional[bytes]:
        """
        Read the contents of a file from the cache if it exists.

        :param key: The key corresponding to the file to be read.

        :return: The contents of the file as bytes if found in the cache, otherwise None.
        """
        success = True
        try:
            try:
                if self.contains(key):
                    with open(self.get_cache_file_path(key), "rb") as fp:
                        return fp.read()
            except OSError:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="READ", success=success)

    def open(self, key: str, mode: str = "rb") -> Optional[Any]:
        """
        Open a file from the cache and return the file object.

        :param key: The key corresponding to the file to be opened.
        :param mode: The mode in which to open the file (default is ``rb`` for read binary).

        :return: The file object if the file is found in the cache, otherwise None.
        """
        success = True
        try:
            try:
                if self.contains(key):
                    return open(self.get_cache_file_path(key), mode)
            except OSError:
                pass

            # cache miss
            success = False
            return None
        finally:
            self._metrics_helper.increase(operation="OPEN", success=success)

    def set(self, key: str, source: Union[str, bytes]) -> None:
        """
        Store a file in the cache.

        :param key: The key corresponding to the file to be stored.
        :param source: The source data to be stored, either a path to a file or bytes.
        """
        success = True
        try:
            hashed_name = self._get_cache_key(key)
            file_path = os.path.join(self._cache_dir, self._profile, hashed_name)

            if isinstance(source, str):
                # Move the file to the cache directory
                os.rename(src=source, dst=file_path)
                # Only allow the owner to read and write the file
                os.chmod(file_path, mode=stat.S_IRUSR | stat.S_IWUSR)
            else:
                # Create a temporary file and move the file to the cache directory
                with tempfile.NamedTemporaryFile(
                    mode="wb", delete=False, dir=os.path.dirname(file_path), prefix="."
                ) as temp_file:
                    temp_file_path = temp_file.name
                    temp_file.write(source)
                os.rename(src=temp_file_path, dst=file_path)
                # Only allow the owner to read and write the file
                os.chmod(file_path, mode=stat.S_IRUSR | stat.S_IWUSR)

            # Refresh cache after a few minutes
            if self._should_refresh_cache():
                thread = threading.Thread(target=self.refresh_cache)
                thread.start()
        except Exception:
            success = False
        finally:
            self._metrics_helper.increase(operation="SET", success=success)

    def contains(self, key: str) -> bool:
        """
        Check if the cache contains a file corresponding to the given key.

        :param key: The key corresponding to the file.

        :return: True if the file is found in the cache, False otherwise.
        """
        hashed_name = self._get_cache_key(key)
        file_path = os.path.join(self._cache_dir, self._profile, hashed_name)
        return os.path.exists(file_path)

    def delete(self, key: str) -> None:
        """
        Delete a file from the cache.
        """
        try:
            hashed_name = self._get_cache_key(key)
            self._delete(hashed_name)
        finally:
            self._metrics_helper.increase(operation="DELETE", success=True)

    def cache_size(self) -> int:
        """
        Return the current size of the cache in bytes.

        :return: The cache size in bytes.
        """
        file_size = 0

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                if os.path.isfile(file_path) and not file_name.endswith(".lock"):
                    size = self._get_file_size(file_path)
                    if size:
                        file_size += size

        return file_size

    def _evict_files(self) -> None:
        """
        Evict cache entries based on the last modification time.
        """
        # list of (file name, last modified time, file size)
        file_paths: List[Tuple[str, float, Optional[int]]] = []

        # Traverse the directory and subdirectories
        for dirpath, _, filenames in os.walk(self._cache_dir):
            for file_name in filenames:
                file_path = os.path.join(dirpath, file_name)
                # Skip lock and hidden files
                if file_name.endswith(".lock") or file_name.startswith("."):
                    continue
                try:
                    if os.path.isfile(file_path):
                        mtime = os.path.getmtime(file_path)
                        fsize = self._get_file_size(file_path)
                        file_paths.append((file_path, mtime, fsize))
                except OSError:
                    # Ignore if file has already been evicted
                    pass

        # Sort the files based on the last modified time
        file_paths.sort(key=lambda tup: tup[1])

        # Rebuild the cache
        cache = OrderedDict()
        cache_size = 0
        for file_path, _, file_size in file_paths:
            if file_size:
                cache[file_path] = file_size
                cache_size += file_size

        # Evict old files if necessary in case the existing files exceed cache size
        while cache_size > self._max_cache_size:
            # Pop the first (oldest) item in the OrderedDict (LRU eviction)
            oldest_file, file_size = cache.popitem(last=False)
            cache_size -= file_size
            self._delete(oldest_file)

    def refresh_cache(self) -> bool:
        """
        Scan the cache directory and evict cache entries based on the last modification time.
        This method is protected by a :py:class:`filelock.FileLock` that only allows a single process to evict the cached files.
        """
        try:
            # If the process acquires the lock, then proceed with the cache eviction
            with self._cache_refresh_lock_file.acquire(blocking=False):
                self._evict_files()
                self._last_refresh_time = datetime.now()
                return True
        except Timeout:
            # If the process cannot acquire the lock, ignore and wait for the next turn
            pass

        return False

    def acquire_lock(self, key: str) -> BaseFileLock:
        """
        Create a :py:class:`filelock.FileLock` object for a given key.

        :return: :py:class:`filelock.FileLock` object.
        """
        hashed_name = self._get_cache_key(key)
        lock_file = os.path.join(self._cache_dir, self._profile, f".{hashed_name}.lock")
        return FileLock(lock_file, timeout=DEFAULT_LOCK_TIMEOUT)

    def delete_lock(self, lock: BaseFileLock) -> None:
        """
        Delete the lock file.

        :param key: :py:class:`filelock.FileLock` object.
        """
        try:
            if os.path.exists(lock.lock_file):
                os.unlink(lock.lock_file)
        except OSError:
            pass  # Ignore errors if the file is already deleted or inaccessible
