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

import os
from typing import Any, Dict, Iterator, List, Optional, Union

from .config import StorageClientConfig
from .file import ObjectFile, PosixFile
from .providers.posix_file import PosixFileStorageProvider
from .retry import retry
from .types import MSC_PROTOCOL, ObjectMetadata, Range
from .utils import join_paths
from .instrumentation.utils import instrumented


@instrumented
class StorageClient:
    """
    A client for interacting with different storage providers.
    """
    _config: StorageClientConfig

    def __init__(self, config: StorageClientConfig):
        """
        Initializes the StorageClient with the given configuration.

        :param config: The configuration object for the storage client.
        """
        self._initialize_providers(config)

    def _initialize_providers(self, config: StorageClientConfig) -> None:
        self._config = config
        self._credentials_provider = self._config.credentials_provider
        self._storage_provider = self._config.storage_provider
        self._metadata_provider = self._config.metadata_provider
        self._cache_config = self._config.cache_config
        self._retry_config = self._config.retry_config
        self._cache_manager = self._config.cache_manager

    def _build_cache_path(self, path: str) -> str:
        """
        Build cache path with or without etag.
        """
        cache_path = f"{path}:{None}"

        if self._metadata_provider:
            if self._cache_manager and self._cache_manager.use_etag():
                metadata = self._metadata_provider.get_object_metadata(path)
                cache_path = f"{path}:{metadata.etag}"
        else:
            if self._cache_manager and self._cache_manager.use_etag():
                metadata = self._storage_provider.get_object_metadata(path)
                cache_path = f"{path}:{metadata.etag}"

        return cache_path

    def _is_cache_enabled(self) -> bool:
        return (self._cache_manager is not None and not self._is_posix_file_storage_provider())

    def _is_posix_file_storage_provider(self) -> bool:
        return isinstance(self._storage_provider, PosixFileStorageProvider)

    @retry
    def read(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        """
        Reads an object from the storage provider at the specified path.

        :param path: The path of the object to read.
        :return: The content of the object.
        """
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{path}' was not found.")

        # Read from cache if the file exists
        if self._is_cache_enabled():
            assert self._cache_manager is not None
            cache_path = self._build_cache_path(path)
            data = self._cache_manager.read(cache_path)

            if data:
                if byte_range:
                    return data[byte_range.offset: byte_range.offset + byte_range.size]
                else:
                    return data
            else:
                # Only cache the entire file
                if byte_range is None:
                    data = self._storage_provider.get_object(path)
                    self._cache_manager.set(cache_path, data)
                    return data

        return self._storage_provider.get_object(path, byte_range=byte_range)

    def info(self, path: str) -> ObjectMetadata:
        """
        Retrieves metadata or information about an object stored at the specified path.

        :param path: The path to the object for which metadata or information is being retrieved.

        :return: A dictionary containing metadata or information about the object.
        """
        if self._metadata_provider:
            return self._metadata_provider.get_object_metadata(path)
        else:
            return self._storage_provider.get_object_metadata(path)

    @retry
    def download_file(self, remote_path: str, local_path: str) -> None:
        """
        Downloads a file from the storage provider to the local file system.

        :param remote_path: The path of the file in the storage provider.
        :param local_path: The local path where the file should be downloaded.
        """

        if self._metadata_provider:
            real_path, exists = self._metadata_provider.realpath(remote_path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{remote_path}' was not found by metadata provider.")
            metadata = self._metadata_provider.get_object_metadata(remote_path)
            self._storage_provider.download_file(real_path, local_path, metadata)
        else:
            self._storage_provider.download_file(remote_path, local_path)

    def upload_file(self, remote_path: str, local_path: str) -> None:
        """
        Uploads a file from the local file system to the storage provider.

        :param remote_path: The path where the file should be stored in the storage provider.
        :param local_path: The local path of the file to upload.
        """
        if self._metadata_provider:
            remote_path, exists = self._metadata_provider.realpath(remote_path)
            if exists:
                raise FileExistsError(f"The file at path '{remote_path}' already exists; "
                                      f"overwriting is not yet allowed when using a metadata provider.")
        self._storage_provider.upload_file(remote_path, local_path)
        if self._metadata_provider:
            metadata = self._storage_provider.get_object_metadata(remote_path)
            self._metadata_provider.add_file(remote_path, metadata)

    def write(self, path: str, body: bytes) -> None:
        """
        Writes an object to the storage provider at the specified path.

        :param path: The path where the object should be written.
        :param body: The content to write to the object.
        """
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if exists:
                raise FileExistsError(f"The file at path '{path}' already exists; "
                                      f"overwriting is not yet allowed when using a metadata provider.")
        self._storage_provider.put_object(path, body)
        if self._metadata_provider:
            # TODO(NGCDP-3016): Handle eventual consistency of Swiftstack, without wait.
            metadata = self._storage_provider.get_object_metadata(path)
            self._metadata_provider.add_file(path, metadata)

    def delete(self, path: str) -> None:
        """
        Deletes an object from the storage provider at the specified path.

        :param path: The path of the object to delete.
        """
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if not exists:
                raise FileNotFoundError(f"The file at path '{path}' was not found.")
            self._metadata_provider.remove_file(path)

        self._storage_provider.delete_object(path)

        # Delete cached files
        if self._is_cache_enabled():
            assert self._cache_manager is not None
            cache_path = self._build_cache_path(path)
            self._cache_manager.delete(cache_path)

    def glob(self, pattern: str, include_url_prefix: bool = False) -> List[str]:
        """
        Matches and retrieves a list of objects in the storage provider that
        match the specified pattern.

        :param pattern: The pattern to match object paths against, supporting wildcards (e.g., ``*.txt``).
        :param include_url_prefix: Whether to include the URL prefix ``msc://profile`` in the result. Defaults to False.

        :return: A list of object paths that match the pattern.
        """
        if self._metadata_provider:
            results = self._metadata_provider.glob(pattern)
        else:
            results = self._storage_provider.glob(pattern)

        if include_url_prefix:
            results = [join_paths(f'{MSC_PROTOCOL}{self._config.profile}', path) for path in results]

        return results

    def list(self, prefix: str = "", start_after: Optional[str] = None,
             end_at: Optional[str] = None) -> Iterator[ObjectMetadata]:
        """
        Lists objects in the storage provider under the specified prefix.

        :param prefix: The prefix to list objects under.
        :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
        :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.

        :return: An iterator over objects.
        """
        if self._metadata_provider:
            return self._metadata_provider.list_objects(prefix, start_after, end_at)
        else:
            return self._storage_provider.list_objects(prefix, start_after, end_at)

    def open(self, path: str, mode: str = "rb") -> Union[PosixFile, ObjectFile]:
        """
        Returns a file-like object from the storage provider at the specified path.

        :param path: The path of the object to read.
        :param mode: The file mode.

        :return: A file-like object.
        """
        if self._metadata_provider:
            path, exists = self._metadata_provider.realpath(path)
            if "w" in mode and exists:
                raise FileExistsError(f"The file at path '{path}' already exists.")
            if "r" in mode and not exists:
                raise FileNotFoundError(f"The file at path '{path}' was not found.")

        if self._is_posix_file_storage_provider():
            realpath = self._storage_provider._realpath(path)  # type: ignore
            return PosixFile(path=realpath, mode=mode)
        else:
            return ObjectFile(self._storage_provider, remote_path=path, mode=mode, cache_manager=self._cache_manager)

    def is_file(self, path: str) -> bool:
        """
        Checks whether the specified path points to a file (rather than a directory or folder).

        :param path: The path to check.

        :return: True if the path points to a file, False otherwise.
        """
        if self._metadata_provider:
            _, exists = self._metadata_provider.realpath(path)
            return exists
        return self._storage_provider.is_file(path)

    def commit_updates(self, prefix: Optional[str] = None) -> None:
        """
        Commits any pending updates to the metadata provider. No-op if not using a metadata provider.

        :param prefix: If provided, scans the prefix to find files to commit.
        """
        if self._metadata_provider:
            if prefix:
                for obj in self._storage_provider.list_objects(prefix=prefix):
                    fullpath = os.path.join(prefix, obj.key)
                    self._metadata_provider.add_file(fullpath, obj)
            self._metadata_provider.commit_updates()

    def is_empty(self, path: str) -> bool:
        """
        Checks whether the specified path is empty. A path is considered empty if there are no
        objects whose keys start with the given path as a prefix.

        :param path: The path to check. This is typically a prefix representing a directory or folder.

        :return: True if no objects exist under the specified path prefix, False otherwise.
        """
        objects = self._storage_provider.list_objects(path)
        try:
            return next(objects) is None
        except StopIteration:
            pass
        return True

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        del state['_credentials_provider']
        del state['_storage_provider']
        del state['_metadata_provider']
        del state['_cache_manager']
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        config = state["_config"]
        self._initialize_providers(config)
