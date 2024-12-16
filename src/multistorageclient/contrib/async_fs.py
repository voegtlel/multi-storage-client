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
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os
from typing import Any, Callable, Dict, List, Tuple, Union, Optional

from fsspec.asyn import AsyncFileSystem

from ..client import StorageClient
from ..file import ObjectFile, PosixFile
from ..shortcuts import resolve_storage_client
from ..types import MSC_PROTOCOL, MSC_PROTOCOL_NAME

_global_thread_pool = ThreadPoolExecutor(max_workers=int(os.getenv('MSC_MAX_WORKERS', '8')))


# pylint: disable=abstract-method
class MultiAsyncFileSystem(AsyncFileSystem):
    """
    Custom fsspec AsyncFileSystem implementation for MSC protocol (msc://).
    Uses StorageClient for backend operations.
    """

    def __init__(self, **kwargs: Any) -> None:
        """
        Initializes the MultiAsyncFileSystem.

        Args:
            kwargs: Additional arguments for the fsspec.AsyncFileSystem.
        """
        super().__init__(**kwargs)
        self.protocol = MSC_PROTOCOL_NAME

    def resolve_path_and_storage_client(self, path: str) -> Tuple[StorageClient, str]:
        """
        Resolves the path and retrieves the associated StorageClient.

        Args:
            path: The file path to resolve.

        Returns:
            A tuple containing the StorageClient and the resolved path.
        """
        # Use unstrip_protocol to prepend our 'msc://' protocol only if it wasn't given in "path".
        return resolve_storage_client(self.unstrip_protocol(path))

    @staticmethod
    def asynchronize_sync(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Runs a synchronous function asynchronously using asyncio.

        Args:
            func: The synchronous function to be executed asynchronously.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The result of the asynchronous execution of the function.
        """
        loop = asyncio.get_event_loop()
        return loop.run_in_executor(_global_thread_pool, partial(func, *args, **kwargs))

    def glob(self, path: str, maxdepth: Optional[int] = None, **kwargs: Any) -> List[str]:
        """
        Matches and retrieves a list of objects in the storage provider that
        match the specified pattern.

        Args:
            :param path: The pattern to match object paths against, supporting wildcards (e.g., ``*.txt``).
            :param maxdepth: maxdepth of the pattern match

        Returns:
            A list of object paths that match the pattern.
        """
        storage_client, file_path = self.resolve_path_and_storage_client(path)
        return storage_client.glob(file_path, include_url_prefix=path.startswith(MSC_PROTOCOL))

    async def _glob(self, path: str, maxdepth: Optional[int] = None, **kwargs: Any) -> List[str]:
        """
        Asynchronously matches and retrieves a list of objects in the storage provider that
        match the specified pattern.

        Args:
            :param path: The pattern to match object paths against, supporting wildcards (e.g., ``*.txt``).
            :param maxdepth: maxdepth of the pattern match

        Returns:
            A list of object paths that match the pattern.
        """
        return await self.asynchronize_sync(self.glob, path, maxdepth, **kwargs)

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> Union[List[Dict[str, Any]], List[str]]:
        """
        Lists the contents of a directory.

        Args:
            path: The directory path to list.
            detail: Whether to return detailed information for each file.
            kwargs: Additional arguments for list functionality.

        Returns:
            A list of file names or detailed information depending on the 'detail' argument.
        """
        storage_client, dir_path = self.resolve_path_and_storage_client(path)
        objects = storage_client.list(dir_path)
        if detail:
            return [
                {
                    "name": obj.key,
                    "ETag": obj.etag,
                    "LastModified": obj.last_modified,
                    "size": obj.content_length,
                    "ContentType": obj.content_type,
                    "type": obj.type,
                }
                for obj in objects
            ]
        else:
            return [obj.key for obj in objects]

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> Union[List[Dict[str, Any]], List[str]]:
        """
        Asynchronously lists the contents of a directory.

        Args:
            path: The directory path to list.
            detail: Whether to return detailed information for each file.
            kwargs: Additional arguments for list functionality.

        Returns:
            A list of file names or detailed information depending on the 'detail' argument.
        """
        return await self.asynchronize_sync(self.ls, path, detail, **kwargs)

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        """
        Retrieves metadata information for a file.

        Args:
            path: The file path to retrieve information for.
            kwargs: Additional arguments for info functionality.

        Returns:
            A dictionary containing file metadata such as ETag, last modified, and size.
        """
        storage_client, file_path = self.resolve_path_and_storage_client(path)
        metadata = storage_client.info(file_path)
        return {
            "name": metadata.key,
            "ETag": metadata.etag,
            "LastModified": metadata.last_modified,
            "size": metadata.content_length,
            "ContentType": metadata.content_type,
            "type": metadata.type,
        }

    async def _info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        """
        Asynchronously retrieves metadata information for a file.

        Args:
            path: The file path to retrieve information for.
            kwargs: Additional arguments for info functionality.

        Returns:
            A dictionary containing file metadata such as ETag, last modified, and size.
        """
        return await self.asynchronize_sync(self.info, path, **kwargs)

    def rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """
        Removes a file or directory.

        Args:
            path: The file or directory path to remove.
            recursive: If True, will remove directories and their contents recursively.
            kwargs: Additional arguments for remove functionality.

        Raises:
            IsADirectoryError: If the path is a directory and recursive is not set to True.
        """
        storage_client, path = self.resolve_path_and_storage_client(path)
        if recursive:
            if not storage_client.is_file(path):
                files = [object.key for object in storage_client.list(path)]
                for file_path in files:
                    self.rm(file_path, recursive=True)
                storage_client.delete(path)
            else:
                storage_client.delete(path)
        else:
            if not storage_client.is_file(path):
                raise IsADirectoryError(f"'{path}' is a directory. Use recursive=True to remove directories.")
            storage_client.delete(path)

    async def _rm(self, path: str, recursive: bool = False, **kwargs: Any) -> None:
        """
        Asynchronously removes a file or directory.

        Args:
            path: The file or directory path to remove.
            recursive: If True, will remove directories and their contents recursively.
            kwargs: Additional arguments for remove functionality.
        """
        await self.asynchronize_sync(self.rm, path, recursive, **kwargs)

    def get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        """
        Downloads a file from the remote path to the local path.

        Args:
            rpath: The remote path of the file to download.
            lpath: The local path to store the file.
            kwargs: Additional arguments for file retrieval functionality.
        """
        storage_client, rpath = self.resolve_path_and_storage_client(rpath)
        storage_client.download_file(rpath, lpath)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        """
        Asynchronously downloads a file from the remote path to the local path.

        Args:
            rpath: The remote path of the file to download.
            lpath: The local path to store the file.
            kwargs: Additional arguments for file retrieval functionality.
        """
        await self.asynchronize_sync(self.get_file, rpath, lpath, **kwargs)

    def put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        """
        Uploads a local file to the remote path.

        Args:
            lpath: The local path of the file to upload.
            rpath: The remote path to store the file.
            kwargs: Additional arguments for file upload functionality.
        """
        storage_client, rpath = self.resolve_path_and_storage_client(rpath)
        storage_client.upload_file(rpath, lpath)

    async def _put_file(self, lpath: str, rpath: str, **kwargs: Any) -> None:
        """
        Asynchronously uploads a local file to the remote path.

        Args:
            lpath: The local path of the file to upload.
            rpath: The remote path to store the file.
            kwargs: Additional arguments for file upload functionality.
        """
        await self.asynchronize_sync(self.put_file, lpath, rpath, **kwargs)

    def open(self, path: str, mode: str = 'rb', **kwargs: Any) -> Union[PosixFile, ObjectFile]:
        """
        Opens a file at the given path.

        Args:
            path: The file path to open.
            mode: The mode in which to open the file (default: 'rb').
            kwargs: Additional arguments for file opening.

        Returns:
            A ManagedFile object representing the opened file.
        """
        storage_client, path = self.resolve_path_and_storage_client(path)
        return storage_client.open(path, mode)

    async def _open(self, path: str, mode: str = 'rb', **kwargs: Any) -> Union[PosixFile, ObjectFile]:
        """
        Asynchronously opens a file at the given path.

        Args:
            path: The file path to open.
            mode: The mode in which to open the file (default: 'rb').
            kwargs: Additional arguments for file opening.

        Returns:
            A ManagedFile object representing the opened file.
        """
        return await self.asynchronize_sync(self.open, path, mode, **kwargs)

    def pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """
        Writes a value (bytes) directly to a file at the given path.

        Args:
            path: The file path to write the value to.
            value: The bytes to write to the file.
            kwargs: Additional arguments for writing functionality.
        """
        storage_client, path = self.resolve_path_and_storage_client(path)
        storage_client.write(path, value)

    async def _pipe_file(self, path: str, value: bytes, **kwargs: Any) -> None:
        """
        Asynchronously writes a value (bytes) directly to a file at the given path.

        Args:
            path: The file path to write the value to.
            value: The bytes to write to the file.
            kwargs: Additional arguments for writing functionality.
        """
        await self.asynchronize_sync(self.pipe_file, path, value, **kwargs)

    def cat_file(self, path: str, **kwargs: Any) -> bytes:
        """
        Reads the contents of a file at the given path.

        Args:
            path: The file path to read from.
            kwargs: Additional arguments for file reading functionality.

        Returns:
            The contents of the file as bytes.
        """
        storage_client, path = self.resolve_path_and_storage_client(path)
        return storage_client.read(path)

    async def _cat_file(self, path: str, **kwargs: Any) -> bytes:
        """
        Asynchronously reads the contents of a file at the given path.

        Args:
            path: The file path to read from.
            kwargs: Additional arguments for file reading functionality.

        Returns:
            The contents of the file as bytes.
        """
        return await self.asynchronize_sync(self.cat_file, path, **kwargs)
