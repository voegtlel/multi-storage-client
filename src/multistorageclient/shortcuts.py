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
import threading
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import ParseResult, urlparse

from .client import StorageClient
from .config import StorageClientConfig
from .file import ObjectFile, PosixFile
from .types import DEFAULT_POSIX_PROFILE_NAME, MSC_PROTOCOL, ObjectMetadata

_instance_cache: Dict[str, StorageClient] = {}
_cache_lock = threading.Lock()


def _build_full_path(pr: ParseResult) -> str:
    """
    Helper function to construct the full path from a parsed URL, including query and fragment.

    :param pr: The parsed URL result from urlparse
    :return: The complete path including query and fragment if present
    """
    path = pr.path
    if pr.query:
        path += "?" + pr.query
    if pr.fragment:
        path += "#" + pr.fragment
    return path


def resolve_storage_client(url: str) -> Tuple[StorageClient, str]:
    """
    Build and return a :py:class:`multistorageclient.StorageClient` instance based on the provided URL or path.

    This function parses the given URL or path and determines the appropriate storage profile and path.
    It supports URLs with the protocol ``msc://``, as well as POSIX paths or ``file://`` URLs for local file
    system access. If the profile has already been instantiated, it returns the cached client. Otherwise,
    it creates a new :py:class:`StorageClient` and caches it.

    :param url: The storage location, which can be:
                - A URL in the format ``msc://profile/path`` for object storage.
                - A local file system path (absolute POSIX path) or a ``file://`` URL.

    :return: A tuple containing the :py:class:`multistorageclient.StorageClient` instance and the parsed path.

    :raises ValueError: If the URL's protocol is neither ``msc`` nor a valid local file system path.
    """
    if url.startswith(MSC_PROTOCOL):
        pr = urlparse(url)
        profile = pr.netloc
        path = _build_full_path(pr)
        if path.startswith("/"):
            path = path[1:]
    elif url.startswith("file://"):
        pr = urlparse(url)
        profile = DEFAULT_POSIX_PROFILE_NAME
        path = _build_full_path(pr)
    elif url.startswith("/"):
        # POSIX paths (only absolute paths are supported)
        url = os.path.normpath(url)
        if os.path.isabs(url):
            profile = DEFAULT_POSIX_PROFILE_NAME
            path = url
        else:
            raise ValueError(f'Invalid POSIX path "{url}", only absolute path is allowed')
    else:
        raise ValueError(f'Unknown URL "{url}", expecting "{MSC_PROTOCOL}" or a POSIX path')

    # Check if the profile has already been instantiated
    if profile in _instance_cache:
        return _instance_cache[profile], path

    # Create a new StorageClient instance and cache it
    with _cache_lock:
        if profile in _instance_cache:
            return _instance_cache[profile], path
        else:
            client = StorageClient(config=StorageClientConfig.from_file(profile=profile))
            _instance_cache[profile] = client

    return client, path


def open(url: str, mode: str = "rb", **kwargs: Any) -> Union[PosixFile, ObjectFile]:
    """
    Open a file at the given URL using the specified mode.

    The function utilizes the :py:class:`multistorageclient.StorageClient` to open a file at the provided path.
    The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient` is retrieved or built.

    :param url: The URL of the file to open. (example: ``msc://profile/prefix/dataset.tar``)
    :param mode: The file mode to open the file in.

    :return: A file-like object that allows interaction with the file.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.open(path, mode, **kwargs)


def glob(pattern: str) -> List[str]:
    """
    Return a list of files matching a pattern.

    This function supports glob-style patterns for matching multiple files within a storage system. The pattern is
    parsed, and the associated :py:class:`multistorageclient.StorageClient` is used to retrieve the
    list of matching files.

    :param pattern: The glob-style pattern to match files. (example: ``msc://profile/prefix/**/*.tar``)

    :return: A list of file paths matching the pattern.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(pattern)
    if not pattern.startswith(MSC_PROTOCOL) and client.profile == DEFAULT_POSIX_PROFILE_NAME:
        return client.glob(path, include_url_prefix=False)
    else:
        return client.glob(path, include_url_prefix=True)


def upload_file(url: str, local_path: str) -> None:
    """
    Upload a file to the given URL from a local path.

    The function utilizes the :py:class:`multistorageclient.StorageClient` to upload a file (object) to the
    provided path. The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient`
    is retrieved or built.

    :param url: The URL of the file. (example: ``msc://profile/prefix/dataset.tar``)
    :param local_path: The local path of the file.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.upload_file(remote_path=path, local_path=local_path)


def download_file(url: str, local_path: str) -> None:
    """
    Download a file in a given remote_path to a local path

    The function utilizes the :py:class:`multistorageclient.StorageClient` to download a file (object) at the
    provided path. The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient`
    is retrieved or built.

    :param url: The URL of the file to download. (example: ``msc://profile/prefix/dataset.tar``)
    :param local_path: The local path where the file should be downloaded.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.download_file(remote_path=path, local_path=local_path)


def is_empty(url: str) -> bool:
    """
    Checks whether the specified URL contains any objects.

    :param url: The URL to check, typically pointing to a storage location.
    :return: ``True`` if there are no objects/files under this URL, ``False`` otherwise.

    :raises ValueError: If the URL's protocol does not match the expected protocol ``msc``.
    """
    client, path = resolve_storage_client(url)
    return client.is_empty(path)


def is_file(url: str) -> bool:
    """
    Checks whether the specified url points to a file (rather than a directory or folder).

    The function utilizes the :py:class:`multistorageclient.StorageClient` to check if a file (object) exists
    at the provided path. The URL is parsed, and the corresponding :py:class:`multistorageclient.StorageClient`
    is retrieved or built.

    :param url: The URL to check the existence of a file. (example: ``msc://profile/prefix/dataset.tar``)
    """
    client, path = resolve_storage_client(url)
    return client.is_file(path=path)


def sync(source_url: str, target_url: str, delete_unmatched_files: bool = False) -> None:
    """
    Syncs files from the source storage to the target storage.

    :param source_url: The URL for the source storage.
    :param target_url: The URL for the target storage.
    :param delete_unmatched_files: Whether to delete files at the target that are not present at the source.
    """
    source_client, source_path = resolve_storage_client(source_url)
    target_client, target_path = resolve_storage_client(target_url)
    target_client.sync_from(source_client, source_path, target_path, delete_unmatched_files)


def list(
    url: str, start_after: Optional[str] = None, end_at: Optional[str] = None, include_directories: bool = False
) -> Iterator[ObjectMetadata]:
    """
    Lists the contents of the specified URL prefix.

    This function retrieves the corresponding :py:class:`multistorageclient.StorageClient`
    for the given URL and returns an iterator of objects (files or directories) stored under the provided prefix.

    :param url: The prefix to list objects under.
    :param start_after: The key to start after (i.e. exclusive). An object with this key doesn't have to exist.
    :param end_at: The key to end at (i.e. inclusive). An object with this key doesn't have to exist.
    :param include_directories: Whether to include directories in the result. When True, directories are returned alongside objects.

    :return: An iterator of :py:class:`ObjectMetadata` objects representing the files (and optionally directories)
             accessible under the specified URL prefix. The returned keys will always be prefixed with msc://.
    """
    client, prefix = resolve_storage_client(url)
    return client.list(
        prefix=prefix,
        start_after=start_after,
        end_at=end_at,
        include_directories=include_directories,
        include_url_prefix=True,
    )


def write(url: str, body: bytes) -> None:
    """
    Writes an object to the storage provider at the specified path.

    :param url: The path where the object should be written.
    :param body: The content to write to the object.
    """
    client, path = resolve_storage_client(url)
    client.write(path=path, body=body)


def delete(url: str) -> None:
    """
    Deletes the specified object from the storage provider.

    This function retrieves the corresponding :py:class:`multistorageclient.StorageClient`
    for the given URL and deletes the object at the specified path.

    :param url: The URL of the object to delete. (example: ``msc://profile/prefix/file.txt``)
    """
    client, path = resolve_storage_client(url)
    client.delete(path)
