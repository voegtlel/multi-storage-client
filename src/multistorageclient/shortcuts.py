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

import threading
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import urlparse

from .client import StorageClient
from .config import StorageClientConfig
from .file import ObjectFile, PosixFile
from .types import DEFAULT_POSIX_PROFILE_NAME, MSC_PROTOCOL, MSC_PROTOCOL_NAME

_instance_cache: Dict[str, StorageClient] = {}
_cache_lock = threading.Lock()


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
    pr = urlparse(url)
    if pr.scheme == MSC_PROTOCOL_NAME:
        profile = pr.netloc

        # Remove the leading slash
        if pr.path.startswith("/"):
            path = pr.path[1:]
        else:
            path = pr.path
    elif pr.scheme == "" or pr.scheme == "file":
        if Path(pr.path).is_absolute():
            profile = DEFAULT_POSIX_PROFILE_NAME
            path = pr.path
        else:
            raise ValueError(f'Invalid POSIX path "{url}", only absolute path is allowed')
    else:
        raise ValueError(f'Unknown URL "{url}", expecting "{MSC_PROTOCOL}" or a POSIX path')

    if profile in _instance_cache:
        return _instance_cache[profile], path

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
