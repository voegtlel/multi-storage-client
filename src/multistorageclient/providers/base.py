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
from abc import abstractmethod
from typing import IO, Iterator, List, Optional, Union

from ..instrumentation.utils import StorageProviderMetricsHelper
from ..types import ObjectMetadata, Range, StorageProvider
from ..utils import glob, split_path, extract_prefix_from_glob
from ..instrumentation.utils import instrumented


@instrumented
class BaseStorageProvider(StorageProvider):
    """
    Base class for implementing a storage provider that manages object storage paths.

    This class abstracts the translation of paths so that private methods (_put_object, _get_object, etc.)
    always operate on full paths, not relative paths. This is achieved using a `base_path`, which is automatically
    prepended to all provided paths, making the code simpler and more consistent.
    """

    def __init__(self, base_path: str, provider_name: str):
        self._base_path = base_path
        self._provider_name = provider_name
        self._metric_helper = StorageProviderMetricsHelper()

    def __str__(self) -> str:
        return self._provider_name

    def _append_delimiter(self, s: str, delimiter: str = "/") -> str:
        if not s.endswith(delimiter):
            s += delimiter
        return s

    def _realpath(self, path: str) -> str:
        return os.path.join(self._base_path, path.lstrip("/"))

    def put_object(self, path: str, body: bytes) -> None:
        path = self._realpath(path)
        return self._put_object(path, body)

    def get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        path = self._realpath(path)
        return self._get_object(path, byte_range)

    def copy_object(self, src_path: str, dest_path: str) -> None:
        src_path = self._realpath(src_path)
        dest_path = self._realpath(dest_path)
        return self._copy_object(src_path, dest_path)

    def delete_object(self, path: str) -> None:
        path = self._realpath(path)
        return self._delete_object(path)

    def get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        path = self._realpath(path)
        return self._get_object_metadata(path, strict=strict)

    def list_objects(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        if (start_after is not None) and (end_at is not None) and not (start_after < end_at):
            raise ValueError(f"start_after ({start_after}) must be before end_at ({end_at})!")

        prefix = self._realpath(prefix)
        if self._base_path:
            # self.base_path will always contain bucket first, so we can safely split
            _, base_prefix = split_path(self._base_path)
            for object in self._list_objects(prefix, start_after, end_at, include_directories):
                object.key = object.key.replace(base_prefix, "", 1).lstrip("/")
                yield object
        else:
            yield from self._list_objects(prefix, start_after, end_at, include_directories)

    def upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        remote_path = self._realpath(remote_path)
        return self._upload_file(remote_path, f)

    def download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        remote_path = self._realpath(remote_path)
        return self._download_file(remote_path, f, metadata)

    def glob(self, pattern: str) -> List[str]:
        prefix = extract_prefix_from_glob(pattern)
        if self._base_path:
            keys = [object.key for object in self.list_objects(prefix)]
            return [key for key in glob(keys, pattern)]
        else:
            bucket, pattern = split_path(pattern)
            keys = [object.key for object in self.list_objects(prefix)]
            return [f"{bucket}/{key}" for key in glob(keys, pattern)]

    def is_file(self, path: str) -> bool:
        try:
            metadata = self.get_object_metadata(path)
            return metadata.type == "file"
        except FileNotFoundError:
            return False

    @abstractmethod
    def _put_object(self, path: str, body: bytes) -> None:
        pass

    @abstractmethod
    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        pass

    @abstractmethod
    def _copy_object(self, src_path: str, dest_path: str) -> None:
        pass

    @abstractmethod
    def _delete_object(self, path: str) -> None:
        pass

    @abstractmethod
    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        pass

    @abstractmethod
    def _list_objects(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        pass

    @abstractmethod
    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        pass

    @abstractmethod
    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        pass
