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

import glob
import os
import tempfile
import time
from datetime import datetime, timezone
from io import BytesIO, StringIO
from typing import IO, Any, Callable, Iterator, List, Optional, Union

from ..types import ObjectMetadata, Range
from .base import BaseStorageProvider

PROVIDER = "file"


def atomic_write(source: Union[str, IO], destination: str):
    """
    Writes the contents of a file to the specified destination path.

    This function ensures that the file write operation is atomic, meaning the output file is either fully written or not modified at all.
    This is achieved by writing to a temporary file first and then renaming it to the destination path.

    :param source: The input file to read from. It can be a string representing the path to a file, or an open file-like object (IO).
    :param destination: The path to the destination file where the contents should be written.
    """
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(destination), prefix=".") as fp:
        temp_file_path = fp.name
        if isinstance(source, str):
            with open(source, mode="rb") as src:
                fp.write(src.read())
        else:
            fp.write(source.read())
    os.rename(src=temp_file_path, dst=destination)


class PosixFileStorageProvider(BaseStorageProvider):
    def __init__(self, base_path: str, **kwargs: Any) -> None:
        # Validate POSIX path
        if base_path == "":
            base_path = "/"

        if not base_path.startswith("/"):
            raise ValueError(f"The base_path {base_path} must be an absolute path.")

        super().__init__(base_path=base_path, provider_name=PROVIDER)

    def _collect_metrics(
        self,
        func: Callable,
        operation: str,
        path: str,
        put_object_size: Optional[int] = None,
        get_object_size: Optional[int] = None,
    ) -> Any:
        """
        Collects and records performance metrics around file operations such as PUT, GET, DELETE, etc.

        This method wraps an file operation and measures the time it takes to complete, along with recording
        the size of the object if applicable.

        :param func: The function that performs the actual file operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param path: The path to the object.
        :param put_object_size: The size of the object being uploaded, if applicable (for PUT operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for GET operations).

        :return: The result of the file operation, typically the return value of the `func` callable.
        """
        start_time = time.time()
        status_code = 200

        object_size = None
        if operation == "PUT":
            object_size = put_object_size
        elif operation == "GET" and get_object_size is not None:
            object_size = get_object_size

        try:
            result = func()
            if operation == "GET" and object_size is None:
                object_size = len(result)
            return result
        except FileNotFoundError as error:
            status_code = 404
            raise error
        except Exception as error:
            status_code = -1
            raise RuntimeError(f"Failed to {operation} object(s) at {path}") from error
        finally:
            elapsed_time = time.time() - start_time
            self._metric_helper.record_duration(
                elapsed_time, provider=PROVIDER, operation=operation, bucket="", status_code=status_code
            )
            if object_size:
                self._metric_helper.record_object_size(
                    object_size, provider=PROVIDER, operation=operation, bucket="", status_code=status_code
                )

    def _put_object(self, path: str, body: bytes) -> None:
        def _invoke_api() -> None:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            atomic_write(source=BytesIO(body), destination=path)

        return self._collect_metrics(_invoke_api, operation="PUT", path=path, put_object_size=len(body))

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        def _invoke_api() -> bytes:
            if byte_range:
                with open(path, "rb") as f:
                    f.seek(byte_range.offset)
                    return f.read(byte_range.size)
            else:
                with open(path, "rb") as f:
                    return f.read()

        return self._collect_metrics(_invoke_api, operation="GET", path=path)

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        def _invoke_api() -> None:
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            atomic_write(source=src_path, destination=dest_path)

        src_object = self._get_object_metadata(src_path)

        return self._collect_metrics(
            _invoke_api,
            operation="COPY",
            path=src_path,
            put_object_size=src_object.content_length,
        )

    def _delete_object(self, path: str) -> None:
        def _invoke_api() -> None:
            if os.path.exists(path) and os.path.isfile(path):
                os.remove(path)

        return self._collect_metrics(_invoke_api, operation="DELETE", path=path)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        is_dir = os.path.isdir(path)
        if is_dir:
            path = self._append_delimiter(path)

        def _invoke_api() -> ObjectMetadata:
            return ObjectMetadata(
                key=path,
                type="directory" if is_dir else "file",
                content_length=os.path.getsize(path),
                last_modified=datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc),
            )

        return self._collect_metrics(_invoke_api, operation="HEAD", path=path)

    def _list_objects(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        def _invoke_api() -> Iterator[ObjectMetadata]:
            # Assume the file system guarantees lexicographical order (some don't).
            for root, dirs, files in os.walk(prefix):
                if include_directories:
                    for dir in dirs:
                        full_path = os.path.join(root, dir)
                        relative_path = os.path.relpath(full_path, self._base_path)
                        yield ObjectMetadata(
                            key=relative_path,
                            content_length=0,
                            type="directory",
                            last_modified=datetime.min,
                        )

                # This is in reverse lexicographical order on some systems for some reason.
                for name in sorted(files):
                    full_path = os.path.join(root, name)
                    # Changed the relative path from relative to prefix → relative to base path.
                    relative_path = os.path.relpath(full_path, self._base_path)
                    if (start_after is None or start_after < relative_path) and (
                        end_at is None or relative_path <= end_at
                    ):
                        yield ObjectMetadata(
                            key=relative_path,
                            content_length=os.path.getsize(full_path),
                            last_modified=datetime.fromtimestamp(os.path.getmtime(full_path), tz=timezone.utc),
                        )
                    elif end_at is not None and end_at < relative_path:
                        return

                # Only walk one level
                if include_directories:
                    break

        return self._collect_metrics(_invoke_api, operation="LIST", path=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        os.makedirs(os.path.dirname(remote_path), exist_ok=True)

        def _invoke_api() -> None:
            atomic_write(source=f, destination=remote_path)

        if isinstance(f, str):
            filesize = os.path.getsize(f)
            return self._collect_metrics(_invoke_api, operation="PUT", path=remote_path, put_object_size=filesize)
        elif isinstance(f, StringIO):
            filesize = len(f.getvalue().encode("utf-8"))
            return self._collect_metrics(_invoke_api, operation="PUT", path=remote_path, put_object_size=filesize)
        else:
            filesize = len(f.getvalue())  # type: ignore
            return self._collect_metrics(_invoke_api, operation="PUT", path=remote_path, put_object_size=filesize)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        filesize = metadata.content_length if metadata else os.path.getsize(remote_path)

        if isinstance(f, str):

            def _invoke_api() -> None:
                os.makedirs(os.path.dirname(f), exist_ok=True)
                atomic_write(source=remote_path, destination=f)

            return self._collect_metrics(_invoke_api, operation="GET", path=remote_path, get_object_size=filesize)
        elif isinstance(f, StringIO):

            def _invoke_api() -> None:
                with open(remote_path, "r", encoding="utf-8") as src:
                    f.write(src.read())

            return self._collect_metrics(_invoke_api, operation="GET", path=remote_path, get_object_size=filesize)
        else:

            def _invoke_api() -> None:
                with open(remote_path, "rb") as src:
                    f.write(src.read())

            return self._collect_metrics(_invoke_api, operation="GET", path=remote_path, get_object_size=filesize)

    def glob(self, pattern: str) -> List[str]:
        pattern = self._realpath(pattern)
        keys = list(glob.glob(pattern, recursive=True))
        if self._base_path == "/":
            return keys
        else:
            # NOTE: PosixStorageProvider does not have the concept of bucket and prefix.
            # So we drop the base_path from it.
            return [key.replace(self._base_path, "", 1).lstrip("/") for key in keys]

    def is_file(self, path: str) -> bool:
        path = self._realpath(path)
        return os.path.isfile(path)
