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

import io
import os
import tempfile
import time
from datetime import datetime
from typing import IO, Any, Callable, Iterator, Optional, Union

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobPrefix, BlobServiceClient

from ..types import (
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    Range,
)
from ..utils import split_path
from .base import BaseStorageProvider

PROVIDER = "azure"


class StaticAzureCredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that provides static Azure credentials.
    """

    _connection: str

    def __init__(self, connection: str):
        """
        Initializes the :py:class:`StaticAzureCredentialsProvider` with the provided connection string.

        :param connection: The connection string for Azure Blob Storage authentication.
        """
        self._connection = connection

    def get_credentials(self) -> Credentials:
        return Credentials(
            access_key=self._connection,
            secret_key="",
            token=None,
            expiration=None,
        )

    def refresh_credentials(self) -> None:
        pass


class AzureBlobStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with Azure Blob Storage.
    """

    def __init__(
        self, endpoint_url: str, base_path: str = "", credentials_provider: Optional[CredentialsProvider] = None
    ):
        """
        Initializes the :py:class:`AzureBlobStorageProvider` with the endpoint URL and optional credentials provider.

        :param endpoint_url: The Azure storage account URL.
        :param base_path: The root prefix path within the container where all operations will be scoped.
        :param credentials_provider: The provider to retrieve Azure credentials.
        """
        super().__init__(base_path=base_path, provider_name=PROVIDER)

        self._account_url = endpoint_url
        self._credentials_provider = credentials_provider
        self._blob_service_client = self._create_blob_service_client()

    def _create_blob_service_client(self) -> BlobServiceClient:
        """
        Creates and configures the Azure BlobServiceClient using the current credentials.

        :return: The configured BlobServiceClient.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()
            return BlobServiceClient.from_connection_string(credentials.access_key)
        else:
            return BlobServiceClient(account_url=self._account_url)

    def _refresh_blob_service_client_if_needed(self) -> None:
        """
        Refreshes the BlobServiceClient if the current credentials are expired.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()
            if credentials.is_expired():
                self._credentials_provider.refresh_credentials()
                self._blob_service_client = self._create_blob_service_client()

    def _collect_metrics(
        self,
        func: Callable,
        operation: str,
        container: str,
        blob: str,
        put_object_size: Optional[int] = None,
        get_object_size: Optional[int] = None,
    ) -> Any:
        """
        Collects and records performance metrics around Azure operations such as PUT, GET, DELETE, etc.

        This method wraps an Azure operation and measures the time it takes to complete, along with recording
        the size of the object if applicable. It handles errors like timeouts and client errors and ensures
        proper logging of duration and object size.

        :param func: The function that performs the actual GCS operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param container: The name of the Azure container involved in the operation.
        :param blob: The name of the blob within the Azure container.
        :param put_object_size: The size of the object being uploaded, if applicable (for PUT operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for GET operations).

        :return The result of the GCS operation, typically the return value of the `func` callable.
        """
        start_time = time.time()
        status_code = 200

        object_size = None
        if operation == "PUT":
            object_size = put_object_size
        elif operation == "GET" and get_object_size:
            object_size = get_object_size

        try:
            result = func()
            if operation == "GET" and object_size is None:
                object_size = len(result)
            return result
        except ResourceNotFoundError:
            status_code = 404
            raise FileNotFoundError(f"Object {container}/{blob} does not exist.")  # pylint: disable=raise-missing-from
        except Exception as error:
            status_code = -1
            raise RuntimeError(f"Failed to {operation} object(s) at {container}/{blob}") from error
        finally:
            elapsed_time = time.time() - start_time
            self._metric_helper.record_duration(
                elapsed_time, provider=PROVIDER, operation=operation, bucket=container, status_code=status_code
            )
            if object_size:
                self._metric_helper.record_object_size(
                    object_size, provider=PROVIDER, operation=operation, bucket=container, status_code=status_code
                )

    def _put_object(self, path: str, body: bytes) -> None:
        container_name, blob_name = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> None:
            blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.upload_blob(body, overwrite=True)

        return self._collect_metrics(_invoke_api, operation="PUT", container=container_name, blob=blob_name)

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        container_name, blob_name = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> bytes:
            blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            if byte_range:
                stream = blob_client.download_blob(offset=byte_range.offset, length=byte_range.size)
            else:
                stream = blob_client.download_blob()
            return stream.readall()

        return self._collect_metrics(_invoke_api, operation="GET", container=container_name, blob=blob_name)

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        src_container, src_blob = split_path(src_path)
        dest_container, dest_blob = split_path(dest_path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> None:
            src_blob_client = self._blob_service_client.get_blob_client(container=src_container, blob=src_blob)
            dest_blob_client = self._blob_service_client.get_blob_client(container=dest_container, blob=dest_blob)
            dest_blob_client.start_copy_from_url(src_blob_client.url)

        src_object = self._get_object_metadata(src_path)

        return self._collect_metrics(
            _invoke_api,
            operation="COPY",
            container=src_container,
            blob=src_blob,
            put_object_size=src_object.content_length,
        )

    def _delete_object(self, path: str) -> None:
        container_name, blob_name = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> None:
            blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_client.delete_blob()

        return self._collect_metrics(_invoke_api, operation="DELETE", container=container_name, blob=blob_name)

    def _is_dir(self, path: str) -> bool:
        # Ensure the path ends with '/' to mimic a directory
        path = self._append_delimiter(path)

        container_name, prefix = split_path(path)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> bool:
            # List objects with the given prefix
            container_client = self._blob_service_client.get_container_client(container=container_name)
            blobs = container_client.walk_blobs(name_starts_with=prefix, delimiter="/")
            # Check if there are any contents or common prefixes
            return any(True for _ in blobs)

        return self._collect_metrics(_invoke_api, operation="LIST", container=container_name, blob=prefix)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        if path.endswith("/"):
            # If path is a "directory", then metadata is not guaranteed to exist if
            # it is a "virtual prefix" that was never explicitly created.
            if self._is_dir(path):
                return ObjectMetadata(
                    key=self._append_delimiter(path),
                    type="directory",
                    content_length=0,
                    last_modified=datetime.min,
                )
            else:
                raise FileNotFoundError(f"Directory {path} does not exist.")
        else:
            container_name, blob_name = split_path(path)
            self._refresh_blob_service_client_if_needed()

            def _invoke_api() -> ObjectMetadata:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                properties = blob_client.get_blob_properties()
                return ObjectMetadata(
                    key=path,
                    content_length=properties.size,
                    content_type=properties.content_settings.content_type,
                    last_modified=properties.last_modified,
                    etag=properties.etag.strip('"') if properties.etag else "",
                )

            try:
                return self._collect_metrics(_invoke_api, operation="HEAD", container=container_name, blob=blob_name)
            except FileNotFoundError as error:
                if strict:
                    # If the object does not exist on the given path, we will append a trailing slash and
                    # check if the path is a directory.
                    path = self._append_delimiter(path)
                    if self._is_dir(path):
                        return ObjectMetadata(
                            key=path,
                            type="directory",
                            content_length=0,
                            last_modified=datetime.min,
                        )
                raise error

    def _list_objects(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        container_name, prefix = split_path(prefix)
        self._refresh_blob_service_client_if_needed()

        def _invoke_api() -> Iterator[ObjectMetadata]:
            container_client = self._blob_service_client.get_container_client(container=container_name)
            # Azure has no start key option like other object stores.
            if include_directories:
                blobs = container_client.walk_blobs(name_starts_with=prefix, delimiter="/")
            else:
                blobs = container_client.list_blobs(name_starts_with=prefix)
            # Azure guarantees lexicographical order.
            for blob in blobs:
                if isinstance(blob, BlobPrefix):
                    yield ObjectMetadata(
                        key=blob.name.rstrip("/"),
                        type="directory",
                        content_length=0,
                        last_modified=datetime.min,
                    )
                else:
                    key = blob.name
                    if (start_after is None or start_after < key) and (end_at is None or key <= end_at):
                        yield ObjectMetadata(
                            key=key,
                            content_length=blob.size,
                            content_type=blob.content_settings.content_type,
                            last_modified=blob.last_modified,
                            etag=blob.etag.strip('"') if blob.etag else "",
                        )
                    elif end_at is not None and end_at < key:
                        return

        return self._collect_metrics(_invoke_api, operation="LIST", container=container_name, blob=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        container_name, blob_name = split_path(remote_path)
        self._refresh_blob_service_client_if_needed()

        if isinstance(f, str):
            file_size = os.path.getsize(f)

            def _invoke_api() -> None:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                with open(f, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

            return self._collect_metrics(
                _invoke_api, operation="PUT", container=container_name, blob=blob_name, put_object_size=file_size
            )
        else:
            # Convert StringIO to BytesIO before upload
            if isinstance(f, io.StringIO):
                fp: IO = io.BytesIO(f.getvalue().encode("utf-8"))  # type: ignore
            else:
                fp = f

            fp.seek(0, io.SEEK_END)
            file_size = fp.tell()
            fp.seek(0)

            def _invoke_api() -> None:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                blob_client.upload_blob(fp, overwrite=True)

            return self._collect_metrics(
                _invoke_api, operation="PUT", container=container_name, blob=blob_name, put_object_size=file_size
            )

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        if not metadata:
            metadata = self._get_object_metadata(remote_path)

        container_name, blob_name = split_path(remote_path)
        self._refresh_blob_service_client_if_needed()

        if isinstance(f, str):
            os.makedirs(os.path.dirname(f), exist_ok=True)

            def _invoke_api() -> None:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    stream = blob_client.download_blob()
                    fp.write(stream.readall())
                os.rename(src=temp_file_path, dst=f)

            return self._collect_metrics(
                _invoke_api,
                operation="GET",
                container=container_name,
                blob=blob_name,
                get_object_size=metadata.content_length,
            )
        else:

            def _invoke_api() -> None:
                blob_client = self._blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                stream = blob_client.download_blob()
                if isinstance(f, io.StringIO):
                    f.write(stream.readall().decode("utf-8"))
                else:
                    f.write(stream.readall())

            return self._collect_metrics(
                _invoke_api,
                operation="GET",
                container=container_name,
                blob=blob_name,
                get_object_size=metadata.content_length,
            )
