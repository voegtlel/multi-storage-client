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

from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.oauth2.credentials import Credentials as GoogleCredentials

from ..types import CredentialsProvider, ObjectMetadata, Range
from ..utils import split_path
from .base import BaseStorageProvider

PROVIDER = "gcs"


class GoogleStorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with Google Cloud Storage.
    """

    def __init__(
        self,
        project_id: str,
        endpoint_url: str = "",
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
    ):
        """
        Initializes the :py:class:`GoogleStorageProvider` with the project ID and optional credentials provider.

        :param project_id: The Google Cloud project ID.
        :param endpoint_url: The custom endpoint URL for the GCS service.
        :param base_path: The root prefix path within the bucket where all operations will be scoped.
        :param credentials_provider: The provider to retrieve GCS credentials.
        """
        super().__init__(base_path=base_path, provider_name=PROVIDER)

        self._project_id = project_id
        self._endpoint_url = endpoint_url
        self._credentials_provider = credentials_provider
        self._gcs_client = self._create_gcs_client()

    def _create_gcs_client(self) -> storage.Client:
        client_options = {}
        if self._endpoint_url:
            client_options["api_endpoint"] = self._endpoint_url

        if self._credentials_provider:
            access_token = self._credentials_provider.get_credentials().token
            creds = GoogleCredentials(token=access_token)
            return storage.Client(project=self._project_id, credentials=creds, client_options=client_options)
        else:
            return storage.Client(project=self._project_id, client_options=client_options)

    def _refresh_gcs_client_if_needed(self) -> None:
        """
        Refreshes the GCS client if the current credentials are expired.
        """
        if self._credentials_provider:
            credentials = self._credentials_provider.get_credentials()
            if credentials.is_expired():
                self._credentials_provider.refresh_credentials()
                self._gcs_client = self._create_gcs_client()

    def _collect_metrics(
        self,
        func: Callable,
        operation: str,
        bucket: str,
        key: str,
        put_object_size: Optional[int] = None,
        get_object_size: Optional[int] = None,
    ) -> Any:
        """
        Collects and records performance metrics around GCS operations such as PUT, GET, DELETE, etc.

        This method wraps an GCS operation and measures the time it takes to complete, along with recording
        the size of the object if applicable. It handles errors like timeouts and client errors and ensures
        proper logging of duration and object size.

        :param func: The function that performs the actual GCS operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param bucket: The name of the GCS bucket involved in the operation.
        :param key: The key of the object within the GCS bucket.
        :param put_object_size: The size of the object being uploaded, if applicable (for PUT operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for GET operations).

        :return: The result of the GCS operation, typically the return value of the `func` callable.
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
        except NotFound:
            status_code = 404
            raise FileNotFoundError(f"Object {bucket}/{key} does not exist.")  # pylint: disable=raise-missing-from
        except Exception as error:
            status_code = -1
            raise RuntimeError(f"Failed to {operation} object(s) at {bucket}/{key}") from error
        finally:
            elapsed_time = time.time() - start_time
            self._metric_helper.record_duration(
                elapsed_time, provider=PROVIDER, operation=operation, bucket=bucket, status_code=status_code
            )
            if object_size:
                self._metric_helper.record_object_size(
                    object_size, provider=PROVIDER, operation=operation, bucket=bucket, status_code=status_code
                )

    def _put_object(self, path: str, body: bytes) -> None:
        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> None:
            bucket_obj = self._gcs_client.bucket(bucket)
            blob = bucket_obj.blob(key)
            blob.upload_from_string(body)

        return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=len(body))

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> bytes:
            bucket_obj = self._gcs_client.bucket(bucket)
            blob = bucket_obj.blob(key)
            if byte_range:
                return blob.download_as_bytes(start=byte_range.offset, end=byte_range.offset + byte_range.size - 1)
            return blob.download_as_bytes()

        return self._collect_metrics(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        src_bucket, src_key = split_path(src_path)
        dest_bucket, dest_key = split_path(dest_path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> None:
            source_bucket_obj = self._gcs_client.bucket(src_bucket)
            source_blob = source_bucket_obj.blob(src_key)

            destination_bucket_obj = self._gcs_client.bucket(dest_bucket)
            destination_blob = destination_bucket_obj.blob(dest_key)

            rewrite_tokens = [None]
            while len(rewrite_tokens) > 0:
                rewrite_token = rewrite_tokens.pop()
                next_rewrite_token, _, _ = destination_blob.rewrite(source=source_blob, token=rewrite_token)
                if next_rewrite_token is not None:
                    rewrite_tokens.append(next_rewrite_token)

        src_object = self._get_object_metadata(src_path)

        return self._collect_metrics(
            _invoke_api,
            operation="COPY",
            bucket=src_bucket,
            key=src_key,
            put_object_size=src_object.content_length,
        )

    def _delete_object(self, path: str) -> None:
        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> None:
            bucket_obj = self._gcs_client.bucket(bucket)
            blob = bucket_obj.blob(key)
            blob.delete()

        return self._collect_metrics(_invoke_api, operation="DELETE", bucket=bucket, key=key)

    def _is_dir(self, path: str) -> bool:
        # Ensure the path ends with '/' to mimic a directory
        path = self._append_delimiter(path)

        bucket, key = split_path(path)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> bool:
            bucket_obj = self._gcs_client.bucket(bucket)
            # List objects with the given prefix
            blobs = bucket_obj.list_blobs(
                prefix=key,
                delimiter="/",
            )
            # Check if there are any contents or common prefixes
            return any(True for _ in blobs) or any(True for _ in blobs.prefixes)

        return self._collect_metrics(_invoke_api, operation="LIST", bucket=bucket, key=key)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        if path.endswith("/"):
            # If path is a "directory", then metadata is not guaranteed to exist if
            # it is a "virtual prefix" that was never explicitly created.
            if self._is_dir(path):
                return ObjectMetadata(
                    key=path,
                    type="directory",
                    content_length=0,
                    last_modified=datetime.min,
                )
            else:
                raise FileNotFoundError(f"Directory {path} does not exist.")
        else:
            bucket, key = split_path(path)
            self._refresh_gcs_client_if_needed()

            def _invoke_api() -> ObjectMetadata:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.get_blob(key)
                if not blob:
                    raise NotFound(f"Blob {key} not found in bucket {bucket}")
                return ObjectMetadata(
                    key=path,
                    content_length=blob.size or 0,
                    content_type=blob.content_type,
                    last_modified=blob.updated or datetime.min,
                    etag=blob.etag,
                )

            try:
                return self._collect_metrics(_invoke_api, operation="HEAD", bucket=bucket, key=key)
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
        bucket, prefix = split_path(prefix)
        self._refresh_gcs_client_if_needed()

        def _invoke_api() -> Iterator[ObjectMetadata]:
            bucket_obj = self._gcs_client.bucket(bucket)
            if include_directories:
                blobs = bucket_obj.list_blobs(
                    prefix=prefix,
                    # This is ≥ instead of >.
                    start_offset=start_after,
                    delimiter="/",
                )
            else:
                blobs = bucket_obj.list_blobs(
                    prefix=prefix,
                    # This is ≥ instead of >.
                    start_offset=start_after,
                )

            # GCS guarantees lexicographical order.
            for blob in blobs:
                key = blob.name
                if (start_after is None or start_after < key) and (end_at is None or key <= end_at):
                    yield ObjectMetadata(
                        key=key,
                        content_length=blob.size,
                        content_type=blob.content_type,
                        last_modified=blob.updated,
                        etag=blob.etag,
                    )
                elif start_after != key:
                    return

            # The directories must be accessed last.
            if include_directories:
                for directory in blobs.prefixes:
                    yield ObjectMetadata(
                        key=directory.rstrip("/"),
                        type="directory",
                        content_length=0,
                        last_modified=datetime.min,
                    )

        return self._collect_metrics(_invoke_api, operation="LIST", bucket=bucket, key=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        bucket, key = split_path(remote_path)
        self._refresh_gcs_client_if_needed()

        if isinstance(f, str):
            filesize = os.path.getsize(f)

            def _invoke_api() -> None:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.blob(key)
                blob.upload_from_filename(f)

            return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=filesize)
        else:
            f.seek(0, io.SEEK_END)
            filesize = f.tell()
            f.seek(0)

            def _invoke_api() -> None:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.blob(key)
                blob.upload_from_string(f.read())

            return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=filesize)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        self._refresh_gcs_client_if_needed()

        if not metadata:
            metadata = self._get_object_metadata(remote_path)

        bucket, key = split_path(remote_path)

        if isinstance(f, str):
            os.makedirs(os.path.dirname(f), exist_ok=True)

            def _invoke_api() -> None:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.blob(key)

                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    blob.download_to_filename(temp_file_path)
                os.rename(src=temp_file_path, dst=f)

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
        else:

            def _invoke_api() -> None:
                bucket_obj = self._gcs_client.bucket(bucket)
                blob = bucket_obj.blob(key)
                if isinstance(f, io.TextIOBase):
                    content = blob.download_as_text()
                    f.write(content)
                else:
                    blob.download_to_file(f)

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
