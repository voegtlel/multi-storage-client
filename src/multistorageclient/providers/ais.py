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
import time
from datetime import datetime
from typing import IO, Any, Callable, Iterator, Optional, Tuple, Union

from aistore.sdk import Client
from aistore.sdk.authn import AuthNClient
from aistore.sdk.errors import AISError
from requests.exceptions import HTTPError

from ..types import (
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    Range,
)
from ..utils import split_path
from .base import BaseStorageProvider

PROVIDER = "ais"


class StaticAISCredentialProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that provides static S3 credentials.
    """

    _username: Optional[str]
    _password: Optional[str]
    _authn_endpoint: Optional[str]
    _token: Optional[str]
    _skip_verify: bool
    _ca_cert: Optional[str]

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        authn_endpoint: Optional[str] = None,
        token: Optional[str] = None,
        skip_verify: bool = True,
        ca_cert: Optional[str] = None,
    ):
        """
        Initializes the :py:class:`StaticAISCredentialProvider` with the given credentials.

        :param username: The username for the AIStore authentication.
        :param password: The password for the AIStore authentication.
        :param authn_endpoint: The AIStore authentication endpoint.
        :param token: The AIStore authentication token. This is used for authentication if username,
                        password and authn_endpoint are not provided.
        :param skip_verify: If true, skip SSL certificate verification.
        :param ca_cert: Path to a CA certificate file for SSL verification.

        """
        self._username = username
        self._password = password
        self._authn_endpoint = authn_endpoint
        self._token = token
        self._skip_verify = skip_verify
        self._ca_cert = ca_cert

    def get_credentials(self) -> Credentials:
        if self._username and self._password and self._authn_endpoint:
            authn_client = AuthNClient(self._authn_endpoint, self._skip_verify, self._ca_cert)
            self._token = authn_client.login(self._username, self._password)
        return Credentials(token=self._token, access_key="", secret_key="", expiration=None)

    def refresh_credentials(self) -> None:
        pass


class AIStoreStorageProvider(BaseStorageProvider):
    def __init__(
        self,
        endpoint: str,
        provider: str = PROVIDER,
        skip_verify: bool = True,
        ca_cert: Optional[str] = None,
        timeout: Optional[Union[float, Tuple[float, float]]] = None,
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        **kwargs: Any,
    ) -> None:
        """
        AIStore client for managing buckets, objects, and ETL jobs.

        :param endpoint: The AIStore endpoint.
        :param skip_verify: Whether to skip SSL certificate verification.
        :param ca_cert: Path to a CA certificate file for SSL verification.
        :param timeout: Request timeout in seconds; a single float
            for both connect/read timeouts (e.g., ``5.0``), a tuple for separate connect/read
            timeouts (e.g., ``(3.0, 10.0)``), or ``None`` to disable timeout.
        :param token: Authorization token. If not provided, the ``AIS_AUTHN_TOKEN`` environment variable will be used.
        :param base_path: The root prefix path within the bucket where all operations will be scoped.
        """
        super().__init__(base_path=base_path, provider_name=PROVIDER)

        token = None
        if credentials_provider:
            token = credentials_provider.get_credentials().token
            self.client = Client(
                endpoint=endpoint, skip_verify=skip_verify, ca_cert=ca_cert, timeout=timeout, token=token
            )
        else:
            self.client = Client(endpoint=endpoint)
        self.provider = provider

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
        Collects and records performance metrics around object storage operations
        such as ``PUT``, ``GET``, ``DELETE``, etc.

        This method wraps an object storage operation and measures the time it takes to complete, along with recording
        the size of the object if applicable. It handles errors like timeouts and client errors and ensures
        proper logging of duration and object size.

        :param func: The function that performs the actual object storage operation.
        :param operation: The type of operation being performed (e.g., ``PUT``, ``GET``, ``DELETE``).
        :param bucket: The name of the object storage bucket involved in the operation.
        :param key: The key of the object within the object storage bucket.
        :param put_object_size: The size of the object being uploaded, if applicable (for ``PUT`` operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for ``GET`` operations).

        :return: The result of the object storage operation, typically the return value of the `func` callable.
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
        except AISError as error:
            status_code = error.status_code
            raise RuntimeError(f"Failed to {operation} object(s) at {bucket}/{key}") from error
        except HTTPError as error:
            status_code = error.response.status_code
            if status_code == 404:
                raise FileNotFoundError(f"Object {bucket}/{key} does not exist.")  # pylint: disable=raise-missing-from
            else:
                raise RuntimeError(f"Failed to {operation} object(s) at {bucket}/{key}") from error
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

        def _invoke_api() -> None:
            obj = self.client.bucket(bucket, self.provider).object(obj_name=key)
            obj.put_content(body)

        return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=len(body))

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        bucket, key = split_path(path)
        if byte_range:
            bytes_range = f"bytes={byte_range.offset}-{byte_range.offset + byte_range.size - 1}"
        else:
            bytes_range = None

        def _invoke_api() -> bytes:
            obj = self.client.bucket(bucket, self.provider).object(obj_name=key)
            if byte_range:
                reader = obj.get(byte_range=bytes_range)  # pyright: ignore [reportArgumentType]
            else:
                reader = obj.get()
            return reader.read_all()

        return self._collect_metrics(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        raise AttributeError("AIStore does not support copy operations")

    def _delete_object(self, path: str) -> None:
        bucket, key = split_path(path)

        def _invoke_api() -> None:
            obj = self.client.bucket(bucket, self.provider).object(obj_name=key)
            obj.delete()

        return self._collect_metrics(_invoke_api, operation="DELETE", bucket=bucket, key=key)

    def _get_object_metadata(self, path: str, strict: bool = True) -> ObjectMetadata:
        bucket, key = split_path(path)

        def _invoke_api() -> ObjectMetadata:
            obj = self.client.bucket(bck_name=bucket, provider=self.provider).object(obj_name=key)
            props = obj.head()
            return ObjectMetadata(
                key=key,
                content_length=int(props.get("Content-Length")),  # pyright: ignore [reportArgumentType]
                last_modified=datetime.min,
                etag=props.get("Ais-Checksum-Value", None),
            )

        return self._collect_metrics(_invoke_api, operation="HEAD", bucket=bucket, key=key)

    def _list_objects(
        self,
        prefix: str,
        start_after: Optional[str] = None,
        end_at: Optional[str] = None,
        include_directories: bool = False,
    ) -> Iterator[ObjectMetadata]:
        bucket, prefix = split_path(prefix)

        def _invoke_api() -> Iterator[ObjectMetadata]:
            # AIS has no start key option like other object stores.
            all_objects = self.client.bucket(bck_name=bucket, provider=self.provider).list_all_objects_iter(
                prefix=prefix, props="name,size,atime,checksum,cone"
            )

            # Assume AIS guarantees lexicographical order.
            for obj in all_objects:
                key = obj.name
                if (start_after is None or start_after < key) and (end_at is None or key <= end_at):
                    yield ObjectMetadata(
                        key=key,
                        content_length=int(obj.props.size),
                        last_modified=datetime.min,
                        etag=obj.props.checksum_value,
                    )
                elif end_at is not None and end_at < key:
                    return

        return self._collect_metrics(_invoke_api, operation="LIST", bucket=bucket, key=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        if isinstance(f, str):
            with open(f, "rb") as fp:
                self._put_object(remote_path, fp.read())
        else:
            if isinstance(f, io.StringIO):
                self._put_object(remote_path, f.read().encode("utf-8"))
            else:
                self._put_object(remote_path, f.read())

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        if not metadata:
            metadata = self._get_object_metadata(remote_path)

        if isinstance(f, str):
            os.makedirs(os.path.dirname(f), exist_ok=True)
            with open(f, "wb") as fp:
                fp.write(self._get_object(remote_path))
        else:
            if isinstance(f, io.StringIO):
                f.write(self._get_object(remote_path).decode("utf-8"))
            else:
                f.write(self._get_object(remote_path))
