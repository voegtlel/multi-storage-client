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

import boto3
from boto3.s3.transfer import TransferConfig
import botocore
from botocore.credentials import RefreshableCredentials
from botocore.exceptions import (
    ClientError,
    ReadTimeoutError,
    IncompleteReadError,
    ResponseStreamingError,
)
from botocore.session import get_session

from ..types import (
    Credentials,
    CredentialsProvider,
    ObjectMetadata,
    Range,
    RetryableError,
)
from ..utils import split_path
from .base import BaseStorageProvider

BOTO3_MAX_POOL_CONNECTIONS = 32
BOTO3_CONNECT_TIMEOUT = 10
BOTO3_READ_TIMEOUT = 10

MB = 1024 * 1024

MULTIPART_THRESHOLD = 512 * MB
MULTIPART_CHUNK_SIZE = 256 * MB
IO_CHUNK_SIZE = 128 * MB
MAX_CONCURRENCY = 16
PROVIDER = "s3"


class StaticS3CredentialsProvider(CredentialsProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.CredentialsProvider` that provides static S3 credentials.
    """

    _access_key: str
    _secret_key: str
    _session_token: Optional[str]

    def __init__(self, access_key: str, secret_key: str, session_token: Optional[str] = None):
        """
        Initializes the :py:class:`StaticS3CredentialsProvider` with the provided access key, secret key, and optional
        session token.

        :param access_key: The access key for S3 authentication.
        :param secret_key: The secret key for S3 authentication.
        :param session_token: An optional session token for temporary credentials.
        """
        self._access_key = access_key
        self._secret_key = secret_key
        self._session_token = session_token

    def get_credentials(self) -> Credentials:
        return Credentials(
            access_key=self._access_key,
            secret_key=self._secret_key,
            token=self._session_token,
            expiration=None,
        )

    def refresh_credentials(self) -> None:
        pass


class S3StorageProvider(BaseStorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with Amazon S3 or S3-compatible object stores.
    """

    def __init__(
        self,
        region_name: str = "",
        endpoint_url: str = "",
        base_path: str = "",
        credentials_provider: Optional[CredentialsProvider] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initializes the :py:class:`S3StorageProvider` with the region, endpoint URL, and optional credentials provider.

        :param region_name: The AWS region where the S3 bucket is located.
        :param endpoint_url: The custom endpoint URL for the S3 service.
        :param base_path: The root prefix path within the S3 bucket where all operations will be scoped.
        :param credentials_provider: The provider to retrieve S3 credentials.
        """
        super().__init__(base_path=base_path, provider_name=PROVIDER)

        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._credentials_provider = credentials_provider
        self._signature_version = kwargs.get("signature_version", "")
        self._s3_client = self._create_s3_client(
            request_checksum_calculation=kwargs.get("request_checksum_calculation"),
            response_checksum_validation=kwargs.get("response_checksum_validation"),
            max_pool_connections=kwargs.get("max_pool_connections", BOTO3_MAX_POOL_CONNECTIONS),
            connect_timeout=kwargs.get("connect_timeout", BOTO3_CONNECT_TIMEOUT),
            read_timeout=kwargs.get("read_timeout", BOTO3_READ_TIMEOUT),
        )
        self._transfer_config = TransferConfig(
            multipart_threshold=int(kwargs.get("multipart_threshold", MULTIPART_THRESHOLD)),
            max_concurrency=int(kwargs.get("max_concurrency", MAX_CONCURRENCY)),
            multipart_chunksize=int(kwargs.get("multipart_chunksize", MULTIPART_CHUNK_SIZE)),
            io_chunksize=int(kwargs.get("io_chunk_size", IO_CHUNK_SIZE)),
            use_threads=True,
        )

    def _create_s3_client(
        self,
        request_checksum_calculation: Optional[str] = None,
        response_checksum_validation: Optional[str] = None,
        max_pool_connections: int = BOTO3_MAX_POOL_CONNECTIONS,
        connect_timeout: int = BOTO3_CONNECT_TIMEOUT,
        read_timeout: int = BOTO3_READ_TIMEOUT,
    ):
        """
        Creates and configures the boto3 S3 client, using refreshable credentials if possible.

        :param request_checksum_calculation: When the underlying S3 client should calculate request checksums. See the equivalent option in the `AWS configuration file <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file>`_.
        :param response_checksum_validation: When the underlying S3 client should validate response checksums. See the equivalent option in the `AWS configuration file <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-a-configuration-file>`_.
        :return The configured S3 client.
        """
        options = {
            "region_name": self._region_name,
            "config": boto3.session.Config(  # pyright: ignore [reportAttributeAccessIssue]
                max_pool_connections=max_pool_connections,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                retries=dict(mode="standard"),
                request_checksum_calculation=request_checksum_calculation,
                response_checksum_validation=response_checksum_validation,
            ),
        }
        if self._endpoint_url:
            options["endpoint_url"] = self._endpoint_url

        if self._credentials_provider:
            creds = self._fetch_credentials()
            if "expiry_time" in creds and creds["expiry_time"]:
                # Use RefreshableCredentials if expiry_time provided.
                refreshable_credentials = RefreshableCredentials.create_from_metadata(
                    metadata=creds, refresh_using=self._fetch_credentials, method="custom-refresh"
                )

                botocore_session = get_session()
                botocore_session._credentials = refreshable_credentials

                boto3_session = boto3.Session(botocore_session=botocore_session)

                return boto3_session.client("s3", **options)
            else:
                # Add static credentials to the options dictionary
                options["aws_access_key_id"] = creds["access_key"]
                options["aws_secret_access_key"] = creds["secret_key"]
                if creds["token"]:
                    options["aws_session_token"] = creds["token"]

        if self._signature_version:
            signature_config = botocore.config.Config(  # pyright: ignore[reportAttributeAccessIssue]
                signature_version=botocore.UNSIGNED
                if self._signature_version == "UNSIGNED"
                else self._signature_version
            )
            options["config"] = options["config"].merge(signature_config)

        # Fallback to standard credential chain.
        return boto3.client("s3", **options)

    def _fetch_credentials(self) -> dict:
        """
        Refreshes the S3 client if the current credentials are expired.
        """
        if not self._credentials_provider:
            raise RuntimeError("Cannot fetch credentials if no credential provider configured.")
        self._credentials_provider.refresh_credentials()
        credentials = self._credentials_provider.get_credentials()
        return {
            "access_key": credentials.access_key,
            "secret_key": credentials.secret_key,
            "token": credentials.token,
            "expiry_time": credentials.expiration,
        }

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
        Collects and records performance metrics around S3 operations such as PUT, GET, DELETE, etc.

        This method wraps an S3 operation and measures the time it takes to complete, along with recording
        the size of the object if applicable. It handles errors like timeouts and client errors and ensures
        proper logging of duration and object size.

        :param func: The function that performs the actual S3 operation.
        :param operation: The type of operation being performed (e.g., "PUT", "GET", "DELETE").
        :param bucket: The name of the S3 bucket involved in the operation.
        :param key: The key of the object within the S3 bucket.
        :param put_object_size: The size of the object being uploaded, if applicable (for PUT operations).
        :param get_object_size: The size of the object being downloaded, if applicable (for GET operations).

        :return: The result of the S3 operation, typically the return value of the `func` callable.
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
        except ClientError as error:
            status_code = error.response["ResponseMetadata"]["HTTPStatusCode"]
            request_id = error.response["ResponseMetadata"].get("RequestId")
            host_id = error.response["ResponseMetadata"].get("HostId")

            request_info = f"request_id: {request_id}, host_id: {host_id}, status_code: {status_code}"

            if status_code == 404:
                raise FileNotFoundError(f"Object {bucket}/{key} does not exist. {request_info}")  # pylint: disable=raise-missing-from
            elif status_code == 429:
                raise RetryableError(
                    f"Too many request to {operation} object(s) at {bucket}/{key}. {request_info}"
                ) from error
            elif status_code == 503:
                raise RetryableError(
                    f"Service unavailable when {operation} object(s) at {bucket}/{key}. {request_info}"
                ) from error
            else:
                raise RuntimeError(
                    f"Failed to {operation} object(s) at {bucket}/{key}. {request_info}, "
                    f"error_type: {type(error).__name__}"
                ) from error
        except FileNotFoundError as error:
            status_code = -1
            raise error
        except (ReadTimeoutError, IncompleteReadError, ResponseStreamingError) as error:
            status_code = -1
            raise RetryableError(
                f"Failed to {operation} object(s) at {bucket}/{key} due to network timeout or incomplete read. "
                f"error_type: {type(error).__name__}"
            ) from error
        except Exception as error:
            status_code = -1
            raise RuntimeError(
                f"Failed to {operation} object(s) at {bucket}/{key}. error type: {type(error).__name__}"
            ) from error
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
            self._s3_client.put_object(Bucket=bucket, Key=key, Body=body)

        return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=len(body))

    def _get_object(self, path: str, byte_range: Optional[Range] = None) -> bytes:
        bucket, key = split_path(path)

        def _invoke_api() -> bytes:
            if byte_range:
                bytes_range = f"bytes={byte_range.offset}-{byte_range.offset + byte_range.size - 1}"
                response = self._s3_client.get_object(Bucket=bucket, Key=key, Range=bytes_range)
            else:
                response = self._s3_client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()

        return self._collect_metrics(_invoke_api, operation="GET", bucket=bucket, key=key)

    def _copy_object(self, src_path: str, dest_path: str) -> None:
        src_bucket, src_key = split_path(src_path)
        dest_bucket, dest_key = split_path(dest_path)

        def _invoke_api() -> None:
            self._s3_client.copy_object(
                CopySource={"Bucket": src_bucket, "Key": src_key}, Bucket=dest_bucket, Key=dest_key
            )

        src_object = self._get_object_metadata(src_path)

        return self._collect_metrics(
            _invoke_api,
            operation="COPY",
            bucket=dest_bucket,
            key=dest_key,
            put_object_size=src_object.content_length,
        )

    def _delete_object(self, path: str) -> None:
        bucket, key = split_path(path)

        def _invoke_api() -> None:
            self._s3_client.delete_object(Bucket=bucket, Key=key)

        return self._collect_metrics(_invoke_api, operation="DELETE", bucket=bucket, key=key)

    def _is_dir(self, path: str) -> bool:
        # Ensure the path ends with '/' to mimic a directory
        path = self._append_delimiter(path)

        bucket, key = split_path(path)

        def _invoke_api() -> bool:
            # List objects with the given prefix
            response = self._s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1, Delimiter="/")
            # Check if there are any contents or common prefixes
            return bool(response.get("Contents", []) or response.get("CommonPrefixes", []))

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

            def _invoke_api() -> ObjectMetadata:
                response = self._s3_client.head_object(Bucket=bucket, Key=key)
                return ObjectMetadata(
                    key=path,
                    type="file",
                    content_length=response["ContentLength"],
                    content_type=response["ContentType"],
                    last_modified=response["LastModified"],
                    etag=response["ETag"].strip('"'),
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

        def _invoke_api() -> Iterator[ObjectMetadata]:
            paginator = self._s3_client.get_paginator("list_objects_v2")
            if include_directories:
                page_iterator = paginator.paginate(
                    Bucket=bucket, Prefix=prefix, Delimiter="/", StartAfter=(start_after or "")
                )
            else:
                page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, StartAfter=(start_after or ""))

            for page in page_iterator:
                for item in page.get("CommonPrefixes", []):
                    yield ObjectMetadata(
                        key=item["Prefix"].rstrip("/"),
                        type="directory",
                        content_length=0,
                        last_modified=datetime.min,
                    )

                # S3 guarantees lexicographical order for general purpose buckets (for
                # normal S3) but not directory buckets (for S3 Express One Zone).
                for response_object in page.get("Contents", []):
                    key = response_object["Key"]
                    if end_at is None or key <= end_at:
                        yield ObjectMetadata(
                            key=key,
                            type="file",
                            content_length=response_object["Size"],
                            last_modified=response_object["LastModified"],
                            etag=response_object["ETag"].strip('"'),
                        )
                    else:
                        return

        return self._collect_metrics(_invoke_api, operation="LIST", bucket=bucket, key=prefix)

    def _upload_file(self, remote_path: str, f: Union[str, IO]) -> None:
        if isinstance(f, str):
            filesize = os.path.getsize(f)

            # Upload small files
            if filesize <= self._transfer_config.multipart_threshold:
                with open(f, "rb") as fp:
                    self._put_object(remote_path, fp.read())
                return

            # Upload large files using TransferConfig
            bucket, key = split_path(remote_path)

            def _invoke_api() -> None:
                self._s3_client.upload_file(
                    Filename=f,
                    Bucket=bucket,
                    Key=key,
                    Config=self._transfer_config,
                )

            return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=filesize)
        else:
            # Upload small files
            f.seek(0, io.SEEK_END)
            filesize = f.tell()
            f.seek(0)

            if filesize <= self._transfer_config.multipart_threshold:
                if isinstance(f, io.StringIO):
                    self._put_object(remote_path, f.read().encode("utf-8"))
                else:
                    self._put_object(remote_path, f.read())
                return

            # Upload large files using TransferConfig
            bucket, key = split_path(remote_path)

            def _invoke_api() -> None:
                self._s3_client.upload_fileobj(
                    Fileobj=f,
                    Bucket=bucket,
                    Key=key,
                    Config=self._transfer_config,
                )

            return self._collect_metrics(_invoke_api, operation="PUT", bucket=bucket, key=key, put_object_size=filesize)

    def _download_file(self, remote_path: str, f: Union[str, IO], metadata: Optional[ObjectMetadata] = None) -> None:
        if not metadata:
            metadata = self._get_object_metadata(remote_path)

        if isinstance(f, str):
            os.makedirs(os.path.dirname(f), exist_ok=True)
            # Download small files
            if metadata.content_length <= self._transfer_config.multipart_threshold:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    fp.write(self._get_object(remote_path))
                os.rename(src=temp_file_path, dst=f)
                return

            # Download large files using TransferConfig
            bucket, key = split_path(remote_path)

            def _invoke_api() -> None:
                with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=os.path.dirname(f), prefix=".") as fp:
                    temp_file_path = fp.name
                    self._s3_client.download_fileobj(
                        Bucket=bucket,
                        Key=key,
                        Fileobj=fp,
                        Config=self._transfer_config,
                    )
                os.rename(src=temp_file_path, dst=f)

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
        else:
            # Download small files
            if metadata.content_length <= self._transfer_config.multipart_threshold:
                if isinstance(f, io.StringIO):
                    f.write(self._get_object(remote_path).decode("utf-8"))
                else:
                    f.write(self._get_object(remote_path))
                return

            # Download large files using TransferConfig
            bucket, key = split_path(remote_path)

            def _invoke_api() -> None:
                self._s3_client.download_fileobj(
                    Bucket=bucket,
                    Key=key,
                    Fileobj=f,
                    Config=self._transfer_config,
                )

            return self._collect_metrics(
                _invoke_api, operation="GET", bucket=bucket, key=key, get_object_size=metadata.content_length
            )
