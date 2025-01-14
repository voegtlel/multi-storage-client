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
import shutil
import tempfile
from typing import Set

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.providers import AIStoreStorageProvider, GoogleStorageProvider, S3StorageProvider

MB = 1024 * 1024


def verify_storage_provider(config: StorageClientConfig) -> None:
    prefix = "files"

    storage_client = StorageClient(config)

    body = b"A" * (64 * MB)
    text = '{"text":"✅ Unicode Test ✅"}'

    # use a smaller file for GCS because emulator does not support large files.
    if isinstance(config.storage_provider, GoogleStorageProvider):
        body = b"A" * 1024

    # cleanup
    for object in storage_client.list(f"{prefix}"):
        storage_client.delete(f"{object.key}")

    # write file
    dirname = f"{prefix}/testdir/"
    filename = f"{dirname}testfile.bin"
    destination_filename = f"{dirname}destination_testfile.bin"
    storage_client.write(filename, body)
    assert len(list(storage_client.list(f"{prefix}"))) == 1

    # is file
    assert storage_client.is_file(filename)
    assert not storage_client.is_file(f"{prefix}")
    assert not storage_client.is_file("not-exist-prefix")

    # glob
    assert len(storage_client.glob("*.py")) == 0
    assert len(storage_client.glob("**/*.bin")) == 1
    assert storage_client.glob("**/*.bin")[0] == filename

    # verify file is written
    assert storage_client.read(filename) == body
    info = storage_client.info(filename)
    assert info is not None
    assert info.content_length == len(body)
    assert info.type == "file"
    assert storage_client.is_file(filename)
    assert not storage_client.is_file(f"{prefix}")

    # TODO: enable this test for all providers, once "type" has been added (currently only on posix, S3)
    # verify directory info, both with and without an ending "/"
    if config.storage_provider is S3StorageProvider:
        info = storage_client.info(dirname)
        assert info is not None
        assert info.key.endswith(dirname)
        assert info.type == "directory"

        info = storage_client.info(dirname.removesuffix("/"))
        assert info is not None
        assert info.key.endswith(dirname)
        assert info.type == "directory"

    # verify directories
    flat_list = list(storage_client.list(f"{prefix}/", include_directories=True))
    assert len(flat_list) == 1
    assert flat_list[0].type == "directory"
    assert flat_list[0].key == dirname.rstrip("/")

    # upload
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(body)
    temp_file.seek(0)
    temp_file.flush()
    storage_client.upload_file(filename, temp_file.name)
    os.unlink(temp_file.name)

    # download
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    storage_client.download_file(filename, temp_file.name)
    assert os.path.getsize(temp_file.name) == len(body)
    os.unlink(temp_file.name)

    # open file
    with storage_client.open(filename, "wb") as fp:
        fp.write(body)

    assert len(list(storage_client.list(f"{prefix}"))) == 1

    with storage_client.open(filename, "rb") as fp:
        content = fp.read()
        assert content == body
        assert isinstance(content, bytes)

    # copy file
    if config.storage_provider is not AIStoreStorageProvider:
        storage_client.copy(filename, destination_filename)
        assert storage_client.read(destination_filename) == body
        storage_client.delete(destination_filename)

    # delete file
    storage_client.delete(filename)
    assert len(list(storage_client.list(f"{prefix}"))) == 0

    # GoogleStorageProvider simulator does not support large file uploads.
    if not isinstance(config.storage_provider, GoogleStorageProvider):
        # large file
        body_large = b"*" * (550 * MB)
        with storage_client.open(filename, "wb") as fp:
            fp.write(body_large)

        assert len(list(storage_client.list(f"{prefix}"))) == 1

        with storage_client.open(filename, "rb") as fp:
            read_size = 128 * MB
            content = fp.read(read_size)
            assert len(content) == read_size

            content += fp.read(read_size)
            assert len(content) == 2 * read_size

            content += fp.read(read_size)
            content += fp.read()
            assert len(content) == len(body_large)
            assert isinstance(content, bytes)

        with storage_client.open(filename, "rb") as fp:
            read_size = 128
            buffer = bytearray(read_size)
            assert read_size == fp.readinto(buffer)
            assert b"*" * read_size == buffer

        # delete file
        storage_client.delete(filename)
        assert len(list(storage_client.list(f"{prefix}"))) == 0

    # unicode file
    filename = f"{prefix}/testfile.txt"
    with storage_client.open(filename, "w") as fp:
        fp.write(text)

    assert len(list(storage_client.list(f"{prefix}"))) == 1

    with storage_client.open(filename, "r") as fp:
        content = fp.read()
        assert content == text
        assert isinstance(content, str)

    # delete file
    storage_client.delete(filename)
    assert len(list(storage_client.list(f"{prefix}"))) == 0

    # append mode
    filename = f"{prefix}/testfile-append.txt"
    with storage_client.open(filename, "a") as fp:
        fp.write(text)
    with storage_client.open(filename, "a") as fp:
        fp.write(text)
    with storage_client.open(filename, "r") as fp:
        assert fp.read() == text * 2
    storage_client.delete(filename)

    filename = f"{prefix}/testfile-append.bin"
    with storage_client.open(filename, "ab") as fp:
        fp.write(body)
    with storage_client.open(filename, "ab") as fp:
        fp.write(body)
    with storage_client.open(filename, "rb") as fp:
        assert fp.read() == body * 2
    storage_client.delete(filename)


def verify_storage_provider_list_segment(config: StorageClientConfig) -> None:
    prefix = "list-segment"
    storage_client = StorageClient(config)

    keys: Set[str] = set()
    # Create some files.
    for i in range(1, 4):
        key = os.path.join(prefix, f"{i}.txt")
        storage_client.write(key, "hello".encode())
        keys.add(key)

    # Range over the files.
    for i in range(1, 4):
        assert {os.path.join(prefix, f"{i}.txt")} == {
            object_metadatum.key
            for object_metadatum in storage_client.list(
                prefix=prefix, start_after=os.path.join(prefix, f"{i - 1}.txt"), end_at=os.path.join(prefix, f"{i}.txt")
            )
        }

    for key in keys:
        storage_client.delete(key)


def test_s3_local():
    """
    Use MinIO for local S3 storage testing.
    """
    import boto3

    bucket_name = "test-bucket-0001"

    minio_access_key = "minioadmin"
    minio_secret_key = "minioadmin"
    endpoint_url = "http://localhost:9000"

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name="us-east-1",
    )

    # Delete all objects in the bucket
    try:
        objects = client.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in objects:
            client.delete_object(Bucket=bucket_name, Key=obj["Key"])
        client.delete_bucket(Bucket=bucket_name)
    except client.exceptions.NoSuchBucket:
        pass

    try:
        client.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Failed to create bucket: {e}")
        pass

    config = StorageClientConfig.from_dict(
        {
            "profiles": {
                "s3-local": {
                    "storage_provider": {
                        "type": "s3",
                        "options": {
                            "endpoint_url": endpoint_url,
                            "region_name": "us-east-1",
                            "base_path": bucket_name,
                        },
                    },
                    "credentials_provider": {
                        "type": "S3Credentials",
                        "options": {
                            "access_key": minio_access_key,
                            "secret_key": minio_secret_key,
                        },
                    },
                }
            }
        },
        profile="s3-local",
    )

    verify_storage_provider(config=config)
    verify_storage_provider_list_segment(config=config)

    client.delete_bucket(Bucket=bucket_name)


def test_azure_local():
    """
    Use Azurite emulator for local Azure storage testing.

    Installation: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite
    """
    from azure.storage.blob import BlobServiceClient

    connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
    client = BlobServiceClient.from_connection_string(connection_string)

    container_name = "test-bucket-0001"

    # Clean the container by deleting all blobs
    container_client = client.get_container_client(container_name)

    try:
        if container_client.exists():
            blobs = container_client.list_blobs()
            for blob in blobs:
                container_client.delete_blob(blob.name)
    except Exception:
        pass

    try:
        client.delete_container(container_name)
    except Exception:
        pass

    # Create the test container
    client.create_container(container_name)

    # Delete files in the cache directory
    cache_dir = os.path.join(tempfile.gettempdir(), ".msc_cache")
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)

    config = StorageClientConfig.from_dict(
        {
            "profiles": {
                "azure-local": {
                    "storage_provider": {
                        "type": "azure",
                        "options": {
                            "endpoint_url": "http://127.0.0.1:10000/devstoreaccount1",
                            "base_path": container_name,
                        },
                    },
                    "credentials_provider": {
                        "type": "AzureCredentials",
                        "options": {
                            "connection": connection_string,
                        },
                    },
                }
            },
            "cache": {
                "location": cache_dir,
                "size_mb": 5000,
            },
        },
        profile="azure-local",
    )

    verify_storage_provider(config=config)
    verify_storage_provider_list_segment(config=config)

    client.delete_container(container_name)


def test_gcs_local():
    # Delete files in the cache directory
    cache_dir = os.path.join(tempfile.gettempdir(), ".msc_cache")
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)

    config = StorageClientConfig.from_dict(
        {
            "profiles": {
                "gcs": {
                    "storage_provider": {
                        "type": "gcs",
                        "options": {"project_id": "local-project-id", "base_path": "files"},
                    }
                }
            }
        },
        profile="gcs",
    )

    verify_storage_provider(config=config)
    verify_storage_provider_list_segment(config=config)
