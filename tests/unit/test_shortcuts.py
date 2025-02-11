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

import json
from concurrent.futures import ThreadPoolExecutor
import mmap
import os
import tempfile
from typing import Tuple

import multistorageclient as msc
import numpy as np
import pytest
from multistorageclient.client import StorageClient
from multistorageclient.file import ObjectFile
from multistorageclient.types import MSC_PROTOCOL

MB = 1024 * 1024


def test_resolve_storage_client(file_storage_config):
    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client(f"{MSC_PROTOCOL}fake/bucket/testfile.bin")

    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client("http://fake/bucket/testfile.bin")

    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client("relative/to/current/path")

    # Verify the three ways to access local filesystem are the same
    sc1, _ = msc.resolve_storage_client("/usr/local/fake/bucket/testfile.bin")
    sc2, _ = msc.resolve_storage_client("file:///usr/local/fake/bucket/testfile.bin")
    sc3, _ = msc.resolve_storage_client("msc://default/usr/local/fake/bucket/testfile.bin")
    assert sc1 == sc2 == sc3

    # Multithreading test to verify the storage_client instance is the same
    def storage_client_thread(number: int) -> Tuple[StorageClient, str]:
        tempdir = tempfile.mkdtemp()
        storage_client, path = msc.resolve_storage_client(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin")
        return storage_client, path

    num_threads = 32
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(executor.map(storage_client_thread, range(num_threads)))
        assert len(results) > 0
        storage_client, _ = results[0]
        for i in range(1, num_threads):
            assert results[i][0] is storage_client, "All threads should return the same StorageClient instance"


def test_glob_with_posix_path(file_storage_config):
    for filepath in msc.glob("/etc/**/*.conf"):
        assert filepath.startswith("msc://") is False

    for filepath in msc.glob("msc://default/etc/**/*.conf"):
        assert filepath.startswith("msc://")


def test_open_url(file_storage_config):
    body = b"A" * 64 * MB
    tempdir = tempfile.mkdtemp()

    fp = msc.open(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin", "wb")
    fp.write(body)
    fp.close()

    fp = msc.open(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin", "rb")
    content = fp.read()
    fp.close()
    assert body == content

    results = msc.glob(f"{MSC_PROTOCOL}default{tempdir}/*.bin")
    assert len(results) == 1
    assert results[0] == f"{MSC_PROTOCOL}default{tempdir}/testfile.bin"


def test_download_file(file_storage_config):
    body = b"A" * 64 * MB
    tempdir = tempfile.mkdtemp()

    # Write to a test file
    remote_file_path = f"{MSC_PROTOCOL}default{tempdir}/testfile.bin"
    fp = msc.open(remote_file_path, "wb")
    fp.write(body)
    fp.close()

    assert msc.is_file(url=remote_file_path)

    local_tempdir = tempfile.mkdtemp()
    local_file_path = f"{local_tempdir}/testfile.bin"
    msc.download_file(url=remote_file_path, local_path=local_file_path)

    fp = msc.open(f"{MSC_PROTOCOL}default{local_file_path}", "rb")
    content = fp.read()
    fp.close()
    assert body == content

    results = msc.glob(f"{MSC_PROTOCOL}default{local_tempdir}/*.bin")
    assert len(results) == 1
    assert results[0] == f"{MSC_PROTOCOL}default{local_tempdir}/testfile.bin"


def test_glob_include_prefix(file_storage_config):
    data_dir = os.path.join(tempfile.gettempdir(), "test_data")
    profile_name = "test_glob_include_prefix"
    config_json = json.dumps(
        {
            "profiles": {
                profile_name: {
                    "storage_provider": {
                        "type": "file",
                        "options": {
                            "base_path": data_dir,
                        },
                    }
                }
            }
        }
    )
    config_filename = os.path.join(tempfile.gettempdir(), ".msc_config.json")

    with open(config_filename, "w") as fp:
        fp.write(config_json)

    os.environ["MSC_CONFIG"] = config_filename

    body = b"A" * 64 * MB
    sub_prefix = os.path.basename(tempfile.mkdtemp())

    os.makedirs(os.path.join(data_dir, sub_prefix), exist_ok=True)

    # Write to a test file
    remote_file_path = f"{MSC_PROTOCOL}{profile_name}/{sub_prefix}/testfile.bin"
    with msc.open(remote_file_path, "wb") as fp:
        fp.write(body)

    # NOTE: The URL here does not include the base_path, but profile name and sub-prefix
    results = msc.glob(f"{MSC_PROTOCOL}{profile_name}/{sub_prefix}/**/*.bin")
    assert len(results) == 1

    with msc.open(results[0], "rb") as fp:
        assert fp.read(10) == b"A" * 10


def test_is_empty(file_storage_config):
    assert msc.is_empty("/usr/bin") is False
    assert msc.is_empty("/tmp/dir/not/exist")

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, "testfile.bin")
        with msc.open(filepath, "wb") as fp:
            fp.write(b"TEST")

        assert msc.is_empty(f"{MSC_PROTOCOL}default{tempdir}") is False


class TestS3Local:
    def __init__(self, bucket_name: str = "test-bucket-0002"):
        self.bucket_name = bucket_name
        self.minio_access_key = "minioadmin"
        self.minio_secret_key = "minioadmin"
        self.endpoint_url = "http://localhost:9000"

        import boto3

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            region_name="us-east-1",
        )

    def setup_s3_local(self, base_path: str = ""):
        self.clean_bucket()

        try:
            self.client.create_bucket(Bucket=self.bucket_name)
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            pass

        config_json = json.dumps(
            {
                "profiles": {
                    "s3-iad": {
                        "storage_provider": {
                            "type": "s3",
                            "options": {
                                "endpoint_url": self.endpoint_url,
                                "region_name": "us-east-1",
                                "base_path": "" if not base_path else base_path,
                            },
                        },
                        "credentials_provider": {
                            "type": "S3Credentials",
                            "options": {
                                "access_key": self.minio_access_key,
                                "secret_key": self.minio_secret_key,
                            },
                        },
                    }
                },
                "cache": {"size_mb": 5000},
            }
        )

        config_filename = os.path.join(tempfile.gettempdir(), ".msc_config.json")

        with open(config_filename, "w") as fp:
            fp.write(config_json)

        os.environ["MSC_CONFIG"] = config_filename

    def clean_bucket(self):
        """Delete all objects in the bucket"""
        try:
            objects = self.client.list_objects_v2(Bucket=self.bucket_name).get("Contents", [])
            for obj in objects:
                self.client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])
            self.client.delete_bucket(Bucket=self.bucket_name)
        except self.client.exceptions.NoSuchBucket:
            pass


def verify_shortcuts(profile: str, prefix: str):
    prefix = f"{prefix}/data"
    body = b"A" * (64 * MB)

    # open files
    for i in range(10):
        with msc.open(f"msc://{profile}/{prefix}/data-{i}.bin", "wb") as fp:
            fp.write(body)

    # glob
    assert len(msc.glob(f"msc://{profile}/{prefix}/**/*.bin")) == 10

    # upload
    fp = tempfile.NamedTemporaryFile(mode="wb", delete=False)
    fp.write(body)
    fp.close()
    msc.upload_file(f"msc://{profile}/{prefix}/data-11.bin", fp.name)

    file_list = msc.glob(f"msc://{profile}/{prefix}/**/*.bin")
    assert len(file_list) == 11

    for file_url in file_list:
        assert msc.is_file(file_url)

    # download
    filepath = os.path.join(tempfile.gettempdir(), "data-11.bin")
    msc.download_file(f"msc://{profile}/{prefix}/data-11.bin", filepath)
    assert os.path.exists(filepath)

    # numpy
    arr = np.array([1, 2, 3, 4, 5], dtype=np.int32)
    msc.numpy.save(f"msc://{profile}/{prefix}/arr-01.npy", arr)
    assert msc.numpy.load(f"msc://{profile}/{prefix}/arr-01.npy").all() == arr.all()
    assert msc.numpy.memmap(f"msc://{profile}/{prefix}/arr-01.npy", dtype=np.int32, shape=(5,)).all() == arr.all()

    # mmap
    with msc.open(f"msc://{profile}/{prefix}/data-2.bin") as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            content = mm[:]
            assert content == body

    # open file without cache
    with msc.open(f"msc://{profile}/{prefix}/data-2.bin", disable_read_cache=True) as fp:
        assert isinstance(fp, ObjectFile)
        assert fp._cache_manager is None


def test_msc_shortcuts_with_s3() -> None:
    s3_test_helper = TestS3Local()
    s3_test_helper.setup_s3_local(base_path=s3_test_helper.bucket_name)
    try:
        verify_shortcuts(profile="s3-iad", prefix="files")
    finally:
        s3_test_helper.clean_bucket()


def test_msc_shortcuts_with_empty_base_path() -> None:
    s3_test_helper = TestS3Local()
    s3_test_helper.setup_s3_local()
    try:
        verify_shortcuts(profile="s3-iad", prefix=f"{s3_test_helper.bucket_name}/files")
    finally:
        s3_test_helper.clean_bucket()
