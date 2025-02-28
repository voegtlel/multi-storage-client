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

import mmap
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, Type

import numpy as np
import pytest

import multistorageclient as msc
from multistorageclient.client import StorageClient
from multistorageclient.file import ObjectFile
from multistorageclient.types import MSC_PROTOCOL
from test_multistorageclient.unit.utils import tempdatastore, config

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


def test_list(file_storage_config):
    # Create test file
    body = b"A" * 64 * MB
    tempdir = tempfile.mkdtemp()

    fp = msc.open(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin", "wb")
    fp.write(body)
    fp.close()

    # Test listing without glob pattern
    results = list(msc.list(f"{MSC_PROTOCOL}default{tempdir}"))
    assert len(results) == 1
    assert "testfile.bin" in results[0].key


def test_write(file_storage_config):
    tempdir = tempfile.mkdtemp()
    filepath = os.path.join(tempdir, "testfile.bin")

    # Test writing bytes
    body = b"A" * 64 * MB
    msc.write(f"{MSC_PROTOCOL}default{filepath}", body)

    # Verify content was written correctly
    with msc.open(f"{MSC_PROTOCOL}default{filepath}", "rb") as fp:
        content = fp.read()
        assert body == content


def test_delete(file_storage_config):
    tempdir = tempfile.mkdtemp()
    filepath = os.path.join(tempdir, "testfile.bin")

    # Create test file
    body = b"A" * 64 * MB
    with msc.open(f"{MSC_PROTOCOL}default{filepath}", "wb") as fp:
        fp.write(body)

    # Verify file exists
    with msc.open(f"{MSC_PROTOCOL}default{filepath}", "rb") as fp:
        assert fp.read() == body

    # Delete file
    msc.delete(f"{MSC_PROTOCOL}default{filepath}")

    # Verify file is deleted
    with pytest.raises(FileNotFoundError):
        with msc.open(f"{MSC_PROTOCOL}default{filepath}", "rb") as fp:
            fp.read()


def test_is_empty(file_storage_config):
    assert msc.is_empty("/usr/bin") is False
    assert msc.is_empty("/tmp/dir/not/exist")

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, "testfile.bin")
        with msc.open(filepath, "wb") as fp:
            fp.write(b"TEST")

        assert msc.is_empty(f"{MSC_PROTOCOL}default{tempdir}") is False


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


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_msc_shortcuts_with_s3(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._instance_cache.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
                "cache": {},
            }
        )

        verify_shortcuts(profile="test", prefix="files")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_msc_shortcuts_with_empty_base_path(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._instance_cache.clear()

    with temp_data_store_type() as temp_data_store:
        profile_dict = temp_data_store.profile_config_dict()
        profile_dict["storage_provider"]["options"]["base_path"] = ""
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": profile_dict,
                },
                "cache": {},
            }
        )

        verify_shortcuts(profile="test", prefix=f"{temp_data_store._bucket_name}/files")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_glob_include_prefix(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._instance_cache.clear()

    with temp_data_store_type() as temp_data_store:
        profile_name = "test_glob_include_prefix"

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    profile_name: temp_data_store.profile_config_dict(),
                }
            }
        )

        body = b"A" * 64 * MB
        sub_prefix = os.path.basename(tempfile.mkdtemp())

        # Write to a test file
        remote_file_path = f"{MSC_PROTOCOL}{profile_name}/{sub_prefix}/testfile.bin"
        with msc.open(remote_file_path, "wb") as fp:
            fp.write(body)

        # NOTE: The URL here does not include the base_path, but profile name and sub-prefix
        results = msc.glob(f"{MSC_PROTOCOL}{profile_name}/{sub_prefix}/**/*.bin")
        assert len(results) == 1

        with msc.open(results[0], "rb") as fp:
            assert fp.read(10) == b"A" * 10


def test_download_and_sync_files(file_storage_config):
    body = b"A" * 4 * MB
    tempdir = tempfile.mkdtemp()

    file_names = ["dir1/testfile1.bin", "dir1/testfile2.bin", "dir2/testfile3.bin"]

    # Write three test files
    for file_name in file_names:
        remote_file_path = f"{MSC_PROTOCOL}default{tempdir}/{file_name}"
        with msc.open(remote_file_path, "wb") as fp:
            fp.write(body)
        assert msc.is_file(url=remote_file_path)

    # Sync to a different destination directory
    sync_dest_tempdir = tempfile.mkdtemp()
    msc.sync(source_url=f"{MSC_PROTOCOL}default{tempdir}/", target_url=f"{MSC_PROTOCOL}default{sync_dest_tempdir}/")

    expected_synced_files = [f"{MSC_PROTOCOL}default{sync_dest_tempdir}/{file_name}" for file_name in file_names]

    for synced_file in expected_synced_files:
        with msc.open(synced_file, "rb") as fp:
            synced_content = fp.read()
        assert synced_content == body

    # Test, by syncing again and verify the data hasn't changed?
    msc.sync(source_url=f"{MSC_PROTOCOL}default{tempdir}/", target_url=f"{MSC_PROTOCOL}default{sync_dest_tempdir}/")
