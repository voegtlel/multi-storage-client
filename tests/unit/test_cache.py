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
from unittest.mock import MagicMock

import pytest
from multistorageclient.cache import CacheConfig, CacheManager


@pytest.fixture
def profile_name():
    return "test-cache"


@pytest.fixture
def cache_config(tmpdir):
    """Fixture for CacheConfig object."""
    return CacheConfig(location=str(tmpdir), size_mb=10, use_etag=False)


@pytest.fixture
def cache_manager(profile_name, cache_config):
    """Fixture for CacheManager object."""
    return CacheManager(profile=profile_name, cache_config=cache_config)


def test_cache_config_size_bytes(cache_config):
    """Test that CacheConfig size_bytes converts MB to bytes correctly."""
    assert cache_config.size_bytes() == 10 * 1024 * 1024  # 10 MB


def test_cache_manager_read_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can read a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    cache_manager.set("bucket/test_file.txt", str(file))
    assert cache_manager.read("bucket/test_file.txt") == b"cached data"

    cache_manager.set("bucket/test_file.bin", b"binary data")
    assert cache_manager.read("bucket/test_file.bin") == b"binary data"


def test_cache_manager_read_delete_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can read a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    key = "bucket/test_file.txt"

    with cache_manager.acquire_lock(key):
        cache_manager.set(key, str(file))

    # Verify the lock file
    assert os.path.exists(os.path.join(tmpdir, profile_name, f".{cache_manager._get_cache_key(key)}.lock"))

    assert cache_manager.read(key) == b"cached data"

    cache_manager.delete(key)

    # Verify the file is deleted
    assert not os.path.exists(os.path.join(tmpdir, profile_name, cache_manager._get_cache_key(key)))

    assert not os.path.exists(os.path.join(tmpdir, profile_name, f".{cache_manager._get_cache_key(key)}.lock"))


def test_cache_manager_open_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can open a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    key = "bucket/test_file.txt"

    cache_manager.set(key, str(file))

    with cache_manager.open(key, "r") as result:
        assert result.read() == "cached data"
        assert result.name == os.path.join(tmpdir, profile_name, cache_manager._get_cache_key(key))

    with cache_manager.open(key, "rb") as result:
        assert result.read() == b"cached data"
        assert result.name == os.path.join(tmpdir, profile_name, cache_manager._get_cache_key(key))


def test_cache_manager_refresh_cache(cache_manager):
    data_10mb = b"*" * 10 * 1024 * 1024
    for i in range(20):
        cache_manager.set(f"bucket/test_{i:04d}.bin", data_10mb)

    cache_manager.refresh_cache()
    assert cache_manager.cache_size() <= 10 * 1024 * 1024


def test_cache_manager_metrics(profile_name, tmpdir, cache_manager):
    cache_manager._metrics_helper = MagicMock()

    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    cache_manager.set("bucket/test_file.txt", str(file))
    cache_manager._metrics_helper.increase.assert_called_with(operation="SET", success=True)

    cache_manager.read("bucket/test_file.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="READ", success=True)

    cache_manager.read("bucket/test_file_not_exist.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="READ", success=False)

    cache_manager.open("bucket/test_file.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="OPEN", success=True)

    cache_manager.open("bucket/test_file_not_exist.txt")
    cache_manager._metrics_helper.increase.assert_called_with(operation="OPEN", success=False)
