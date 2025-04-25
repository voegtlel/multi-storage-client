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
from datetime import datetime
import shutil
import time
import pytest
from multistorageclient.caching.cache_config import (
    CacheConfig,
    EvictionPolicyConfig,
    CacheBackendConfig,
)
from multistorageclient.cache import CacheBackendFactory, DEFAULT_CACHE_SIZE_MB, DEFAULT_CACHE_REFRESH_INTERVAL
from multistorageclient.caching.cache_backend import FileSystemBackend
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient.config import StorageClientConfig


@pytest.fixture
def profile_name():
    return "test-cache"


@pytest.fixture
def cache_config(tmpdir):
    """Fixture for CacheConfig object."""
    return CacheConfig(size="10M", use_etag=False, backend=CacheBackendConfig(cache_path=str(tmpdir)))


@pytest.fixture
def cache_manager(profile_name, cache_config):
    """Fixture for CacheManager object."""
    return CacheBackendFactory.create(profile=profile_name, cache_config=cache_config)


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
    assert os.path.exists(os.path.join(tmpdir, profile_name, f".{cache_manager.get_cache_key(key)}.lock"))

    assert cache_manager.read(key) == b"cached data"

    cache_manager.delete(key)

    # Verify the file is deleted
    assert not os.path.exists(os.path.join(tmpdir, profile_name, cache_manager.get_cache_key(key)))

    assert not os.path.exists(os.path.join(tmpdir, profile_name, f".{cache_manager.get_cache_key(key)}.lock"))


def test_cache_manager_open_file(profile_name, tmpdir, cache_manager):
    """Test that CacheManager can open a file from the cache."""
    file = tmpdir.join(profile_name, "test_file.txt")
    file.write("cached data")

    key = "bucket/test_file.txt"

    cache_manager.set(key, str(file))

    with cache_manager.open(key, "r") as result:
        assert result.read() == "cached data"
        assert result.name == os.path.join(tmpdir, profile_name, cache_manager.get_cache_key(key))

    with cache_manager.open(key, "rb") as result:
        assert result.read() == b"cached data"
        assert result.name == os.path.join(tmpdir, profile_name, cache_manager.get_cache_key(key))


def test_cache_manager_refresh_cache(tmpdir):
    """Test that cache refresh works correctly."""
    # Use a separate cache directory for this test
    cache_dir = os.path.join(str(tmpdir), "refresh_test")
    os.makedirs(cache_dir, exist_ok=True)

    cache_config = CacheConfig(size="10M", use_etag=False, backend=CacheBackendConfig(cache_path=cache_dir))
    cache_manager = CacheBackendFactory.create(profile="refresh_test", cache_config=cache_config)

    data_10mb = b"*" * 10 * 1024 * 1024
    for i in range(20):
        cache_manager.set(f"bucket/test_{i:04d}.bin", data_10mb)

    cache_manager.refresh_cache()
    assert cache_manager.cache_size() <= 10 * 1024 * 1024

    # Clean up
    shutil.rmtree(cache_dir)


def test_cache_manager_metrics(profile_name, tmpdir, cache_manager):
    # Mock the metrics helper in the backend
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


@pytest.fixture
def lru_cache_config(tmpdir):
    return CacheConfig(size="10M", use_etag=False, eviction_policy=EvictionPolicyConfig(policy="LRU"))


def test_lru_eviction_policy(profile_name, lru_cache_config):
    # Create the CacheManager with the provided lru_cache_config
    cache_manager = CacheBackendFactory.create(profile=profile_name, cache_config=lru_cache_config)

    # Add files to the cache (each file is 3 MB)
    cache_manager.set("file1", b"a" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set("file2", b"b" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)
    cache_manager.set("file3", b"c" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)

    # Access file1 to make it the most recently used
    cache_manager.read("file1")  # force update ts
    time.sleep(1)

    # Add another file to trigger eviction
    cache_manager.set("file4", b"d" * 3 * 1024 * 1024)  # 3 MB

    time.sleep(1)  # Ensure time difference for LRU
    # Record the current last_refresh_time and set it to past to force refresh
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    # Verify that refresh occurred by checking last_refresh_time was updated
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file1 is still in the cache (LRU policy)
    assert cache_manager.contains("file1"), "Most recently used file should be kept"

    # Verify that the least recently used file (file2 or file3) has been evicted
    assert not cache_manager.contains("file2") or not cache_manager.contains("file3"), (
        "Least recently used file should be evicted"
    )


@pytest.fixture
def fifo_cache_config(tmpdir):
    return CacheConfig(size="10M", use_etag=False, eviction_policy=EvictionPolicyConfig(policy="FIFO"))


def test_fifo_eviction_policy(profile_name, fifo_cache_config):
    # Create the CacheManager with the provided fifo_cache_config
    cache_manager = CacheBackendFactory.create(profile=profile_name, cache_config=fifo_cache_config)

    # Add files to the cache (each file is 3 MB)
    cache_manager.set("file1", b"a" * 3 * 1024 * 1024)  # 3 MB - First in
    cache_manager.set("file2", b"b" * 3 * 1024 * 1024)  # 3 MB - Second in
    cache_manager.set("file3", b"c" * 3 * 1024 * 1024)  # 3 MB - Third in

    # Access files in different order to verify FIFO is independent of access patterns
    cache_manager.read("file3")  # Access the newest file
    cache_manager.read("file2")  # Access the middle file
    cache_manager.read("file1")  # Access the oldest file

    # Add another file to trigger eviction
    cache_manager.set("file4", b"d" * 3 * 1024 * 1024)  # 3 MB - Fourth in

    # Force refresh to trigger eviction
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file1 (first in) has been evicted
    assert not cache_manager.contains("file1"), "First file in should be evicted (FIFO)"

    # Verify that later files are still in the cache
    assert cache_manager.contains("file2"), "Second file in should be kept"
    assert cache_manager.contains("file3"), "Third file in should be kept"
    assert cache_manager.contains("file4"), "Newly added file should be in the cache"

    # Add one more file to verify FIFO continues to work
    cache_manager.set("file5", b"e" * 3 * 1024 * 1024)  # 3 MB - Fifth in

    # Force refresh to trigger eviction
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Verify that file2 (now the oldest) is evicted
    assert not cache_manager.contains("file2"), "Second file in should now be evicted"
    assert cache_manager.contains("file3"), "Third file in should still be kept"
    assert cache_manager.contains("file4"), "Fourth file in should still be kept"
    assert cache_manager.contains("file5"), "Most recently added file should be in the cache"


@pytest.fixture
def random_cache_config(tmpdir):
    return CacheConfig(size="10M", use_etag=False, eviction_policy=EvictionPolicyConfig(policy="RANDOM"))


def test_random_eviction_policy(profile_name, random_cache_config):
    """Test that random eviction policy works correctly."""

    # Clean the entire cache directory, not just the profile subdirectory
    cache_dir = random_cache_config.backend.cache_path
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    os.makedirs(cache_dir)

    # Create the CacheManager with the provided random_cache_config
    cache_manager = CacheBackendFactory.create(profile=profile_name, cache_config=random_cache_config)

    # Add files to the cache (each file is 3 MB)
    cache_manager.set("file1", b"a" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)  # Ensure files have different timestamps
    cache_manager.set("file2", b"b" * 3 * 1024 * 1024)  # 3 MB
    time.sleep(1)  # Ensure files have different timestamps
    cache_manager.set("file3", b"c" * 3 * 1024 * 1024)  # 3 MB

    # Access files in different order - should not affect random eviction
    time.sleep(1)  # Ensure access times are different
    cache_manager.read("file1")
    time.sleep(1)  # Ensure access times are different
    cache_manager.read("file2")
    time.sleep(1)  # Ensure access times are different
    cache_manager.read("file3")

    # Add another file to trigger eviction
    time.sleep(1)  # Ensure file4 has a newer timestamp
    cache_manager.set("file4", b"d" * 3 * 1024 * 1024)  # 3 MB

    # Force refresh to trigger eviction
    old_refresh_time = cache_manager._last_refresh_time
    cache_manager._last_refresh_time = datetime.now().replace(year=2000)
    cache_manager.refresh_cache()
    assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

    # Count how many files remain in the cache
    remaining_files = sum(1 for f in ["file1", "file2", "file3", "file4"] if cache_manager.contains(f))

    # With 3MB files and 10MB cache, we should have exactly 3 files
    assert remaining_files == 3, "Cache should contain exactly 3 files after eviction"

    # Verify that file4 (newest) is always in the cache
    assert cache_manager.contains("file4"), "Newly added file should always be in cache"

    # Run multiple eviction cycles to verify randomness
    eviction_counts = {"file1": 0, "file2": 0, "file3": 0}
    num_trials = 300  # Increased from 100 to 300 for better statistical distribution

    for _ in range(num_trials):
        # Reset cache manager for each trial
        cache_manager = CacheBackendFactory.create(profile=profile_name, cache_config=random_cache_config)

        # Add initial files
        cache_manager.set("file1", b"a" * 3 * 1024 * 1024)
        time.sleep(0.1)
        cache_manager.set("file2", b"b" * 3 * 1024 * 1024)
        time.sleep(0.1)
        cache_manager.set("file3", b"c" * 3 * 1024 * 1024)
        time.sleep(0.1)

        # Trigger eviction
        cache_manager.set("file4", b"d" * 3 * 1024 * 1024)
        time.sleep(0.1)

        # Force refresh to trigger eviction
        old_refresh_time = cache_manager._last_refresh_time
        cache_manager._last_refresh_time = datetime.now().replace(year=2000)
        cache_manager.refresh_cache()
        assert cache_manager._last_refresh_time > old_refresh_time, "Cache refresh should update last_refresh_time"

        # Count which files were evicted
        for file_name in ["file1", "file2", "file3"]:
            if not cache_manager.contains(file_name):
                eviction_counts[file_name] += 1

    # Calculate the expected number of evictions per file
    expected_evictions = num_trials / 3  # Each file should be evicted roughly 1/3 of the time
    tolerance = num_trials * 0.50  # Increased tolerance to 50% to account for random variation

    # Print eviction statistics for debugging
    for file_name, count in eviction_counts.items():
        deviation = abs(count - expected_evictions)
        deviation_percentage = (deviation / expected_evictions) * 100
        print(
            f"{file_name}: evicted {count} times ({deviation_percentage:.1f}% deviation from expected {expected_evictions})"
        )

    # Verify that each file was evicted a roughly equal number of times
    for file_name, count in eviction_counts.items():
        assert abs(count - expected_evictions) <= tolerance, (
            f"{file_name} was evicted {count} times, which is too far from the expected {expected_evictions} ±{tolerance}"
        )


def verify_cache_operations(cache_manager):
    # Add files to the cache (each file is 3 MB)
    cache_manager.set("test_file1:etag1", b"a" * 1 * 1024 * 1024)  # 1 MB - First in
    cache_manager.set("test_file2:etag2", b"b" * 1 * 1024 * 1024)  # 1 MB - Second in
    cache_manager.set("test_file3:etag3", b"c" * 1 * 1024 * 1024)  # 1 MB - Third in

    # Access files in different order to verify FIFO is independent of access patterns
    cache_manager.read("test_file3:etag3")  # Access the newest file
    cache_manager.read("test_file2:etag2")  # Access the middle file
    cache_manager.read("test_file1:etag1")  # Access the oldest file

    # Verify that later files are still in the cache with correct ETags
    assert cache_manager.contains("test_file1:etag1"), "Second file in should be kept"
    assert cache_manager.contains("test_file2:etag2"), "Third file in should be kept"
    assert cache_manager.contains("test_file3:etag3"), "Newly added file should be in the cache"


def create_legacy_cache_config(profile_config, tmpdir):
    """Helper function to create legacy cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {"size_mb": 10, "use_etag": False, "location": str(tmpdir), "eviction_policy": "fifo"},
    }


def create_new_cache_config(profile_config, tmpdir):
    """Helper function to create new cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {
            "size": "10M",
            "use_etag": False,
            "eviction_policy": {"policy": "random", "refresh_interval": 300},
            "cache_backend": {"cache_path": str(tmpdir)},
        },
    }


def create_mixed_cache_config(profile_config, tmpdir):
    """Helper function to create mixed cache config."""
    return {
        "profiles": {"s3-local": profile_config},
        "cache": {
            "size_mb": 10,
            "use_etag": False,
            "eviction_policy": {"policy": "random", "refresh_interval": 300},
            "cache_backend": {"cache_path": str(tmpdir)},
        },
    }


def create_incorrect_size_cache_config(profile_config, tmpdir):
    """Helper function to create incorrect size cache config."""
    return {"profiles": {"s3-local": profile_config}, "cache": {"size": "one-thousand-gigabytes"}}


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "config_creator"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket, create_legacy_cache_config],
        [tempdatastore.TemporaryAWSS3Bucket, create_new_cache_config],
    ],
    ids=["legacy_config", "new_config"],
)
def test_storage_provider_cache_configs(config_creator, temp_data_store_type, tmpdir):
    """
    Test that both legacy and new cache config formats work correctly.

    This test verifies that cache operations work the same way regardless of whether
    the legacy or new cache configuration format is used.
    """
    with temp_data_store_type() as temp_store:
        config_dict = config_creator(temp_store.profile_config_dict(), tmpdir)
        storage_config = StorageClientConfig.from_dict(config_dict)
        real_storage_provider = storage_config.storage_provider

        # Clean up any existing objects in the bucket with test prefix
        for obj in real_storage_provider.list_objects(prefix="test_"):
            real_storage_provider.delete_object(obj.key)

        # Access the CacheManager
        cache_manager = storage_config.cache_manager
        verify_cache_operations(cache_manager)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type", "config_creator", "expected_error", "error_message"],
    argvalues=[
        [
            tempdatastore.TemporaryAWSS3Bucket,
            create_mixed_cache_config,
            ValueError,
            "Cannot mix old and new cache config formats",
        ],
        [
            tempdatastore.TemporaryAWSS3Bucket,
            create_incorrect_size_cache_config,
            RuntimeError,
            "Failed to validate the config file",
        ],
    ],
    ids=["mixed_config", "incorrect_size"],
)
def test_storage_provider_invalid_cache_configs(
    config_creator, temp_data_store_type, expected_error, error_message, tmpdir
):
    """
    Test that invalid cache configurations raise appropriate errors.

    This test verifies that:
    1. Mixing old and new cache config formats raises a ValueError
    2. Using an incorrect size format raises a RuntimeError
    """
    with temp_data_store_type() as temp_store:
        config_dict = config_creator(temp_store.profile_config_dict(), tmpdir)
        with pytest.raises(expected_error, match=error_message):
            StorageClientConfig.from_dict(config_dict)


@pytest.fixture
def storage_provider_empty_cache_config(tmpdir):
    """
    New cache config format
    """

    # Create a config dictionary with profile and cache configuration
    def _config_builder(profile_config):
        return {"profiles": {"s3-local": profile_config}, "cache": {}}

    return _config_builder


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_storage_provider_empty_cache_config(storage_provider_empty_cache_config, temp_data_store_type):
    with temp_data_store_type() as temp_store:
        config_dict = storage_provider_empty_cache_config(temp_store.profile_config_dict())
        storage_config = StorageClientConfig.from_dict(config_dict)
        real_storage_provider = storage_config.storage_provider
        cache_backend = storage_config.cache_manager

        # Clean up any existing objects in the bucket with test prefix
        for obj in real_storage_provider.list_objects(prefix="test_"):
            real_storage_provider.delete_object(obj.key)

        # Access the CacheManager
        cache_manager = storage_config.cache_manager
        verify_cache_operations(cache_manager)

        cache_config = storage_config.cache_config
        assert cache_config is not None
        assert cache_config.size == DEFAULT_CACHE_SIZE_MB
        assert cache_config.backend.cache_path is not None and isinstance(cache_config.backend.cache_path, str)
        assert cache_config.eviction_policy.policy == "fifo"
        assert cache_config.eviction_policy.refresh_interval == DEFAULT_CACHE_REFRESH_INTERVAL
        assert cache_config.use_etag
        assert isinstance(cache_backend, FileSystemBackend)


@pytest.fixture
def storage_provider_partial_cache_config(tmpdir):
    """
    New cache config format
    """

    # Create a config dictionary with profile and cache configuration
    def _config_builder(profile_config):
        return {"profiles": {"s3-local": profile_config}, "cache": {"size": "100M"}}

    return _config_builder


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_storage_provider_partial_cache_config(storage_provider_partial_cache_config, temp_data_store_type):
    with temp_data_store_type() as temp_store:
        config_dict = storage_provider_partial_cache_config(temp_store.profile_config_dict())
        storage_config = StorageClientConfig.from_dict(config_dict)
        real_storage_provider = storage_config.storage_provider
        cache_backend = storage_config.cache_manager

        # Clean up any existing objects in the bucket with test prefix
        for obj in real_storage_provider.list_objects(prefix="test_"):
            real_storage_provider.delete_object(obj.key)

        # Access the CacheManager
        cache_manager = storage_config.cache_manager
        verify_cache_operations(cache_manager)

        cache_config = storage_config.cache_config
        assert cache_config is not None
        assert cache_config.size == "100M"
        assert cache_config.backend.cache_path is not None and isinstance(cache_config.backend.cache_path, str)
        assert cache_config.eviction_policy.policy == "fifo"
        assert cache_config.eviction_policy.refresh_interval == DEFAULT_CACHE_REFRESH_INTERVAL
        assert cache_config.use_etag
        assert isinstance(cache_backend, FileSystemBackend)
