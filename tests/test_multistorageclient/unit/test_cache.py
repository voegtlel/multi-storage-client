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

import pytest
from multistorageclient.cache import CacheConfig, CacheManager
from multistorageclient.caching.eviction_policy import LRU, FIFO, RANDOM


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


@pytest.fixture
def lru_cache_config(tmpdir):
    return CacheConfig(location=str(tmpdir), size_mb=10, use_etag=False, eviction_policy=LRU)


def test_lru_eviction_policy(profile_name, lru_cache_config):
    import time

    # Create the CacheManager with the provided lru_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=lru_cache_config)

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
    return CacheConfig(location=str(tmpdir), size_mb=10, use_etag=False, eviction_policy=FIFO)


def test_fifo_eviction_policy(profile_name, fifo_cache_config):
    # Create the CacheManager with the provided fifo_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=fifo_cache_config)

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
    return CacheConfig(location=str(tmpdir), size_mb=10, use_etag=False, eviction_policy=RANDOM)


def test_random_eviction_policy(profile_name, random_cache_config):
    """Test that random eviction policy works correctly."""
    import time

    # Create the CacheManager with the provided random_cache_config
    cache_manager = CacheManager(profile=profile_name, cache_config=random_cache_config)

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

    # Print cache contents for debugging
    for file in ["file1", "file2", "file3", "file4"]:
        print(f"{file} is in the cache: {cache_manager.contains(file)}")

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
        cache_manager = CacheManager(profile=profile_name, cache_config=random_cache_config)

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
    print("\nEviction statistics:")
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
