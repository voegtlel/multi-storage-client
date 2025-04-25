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

import time
import pytest
import uuid
from multistorageclient.caching.distributed_hint import DistributedHint
from multistorageclient import StorageClient, StorageClientConfig
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from datetime import timedelta
import threading
from multiprocessing import Process, Value
from test_multistorageclient.unit.caching.hint_utils import attempt_acquire_lock


@pytest.fixture
def temp_s3_bucket():
    """Create a temporary S3 bucket for testing."""
    with tempdatastore.TemporaryAWSS3Bucket() as temp_store:
        yield temp_store


@pytest.fixture
def storage_client(temp_s3_bucket):
    """Create a storage client with the temporary S3 bucket."""
    profile = "data"
    config_dict = {"profiles": {profile: temp_s3_bucket.profile_config_dict()}}
    return StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))


def test_acquire_and_release_hint(storage_client):
    """Test basic hint acquisition and release, using a unique hint prefix."""

    test_uuid = str(uuid.uuid4())
    distributed_hint = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=5.0),  # Short interval for testing
        heartbeat_buffer=timedelta(seconds=2.0),  # Short buffer for testing
    )

    # First acquisition should succeed
    assert distributed_hint.acquire()

    # Release the hint
    distributed_hint.release()

    # After release, we should be able to acquire it again
    assert distributed_hint.acquire()


def test_lease_expiration(storage_client):
    """Test that hint expires after duration."""
    test_uuid = str(uuid.uuid4())
    distributed_hint = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=5.0),  # Short interval for testing
        heartbeat_buffer=timedelta(seconds=2.0),  # Short buffer for testing
    )
    # Acquire hint
    assert distributed_hint.acquire()

    # Wait for lease to expire
    time.sleep(7.0)  # Longer than heartbeat_interval + heartbeat_buffer

    # Should be able to acquire it again since the previous lease has expired
    assert distributed_hint.acquire()

    # Clean up
    distributed_hint.release()


def test_hint_refresh(storage_client):
    """Test that the hint is refreshed before expiration."""
    test_uuid = str(uuid.uuid4())
    distributed_hint = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=5.0),  # Short interval for testing
        heartbeat_buffer=timedelta(seconds=2.0),  # Short buffer for testing
    )
    # Acquire the hint
    assert distributed_hint.acquire()
    assert distributed_hint._hint_object is not None, "Hint object should not be None after successful acquisition"

    # Get initial metadata
    initial_metadata = distributed_hint._hint_object.metadata

    # Wait for the hint to be refreshed (need to wait > heartbeat_interval)
    time.sleep(5.5)  # Wait for one full heartbeat interval (5.0) plus buffer

    # Check that the hint was refreshed
    assert distributed_hint._hint_object is not None
    assert distributed_hint._hint_object.metadata.etag != initial_metadata.etag


def test_concurrent_hint_acquisition(storage_client):
    """Test hint acquisition between two clients."""
    # Create two hint instances
    test_uuid = str(uuid.uuid4())
    hint1 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=5.0),
        heartbeat_buffer=timedelta(seconds=2.0),
    )
    hint2 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=5.0),
        heartbeat_buffer=timedelta(seconds=2.0),
    )

    # First client acquires hint
    assert hint1.acquire()

    # Wait a bit longer to ensure hint1 has time to refresh its hint
    time.sleep(2.0)  # Wait 2 seconds to ensure refresh happens

    # Second client should fail to acquire the hint
    assert not hint2.acquire()

    # Release the hint
    hint1.release()

    # Wait for hint to expire
    time.sleep(6.0)

    # Second client should now succeed
    assert hint2.acquire()

    # Clean up
    hint2.release()


def test_multiple_threads_acquire_hint(storage_client):
    """Test that multiple threads cannot acquire the hint simultaneously."""
    # Create two hint instances
    test_uuid = str(uuid.uuid4())
    hint1 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=3.0),
        heartbeat_buffer=timedelta(seconds=1.0),
    )
    hint2 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=3.0),
        heartbeat_buffer=timedelta(seconds=1.0),
    )

    # First hint acquires and holds the lock
    assert hint1.acquire()

    # List to store results from threads
    results = []
    lock = threading.Lock()

    def try_acquire():
        acquired = hint2.acquire()
        with lock:
            results.append(acquired)

    # Create multiple threads trying to acquire the hint
    threads = []
    for _ in range(5):
        t = threading.Thread(target=try_acquire)
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # No thread should acquire the hint while hint1 holds it
    assert sum(results) == 0, "No thread should acquire the hint while hint1 holds it"

    # Clean up
    hint1.release()


def test_multiple_processes_acquire_hint(temp_s3_bucket):
    """Test that only one process acquires the hint."""
    test_uuid = str(uuid.uuid4())
    hint_prefix = f"{test_uuid}/test-lock"
    acquired_count = Value("i", 0)  # Shared integer initialized to 0
    bucket_config = temp_s3_bucket.profile_config_dict()

    # Create both processes but don't start them yet
    p1 = Process(target=attempt_acquire_lock, args=(hint_prefix, 1, acquired_count, bucket_config))
    p2 = Process(target=attempt_acquire_lock, args=(hint_prefix, 2, acquired_count, bucket_config))

    # Start both processes as close together as possible
    p1.start()
    p2.start()

    # Wait for both processes to complete
    p1.join()
    p2.join()

    # Verify that only one process acquired the hint
    assert acquired_count.value == 1, f"Expected 1 process to acquire the hint, got {acquired_count.value}"
    print("Exactly one process acquired the hint.")


def test_heartbeat_thread_termination(storage_client):
    """Test that release() terminates promptly without waiting for the full heartbeat interval."""
    test_uuid = str(uuid.uuid4())
    # Create hint with longer heartbeat interval to better verify quick release
    distributed_hint = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=f"{test_uuid}/test-lock",
        heartbeat_interval=timedelta(seconds=30.0),  # Long interval to verify quick release
        heartbeat_buffer=timedelta(seconds=2.0),
    )

    # Acquire the hint
    assert distributed_hint.acquire()

    # Call release and measure how long it takes
    start_time = time.time()
    distributed_hint.release()
    end_time = time.time()

    # Verify that release completes within the timeout period (1 second)
    # and doesn't wait for the full heartbeat interval (30 seconds)
    assert end_time - start_time <= 1.0, "Release took longer than the 1 second timeout"
