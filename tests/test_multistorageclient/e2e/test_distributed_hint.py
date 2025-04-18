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
import multiprocessing
import multistorageclient as msc
from multistorageclient.caching.distributed_hint import DistributedHint
from datetime import timedelta
from . import common
from test_multistorageclient.unit.caching.hint_utils import attempt_acquire_lock
import threading


def verify_hint_acquisition(storage_client: msc.StorageClient, hint_prefix: str) -> None:
    """Verify that hint can be acquired."""

    # storage_client._storage_provider._delete_object(hint_prefix)
    hint = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=3.0),
        heartbeat_buffer=timedelta(seconds=1.0),
    )

    assert hint.acquire()
    hint.release()


def verify_consecutive_acquisition(storage_client: msc.StorageClient, hint_prefix: str) -> None:
    """Verify that only one process can acquire the hint at a time."""
    # Create two hint instances with shorter heartbeat interval
    hint1 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=2.0),  # Reduced from 5.0
        heartbeat_buffer=timedelta(seconds=1.0),  # Reduced from 2.0
    )

    hint2 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=2.0),  # Reduced from 5.0
        heartbeat_buffer=timedelta(seconds=1.0),  # Reduced from 2.0
    )

    # First client acquires hint
    assert hint1.acquire()

    # Wait for at least one heartbeat interval to ensure hint1 has refreshed its hint
    time.sleep(2.5)  # Slightly more than heartbeat_interval

    # Second client should fail to acquire the hint
    assert not hint2.acquire()

    # Release the hint
    hint1.release()

    # Second client should now succeed
    assert hint2.acquire()

    # Clean up
    hint2.release()


def verify_hint_release_and_acquire(storage_client: msc.StorageClient, hint_prefix: str) -> None:
    """Verify that hint can be acquired after release."""
    # storage_client._storage_provider._delete_object(hint_prefix)
    # Create two hint instances
    hint1 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=5.0),
        heartbeat_buffer=timedelta(seconds=2.0),
    )

    hint2 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=5.0),
        heartbeat_buffer=timedelta(seconds=2.0),
    )

    # First client acquires and releases
    assert hint1.acquire()
    hint1.release()

    # Second client should be able to acquire after first process releases
    assert hint2.acquire()

    # Clean up
    hint2.release()


def verify_hint_expiration(storage_client: msc.StorageClient, hint_prefix: str) -> None:
    """Verify that hint can be acquired after expiration."""
    # Create two hint instances with shorter intervals for testing
    hint1 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=1.0),  # Shorter interval for faster testing
        heartbeat_buffer=timedelta(seconds=0.5),  # Shorter buffer for faster testing
    )

    hint2 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=1.0),
        heartbeat_buffer=timedelta(seconds=0.5),
    )

    try:
        # First client acquires the hint
        assert hint1.acquire(), "hint1 should acquire the hint"

        # Wait for one heartbeat to ensure hint1 has refreshed its hint
        time.sleep(1.5)  # Slightly more than heartbeat_interval

        # Stop heartbeat thread to simulate expiration
        if hint1._heartbeat_daemon:
            hint1._stop_heartbeat.set()
            hint1._heartbeat_daemon.join(timeout=1)  # This adds 1 second to the wait

        # Wait for hint to expire (heartbeat_interval + heartbeat_buffer)
        time.sleep(1.5)  # Total wait: 1.5 (initial) + 1 (join) + 1.5 = 4.0 seconds

        # Second client should now be able to acquire it
        assert hint2.acquire(), "hint2 should acquire the hint after expiration"

    finally:
        # Clean up - only release hint2 since hint1 has expired
        if hint2._hint_object:
            hint2.release()


def verify_multiple_threads_acquisition(
    storage_provider,
    hint_prefix: str,
    heartbeat_interval: float,
    heartbeat_buffer: float,
) -> None:
    """Verify that only one thread can acquire a hint at a time."""

    success_count = 0
    thread_count = 3
    lock = threading.Lock()

    def try_acquire_hint():
        nonlocal success_count
        hint = DistributedHint(
            storage_provider=storage_provider,
            hint_prefix=hint_prefix,
            heartbeat_interval=timedelta(seconds=heartbeat_interval),
            heartbeat_buffer=timedelta(seconds=heartbeat_buffer),
        )

        if hint.acquire():
            with lock:
                nonlocal success_count
                success_count += 1
            time.sleep(5.0)  # workaround, in the future, create one more test with shorter sleep time
            hint.release()

    threads = []
    for _ in range(thread_count):
        t = threading.Thread(target=try_acquire_hint)
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Verify that exactly one thread succeeded, this is the shortcoming of existing implementation, will be fixed in the future
    assert success_count == 1, f"Expected 1 thread to acquire the hint, got {success_count}"


def verify_process_death_takeover(storage_client: msc.StorageClient, hint_prefix: str) -> None:
    """Verify that hint can be acquired after the holding process dies."""
    # Create two hint instances with shorter intervals for testing
    hint1 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=1.0),  # Shorter interval for faster testing
        heartbeat_buffer=timedelta(seconds=0.5),  # Shorter buffer for faster testing
    )

    hint2 = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=1.0),
        heartbeat_buffer=timedelta(seconds=0.5),
    )

    try:
        # First client acquires the hint
        assert hint1.acquire(), "hint1 should acquire the hint"

        # Wait for one heartbeat to ensure hint1 has refreshed its hint
        time.sleep(1.5)  # Slightly more than heartbeat_interval

        # Simulate process death by stopping heartbeat and not releasing
        if hint1._heartbeat_daemon:
            hint1._stop_heartbeat.set()
            hint1._heartbeat_daemon.join(timeout=1)

        # Wait for hint to expire (heartbeat_interval + heartbeat_buffer)
        time.sleep(1.5)  # Total wait: 1.5 (initial) + 1 (join) + 1.5 = 4.0 seconds

        # Second client should now be able to acquire it
        assert hint2.acquire(), "hint2 should acquire the hint after process death"

    finally:
        # Clean up - only release hint2 since hint1 has died
        if hint2._hint_object:
            hint2.release()


@pytest.mark.skip(reason="Flaky. Needs debugging.")
@pytest.mark.parametrize("profile_name", ["test-s3e"])
@pytest.mark.parametrize("config_suffix", ["", ""])
def test_hint_acquisition(profile_name, config_suffix):
    """Test hint acquisition."""
    profile = profile_name + config_suffix
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    hint_prefix = f"hint/test-lock-{int(time.time())}/"  # Unique prefix for each test run, ending with delimiter
    try:
        verify_hint_acquisition(client, hint_prefix)
    finally:
        # Clean up any remaining hints
        common.delete_files(client, "hint/")


@pytest.mark.skip(reason="Flaky. Needs debugging.")
@pytest.mark.parametrize("profile_name", ["test-s3e"])
@pytest.mark.parametrize("config_suffix", ["", ""])
def test_consecutive_hint_acquisition(profile_name, config_suffix):
    """Test consecutive hint acquisition between two clients."""
    profile = profile_name + config_suffix
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    hint_prefix = f"hint/test-lock-{int(time.time())}/"  # Unique prefix for each test run, ending with delimiter
    try:
        verify_consecutive_acquisition(client, hint_prefix)
    finally:
        # Clean up any remaining hints
        common.delete_files(client, "hint/")


@pytest.mark.skip(reason="Flaky. Needs debugging.")
@pytest.mark.parametrize("profile_name", ["test-s3e"])
@pytest.mark.parametrize("config_suffix", ["", ""])
def test_hint_release_and_acquire(profile_name, config_suffix):
    """Test that hint can be acquired after release."""
    profile = profile_name + config_suffix
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    hint_prefix = f"hint/test-lock-{int(time.time())}/"  # Unique prefix for each test run, ending with delimiter
    try:
        verify_hint_release_and_acquire(client, hint_prefix)
    finally:
        # Clean up any remaining hints
        common.delete_files(client, "hint/")


@pytest.mark.skip(reason="Flaky. Needs debugging.")
@pytest.mark.parametrize("profile_name", ["test-s3e"])
@pytest.mark.parametrize("config_suffix", ["", ""])
def test_hint_expiration(profile_name, config_suffix):
    """Test that hint can be acquired after expiration."""
    profile = profile_name + config_suffix
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    hint_prefix = f"hint/test-lock-{int(time.time())}/"  # Unique prefix for each test run, ending with delimiter
    try:
        verify_hint_expiration(client, hint_prefix)
    finally:
        # Clean up any remaining hints
        common.delete_files(client, "hint/")


@pytest.mark.skip(reason="Flaky. Needs debugging.")
@pytest.mark.parametrize("profile_name", ["test-s3e"])
@pytest.mark.parametrize("config_suffix", [""])
def test_multiple_threads_acquire_hint(profile_name, config_suffix):
    """Test that only one thread can acquire the hint at a time."""
    profile = profile_name + config_suffix
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    hint_prefix = f"hint/test-lock-{int(time.time())}/"
    try:
        verify_multiple_threads_acquisition(client._storage_provider, hint_prefix, 2.0, 1.0)
    finally:
        # Clean up any remaining hints
        common.delete_files(client, "hint/")


@pytest.mark.skip(reason="Flaky. Needs debugging.")
@pytest.mark.parametrize("profile_name", ["test-s3e"])
@pytest.mark.parametrize("config_suffix", [""])
def test_multiple_processes_acquire_hint(profile_name, config_suffix):
    """Test that only one process acquires the hint."""
    profile = profile_name + config_suffix
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    hint_prefix = f"hint/test-lock-{int(time.time())}/"  # Unique prefix for each test run
    acquired_count = multiprocessing.Value("i", 0)  # Shared integer initialized to 0

    try:
        # Create two processes but don't start them yet
        p1 = multiprocessing.Process(target=attempt_acquire_lock, args=(hint_prefix, 1, acquired_count, None, profile))
        p2 = multiprocessing.Process(target=attempt_acquire_lock, args=(hint_prefix, 2, acquired_count, None, profile))

        # Start both processes as close together as possible
        p1.start()
        p2.start()

        # Wait for both processes to complete
        p1.join()
        p2.join()

        # Verify that only one process acquired the hint
        assert acquired_count.value == 1, f"Expected 1 process to acquire the hint, got {acquired_count.value}"

    finally:
        # Clean up any remaining hints
        common.delete_files(client, "hint/")


@pytest.mark.skip(reason="Flaky. Needs debugging.")
@pytest.mark.parametrize("profile_name", ["test-s3e"])
@pytest.mark.parametrize("config_suffix", [""])
def test_process_death_takeover(profile_name, config_suffix):
    """Test that hint can be acquired after the holding process dies."""
    profile = profile_name + config_suffix
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    hint_prefix = f"hint/test-lock-{int(time.time())}/"  # Unique prefix for each test run, ending with delimiter
    try:
        verify_process_death_takeover(client, hint_prefix)
    finally:
        # Clean up any remaining hints
        common.delete_files(client, "hint/")
