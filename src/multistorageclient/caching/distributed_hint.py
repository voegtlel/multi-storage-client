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

from datetime import datetime, timedelta, timezone
import threading
import time
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Optional
import logging
import json

from ..types import (
    StorageProvider,
    ObjectMetadata,
    PreconditionFailedError,
)

# Configure logger
logger = logging.Logger(__name__)

DEFAULT_HEARTBEAT_INTERVAL = timedelta(seconds=30)
DEFAULT_HEARTBEAT_BUFFER = timedelta(seconds=10)
DEFAULT_MAX_CONSECUTIVE_ERRORS = 3
HINT_DATA_VERSION = "1.0"  # Version of the hint data format


class HintObject:
    """Represents a storage object with metadata and content."""

    def __init__(self, metadata: ObjectMetadata, data: bytes):
        self.metadata = metadata
        self.data = data


class DistributedHintConflictError(Exception):
    """A distributed hint with the specified key has already been acquired."""

    pass


class DistributedHint(AbstractContextManager):
    """
    This class implements a distributed hint following the DynamoDB lock client protocol.

    * https://aws.amazon.com/blogs/database/building-distributed-locks-with-the-dynamodb-lock-client
    * https://github.com/awslabs/amazon-dynamodb-lock-client

    The resulting object can be used as a context manager. On completion of the context or destruction
    of the distributed hint, the newly created hint is released.
    """

    def __init__(
        self,
        storage_provider: StorageProvider,
        hint_prefix: str,
        heartbeat_interval: timedelta = DEFAULT_HEARTBEAT_INTERVAL,
        heartbeat_buffer: timedelta = DEFAULT_HEARTBEAT_BUFFER,
    ) -> None:
        """
        Initialize a DistributedHint instance.

        :param storage_provider: The storage provider to use for storing the hint.
        :param hint_prefix: The prefix to use for the hint object key.
        :param heartbeat_interval: The interval in seconds between heartbeat updates.
        :param heartbeat_buffer: The buffer time in seconds before a hint is considered expired.
        :param max_retries: The maximum number of retries for operations.
        :param retry_delay: The base delay in seconds between retries.
        """
        self._object_key = f"{hint_prefix.rstrip('/')}/hint"
        self._storage_provider = storage_provider
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_buffer = heartbeat_buffer
        self._heartbeat_lifespan = self._heartbeat_interval + self._heartbeat_buffer

        self._hint_object = None
        self._heartbeat_daemon = None
        self._stop_heartbeat = threading.Event()
        self._transition_lock = threading.Lock()  # Add lock for thread safety

    def _get_hint_data(self) -> bytes:
        """Get the hint data to store.
        :return: bytes of the hint data
        """
        data = {"timestamp": datetime.now(timezone.utc).isoformat()}
        return json.dumps(data).encode()

    def _heartbeat_loop(self) -> None:
        """Background thread to periodically refresh the hint."""
        consecutive_errors = 0
        max_consecutive_errors = DEFAULT_MAX_CONSECUTIVE_ERRORS

        if not self._hint_object:
            logger.debug("No hint object, not starting heartbeat")
            return

        while not self._stop_heartbeat.is_set():
            # Wait up to heartbeat interval or until stop is requested
            stopped_early = self._stop_heartbeat.wait(self._heartbeat_interval.total_seconds())
            if stopped_early or not self._hint_object:
                logger.debug("Stop requested or no hint object, stopping heartbeat")
                break

            try:
                # Update the hint with new timestamp
                self._storage_provider.put_object(
                    self._object_key, self._get_hint_data(), if_match=self._hint_object.metadata.etag
                )
                metadata = self._storage_provider.get_object_metadata(self._object_key)
                self._hint_object = HintObject(metadata=metadata, data=self._get_hint_data())
                consecutive_errors = 0

            except PreconditionFailedError:
                logger.warning("Heartbeat failed due to ETag mismatch — another actor may have taken the hint")
                break  # Stop trying — we lost the hint
            except Exception as e:
                logger.error(f"Error in hint heartbeat: {e}")
                consecutive_errors += 1
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("Too many consecutive errors in heartbeat, stopping")
                    break
                error_backoff = min(1.0 * (2 ** (consecutive_errors - 1)), 30.0)
                self._stop_heartbeat.wait(error_backoff)

    def _start_heartbeat_thread(self) -> None:
        """Start the heartbeat thread to periodically refresh the hint."""
        self._stop_heartbeat.clear()
        self._heartbeat_daemon = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_daemon.start()

    def _acquire_hint_with_condition(
        self,
        if_match: Optional[str] = None,
        if_none_match: Optional[str] = None,
    ) -> bool:
        """Put a hint object with a condition and start the heartbeat thread.

        :param if_match: The etag to match for the put operation.
        :param if_none_match: The etag to not match for the put operation.
        :return: True if the hint was acquired, False otherwise.
        """
        try:
            logger.debug(f"Putting hint with if_match: {if_match} and if_none_match: {if_none_match}")
            self._storage_provider.put_object(
                self._object_key,
                self._get_hint_data(),
                if_match=if_match,
                if_none_match=if_none_match,
            )

            metadata = self._storage_provider.get_object_metadata(self._object_key)
            self._hint_object = HintObject(metadata=metadata, data=self._get_hint_data())
            logger.debug(
                f"Successfully {'created' if if_none_match else 'took over'} hint with etag: {self._hint_object.metadata.etag}"
            )

            # Start heartbeat thread
            self._start_heartbeat_thread()
            return True
        except PreconditionFailedError:
            logger.warning(
                f"Failed to {'create' if if_none_match else 'take over'} hint - {'already exists' if if_none_match else 'etag mismatch'}"
            )
            return False

    def acquire(self) -> bool:
        """Acquire a hint.

        This method acquires a hint by waiting for the heartbeat lifespan to pass.
        If the hint is still valid, it acquires the hint. If the hint is expired, it tries to take over the hint.
        :return: True if hint was acquired, False otherwise
        """
        with self._transition_lock:
            if self._hint_object:
                # We already own the hint and it is still valid
                return True

            # Try to get the current hint state
            try:
                existing_metadata = self._storage_provider.get_object_metadata(self._object_key)

                # wait for the heartbeat lifespan
                logger.debug(f"Waiting for heartbeat lifespan: {self._heartbeat_lifespan.total_seconds()} seconds")

                time.sleep(self._heartbeat_lifespan.total_seconds())

                # Try to take over the existing hint
                logger.debug(
                    f"Trying to take over existing hint {self._object_key} with etag: {existing_metadata.etag}"
                )
                if self._acquire_hint_with_condition(if_match=existing_metadata.etag):
                    return True
            except FileNotFoundError:
                # No existing hint, try to create new one
                logger.debug(f"No existing hint found. Creating new hint {self._object_key}")
                if self._acquire_hint_with_condition(if_none_match="*"):
                    return True

            return False

    def release(self) -> None:
        """Release a hint."""
        with self._transition_lock:
            if not self._hint_object:
                return

            # Stop heartbeat thread if it's running
            if self._heartbeat_daemon and self._heartbeat_daemon.is_alive():
                self._stop_heartbeat.set()
                self._heartbeat_daemon.join(timeout=1.0)
                self._heartbeat_daemon = None

            # Delete the hint with if_match precondition
            self._storage_provider.delete_object(self._object_key, if_match=self._hint_object.metadata.etag)
            logger.debug(f"Released hint: {self._object_key}")
            self._hint_object = None  # Only clear object on successful release

    def __enter__(self) -> "DistributedHint":
        """Enter the context manager.

        :raises Exception: If hint cannot be acquired
        :return: Self
        """
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Release the hint when exiting the context."""
        self.release()

    def __del__(self) -> None:
        """Clean up resources when the object is garbage collected."""
        self.release()
