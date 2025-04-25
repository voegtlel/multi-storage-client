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

import unittest
from unittest.mock import MagicMock, patch
from opentelemetry.trace import StatusCode
from multistorageclient.instrumentation.utils import file_tracer
from typing import Any
import pytest


class MockManagedFile:
    def __init__(self, storage_provider: str, remote_path: str, mode: str, local_path: str) -> None:
        self._trace_span = None

    @file_tracer
    def read(self, size: int) -> bytes:
        return b"test data"

    @file_tracer
    def write(self, data: bytes) -> None:
        pass

    @file_tracer
    def close(self) -> None:
        pass


class TestFileTracer(unittest.TestCase):
    @patch("multistorageclient.instrumentation.utils.TRACER.start_span")
    def test_read_tracing(self, mock_start_span: MagicMock) -> None:
        mock_span = MagicMock()
        mock_span.attributes = {}
        mock_start_span.return_value = mock_span

        managed_file = MockManagedFile("s3", "path/to/file", "r", "/local/file")
        managed_file.read(10)

        mock_start_span.assert_called_with("read", context=None)
        mock_span.set_attribute.assert_any_call("size", 10)
        mock_span.set_attribute.assert_any_call("operation_count", 1)
        mock_span.set_attribute.assert_any_call("bytes_read", 9)
        mock_span.set_status.assert_called_with(StatusCode.OK)

    @patch("multistorageclient.instrumentation.utils.TRACER.start_span")
    def test_write_tracing(self, mock_start_span: MagicMock) -> None:
        mock_span = MagicMock()
        mock_span.attributes = {}
        mock_start_span.return_value = mock_span

        managed_file = MockManagedFile("s3", "path/to/file", "w", "/local/file")
        managed_file.write(b"hello")

        mock_start_span.assert_called_with("write", context=None)
        mock_span.set_attribute.assert_any_call("operation_count", 1)
        mock_span.set_attribute.assert_any_call("bytes_written", 5)
        mock_span.set_status.assert_called_with(StatusCode.OK)

    @pytest.mark.skip(reason="Skipping test_merge_spans")
    @patch("multistorageclient.instrumentation.utils.TRACER.start_span")
    def test_merge_spans(self, mock_start_span: MagicMock) -> None:
        mock_span = MagicMock()
        attributes_dict = {}

        def mock_get_attribute(key: str, default: Any = None) -> Any:
            return attributes_dict.get(key, default)

        def mock_set_attribute(key: str, value: Any) -> None:
            attributes_dict[key] = value

        mock_span.attributes = attributes_dict
        mock_span.set_attribute.side_effect = mock_set_attribute
        mock_start_span.return_value = mock_span

        managed_file = MockManagedFile("s3", "path/to/file", "r", "/local/file")
        # Multiple read operations
        managed_file.read(10)
        mock_span.set_attribute.assert_any_call("bytes_read", 9)
        managed_file.read(10)
        mock_span.set_attribute.assert_any_call("bytes_read", 18)
        managed_file.read(10)
        mock_span.set_attribute.assert_any_call("bytes_read", 27)

        # Verify operation count was incremented
        mock_span.set_attribute.assert_any_call("operation_count", 3)


if __name__ == "__main__":
    unittest.main()
