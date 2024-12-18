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


class MockManagedFile:
    def __init__(self, storage_provider: str, remote_path: str, mode: str, local_path: str) -> None:
        self._trace_span = None

    @file_tracer
    def read(self, size: int) -> bytes:
        return b"test data"

    @file_tracer
    def write(self, data: bytes) -> None:
        pass


class TestFileTracer(unittest.TestCase):
    @patch("multistorageclient.instrumentation.utils.TRACER.start_as_current_span")
    def test_read_tracing(self, mock_start_as_current_span: MagicMock) -> None:
        mock_span = MagicMock()
        mock_start_as_current_span.return_value.__enter__.return_value = mock_span

        managed_file = MockManagedFile("s3", "path/to/file", "r", "/local/file")
        managed_file.read(10)

        mock_start_as_current_span.assert_called_with("read", context=None)
        mock_span.set_attribute.assert_any_call("function_name", "read")
        mock_span.set_attribute.assert_any_call("size", 10)
        mock_span.set_status.assert_called_with(StatusCode.OK)

    @patch("multistorageclient.instrumentation.utils.TRACER.start_as_current_span")
    def test_write_tracing(self, mock_start_as_current_span: MagicMock) -> None:
        mock_span = MagicMock()
        mock_start_as_current_span.return_value.__enter__.return_value = mock_span

        managed_file = MockManagedFile("s3", "path/to/file", "w", "/local/file")
        managed_file.write(b"hello")

        mock_start_as_current_span.assert_called_with("write", context=None)
        mock_span.set_attribute.assert_any_call("function_name", "write")
        mock_span.set_attribute.assert_any_call("bytes_written", 5)
        mock_span.set_status.assert_called_with(StatusCode.OK)


if __name__ == "__main__":
    unittest.main()
