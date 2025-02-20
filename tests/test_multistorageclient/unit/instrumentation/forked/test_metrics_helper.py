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
from unittest.mock import MagicMock, Mock

import pytest
from multistorageclient.instrumentation.utils import StorageProviderMetricsHelper


@pytest.mark.forked
def test_record_duration():
    """
    Test the record_duration method of MetricHelper.
    """
    mock_duration_histogram = MagicMock()
    mock_object_size_histogram = MagicMock()
    mock_duration_percentiles = MagicMock()

    metrics_helper = StorageProviderMetricsHelper(attributes={"job_id": "training-job-00001"})
    metrics_helper._duration_histogram = mock_duration_histogram
    metrics_helper._object_size_histogram = mock_object_size_histogram
    metrics_helper._duration_percentiles = mock_duration_percentiles
    metrics_helper._is_metrics_enabled = Mock(return_value=True)

    metrics_helper.record_duration(duration=2, provider="s3", operation="GET", bucket="my-bucket", status_code=200)

    expected_attributes = {
        "provider": "s3",
        "operation": "GET",
        "bucket": "my-bucket",
        "status_code": 200,
        "proc_id": os.getpid(),
        "job_id": "training-job-00001",
    }

    # Duration should be converted to milliseconds (2 seconds * 1000 = 2000 ms)
    mock_duration_histogram.record.assert_called_once_with(2000, attributes=expected_attributes)
    mock_duration_percentiles.record.assert_called_once_with(2000, attributes=expected_attributes)
