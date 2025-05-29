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
import pickle
from collections.abc import Mapping
from typing import Any
from unittest.mock import MagicMock, Mock, call

import pytest

from multistorageclient.instrumentation.utils import (
    DURATION_P50_GAUGE,
    DURATION_P99_GAUGE,
    DURATION_P999_GAUGE,
    CacheManagerMetricsHelper,
    StorageProviderMetricsHelper,
    TDigestPercentiles,
    collect_default_attributes,
)


def test_merge_attributes():
    """
    Test the _merge_attributes method of MetricHelper.
    """
    metrics_helper = StorageProviderMetricsHelper(attributes={"job_id": "training-job-00001"})
    merged_attributes = metrics_helper._merge_attributes({"operation": "GET"})

    assert merged_attributes == {"job_id": "training-job-00001", "operation": "GET"}


def test_record_object_size():
    """
    Test the record_object_size method of MetricHelper.
    """
    mock_duration_histogram = MagicMock()
    mock_object_size_histogram = MagicMock()
    mock_object_size_percentiles = MagicMock()

    metrics_helper = StorageProviderMetricsHelper(attributes={"job_id": "training-job-00001"})
    metrics_helper._duration_histogram = mock_duration_histogram
    metrics_helper._object_size_histogram = mock_object_size_histogram
    metrics_helper._object_size_percentiles = mock_object_size_percentiles
    metrics_helper._is_metrics_enabled = Mock(return_value=True)

    metrics_helper.record_object_size(
        object_size=10485760,  # 10 MB in bytes
        provider="s3",
        operation="PUT",
        bucket="my-bucket",
        status_code=200,
    )

    expected_attributes = {
        "provider": "s3",
        "operation": "PUT",
        "bucket": "my-bucket",
        "status_code": 200,
        "proc_id": os.getpid(),
        "job_id": "training-job-00001",
    }

    # Object size should be converted to MB (10485760 bytes / (1024 * 1024) = 10 MB)
    mock_object_size_histogram.record.assert_called_once_with(10.0, attributes=expected_attributes)
    mock_object_size_percentiles.record.assert_called_once_with(10.0, attributes=expected_attributes)


def test_record_object_size_not_record_metrics():
    """
    Test the record_object_size method of MetricHelper.
    """
    mock_duration_histogram = MagicMock()
    mock_object_size_histogram = MagicMock()
    mock_object_size_percentiles = MagicMock()

    metrics_helper = StorageProviderMetricsHelper(attributes={"job_id": "training-job-00001"})
    metrics_helper._duration_histogram = mock_duration_histogram
    metrics_helper._object_size_histogram = mock_object_size_histogram
    metrics_helper._object_size_percentiles = mock_object_size_percentiles
    metrics_helper._is_metrics_enabled = Mock(return_value=False)

    metrics_helper.record_object_size(
        object_size=10485760,  # 10 MB in bytes
        provider="s3",
        operation="PUT",
        bucket="my-bucket",
        status_code=200,
    )

    mock_object_size_histogram.record.assert_not_called()
    mock_object_size_percentiles.record.assert_not_called()


@pytest.fixture
def mock_slurm_env() -> Mapping[str, Any]:
    """
    Fixture for mocking Slurm environment variables.
    """
    return {
        "SLURM_JOB_ID": "12345",
        "SLURM_JOB_NAME": "test_job",
        "SLURM_JOB_USER": "test_user",
        "SLURM_NODEID": "1",
        "SLURM_CLUSTER_NAME": "test_cluster",
    }


@pytest.fixture
def mock_k8s_env() -> Mapping[str, Any]:
    """
    Fixture for mocking K8S environment variables.
    """
    return {"KUBERNETES_SERVICE_HOST": "10.0.0.2", "HOSTNAME": "my_pod_name"}


@pytest.fixture
def mock_msc_env() -> Mapping[str, Any]:
    """
    Fixture for mocking MSC environment variables.
    """
    return {
        "MSC_JOB_ID": "54321",
        "MSC_JOB_NAME": "msc_test_job",
        "MSC_JOB_USER": "msc_user",
        "MSC_NODEID": "2",
        "MSC_CLUSTER_NAME": "msc_cluster",
        "MSC_CONFIG": "/path/to/config",
    }


@pytest.fixture
def partial_slurm_env() -> Mapping[str, Any]:
    """
    Fixture for mocking partial Slurm environment variables.
    """
    return {
        "SLURM_JOB_ID": "12345",
        "SLURM_JOB_NAME": "test_job",
        "SLURM_JOB_USER": None,  # Missing some attributes
        "SLURM_NODEID": None,
        "SLURM_CLUSTER_NAME": "test_cluster",
    }


def test_collect_default_attributes_slurm(mock_slurm_env):
    """
    Test collecting attributes when Slurm variables are present.
    """
    result = collect_default_attributes(env=mock_slurm_env)
    expected = {
        "job_id": "12345",
        "job_name": "test_job",
        "job_user": "test_user",
        "node_id": "1",
        "cluster": "test_cluster",
    }
    assert result == expected


def test_collect_default_attributes_k8s(mock_k8s_env, mock_msc_env):
    """
    Test collecting attributes when K8S and MSC environment variables are present.
    """
    merged_env = {**mock_k8s_env, **mock_msc_env}

    result = collect_default_attributes(env=merged_env)
    expected = {
        "job_id": "54321",
        "job_name": "msc_test_job",
        "job_user": "msc_user",
        "node_id": "my_pod_name",  # retrieved from K8S environment variables.
        "cluster": "msc_cluster",
    }
    assert result == expected


def test_collect_default_attributes_msc(mock_msc_env):
    """
    Test collecting attributes when MSC variables are present and Slurm variables are absent.
    """
    result = collect_default_attributes(env=mock_msc_env)
    expected = {
        "job_id": "54321",
        "job_name": "msc_test_job",
        "job_user": "msc_user",
        "node_id": "2",
        "cluster": "msc_cluster",
    }
    assert result == expected


def test_collect_default_attributes_no_env():
    """
    Test collecting attributes when no relevant environment variables are present.
    """
    empty_env = {}
    result = collect_default_attributes(env=empty_env)
    expected = {}
    assert result == expected


def test_collect_default_attributes_partial_slurm(partial_slurm_env):
    """
    Test collecting attributes when only partial Slurm variables are present.
    """
    result = collect_default_attributes(env=partial_slurm_env)
    expected = {"job_id": "12345", "job_name": "test_job", "cluster": "test_cluster"}
    assert result == expected


def test_collect_default_attributes_partial_slurm_with_msc_envs(partial_slurm_env, mock_msc_env):
    """
    Test collecting attributes when only partial Slurm variables are present and MSC env variables are present.
    """
    merged_env = {**partial_slurm_env, **mock_msc_env}

    result = collect_default_attributes(env=merged_env)
    expected = {
        "job_id": "12345",
        "job_name": "test_job",
        "job_user": "msc_user",  # retrieved from MSC environment variables.
        "node_id": "2",  # retrieved from MSC environment variables.
        "cluster": "test_cluster",
    }
    assert result == expected


def test_multiple_records_with_attributes():
    p50_gauge = MagicMock()
    p99_gauge = MagicMock()
    p999_gauge = MagicMock()

    tdigest_percentiles = TDigestPercentiles(p50_gauge, p99_gauge, p999_gauge)

    get_200_attributes = {"method": "GET", "status": "200", "proc_id": os.getpid()}
    post_500_attributes = {"method": "POST", "status": "500", "proc_id": os.getpid()}

    tdigest_percentiles.record(50, attributes=get_200_attributes)
    tdigest_percentiles.record(150, attributes=get_200_attributes)
    tdigest_percentiles.record(200, attributes=post_500_attributes)

    p50_gauge.assert_has_calls(
        [
            call.set(50.0, get_200_attributes),
            call.set(150.0, get_200_attributes),
            call.set(200.0, post_500_attributes),
        ]
    )

    p99_gauge.assert_has_calls(
        [
            call.set(50.0, get_200_attributes),
            call.set(150.0, get_200_attributes),
            call.set(200.0, post_500_attributes),
        ]
    )

    p999_gauge.assert_has_calls(
        [
            call.set(50.0, get_200_attributes),
            call.set(150.0, get_200_attributes),
            call.set(200.0, post_500_attributes),
        ]
    )

    assert len(tdigest_percentiles._tdigests) == 2


def test_cache_manager_metrics_helper():
    counter = MagicMock()
    metrics_helper = CacheManagerMetricsHelper(attributes={"job_id": "training-job-00001"})
    metrics_helper._counter = counter

    expected_attributes = {
        "job_id": "training-job-00001",
        "operation": "SET",
        "success": True,
        "proc_id": os.getpid(),
    }

    metrics_helper.increase(operation="SET", success=True)
    counter.add.assert_called_once_with(1, attributes=expected_attributes)


def test_pickle_tdigest():
    # Get a reference to the module itself
    import multistorageclient.instrumentation.utils as instrument_utils

    p50_gauge = DURATION_P50_GAUGE
    p99_gauge = DURATION_P99_GAUGE
    p999_gauge = DURATION_P999_GAUGE

    tdigest_percentiles = instrument_utils.TDigestPercentiles(p50_gauge, p99_gauge, p999_gauge)
    tdigest_percentiles.record(1000, attributes={"key": "value"})

    data = pickle.dumps(tdigest_percentiles)
    tdigest_percentiles_copy = pickle.loads(data)

    key = (("key", "value"),)
    assert len(tdigest_percentiles._tdigests) == len(tdigest_percentiles_copy._tdigests)

    # Both tdigests should have the same summary
    assert tdigest_percentiles._tdigests[key].to_string() == tdigest_percentiles_copy._tdigests[key].to_string()
