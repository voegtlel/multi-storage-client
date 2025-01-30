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
import logging
import threading
from typing import Any, Dict, Union, Optional

import requests
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.trace.sampling import DEFAULT_ON, ParentBased, StaticSampler
from requests.adapters import HTTPAdapter

from .auth import AccessTokenProvider, AccessTokenProviderFactory
from ..utils import import_class

_TRACE_SAMPLER_MODULE_NAME = "opentelemetry.sdk.trace.sampling"

_OTEL_TRACE_EXPORTER_MAPPING = {
    "console": "opentelemetry.sdk.trace.export.ConsoleSpanExporter",
    "otlp": "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
}

_OTEL_METRIC_EXPORTER_MAPPING = {
    "console": "opentelemetry.sdk.metrics.export.ConsoleMetricExporter",
    "otlp": "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter",
}

_RESOURCE = Resource.create(
    {
        "service.name": "multistorageclient",
        "service.namespace": "client",
        "service.version": "1.0",
    }
)

_IS_SETUP_DONE = False


LATENCY_HISTOGRAM_BUCKETS = [0, 100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200, 102400]

OBJECT_SIZE_HISTOGRAM_BUCKETS = [0, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120]

MAX_RETRIES = 5

_setup_lock = threading.Lock()

logger = logging.Logger(__name__)


class CustomHTTPAdapter(HTTPAdapter):
    """
    Custom HTTP adapter for retry and auth
    """

    def __init__(self, auth_provider, *args, **kwargs):
        kwargs["max_retries"] = kwargs.get("max_retries", MAX_RETRIES)
        super().__init__(*args, **kwargs)
        self.auth_provider = auth_provider

    def send(self, request, *args, **kwargs):
        if self.auth_provider:
            token = self.auth_provider.get_token()
            if token:
                request.headers["Authorization"] = f"Bearer {token}"
            else:
                logger.warning("Warning: Failed to retrieve authentication token. Request might fail.")
        return super().send(request, *args, **kwargs)


def create_session(auth_provider: Optional[AccessTokenProvider] = None) -> requests.Session:
    session = requests.Session()

    # Disable keep-alive
    session.headers.update({"Connection": "close"})

    # use adaptor for retry & auth
    adapter = CustomHTTPAdapter(auth_provider, max_retries=MAX_RETRIES)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def setup_opentelemetry(config: Dict[str, Any]) -> None:
    """
    Setup global OpenTelemetry providers for trace/metrics
    """

    with _setup_lock:
        global _IS_SETUP_DONE

        # This function should only be called once even multiple storage clients
        # with different profiles have been created
        if _IS_SETUP_DONE:
            return

        if config:
            trace_config_dict = config.get("traces", None)
            metric_config_dict = config.get("metrics", None)

            if trace_config_dict is not None:
                # exporter
                trace_exporter_dict = trace_config_dict.get("exporter", None)
                if trace_exporter_dict:
                    module_name, class_name = _OTEL_TRACE_EXPORTER_MAPPING[trace_exporter_dict["type"]].rsplit(".", 1)
                    options = trace_exporter_dict.get("options", {})
                    cls = import_class(class_name, module_name)
                    auth_dict = trace_exporter_dict.get("auth", {})
                    auth_provider = AccessTokenProviderFactory.create_access_token_provider(auth_dict)
                    if class_name != "console":
                        options["session"] = create_session(auth_provider)
                    exporter = cls(**options)
                else:
                    # provide default console exporter if dict is not provided
                    exporter = ConsoleSpanExporter()

                # sampler
                trace_sampler_dict = trace_config_dict.get("sampler", None)
                sampler: Union[StaticSampler, ParentBased, None]
                if trace_sampler_dict:
                    class_name = trace_sampler_dict["type"]
                    options = trace_exporter_dict.get("options", {})
                    cls_or_obj = import_class(class_name, _TRACE_SAMPLER_MODULE_NAME)
                    if isinstance(cls_or_obj, StaticSampler):
                        sampler = cls_or_obj
                    else:
                        sampler = cls_or_obj(**options)
                else:
                    # provide default sampler if dict is not provided
                    sampler = DEFAULT_ON

                # set up trace provider for current process
                tracer_provider = TracerProvider(resource=_RESOURCE, sampler=sampler)
                tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
                trace.set_tracer_provider(tracer_provider)

            if metric_config_dict is not None:
                metric_exporter_dict = metric_config_dict.get("exporter", None)
                if metric_config_dict:
                    exporter_type = metric_exporter_dict["type"]
                    module_name, class_name = _OTEL_METRIC_EXPORTER_MAPPING[exporter_type].rsplit(".", 1)
                    options = metric_exporter_dict.get("options", {})
                    auth_dict = metric_exporter_dict.get("auth", {})
                    auth_provider = AccessTokenProviderFactory.create_access_token_provider(auth_dict)
                    if exporter_type != "console":
                        options["session"] = create_session(auth_provider)
                    cls = import_class(class_name, module_name)
                    exporter = cls(**options)
                else:
                    exporter = ConsoleMetricExporter()

                # set up meter provider for current process
                metric_reader = PeriodicExportingMetricReader(exporter)
                custom_views = [
                    View(
                        instrument_name="storageclient_api_duration",
                        aggregation=ExplicitBucketHistogramAggregation(LATENCY_HISTOGRAM_BUCKETS),
                    ),
                    View(
                        instrument_name="storageclient_object_size",
                        aggregation=ExplicitBucketHistogramAggregation(OBJECT_SIZE_HISTOGRAM_BUCKETS),
                    ),
                ]
                meter_provider = MeterProvider(resource=_RESOURCE, metric_readers=[metric_reader], views=custom_views)
                metrics.set_meter_provider(meter_provider)

            # Only set the _IS_SETUP_DONE = true if the providers are successfully set once
            _IS_SETUP_DONE = True
