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

from multiprocessing import get_context
from multiprocessing.managers import BaseProxy
from multiprocessing.pool import Pool
import multistorageclient.telemetry as telemetry
from opentelemetry.context import get_current
from opentelemetry.metrics import Counter, _Gauge as Gauge, Meter, MeterProvider
from opentelemetry.trace import Span, Tracer, TracerProvider, use_span
import pytest
from typing import Any, Dict, Optional, Tuple, Union


def test_telemetry_local_objects():
    opentelemetry_config = {"metrics": {"exporter": {"type": "null"}}, "traces": {"exporter": {"type": "null"}}}

    # Make sure caching works.
    telemetry_resources_str: Optional[str] = None
    meter_provider_str: Optional[str] = None
    tracer_provider_str: Optional[str] = None

    for _ in range(2):
        telemetry_resources: telemetry.Telemetry = telemetry.init(mode=telemetry.TelemetryMode.LOCAL)
        assert not isinstance(telemetry_resources, BaseProxy)

        if telemetry_resources_str is None:
            telemetry_resources_str = str(telemetry_resources)
        else:
            assert telemetry_resources_str == str(telemetry_resources)

        meter_provider: Optional[MeterProvider] = telemetry_resources.meter_provider(opentelemetry_config["metrics"])
        assert meter_provider is not None
        assert not isinstance(meter_provider, BaseProxy)

        if meter_provider_str is None:
            meter_provider_str = str(meter_provider)
        else:
            assert meter_provider_str == str(meter_provider)

        meter: Meter = meter_provider.get_meter("meter")
        assert not isinstance(meter, BaseProxy)

        counter: Counter = meter.create_counter("counter")
        assert not isinstance(counter, BaseProxy)

        counter.add(1)

        gauge: Gauge = meter.create_gauge("gauge")
        assert not isinstance(gauge, BaseProxy)

        gauge.set(1)

        tracer_provider: Optional[TracerProvider] = telemetry_resources.tracer_provider(opentelemetry_config["traces"])
        assert tracer_provider is not None
        assert not isinstance(tracer_provider, BaseProxy)

        if tracer_provider_str is None:
            tracer_provider_str = str(tracer_provider)
        else:
            assert tracer_provider_str == str(tracer_provider)

        tracer: Tracer = tracer_provider.get_tracer("tracer")
        assert not isinstance(tracer, BaseProxy)

        span: Span = tracer.start_span("span", context=get_current())
        assert not isinstance(span, BaseProxy)

        with use_span(span, end_on_exit=True) as active_span:  # pyright: ignore [reportCallIssue]
            active_span.add_event("event")


# Invoke in a separate process.
def _test_telemetry_proxy_objects_client(
    manager_address: Union[str, Tuple[str, int]],
    opentelemetry_config: Dict[str, Any],
    # Make sure caching works across processes.
    #
    # BaseProxy.__str__ returns the __str__ of the referent.
    # BaseProxy.__repr__ returns the __repr__ of the proxy object.
    #
    # https://docs.python.org/3/library/multiprocessing.html#proxy-objects
    telemetry_resources_referent_str: str,
    telemetry_resources_proxy_repr: str,
    meter_provider_referent_str: str,
    meter_provider_proxy_repr: str,
    tracer_provider_referent_str: str,
    tracer_provider_proxy_repr: str,
):
    telemetry_resources: telemetry.Telemetry = telemetry.init(
        mode=telemetry.TelemetryMode.CLIENT, address=manager_address
    )
    assert isinstance(telemetry_resources, BaseProxy)

    assert telemetry_resources_referent_str == str(telemetry_resources)
    assert telemetry_resources_proxy_repr != repr(telemetry_resources)

    meter_provider: Optional[MeterProvider] = telemetry_resources.meter_provider(opentelemetry_config["metrics"])
    assert meter_provider is not None
    assert isinstance(meter_provider, BaseProxy)

    assert meter_provider_referent_str == str(meter_provider)
    assert meter_provider_proxy_repr != repr(meter_provider)

    meter: Meter = meter_provider.get_meter("meter")
    assert isinstance(meter, BaseProxy)

    counter: Counter = meter.create_counter("counter")
    assert isinstance(counter, BaseProxy)

    counter.add(1)

    gauge: Gauge = meter.create_gauge("gauge")
    assert isinstance(gauge, BaseProxy)

    gauge.set(1)

    tracer_provider: Optional[TracerProvider] = telemetry_resources.tracer_provider(opentelemetry_config["traces"])
    assert tracer_provider is not None
    assert isinstance(tracer_provider, BaseProxy)

    assert tracer_provider_referent_str == str(tracer_provider)
    assert tracer_provider_proxy_repr != repr(tracer_provider)

    tracer: Tracer = tracer_provider.get_tracer("tracer")
    assert isinstance(tracer, BaseProxy)

    # Passes the current process' span context to the remote constructor.
    span: Span = tracer.start_span("span", context=get_current())
    assert isinstance(span, BaseProxy)

    with use_span(span, end_on_exit=True) as active_span:  # pyright: ignore [reportCallIssue]
        active_span.add_event("event")


@pytest.mark.parametrize(argnames=["process_start_method", "manager_port"], argvalues=[["fork", 4315], ["spawn", 4316]])
def test_telemetry_proxy_objects(process_start_method: str, manager_port: int):
    manager_address = ("127.0.0.1", manager_port)
    opentelemetry_config = {"metrics": {"exporter": {"type": "null"}}, "traces": {"exporter": {"type": "null"}}}

    # --------------------------------------------------------------------------------
    #
    # Server mode.
    #
    # We need to keep a reference to the resources so the server stays alive during
    # the client mode portion.
    #
    # --------------------------------------------------------------------------------

    telemetry_resources: telemetry.Telemetry = telemetry.init(
        mode=telemetry.TelemetryMode.SERVER, address=manager_address
    )
    assert isinstance(telemetry_resources, BaseProxy)

    meter_provider: Optional[MeterProvider] = telemetry_resources.meter_provider(opentelemetry_config["metrics"])
    assert meter_provider is not None
    assert isinstance(meter_provider, BaseProxy)

    meter: Meter = meter_provider.get_meter("meter")
    assert isinstance(meter, BaseProxy)

    counter: Counter = meter.create_counter("counter")
    assert isinstance(counter, BaseProxy)

    counter.add(1)

    gauge: Gauge = meter.create_gauge("gauge")
    assert isinstance(gauge, BaseProxy)

    gauge.set(1)

    tracer_provider: Optional[TracerProvider] = telemetry_resources.tracer_provider(opentelemetry_config["traces"])
    assert tracer_provider is not None
    assert isinstance(tracer_provider, BaseProxy)

    tracer: Tracer = tracer_provider.get_tracer("tracer")
    assert isinstance(tracer, BaseProxy)

    # Passes the current process' span context to the remote constructor.
    span: Span = tracer.start_span("span", context=get_current())
    assert isinstance(span, BaseProxy)

    with use_span(span, end_on_exit=True) as active_span:  # pyright: ignore [reportCallIssue]
        active_span.add_event("event")

    # --------------------------------------------------------------------------------
    #
    # Client mode.
    #
    # --------------------------------------------------------------------------------

    pool = Pool(context=get_context(method=process_start_method))

    pool.apply(
        _test_telemetry_proxy_objects_client,
        kwds={
            "manager_address": manager_address,
            "opentelemetry_config": opentelemetry_config,
            "telemetry_resources_referent_str": str(telemetry_resources),
            "telemetry_resources_proxy_repr": repr(telemetry_resources),
            "meter_provider_referent_str": str(meter_provider),
            "meter_provider_proxy_repr": repr(meter_provider),
            "tracer_provider_referent_str": str(tracer_provider),
            "tracer_provider_proxy_repr": repr(tracer_provider),
        },
    )
