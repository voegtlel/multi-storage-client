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

import enum
import inspect
import json
import logging
import multiprocessing
import multiprocessing.managers
import opentelemetry.metrics as api_metrics
import opentelemetry.trace as api_trace
import os
import threading
from typing import Any, Dict, Optional, Tuple, Union
from .. import utils

# MSC telemetry prefers publishing raw samples when possible to support arbitrary post-hoc aggregations.
#
# Some setups, however, may need resampling to reduce sample volume. The resampling methods we use
# sacrifice temporal resolution to preserve other information. Which method is used depends on if
# the expected post-hoc aggregate function is decomposable:
#
# * Decomposable aggregate functions (e.g. count, sum, min, max).
#     * Use client-side aggregation.
#         * E.g. preserve total requests + errors.
# * Non-decomposable aggregate functions (e.g. average, percentile).
#     * Use decimation by an integer factor.
#         * E.g. preserve the shape of the latency distribution (unlike tail sampling).

_METRICS_EXPORTER_MAPPING = {
    "console": "opentelemetry.sdk.metrics.export.ConsoleMetricExporter",
    # Discards the output.
    #
    # For testing. Use instead of the in-memory exporter (to prevent memory leaks)
    # and the standard console exporter (to prevent stdout closed error logs and noise).
    "null": "opentelemetry.sdk.metrics.export.ConsoleMetricExporter",
    "otlp": "opentelemetry.exporter.otlp.proto.http.metric_exporter.OTLPMetricExporter",
}

_TRACE_EXPORTER_MAPPING = {
    "console": "opentelemetry.sdk.trace.export.ConsoleSpanExporter",
    # Discards the output.
    #
    # For testing. Use instead of the in-memory exporter (to prevent memory leaks)
    # and the standard console exporter (to prevent stdout closed error logs and noise).
    "null": "opentelemetry.sdk.trace.export.ConsoleSpanExporter",
    "otlp": "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
}


class Telemetry:
    """
    Telemetry resource provider.

    Instances shouldn't be copied between processes. Not fork-safe or pickleable.

    Instances can be shared between processes by registering with a :py:class:``multiprocessing.Manager`` and using proxy objects.
    """

    _meter_provider_cache_lock: threading.Lock
    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to provider.
    _meter_provider_cache: Dict[str, api_metrics.MeterProvider]
    _tracer_provider_cache_lock: threading.Lock
    # Map of config as a sorted JSON string (since dictionaries can't be hashed) to provider.
    _tracer_provider_cache: Dict[str, api_trace.TracerProvider]

    def __init__(self):
        self._meter_provider_cache_lock = threading.Lock()
        self._meter_provider_cache = {}
        self._tracer_provider_cache_lock = threading.Lock()
        self._tracer_provider_cache = {}

    def meter_provider(self, config: Dict[str, Any]) -> Optional[api_metrics.MeterProvider]:
        """
        Create or return an existing :py:class:``api_metrics.MeterProvider`` for a config.

        :param config: ``.opentelemetry.metrics`` config dict.
        :return: A :py:class:``api_metrics.MeterProvider`` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._meter_provider_cache_lock:
            if config_json in self._meter_provider_cache:
                return self._meter_provider_cache[config_json]
            else:
                if "exporter" in config:
                    try:
                        import opentelemetry.sdk.metrics as sdk_metrics
                        import opentelemetry.sdk.metrics.export as sdk_metrics_export

                        exporter_type = config["exporter"]["type"]
                        exporter_fully_qualified_name = _METRICS_EXPORTER_MAPPING.get(exporter_type, exporter_type)
                        exporter_module_name, exporter_class_name = exporter_fully_qualified_name.rsplit(".", 1)
                        cls = utils.import_class(exporter_class_name, exporter_module_name)
                        exporter_options = config["exporter"].get("options", {})
                        if exporter_type == "null":
                            exporter_options |= {"out": open(os.devnull, "w")}
                        # TODO: Auth options.
                        exporter: sdk_metrics_export.MetricExporter = cls(
                            **exporter_options,
                            # TODO: Custom gauge aggregators for raw samples + resampling.
                            #
                            # https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.view.html#opentelemetry.sdk.metrics.view.Aggregation
                            preferred_aggregation={},
                            preferred_temporality={},
                        )

                        reader: sdk_metrics_export.MetricReader = sdk_metrics_export.PeriodicExportingMetricReader(
                            exporter=exporter
                        )

                        return self._meter_provider_cache.setdefault(
                            config_json, sdk_metrics.MeterProvider(metric_readers=[reader])
                        )
                    except (AttributeError, ImportError):
                        logging.error(
                            "Failed to import OpenTelemetry Python SDK or exporter! Disabling metrics.", exc_info=True
                        )
                        return None
                else:
                    # Don't return a no-op meter provider to avoid unnecessary overhead.
                    logging.error("No exporter configured! Disabling metrics.")
                    return None

    def tracer_provider(self, config: Dict[str, Any]) -> Optional[api_trace.TracerProvider]:
        """
        Create or return an existing :py:class:``api_trace.TracerProvider`` for a config.

        :param config: ``.opentelemetry.traces`` config dict.
        :return: A :py:class:``api_trace.TracerProvider`` or ``None`` if no valid exporter is configured.
        """
        config_json = json.dumps(config, sort_keys=True)
        with self._tracer_provider_cache_lock:
            if config_json in self._tracer_provider_cache:
                return self._tracer_provider_cache[config_json]
            else:
                if "exporter" in config:
                    try:
                        import opentelemetry.sdk.trace as sdk_trace
                        import opentelemetry.sdk.trace.export as sdk_trace_export
                        import opentelemetry.sdk.trace.sampling as sdk_trace_sampling

                        exporter_type = config["exporter"]["type"]
                        exporter_fully_qualified_name = _TRACE_EXPORTER_MAPPING.get(exporter_type, exporter_type)
                        exporter_module_name, exporter_class_name = exporter_fully_qualified_name.rsplit(".", 1)
                        cls = utils.import_class(exporter_class_name, exporter_module_name)
                        exporter_options = config["exporter"].get("options", {})
                        if exporter_type == "null":
                            exporter_options |= {"out": open(os.devnull, "w")}
                        # TODO: Auth options.
                        exporter: sdk_trace_export.SpanExporter = cls(**exporter_options)

                        processor: sdk_trace.SpanProcessor = sdk_trace.SynchronousMultiSpanProcessor()
                        processor.add_span_processor(sdk_trace_export.BatchSpanProcessor(span_exporter=exporter))

                        # TODO: Add sampler to configuration schema.
                        sampler: sdk_trace_sampling.Sampler = sdk_trace_sampling.ALWAYS_ON

                        return self._tracer_provider_cache.setdefault(
                            config_json,
                            sdk_trace.TracerProvider(active_span_processor=processor, sampler=sampler),
                        )
                    except (AttributeError, ImportError):
                        logging.error(
                            "Failed to import OpenTelemetry Python SDK or exporter! Disabling traces.", exc_info=True
                        )
                        return None
                else:
                    logging.error("No exporter configured! Disabling traces.")
                    return None


# To share a single :py:class:``Telemetry`` within a process (e.g. local, manager).
#
# A manager's server processes shouldn't be forked, so this should be safe.
_TELEMETRY_LOCK = threading.Lock()
_TELEMETRY: Optional[Telemetry] = None


def _init() -> Telemetry:
    """
    Create or return an existing :py:class:``Telemetry``.

    :return: A telemetry resource provider.
    """
    global _TELEMETRY_LOCK
    global _TELEMETRY

    with _TELEMETRY_LOCK:
        if _TELEMETRY is None:
            _TELEMETRY = Telemetry()
    return _TELEMETRY


class TelemetryManager(multiprocessing.managers.BaseManager):
    """
    A :py:class:``multiprocessing.Manager`` for telemetry resources.

    The OpenTelemetry Python SDK isn't fork-safe since telemetry sample buffers can be duplicated.

    In addition, Python ≤3.12 doesn't call exit handlers for forked processes.
    This causes the OpenTelemetry Python SDK to not flush telemetry before exiting.

    * https://github.com/open-telemetry/opentelemetry-python/issues/4215
    * https://github.com/open-telemetry/opentelemetry-python/issues/3307

    Forking is multiprocessing's default start method for non-macOS POSIX systems until Python 3.14.

    * https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods

    To fully support multiprocessing, resampling + publishing is handled by
    a single process that's (ideally) a child of (i.e. directly under) the main process. This:

    * Relieves other processes of this work.
        * Avoids issues with duplicate samples when forking and unpublished samples when exiting forks.
    * Allows cross-process resampling.
    * Reuses a single connection pool to telemetry backends.

    The downside is it essentially re-introduces global interpreter lock (GIL) with
    additional IPC overhead. Telemetry operations, however, should be lightweight so
    this isn't expected to be a problem. Remote data store latency should still be
    the primary throughput limiter for storage clients.

    :py:class:``multiprocessing.Manager`` is used for this since it creates
    a separate server process for shared objects.

    Telemetry resources are provided as
    `proxy objects <https://docs.python.org/3/library/multiprocessing.html#proxy-objects>`_
    for location transparency.

    The documentation isn't particularly detailed, but others have written comprehensively on this:

    * https://zpz.github.io/blog/python-mp-manager-1
    * https://zpz.github.io/blog/python-mp-manager-2
    * https://zpz.github.io/blog/python-mp-manager-3

    By specification, metric and tracer providers must call shutdown on any
    underlying metric readers + span processors + exporters.

    * https://opentelemetry.io/docs/specs/otel/metrics/sdk#shutdown
    * https://opentelemetry.io/docs/specs/otel/trace/sdk#shutdown

    In the OpenTelemetry Python SDK, provider shutdown is called automatically
    by exit handlers (when they work at least). Consequently, clients should:

    * Only receive provider proxy objects.
        * Enables metric reader + span processor + exporter re-use across processes.
    * Never call shutdown on the provider proxy objects.
        * The shutdown exit handler is registered on the manager's server process.
        * ⚠️ We expect a finite number of providers (i.e. no dynamic configs) so we don't leak them.
    """

    pass


def _fully_qualified_name(c: type[Any]) -> str:
    """
    Return the fully qualified name for a class (e.g. ``module.Class``).

    For :py:class:``multiprocessing.Manager`` type IDs.
    """
    return ".".join([c.__module__, c.__qualname__])


# Metrics proxy object setup.
TelemetryManager.register(typeid=_fully_qualified_name(api_metrics.Counter))
TelemetryManager.register(typeid=_fully_qualified_name(api_metrics._Gauge))
TelemetryManager.register(
    typeid=_fully_qualified_name(api_metrics.Meter),
    method_to_typeid={
        api_metrics.Meter.create_counter.__name__: _fully_qualified_name(api_metrics.Counter),
        api_metrics.Meter.create_gauge.__name__: _fully_qualified_name(api_metrics._Gauge),
    },
)
TelemetryManager.register(
    typeid=_fully_qualified_name(api_metrics.MeterProvider),
    method_to_typeid={api_metrics.MeterProvider.get_meter.__name__: _fully_qualified_name(api_metrics.Meter)},
)

# Traces proxy object setup.
TelemetryManager.register(
    typeid=_fully_qualified_name(api_trace.Span),
    # Non-public methods (i.e. ones starting with a ``_``) are omitted by default.
    #
    # We need ``__enter__`` and ``__exit__`` so the ``Span`` can be used as a ``ContextManager``.
    exposed=[name for name, _ in inspect.getmembers(api_trace.Span, predicate=inspect.isfunction)],
    method_to_typeid={api_trace.Span.__enter__.__name__: _fully_qualified_name(api_trace.Span)},
)
TelemetryManager.register(
    typeid=_fully_qualified_name(api_trace.Tracer),
    # Can't proxy ``Tracer.start_as_current_span`` since it returns a generator (not pickleable)
    # and tries to use the process-local global context (in this case, the manager's server process).
    #
    # Instead, spans should be constructed by:
    #
    # 1. Calling ``opentelemetry.context.get_current()`` to get the process-local global context (pickleable).
    # 2. Creating a new span with the process-local global context.
    # 3. Calling ``opentelemetry.trace.use_span()`` with the span and ``end_on_exit=True``.
    method_to_typeid={api_trace.Tracer.start_span.__name__: _fully_qualified_name(api_trace.Span)},
)
TelemetryManager.register(
    typeid=_fully_qualified_name(api_trace.TracerProvider),
    method_to_typeid={api_trace.TracerProvider.get_tracer.__name__: _fully_qualified_name(api_trace.Tracer)},
)


# Telemetry proxy object setup.
#
# This should be the only registered type with a ``callable``.
# It's the only type we create directly with a ``TelemetryManager``.
TelemetryManager.register(
    typeid=Telemetry.__name__,
    callable=_init,
    method_to_typeid={
        Telemetry.meter_provider.__name__: _fully_qualified_name(api_metrics.MeterProvider),
        Telemetry.tracer_provider.__name__: _fully_qualified_name(api_trace.TracerProvider),
    },
)


# To share :py:class:``Telemetry`` proxy objects within a process (e.g. client, server).
#
# Forking isn't expected to happen while this is held (may lead to a deadlock).
_TELEMETRY_PROXIES_LOCK = threading.Lock()
# Map of init options as a sorted JSON string (since dictionaries can't be hashed) to telemetry resources.
_TELEMETRY_PROXIES: Dict[str, Telemetry] = {}


class TelemetryMode(enum.Enum):
    #: Keep everything local to the process (not fork-safe).
    LOCAL = "local"
    #: Start + connect to a telemetry IPC server.
    SERVER = "server"
    #: Connect to a telemetry IPC server.
    CLIENT = "client"


def init(
    mode: TelemetryMode = TelemetryMode.SERVER if multiprocessing.parent_process() is None else TelemetryMode.CLIENT,
    # Avoid registered and well-known ports.
    #
    # https://www.iana.org/assignments/service-names-port-numbers/service-names-port-numbers.xhtml
    # https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers
    #
    # Default ports for the OpenTelemetry Protocol (OTLP):
    #
    # * gRPC: 4317
    # * HTTP: 4318
    #
    # https://opentelemetry.io/docs/specs/otlp#otlpgrpc-default-port
    # https://opentelemetry.io/docs/specs/otlp#otlphttp-default-port
    address: Optional[Union[str, Tuple[str, int]]] = os.environ.get("MSC_TELEMETRY_ADDRESS", ("127.0.0.1", 4315)),
) -> Telemetry:
    """
    Create or return an existing :py:class:``Telemetry`` or :py:class:``Telemetry`` proxy object.

    :param mode: How to create a :py:class:``Telemetry`` object.
    :param address: Telemetry IPC server address. Ignored if the mode is local.
    :return: A telemetry resource provider.
    """

    if mode == TelemetryMode.LOCAL:
        return _init()
    elif mode == TelemetryMode.SERVER or mode == TelemetryMode.CLIENT:
        global _TELEMETRY_PROXIES_LOCK
        global _TELEMETRY_PROXIES

        init_options = {"mode": mode.value, "address": address}
        init_options_json = json.dumps(init_options, sort_keys=True)

        with _TELEMETRY_PROXIES_LOCK:
            if init_options_json in _TELEMETRY_PROXIES:
                return _TELEMETRY_PROXIES[init_options_json]
            else:
                telemetry_manager = TelemetryManager(
                    address=address,
                    authkey=str.encode("multistorageclient-telemetry"),
                    # Use spawn instead of the platform-specific default (may be fork) to avoid aforementioned issues with fork.
                    ctx=multiprocessing.get_context(method="spawn"),
                )

                if mode == TelemetryMode.SERVER:
                    logging.debug(f"Creating telemetry manager server at {telemetry_manager.address}.")
                    try:
                        telemetry_manager.start()
                        logging.debug(f"Started telemetry manager server at {telemetry_manager.address}.")
                    except Exception as e:
                        logging.error(
                            f"Failed to create telemetry manager server at {telemetry_manager.address}!", exc_info=True
                        )
                        raise e

                logging.debug(f"Connecting to telemetry manager server at {telemetry_manager.address}.")
                try:
                    telemetry_manager.connect()
                    logging.debug(f"Connected to telemetry manager server at {telemetry_manager.address}.")
                except Exception as e:
                    logging.error(
                        f"Failed to connect to telemetry manager server at {telemetry_manager.address}!", exc_info=True
                    )
                    raise e

                return _TELEMETRY_PROXIES.setdefault(init_options_json, telemetry_manager.Telemetry())  # pyright: ignore [reportAttributeAccessIssue]
    else:
        raise ValueError(f"Unsupported telemetry mode: {mode}")
