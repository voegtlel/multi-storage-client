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

from typing import Any, Optional

from opentelemetry.sdk.trace import ReadableSpan, Span
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.trace import StatusCode


class ErrorAwareBatchSpanProcessor(BatchSpanProcessor):
    """
    A span processor that implements tail sampling by exporting traces containing errors or long-running spans.

    Unlike traditional sampling which decides at span creation, this processor makes sampling decisions
    after spans complete, allowing it to capture important traces based on their outcomes (errors) or
    performance characteristics (duration).

    Note: This processor can only be used with OpenTelemetry's DEFAULT_ON sampler. Other built-in samplers,
    like TraceIdRatioBased, make sampling decisions at span creation time, which conflicts with this tail sampling approach.
    """

    def __init__(
        self,
        span_exporter: SpanExporter,
        long_running_threshold_ms: int = 10000,
        max_queue_size: int = BatchSpanProcessor._default_max_queue_size(),
        schedule_delay_millis: float = BatchSpanProcessor._default_schedule_delay_millis(),
        export_timeout_millis: float = BatchSpanProcessor._default_export_timeout_millis(),
    ):
        """Initialize the error-aware batch span processor.

        Args:
            span_exporter: The exporter to use for sending spans.
            long_running_threshold_ms: The threshold in milliseconds for considering a span as long-running.
            max_queue_size: The maximum number of spans to batch before exporting.
            schedule_delay_millis: The delay in milliseconds between checking the queue for spans to export.
            export_timeout_millis: The timeout in milliseconds for exporting spans.
        """
        super().__init__(
            span_exporter,
            max_queue_size=max_queue_size,
            schedule_delay_millis=schedule_delay_millis,
            export_timeout_millis=export_timeout_millis,
        )
        self.long_running_threshold_ms = long_running_threshold_ms
        self.trace_states: dict[int, dict] = {}

    def on_start(self, span: Span, parent_context: Optional[Any] = None) -> None:
        """Called when a span is started.

        Args:
            span: The span that was started.
            parent_context: The parent context of the span.
        """
        if not span.context:
            return
        trace_id = span.context.trace_id

        if trace_id not in self.trace_states:
            self.trace_states[trace_id] = {"should_export": False, "spans": []}
        # Store span
        self.trace_states[trace_id]["spans"].append(span)

        # Call parent's on_start
        super().on_start(span, parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        """Called when a span is ended.

        Args:
            span: The span that was ended.
        """
        if not span.context:
            return
        trace_id = span.context.trace_id

        trace_state = self.trace_states.get(trace_id)
        if not trace_state:
            return

        # Check for errors and long-running spans
        is_error = span.status.status_code == StatusCode.ERROR
        # Convert from nanoseconds to milliseconds if end_time and start_time are available
        if span.end_time is None or span.start_time is None:
            is_long_running = False
        else:
            span_duration_ms = (span.end_time - span.start_time) / 1_000_000
            is_long_running = span_duration_ms > self.long_running_threshold_ms

        if is_error or is_long_running:
            trace_state["should_export"] = True

        # If this is the root span, export all spans in the trace if needed
        if span.parent is None:
            if trace_state["should_export"]:
                # Export all spans in the trace
                for stored_span in trace_state["spans"]:
                    super().on_end(stored_span)
            # Clean up regardless of whether we exported
            del self.trace_states[trace_id]
