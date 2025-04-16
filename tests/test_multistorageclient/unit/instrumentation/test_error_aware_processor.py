import pytest
from unittest.mock import Mock, patch

from opentelemetry.sdk.trace import Span
from opentelemetry.trace import StatusCode, SpanContext, TraceFlags
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter

from multistorageclient.instrumentation.error_aware_processor import ErrorAwareBatchSpanProcessor


@pytest.fixture
def mock_exporter():
    return Mock(spec=SpanExporter)


@pytest.fixture
def processor(mock_exporter):
    # Create with a very short schedule delay to force quick processing
    return ErrorAwareBatchSpanProcessor(
        span_exporter=mock_exporter,
        long_running_threshold_ms=100,  # Small threshold for testing
        schedule_delay_millis=1,  # Force quick processing
    )


def create_mock_span(trace_id=0x1, span_id=0x2, parent=None, status_code=StatusCode.OK, duration_ms=50):
    """Helper to create a mock span with the desired properties"""
    span = Mock(spec=Span)
    span.context = SpanContext(trace_id=trace_id, span_id=span_id, is_remote=False, trace_flags=TraceFlags(0x1))
    span.parent = parent
    span.status.status_code = status_code

    # Set timing for duration calculations
    span.start_time = 0
    span.end_time = duration_ms * 1_000_000  # Convert to nanoseconds

    return span


def test_normal_trace_not_exported(processor, mock_exporter):
    """Test that normal traces without errors or long durations are not exported"""
    # Create a trace with a root span and child span
    trace_id = 0x1
    root_span = create_mock_span(trace_id=trace_id, span_id=0x2, parent=None)
    child_span = create_mock_span(trace_id=trace_id, span_id=0x3, parent=root_span)

    # Process spans
    processor.on_start(root_span)
    processor.on_start(child_span)
    processor.on_end(child_span)
    processor.on_end(root_span)

    # Verify: Since these are normal spans, parent's on_end should not be called
    mock_exporter.export.assert_not_called()

    # Verify trace_states is cleaned up
    assert trace_id not in processor.trace_states


def test_error_trace_exported(processor):
    """Test that traces with error spans are exported"""
    # Create a trace with a root span and error child span
    trace_id = 0x2
    root_span = create_mock_span(trace_id=trace_id, span_id=0x2, parent=None)
    error_span = create_mock_span(trace_id=trace_id, span_id=0x3, parent=root_span, status_code=StatusCode.ERROR)

    # Patch the parent class's on_end method to verify it's called
    with patch.object(BatchSpanProcessor, "on_end") as mock_on_end:
        # Process spans
        processor.on_start(root_span)
        processor.on_start(error_span)

        # End error span first, this should mark the trace for export but not export yet
        processor.on_end(error_span)

        # Verify the parent's on_end was NOT called yet - exports only happen at root span completion
        mock_on_end.assert_not_called()

        # End root span - this should trigger the export
        processor.on_end(root_span)

        # Verify both spans were exported
        assert mock_on_end.call_count == 2, "BatchSpanProcessor.on_end should be called for both spans"

    # Verify trace_states is cleaned up
    assert trace_id not in processor.trace_states


def test_long_running_trace_exported(processor):
    """Test that traces with long-running spans are exported"""
    # Create a trace with a root span and long-running child span
    trace_id = 0x3
    root_span = create_mock_span(trace_id=trace_id, span_id=0x2, parent=None)
    long_span = create_mock_span(
        trace_id=trace_id,
        span_id=0x3,
        parent=root_span,
        duration_ms=200,  # > 100ms threshold
    )

    # Patch the parent class's on_end method to verify it's called
    with patch.object(BatchSpanProcessor, "on_end") as mock_on_end:
        # Process spans
        processor.on_start(root_span)
        processor.on_start(long_span)

        # End long span first, this should mark the trace for export but not export yet
        processor.on_end(long_span)

        # Verify the parent's on_end was NOT called yet - exports only happen at root span completion
        mock_on_end.assert_not_called()

        # End root span - this should trigger the export
        processor.on_end(root_span)

        # Verify both spans were exported
        assert mock_on_end.call_count == 2, "BatchSpanProcessor.on_end should be called for both spans"

    # Verify trace_states is cleaned up
    assert trace_id not in processor.trace_states


def test_internal_state_tracking(processor):
    """Test that the internal state is correctly tracking spans and export flags"""
    trace_id = 0x4
    root_span = create_mock_span(trace_id=trace_id, span_id=0x2, parent=None)
    error_span = create_mock_span(trace_id=trace_id, span_id=0x3, parent=root_span, status_code=StatusCode.ERROR)

    # Start spans
    processor.on_start(root_span)
    processor.on_start(error_span)

    # Verify spans are tracked
    assert trace_id in processor.trace_states
    assert len(processor.trace_states[trace_id]["spans"]) == 2
    assert not processor.trace_states[trace_id]["should_export"]

    # End error span
    processor.on_end(error_span)

    # Verify export flag is set
    assert processor.trace_states[trace_id]["should_export"]

    # End root span
    processor.on_end(root_span)

    # Verify trace state is cleaned up
    assert trace_id not in processor.trace_states
