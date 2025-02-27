import pytest
from unittest.mock import patch, MagicMock
import importlib
from multistorageclient.instrumentation import utils as instrument_utils


@pytest.fixture
def patch_observability_not_available():
    """Patch to simulate environment without optional observability dependencies"""
    import multistorageclient.instrumentation.utils as instrument_utils

    with (
        patch.dict(
            "sys.modules",
            {"opentelemetry.sdk.metrics": None, "datasketches": None},
        ),
        patch("multistorageclient.instrumentation.HAS_OBSERVABILITY_DEPS", False),
    ):
        # Force reload the module to pick up the patched imports
        importlib.reload(instrument_utils)
        yield
        # Reload again to restore original state
        importlib.reload(instrument_utils)


@pytest.fixture
def reset_observability_state():
    """Reset the module to its original state"""
    import multistorageclient.instrumentation.utils as instrument_utils

    importlib.reload(instrument_utils)
    yield


def test_generic_tracer_when_observability_not_available(patch_observability_not_available):
    """Test that _generic_tracer still works when optional observability deps are not available"""

    def test_func():
        return "test"

    decorated = instrument_utils._generic_tracer(test_func, "TestClass")
    # Should still work with basic tracing
    assert decorated() == "test"


def test_file_tracer_when_observability_not_available(patch_observability_not_available):
    """Test that file_tracer still works when optional observability deps are not available"""

    # Create a mock managed file instance
    mock_file = MagicMock()
    mock_file._trace_span = None

    def test_func(self):
        return "test"

    decorated = instrument_utils.file_tracer(test_func)
    # Should still work with basic tracing
    assert decorated(mock_file) == "test"


def test_instrumented_when_observability_not_available(patch_observability_not_available):
    """Test that instrumented decorator still works when optional observability deps are not available"""

    @instrument_utils.instrumented
    class TestClass:
        def test_method(self):
            return "test"

    obj = TestClass()
    # Method should still work with basic tracing
    assert obj.test_method() == "test"


def test_storage_provider_metrics_helper_when_observability_not_available(patch_observability_not_available):
    """Test that StorageProviderMetricsHelper skips advanced metrics when optional observability deps are not available"""
    helper = instrument_utils.StorageProviderMetricsHelper()
    # Should not raise any errors
    helper.record_duration(1.0, "s3", "GET", "test-bucket", 200)
    helper.record_object_size(1024, "s3", "GET", "test-bucket", 200)
    # Metrics should be disabled
    assert not helper._is_metrics_enabled()


def test_cache_manager_metrics_helper_when_observability_not_available(patch_observability_not_available):
    """Test that CacheManagerMetricsHelper skips advanced metrics when optional observability deps are not available"""
    helper = instrument_utils.CacheManagerMetricsHelper()
    # Should not raise any errors
    helper.increase("SET", True)


def test_storage_provider_metrics_helper_when_observability_available(reset_observability_state):
    """Test that StorageProviderMetricsHelper uses advanced metrics when optional observability deps are available"""
    helper = instrument_utils.StorageProviderMetricsHelper()
    # Should have the required attributes
    assert hasattr(helper, "_duration_histogram")
    assert hasattr(helper, "_duration_percentiles")
    assert hasattr(helper, "_object_size_histogram")
    assert hasattr(helper, "_object_size_percentiles")


def test_cache_manager_metrics_helper_when_observability_available(reset_observability_state):
    """Test that CacheManagerMetricsHelper uses advanced metrics when optional observability deps are available"""
    helper = instrument_utils.CacheManagerMetricsHelper()
    # Should have the required attributes
    assert hasattr(helper, "_counter")
    assert hasattr(helper, "_attributes")


def test_generic_tracer_when_observability_available(reset_observability_state):
    """Test that _generic_tracer works with advanced tracing when optional observability deps are available"""

    def test_func():
        return "test"

    decorated = instrument_utils._generic_tracer(test_func, "TestClass")
    # Should be wrapped
    assert hasattr(decorated, "__wrapped__")
    assert decorated.__wrapped__ == test_func  # pyright: ignore
    assert decorated() == "test"


def test_file_tracer_when_observability_available(reset_observability_state):
    """Test that file_tracer works with advanced tracing when optional observability deps are available"""

    # Create a mock managed file instance
    mock_file = MagicMock()
    mock_file._trace_span = None

    def test_func(self):
        return "test"

    decorated = instrument_utils.file_tracer(test_func)
    # Should be wrapped
    assert hasattr(decorated, "__wrapped__")
    assert decorated.__wrapped__ == test_func  # pyright: ignore
    assert decorated(mock_file) == "test"


def test_instrumented_when_observability_available(reset_observability_state):
    """Test that instrumented decorator works with advanced tracing when optional observability deps are available"""

    @instrument_utils.instrumented
    class TestClass:
        def test_method(self):
            return "test"

    obj = TestClass()
    # Method should be wrapped but still work
    assert obj.test_method() == "test"
