#
# Just configuration.
#
# https://just.systems/man/en
#

# Default to the first Python binary on `PATH`.
python-binary := "python"

# List recipes.
help:
    just --list

# Prepare the virtual environment.
prepare-virtual-environment:
    # Prepare the virtual environment.
    uv sync --python {{python-binary}} --all-extras
    # Create the dependency license summary.
    uv run pip-licenses

# Start the Python REPL.
start-repl: prepare-virtual-environment
    # Start the Python REPL.
    uv run python

# Run static analysis (format, lint, type check).
analyze: prepare-virtual-environment
    # Remove analysis artifacts.
    rm -rf .reports/ruff.json
    # Format.
    if [[ -z "${CI:-}" ]]; then ruff format; else ruff format --check; fi
    # Lint.
    if [[ -z "${CI:-}" ]]; then ruff check --fix; else ruff check --output-format gitlab --output-file .reports/ruff.json; fi
    # Type check.
    uv run pyright

# Stop storage systems.
stop-storage-systems:
    # Ports used by storage systems:
    # - Azurite         -> 10000-10002
    # - fake-gcs-server -> 4443
    # - MinIO           -> 9000
    for PID in $(lsof -i :4443,9000,10000 -t); do kill $PID; done
    # Remove persisted data.
    -rm -rf .minio __blobstorage__ __queuestorage__ __azurite_*__.json

# Start storage systems.
start-storage-systems: stop-storage-systems
    # Start Azurite.
    azurite --inMemoryPersistence --silent --skipApiVersionCheck &
    # Start fake-gcs-server.
    TZ="UTC" fake-gcs-server -backend memory -log-level error -scheme http &
    # Start MinIO.
    minio --quiet server .minio &

    # Wait for Azurite.
    timeout 10s bash -c "until netcat --zero localhost 10000; do sleep 1; done"
    # Wait for fake-gcs-server.
    timeout 10s bash -c "until curl --fail --silent http://localhost:4443/_internal/healthcheck; do sleep 1; done"
    # Wait for MinIO.
    timeout 10s bash -c "until curl --fail --silent http://localhost:9000/minio/health/live; do sleep 1; done"

# Run unit tests.
run-unit-tests: prepare-virtual-environment start-storage-systems && stop-storage-systems
    # Remove test artifacts.
    rm -rf .reports/unit
    # Unit test.
    uv run pytest --junit-xml .reports/unit/pytest.xml --cov --cov-report=term --cov-report=html --cov-report=xml

# Create package archives.
package: prepare-virtual-environment
    # Remove package archives.
    rm -rf dist
    # Create package archives.
    uv build

# Build the documentation.
document: prepare-virtual-environment
    # Remove documentation artifacts.
    rm -rf docs/dist
    # Build the documentation website.
    uv run sphinx-build -b html docs/src docs/dist

# Release build.
build: analyze run-unit-tests package document

# Run E2E tests.
run-e2e-tests: prepare-virtual-environment
    # Remove test artifacts.
    rm -rf .reports/e2e
    # E2E test.
    uv run pytest --junit-xml .reports/e2e/pytest.xml tests/test_multistorageclient/e2e
