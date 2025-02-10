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

# Build the package.
build: prepare-virtual-environment
    # Remove package build artifacts.
    rm -rf .reports/{ruff.json,unit} dist
    # Format.
    if [[ -z "${CI:-}" ]]; then ruff format; else ruff format --check; fi
    # Lint.
    if [[ -z "${CI:-}" ]]; then ruff check --fix; else ruff check --output-format gitlab --output-file .reports/ruff.json; fi
    # Type check.
    uv run pyright
    # Unit test.
    uv run coverage run
    uv run coverage combine
    uv run coverage report
    uv run coverage html
    uv run coverage xml
    # Build the package archives.
    uv build

# Build the documentation.
document: prepare-virtual-environment
    # Remove documentation build artifacts.
    rm -rf docs/dist
    # Format.
    ruff format
    # Build the documentation website.
    uv run sphinx-build -b html docs/src docs/dist

# Stop storage systems.
stop-storage-systems:
    # Ports used by storage systems:
    # - Azurite -> 10000-10002
    # - MinIO   -> 9000
    # - GCS     -> 4443 (use `docker stop` instead)
    for PID in $(lsof -i :9000,10000 -t); do kill $PID; done
    -if [[ -z "${CI:-}" ]]; then docker stop fake-gcs-server; fi
    # Remove persisted data.
    -rm -rf .minio __blobstorage__ __queuestorage__ __azurite_*__.json

# Start storage systems.
start-storage-systems: stop-storage-systems
    # Start Azurite.
    azurite --inMemoryPersistence --silent --skipApiVersionCheck &
    # Start MinIO.
    minio --quiet server .minio &
    # Start FakeGCSServer.
    if [[ -z "${CI:-}" ]]; then docker run --detach --name fake-gcs-server --publish 4443:4443 --rm fsouza/fake-gcs-server:1.52.1 -backend memory -scheme http; fi

    # Wait for Azurite.
    timeout 10s bash -c "until netcat --zero 127.0.0.1 10000; do sleep 1; done"
    # Wait for MinIO.
    timeout 10s bash -c "until curl --fail --silent http://127.0.0.1:9000/minio/health/live; do sleep 1; done"

# Run integration tests.
run-integration-tests: prepare-virtual-environment
    # Remove test artifacts.
    rm -rf .reports/integ
    # Integration test.
    uv run pytest --junit-xml .reports/integ/pytest.xml tests/integ

# Run E2E tests.
run-e2e-tests: prepare-virtual-environment
    # Remove test artifacts.
    rm -rf .reports/e2e
    # E2E test.
    uv run pytest --junit-xml .reports/e2e/pytest.xml tests/e2e
