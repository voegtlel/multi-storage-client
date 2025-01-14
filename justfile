#
# Just configuration.
#
# https://just.systems/man/en
#

python-binary := "python3.12"

# List recipes.
help:
    just --list

# Prepare the virtual environment.
prepare-virtual-environment:
    # Prepare the virtual environment.
    poetry env use {{python-binary}}
    # Install dependencies.
    poetry install --all-extras
    # Create the dependency license summary.
    poetry run pip-licenses

# Start the Python REPL.
start-repl: prepare-virtual-environment
    # Start the Python REPL.
    poetry run python

# Build the package.
build: prepare-virtual-environment
    # Remove package build artifacts.
    rm -rf dist
    # Format.
    ruff format
    # Lint.
    ruff check --fix
    # Type check.
    poetry run pyright
    # Unit test.
    poetry run pytest
    # Build the package archives.
    poetry build

# Build the documentation.
document: prepare-virtual-environment
    # Remove documentation build artifacts.
    rm -rf docs/dist
    # Format.
    ruff format
    # Build the documentation website.
    poetry run sphinx-build -b html docs/src docs/dist

# Start storage systems.
start-storage-systems:
    # Start Azurite.
    azurite --inMemoryPersistence --silent --skipApiVersionCheck &
    # Start MinIO.
    minio --quiet server .minio &

    # Start FakeGCSServer.
    docker run -p 4443:4443 fsouza/fake-gcs-server:1.52.1 -scheme http -data /tmp/fake-gcs-server/data &

    # Wait for Azurite.
    timeout 10s bash -c "until netcat --zero 127.0.0.1 10000; do sleep 1; done"
    # Wait for MinIO.
    timeout 10s bash -c "until curl --fail --silent http://127.0.0.1:9000/minio/health/live; do sleep 1; done"

    # Create a "files" bucket in FakeGCSServer.
    curl -X POST -H "Content-Type: application/json" -d '{"name":"files"}' "http://${FAKE_GCS_SERVER:-127.0.0.1}:4443/storage/v1/b?project=local-project-id"

# Stop storage systems.
stop-storage-systems:
    # Ports used by storage systems:
    # - Azurite => 10000-10002
    # - MinIO   => 9000
    # - GCS     => 4443
    for PID in $(lsof -i :9000,10000,4443 -t); do kill $PID; done
    # Remove persisted data.
    rm -rf .minio __blobstorage__ __queuestorage__ __azurite_*__.json

# Run integration tests.
run-integration-tests: prepare-virtual-environment
    # Integration test.
    STORAGE_EMULATOR_HOST=http://${FAKE_GCS_SERVER:-127.0.0.1}:4443 poetry run pytest tests/integ

# Run E2E tests.
run-e2e-tests: prepare-virtual-environment
    # E2E test.
    poetry run pytest tests/e2e
