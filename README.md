# Multi-Storage Client

`multi-storage-client` is a Python package designed to provide seamless access to multiple object stores, including AWS S3, Google Cloud Storage, Oracle Cloud Infrastructure, and Azure. Its simple API allows for easy interaction with various storage services, making file operations like reading and writing efficient and straightforward.

## Installation

You can install multi-storage-client using one of following methods:

**1. Clone the repository and install:**

```
git clone https://gitlab-master.nvidia.com/nsv-data-platform/multi-storage-client.git
cd multi-storage-client
pip install .
```

**2. Install using pip directly from NVIDIA’s repository:**

```
pip install --extra-index-url https://urm.nvidia.com/artifactory/api/pypi/sw-ngc-data-platform-pypi/simple multi-storage-client
```

**3. Install object storage SDKs:**

You need to install object storage SDKs for the `multi-storage-client` to access the objects.

```
# S3
pip install "multi-storage-client[boto3]"

# Google Cloud Storage
pip install "multi-storage-client[google-cloud-storage]"

# OCI Object Storage
pip install "multi-storage-client[oci]"

# Azure Blob Store
pip install "multi-storage-client[azure-storage-blob]"

# AIStore
pip install "multi-storage-client[aistore]"
```

## Usage

`multi-storage-client` simplifies access to object storage services through an intuitive API, allowing you to read, write, and list files with ease.

### Quick Start

First, create a configuration file to define your storage providers. The default configuration file is located at `~/.msc_config.yaml`, but you can specify a different path using the `MSC_CONFIG` environment variable.

```yaml
profiles:
  default:
    storage_provider:
      type: file
      options:
        base_path: /
  swift-pdx:
    storage_provider:
      type: s3
      options:
        region_name: us-east-1
        endpoint_url: https://pdx.s8k.io
        base_path: my-bucket
    credentials_provider:
      type: S3Credentials
      options:
        access_key: ${S3_ACCESS_KEY}
        secret_key: ${S3_SECRET_KEY}
```

Once your configuration is in place, you can access files using simple methods:

```python
import multistorageclient as msc

# Open file
# Note: The full path of this file is `my-bucket/dataset/webdataset.tar` on PDX.
# Here, `my-bucket` is defined in the `base_path` of the configuration JSON file,
# and `dataset/webdataset.tar` is the relative path within the bucket.
with msc.open('msc://swift-pdx/dataset/webdataset.tar', 'rb') as fp:
    fp.read()

# The open function can also take a POSIX path, which uses the `default` profile.
with msc.open('/usr/local/bin/python3') as fp:
    fp.read()

# List files
files = msc.glob('msc://swift-pdx/dataset/**/*.tar')
# files = ['msc://swift-pdx/dataset/webdataset.tar']

# Open Zarr dataset
zarr_group = msc.zarr.open_consolidated('msc://swift-pdx/my_zarr_datasets.zarr')
# zarr_group = <zarr.hierarchy.Group '/'>
```

The custom URL format is `msc://{profile}/{path}`, where `path` can refer to a local file path or an object store location in the form of `{prefix}/{key}`.

## Developer Guide

Common recipes are provided as Just recipes. To list them, run:

```shell
just
```

### Building the Package

To do a full release build (runs static analysis + unit tests), run:

```shell
just build
```

If you want to use a specific Python binary such as Python 3.9, run:

```shell
just python-binary=python3.9 build
```

### Running Tests

The project includes both unit and integration tests. Here’s how to run them:

#### Unit Tests

Unit tests verify the functionality of individual components:

```shell
poetry run pytest tests/unit/
```

#### Integration Tests

Integration tests verify interactions between components and external services:

```shell
just start-storage-systems

just run-integration-tests

just stop-storage-systems
```

If you want to use a specific Python binary such as Python 3.9, run:

```shell
just python-binary=python3.9 run-integration-tests
```

### Configuration Schema

The basic schema for configuring `multi-storage-client` includes settings for the storage provider and credentials provider. Each profile defines how the client will connect to a specific storage service, such as AWS S3, Google Cloud Storage (GCS), Oracle Cloud Infrastructure (OCI), and Azure.

```yaml
# The profiles section in the configuration file is used to define different storage configurations, allowing you to manage connections to multiple storage systems with ease.
profiles:
  # Define a profile named 'swift-pdx' for accessing a specific object storage.
  swift-pdx:
    storage_provider:
      # Specify the type of storage provider. Supported types: s3, gcs, oci, azure, file.
      type: s3
      # Options for configuring the S3-compatible storage provider.
      options:
        region_name: us-east-1
        endpoint_url: https://pdx.s8k.io
        base_path: mybucket
    
    # Define the credentials provider used for authentication with the storage service.
    # If not specified, the default configuration of the SDK will be used.
    # Credentials can either be provided in environment variables (recommended),
    # or hardcoded into this profile.
    credentials_provider:
      type: S3Credentials
      options:
        access_key: $S3_ACCESS_KEY
        secret_key: $S3_SECRET_KEY

# Configure the local cache for storing files for repeated access.
cache:
  location: /path/to/cache
  size_mb: 50000

# Configure OpenTelemetry integration for publishing metrics and traces.
opentelemetry:
  metrics:
    exporter:
      type: otlp
      options:
        endpoint: http://0.0.0.0:4318/v1/metrics
  traces:
    exporter:
      type: otlp
      options:
        endpoint: http://0.0.0.0:4318/v1/traces
```

### Create Extensions

#### Credentials Provider

A `CredentialsProvider` dynamically supplies credentials for storage access. For example, when using temporary credentials, you can register a custom provider:

```yaml
profiles:
  s3-iad:
    storage_provider:
      type: s3
    credentials_provider:
      type: mymodule.metadata.MyMetadataProvider
      options: {}
```

#### Metadata Provider

A `MetadataProvider` is necessary when metadata is stored separately. This can be useful for optimizing list operations or when metadata needs to be fetched from an external source.

```yaml
profiles:
  s3-iad:
    storage_provider:
      type: s3
    metadata_provider:
      type: mymodule.metadata.MyMetadataProvider
      options: {}
```

#### Provider Bundle

When both metadata and credentials are managed by the same backend service, a `ProviderBundle` can be used to streamline the configuration.

```yaml
profiles:
  s3-iad:
    provider_bundle:
      type: mymodule.metadata.MyProviderBundle
      options: {}
```
