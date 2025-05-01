#######################
Configuration Reference
#######################

This page documents the configuration schema for the Multi-Storage Client (MSC). The configuration file allows you to define 
storage profiles, caching behavior, and observability settings. Each profile can be configured to work with different storage 
providers like S3, Azure Blob Storage, Google Cloud Storage, and others.

*********
Top-Level
*********

The top-level configuration schema consists of four main sections:

- ``profiles``: Dictionary containing profile configurations. Each profile defines storage, metadata, and credentials providers.
- ``cache``: Configuration for local caching of remote objects.
- ``opentelemetry``: Configuration for OpenTelemetry metrics and tracing exporters.
- ``path_mapping``: Configuration for mapping existing non-MSC URLs to existing MSC profiles.

.. code-block:: yaml

    # Required. Dictionary of profile configurations
    profiles:
      <profile_config>
    
    # Optional. Cache configuration
    cache:
      <cache_config>
    
    # Optional. OpenTelemetry configuration
    opentelemetry:
      <opentelemetry_config>
    
    # Optional. Path mapping configuration
    path_mapping:
      <path_mapping_config>

*******
Profile
*******

Each profile in the configuration defines how to interact with storage services through four main sections:

- ``storage_provider``: Configures which storage service to use and how to connect to it.
- ``metadata_provider``: Configures metadata services that provide additional object information.
- ``credentials_provider``: Configures authentication credentials for the storage service.
- ``provider_bundle``: Configures a custom provider implementation that bundles the above providers together.

.. code-block:: yaml

    storage_provider:     # Required. Configuration for the storage provider
      type: <string>      # Required. Provider type ("file", "s3", "azure", "gcs", "oci", "aistore")
      options:            # Required. Provider-specific options
        <provider_options>

    metadata_provider:    # Optional. Configuration for the metadata provider
      type: <string>      # Required. Provider type (e.g. "manifest")
      options:            # Required. Provider-specific options
        <provider_options>

    credentials_provider: # Optional. Configuration for the credentials provider
      type: <string>      # Required. Provider type (S3Credentials, AzureCredentials, AISCredentials)
      options:            # Required. Provider-specific options
        <provider_options>

    provider_bundle:      # Optional.
      type: <string>      # Required. Fully-qualified class name for a custom provider bundle
      options:            # Required. Provider-specific options
        <provider_options>

.. note::
  The configuration follows a consistent pattern across different providers:

  * The ``type`` field specifies which provider implementation to use. This can be:

    * A predefined name (e.g. "s3", "azure", "file") that maps to built-in providers
    * A fully-qualified class name for custom provider implementations

  * The ``options`` field contains provider-specific configuration that will be passed to the provider's constructor. The available options depend on the specific provider implementation being used.
  
  * Profile names must not start with an underscore (_) to prevent collision with :ref:`implicit profiles <implicit-profiles>`.

=================
Storage Providers
=================

The following storage provider types are supported:

``file``
--------
The POSIX filesystem provider.

Options: See parameters in :py:class:`multistorageclient.providers.PosixFileStorageProvider`.

MSC includes a default POSIX filesystem profile that is used when no configuration file is found. This profile provides basic local filesystem access:

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    default:
      storage_provider:
        type: file
        options:
          base_path: /

``s3``
------
AWS S3 and S3-compatible storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.S3StorageProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      storage_provider:
        type: s3
        options:
          base_path: my-bucket
          region_name: us-east-1

``s8k``
-------
SwiftStack provider.

Options: See parameters in :py:class:`multistorageclient.providers.S8KStorageProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      storage_provider:
        type: s8k
        options:
          base_path: my-bucket
          region_name: us-east-1
          endpoint_url: https://s8k.example.com

``azure``
---------
Azure Blob Storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.AzureBlobStorageProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      storage_provider:
        type: azure
        options:
          base_path: my-container
          account_url: https://my-storage-account.blob.core.windows.net

``gcs``
-------
Google Cloud Storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.GoogleStorageProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      storage_provider:
        type: gcs
        options:
          base_path: my-bucket
          project_id: my-project-id

``oci``
-------
OCI Object Storage provider.

Options: See parameters in :py:class:`multistorageclient.providers.OracleStorageProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      storage_provider:
        type: oci
        options:
          base_path: my-bucket
          namespace: my-namespace

``aistore``
-----------
NVIDIA AIStore provider.

Options: See parameters in :py:class:`multistorageclient.providers.AIStoreStorageProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      storage_provider:
        type: aistore
        options:
          endpoint: https://ais.example.com
          base_path: my-bucket

==================
Metadata Providers
==================

``manifest``
------------
The manifest-based metadata provider for accelerated object listing and metadata retrieval. See :doc:`/user_guide/manifests` for more details.

Options: See parameters in :py:class:`multistorageclient.providers.ManifestMetadataProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      storage_provider:
        type: s3
        options:
          base_path: my-bucket
      metadata_provider:
        type: manifest
        options:
          manifest_path: .msc_manifests

=====================
Credentials Providers
=====================

Credentials providers vary by storage service. When running in a cloud service provider's (CSP) managed environment 
(like AWS EC2, Azure VMs, or Google Cloud Compute Engine), credentials are automatically handled through instance 
metadata services. Similarly, when running locally, credentials are typically handled through environment variables 
or configuration files (e.g., AWS credentials file).

Therefore, it's recommended to omit the credentials provider and let the storage service use its default 
authentication mechanism. This approach is more secure than storing credentials in the MSC configuration file 
and ensures credentials are properly rotated when running in cloud environments.

If you need to provide static credentials, it's strongly recommended to pass them through environment variables rather 
than hardcoding them directly in configuration files. See `Environment Variables`_ for more details.

``S3Credentials``
-----------------
Static credentials provider for Amazon S3 and S3-compatible storage services.

Options: See parameters in :py:class:`multistorageclient.providers.StaticS3CredentialsProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      credentials_provider:
        type: S3Credentials
        options:
          access_key: ${AWS_ACCESS_KEY}
          secret_key: ${AWS_SECRET_KEY}

``AzureCredentials``
---------------------
Static credentials provider for Azure Blob Storage.

Options: See parameters in :py:class:`multistorageclient.providers.StaticAzureCredentialsProvider`.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my-profile:
      credentials_provider:
        type: AzureCredentials
        options:
          connection: ${AZURE_CONNECTION_STRING}

``AISCredentials``
-------------------
Static credentials provider for NVIDIA AIStore.

Options: See parameters in :py:class:`multistorageclient.providers.StaticAISCredentialProvider`.

*****
Cache
*****

The MSC cache configuration allows you to specify caching behavior for improved performance. The cache stores 
files locally for faster access on subsequent reads. It maintains a maximum size limit and automatically evicts files 
when the limit is reached. The cache validates file freshness using ETags when enabled. Storage-provider-based cache backend is an 
early access feature that doesn't yet support all storage providers or cache eviction and cleanup operations. 

Note: These cache changes are backward compatible with previous cache configuration.

Options:
  - ``size``: Maximum cache size with unit (e.g., "100M", "1G") (optional, default: "10G")
  - ``use_etag``: Use ETag for cache validation (optional, default: true)
  - ``eviction_policy``: Cache eviction policy configuration
    - ``policy``: Eviction policy type ("fifo", "lru", "random") (optional, default: "fifo")
    - ``refresh_interval``: Interval in seconds to refresh cache (optional, default: 300)
  - ``cache_backend``: Cache backend configuration
    - ``cache_path``: Directory path for storing cached files (optional, default: system temp directory + "/.msc_cache")
    - ``storage_provider_profile``: Optional profile to use for cache storage, should point to a valid s3-express profile. If not provided, FileSystem cachebackend gets used(recommended to use a separate read-only profile) (optional, default: FileSystem cache backend)

.. code-block:: yaml
  :caption: Example configuration when using a storage provider based cache backend.
  
  cache:
    size: "10M"
    use_etag: true
    eviction_policy:
      policy: "fifo"
      refresh_interval: 300
    cache_backend:
      cache_path: tmp/msc_cache  
      storage_provider_profile: s3-express-profile

.. code-block:: yaml
  :caption: Example configuration when using a filesystem based cache backend (local cache). Note that the storage_provider_profile is not provided.
  
  cache:
    size: "10M"
    use_etag: true
    eviction_policy:
      policy: "fifo"
      refresh_interval: 300
    cache_backend:
      cache_path: /tmp/msc_cache

*************
OpenTelemetry
*************

MSC supports OpenTelemetry for collecting client-side metrics and traces to help monitor and debug your application's 
storage operations. This includes:

- Metrics about storage operations (read/write throughput, operation latency, cache hits/misses, etc.)
- Traces showing the flow of storage operations and their timing

.. code-block:: yaml
  :caption: Example configuration.

  opentelemetry:
    metrics:
      exporter:
        type: otlp
        options:
          endpoint: "http://otel-collector:4317"
    traces:
      exporter:
        type: otlp
        options:
          endpoint: "http://otel-collector:4317"

Alternative configuration using console exporters for development:

.. code-block:: yaml
  :caption: Example configuration.

  opentelemetry:
    metrics:
      exporter:
        type: console
    traces:
      exporter:
        type: console

***********
Path Mapping
***********

The ``path_mapping`` section allows mapping non-MSC URLs to MSC URLs.
This enables users to use their existing URLs with MSC without having to change their code/config.

.. code-block:: yaml

    path_mapping:
       /lustrefs/a/b/: msc://profile-for-file-a-b/
       /lustrefs/a/: msc://profile-for-file-a/
       s3://bucket1/: msc://profile-for-s3-bucket1/
       s3://bucket1/a/b/: msc://profile-for-s3-bucket1-a-b/
       gs://bucket1/: msc://profile-for-gcs-bucket1/

Each key-value pair maps a source path to a destination MSC URL. The client
will automatically convert paths that match the source prefix to use the
corresponding MSC URI when accessing files.

.. note::
   Path mapping must adhere to the following constraints:

   **Source Path:**
   
   * Must end with "/" to prevent unintended partial name conflicts and ensure clear mapping of prefixes
   * Must use protocols supported by MSC ("s3", "gs", "ais" currently) or "/" for file paths
   * No duplicate protocol + bucket + prefix combinations are allowed
   
   **Destination Path:**
   
   * Must start with "msc://"
   * Must end with "/"
   * Must reference a profile that is defined in the MSC configuration

   While processing non-MSC URLs, If multiple source paths match a given input path, the longest matching prefix takes precedence.

*********************
Implicit Profiles
*********************

.. _implicit-profiles:

Implicit profiles are automatically created by MSC when users provide non-MSC URLs directly to MSC functions. Unlike explicitly defined profiles in the configuration file, implicit profiles are inferred dynamically from URL patterns.

This feature enables users to:

- Continue using existing URLs without modification
- Use MSC without managing a separate MSC configuration file

When a non-MSC URL is provided to functions like ``msc.open(url)`` or ``msc.resolve_storage_client(url)``, MSC will first check if there is an existing profile applicable through path mapping. If not, MSC will create an implicit profile:

1. Infer the storage provider based on the URL scheme (s3, gs, etc.) and construct an implicit profile name with the convention ``_protocol-bucket`` (e.g., ``_s3-bucket1``, ``_gs-bucket1``) or ``_file`` for file system paths.
2. Configure the storage provider and credential provider with default settings, i.e. credentials will the same as that native SDKs look for (aws credentials file, azure credentials file, etc.)
3. If MSC config is present, inherit global settings like observability and file cache; otherwise, only default settings for file system based cache.

Here are examples of non-MSC URLs that are automatically translated to MSC URIs:

-  s3://bucket1/path/to/object -> msc://_s3-bucket1/path/to/object
-  /path/to/another/file -> msc://_file/path/to/another/file

Implicit profiles are identified by their leading underscore prefix, which is why user-defined profile names cannot start with an underscore.

*********************
Environment Variables
*********************

The MSC configuration file supports environment variable expansion in string values. Environment variables 
can be referenced using either ``${VAR}`` or ``$VAR`` syntax.

.. code-block:: yaml
  :caption: Example configuration.

  profiles:
    my_profile:
      storage_provider:
        type: s3
        options:
          base_path: ${BUCKET_NAME}
      credentials_provider:
        type: S3Credentials
        options:
          access_key: ${AWS_ACCESS_KEY}
          secret_key: ${AWS_SECRET_KEY}

In this example, the values will be replaced with the corresponding environment variables at runtime. If an 
environment variable is not set, the original string will be preserved.

The environment variable expansion works for any string value in the configuration file, including:

- Storage provider options
- Credentials provider options  
- Metadata provider options
- Cache configuration
- OpenTelemetry configuration

This allows sensitive information like credentials to be passed securely through environment variables rather 
than being hardcoded in the configuration file.
