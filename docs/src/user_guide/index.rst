##########
User Guide
##########

********
Concepts
********

MSC has 3 main concepts:

.. glossary::

   storage service
      A service that stores objects/files such as AWS S3, Azure Blob Storage, Google Cloud Storage (GCS),
      NVIDIA AIStore, Oracle Cloud Infrastructure (OCI) Object Storage, POSIX file systems, and more.

   provider
      A provider implements generic object/file operations such as create, read, update, delete, and list
      or supply credentials for a specific :term:`storage service`.

      Providers are further subdivided into storage providers, metadata providers, and credentials providers.

      Storage providers operate on a storage service directly.

      Metadata providers operate on manifest files to accelerate object/file enumeration and metadata retrieval.

      Credentials providers supply credentials for accessing objects/files.

   client
      The client exposes generic object and file operations such as create, read, update, delete, and list.
      It does validation and path translation before calling a :term:`provider`. A client may bundle several
      providers together.

************
Installation
************

MSC is vended as the ``multi-storage-client`` package on PyPI.

The base :term:`client` supports POSIX file systems by default, but there are extras for each :term:`storage service`
which provide the necessary package dependencies for its corresponding storage provider.

.. code-block:: shell
   :caption: Install MSC with storage provider dependencies.

   # POSIX file systems.
   pip install multi-storage-client

   # NVIDIA AIStore.
   pip install "multi-storage-client[aistore]"

   # Azure Blob Storage.
   pip install "multi-storage-client[azure-storage-blob]"

   # AWS S3 and S3-compatible object stores.
   pip install "multi-storage-client[boto3]"

   # Google Cloud Storage (GCS).
   pip install "multi-storage-client[google-cloud-storage]"

   # Oracle Cloud Infrastructure (OCI) Object Storage.
   pip install "multi-storage-client[oci]"

MSC also implements adapters to let higher-level libraries like fsspec or PyTorch work wth the MSC.
Likewise, there are extras for each higher level library.

.. code-block:: shell
   :caption: Install MSC with higher-level library adapter dependencies.

   # fsspec.
   pip install "multi-storage-client[fsspec]"

   # PyTorch.
   pip install "multi-storage-client[torch]"

   # Xarray.
   pip install "multi-storage-client[xarray]"

   # Zarr.
   pip install "multi-storage-client[zarr]"

*****
Usage
*****

Configuration
=============

Before using the MSC, we need to create an MSC configuration. This configuration defines profiles which define
:term:`provider` configurations.

MSC configurations can be file or dictionary-based.

File-Based
----------

File-based configurations are YAML or JSON-based.

.. code-block:: yaml
   :caption: YAML-based configuration.
   :linenos:

   profiles:
     default:
       storage_provider:
         type: file
         options:
           base_path: /
     my-profile:
       storage_provider:
         type: s3
         options:
           base_path: my-bucket
       metadata_provider:
         type: manifest
         options:
           manifest_path: .msc_manifests

.. code-block:: json
   :caption: JSON-based configuration.
   :linenos:

   {
     "profiles": {
       "default": {
         "storage_provider": {
           "type": "file",
           "options": {
             "base_path": "/"
           }
         }
       },
       "my-profile": {
         "storage_provider": {
           "type": "s3",
           "options": {
             "base_path": "my-bucket"
           }
         },
         "metadata_provider": {
           "type": "manifest",
           "options": {
             "manifest_path": ".msc_manifests"
           }
         }
       }
     }
   }

The schema for each profile object is the constructor keyword arguments for
:py:class:`multistorageclient.StorageClientConfig` with these additions:

* A ``type`` field for each provider set to a keyword (e.g. ``file``, ``s3``) or fully-qualified Python class name
  (e.g. ``my_module.providers.CustomProvider``) to indicate which provider to use.
* A ``provider_bundle`` field set to a fully-qualified Python class name
  (e.g. ``my_module.providers.CustomProviderBundle``) which implements
  :py:class:`multistorageclient.types.ProviderBundle` to indicate which provider bundle to use.

  * This takes precedence over the other provider fields.

.. note::

   The ``default`` profile can only use ``file`` as the storage provider type.

   You must create non-default profiles to use other storage providers.

.. note::

   The ``credentials_provider`` field is optional.

   If omitted, the client used by the storage provider will use its default credentials sourcing mechanism
   (e.g. environment variables, configuration files, environment metadata services).

   Omitting this field is recommended if you plan on storing your MSC configuration file in source control (e.g. Git).

The ``options`` field for provider objects is passed as arguments to
:py:mod:`multistorageclient.providers` class constructors.

MSC checks for file-based configurations with the following priority:

#. ``/etc/msc_config.yaml``
#. ``~/.config/msc/config.yaml``
#. ``~/.msc_config.yaml``
#. ``/etc/msc_config.json``
#. ``~/.config/msc/config.json``
#. ``~/.msc_config.json``

Dictionary-Based
----------------

.. note::

   This option can only be used if you create :py:class:`multistorageclient.StorageClient` instances directly.
   See :ref:`operations` for the different ways to interact with MSC.

Dictionary-based configurations use Python dictionaries with :py:meth:`multistorageclient.StorageClientConfig.from_dict`.

The schema is the same as file-based configurations.

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   config = StorageClientConfig.from_dict(
       config_dict={
           "profiles": {
               "default": {
                   "storage_provider": {
                       "type": "file",
                       "options": {
                           "base_path": "/"
                       }
                   }
               }
           }
       }
   )

   client = StorageClient(config=config)

.. _operations:

Object/File Operations
======================

There's 3 ways to interact with MSC:

* Shortcut functions in the :py:mod:`multistorageclient` module.
* The :py:class:`multistorageclient.StorageClient` class.
* Higher-level libraries.

Shortcuts
---------

Shortcuts automatically create and manage :py:class:`multistorageclient.StorageClient` instances for you.
They only support file-based configuration.

.. code-block:: python
   :linenos:

   from multistorageclient import open, download_file

   # Create a client for the default profile and open a file.
   file = open(url="msc://default/animal-photos/giant-panda.png")

   # Reuse the client for the default profile and download a file.
   download_file(
       url="msc://default/animal-photos/red-panda.png",
       local_path="/tmp/animal-photos/red-panda.png"
   )

Shortcuts use ``msc://{profile name}/{file/object path relative to the storage provider's base path}``
URLs for file/object paths.

See :py:mod:`multistorageclient` for all shortcut methods.

Clients
-------

There may be times when you want to create and manage clients by yourself for programmatic configuration or
manual lifecycle control instead of using shortcuts.

You can create :py:class:`multistorageclient.StorageClientConfig` and :py:class:`multistorageclient.StorageClient`
instances directly.

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   # Use a file-based configuration.
   config = StorageClientConfig.from_file()

   # Use a dictionary-based configuration.
   config = StorageClientConfig.from_dict(
       config_dict={
           "profiles": {
               "default": {
                   "storage_provider": {
                       "type": "file",
                       "options": {
                           "base_path": "/"
                       }
                   }
               }
           }
       }
   )

   # Create a client for the default profile.
   client = StorageClient(config=config)

   # Open a file.
   file = client.open("tmp/animal-photos/red-panda.png")

Clients use file/object paths relative to the storage provider's base path.

Higher-Level Libraries
----------------------

The MSC adapters for higher-level libraries use shortcuts under the hood.

fsspec
^^^^^^

:py:mod:`multistorageclient.async_fs` aliases the :py:mod:`multistorageclient.contrib.async_fs` module.

This module provides the :py:class:`multistorageclient.contrib.async_fs.MultiAsyncFileSystem` class which
implements fsspec's ``AsyncFileSystem`` class.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create an MSC-based AsyncFileSystem instance.
   fs = msc.async_fs.MultiAsyncFileSystem()

   # Create a client for the default profile and open a file.
   file = fs.open("msc://default/animal-photos/red-panda.png")

   # Reuse the client for the default profile and download a file.
   fs.get_file(
      rpath="msc://default/animal-photos/red-panda.png",
      lpath="/tmp/animal-photos/red-panda.png"
   )

NumPy
^^^^^

:py:mod:`multistorageclient.numpy` aliases the :py:mod:`multistorageclient.contrib.numpy` module.

This module provides ``load``, ``memmap``, and ``save`` methods for loading and saving NumPy arrays.

.. code-block:: python
   :linenos:

   import multistorageclient as msc
   import numpy

   # Create a client for the default profile and load an array.
   array = msc.numpy.load("msc://default/numpy-arrays/ndarray-1.npz")

   # Reuse the client for the default profile and load a memory-mapped array.
   mmarray = msc.numpy.memmap("msc://default/numpy-arrays/ndarray-1.bin")

   # Reuse the client for the default profile and save an array.
   msc.numpy.save(numpy.array([1, 2, 3, 4, 5], dtype=numpy.int32), "msc://default/numpy-arrays/ndarray-2.npz")

PyTorch
^^^^^^^

:py:mod:`multistorageclient.torch` aliases the :py:mod:`multistorageclient.contrib.torch` module.

This module provides ``load`` and ``save`` methods for loading and saving PyTorch data.

.. code-block:: python
   :linenos:

   import multistorageclient as msc
   import torch

   # Create a client for the default profile and load a tensor.
   tensor = msc.torch.load("msc://default/pytorch-tensors/tensor-1.pt")

   # Reuse the client for the default profile and save a tensor.
   msc.torch.save(torch.tensor([1, 2, 3, 4]), "msc://default/pytorch-tensors/tensor-2.pt")

Xarray
^^^^^^

:py:mod:`multistorageclient.xz` aliases the :py:mod:`multistorageclient.contrib.xarray` module.

This module provides ``open_zarr`` for reading Xarray datasets from Zarr files/objects.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create a client for the default profile and load a Zarr group file/object into an Xarray dataset.
   xarray_dataset = msc.xz.open_zarr("msc://default/zarr-group/")

Zarr
^^^^

:py:mod:`multistorageclient.zarr` aliases the :py:mod:`multistorageclient.contrib.zarr` module.

This module provides ``open_consolidated`` for reading Zarr groups from files/objects.

.. code-block:: python
   :linenos:

   import multistorageclient as msc

   # Create a client for the default profile and load a Zarr group file/object.
   zarr_group = msc.zarr.open_consolidated("msc://default/zarr-group/")
