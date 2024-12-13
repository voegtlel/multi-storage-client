User Guide
==========

🚧 Under construction!

Installation
------------

🚧 Under construction!

Usage
------------

🚧 Under construction!

.. code-block:: python
   :linenos:

   from multistorageclient import StorageClient, StorageClientConfig

   client = StorageClient(StorageClientConfig.from_dict({
       'default': {
            'storage_provider': 'file',
            'metadata_provider': {
                'type': 'manifest',
                'options': {
                    'paths': [
                        # ...
                    ]
                }
            }
        }
   }))
