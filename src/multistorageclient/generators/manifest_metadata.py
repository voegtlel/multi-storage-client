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

import json
from typing import List

from multistorageclient.types import ObjectMetadata

from .. import StorageClient
from ..providers.manifest_metadata import ManifestMetadataProvider, DEFAULT_MANIFEST_BASE_DIR


class ManifestMetadataGenerator:
    """
    Generates a file metadata manifest for use with a :py:class:`multistorageclient.providers.ManifestMetadataProvider`.
    """

    @staticmethod
    def _generate_manifest_part_body(object_metadata: List[ObjectMetadata]) -> bytes:
        return "\n".join(
            [
                json.dumps({**metadata_dict, "size_bytes": metadata_dict.pop("content_length")})
                for metadata in object_metadata
                for metadata_dict in [metadata.to_dict()]
            ]
        ).encode(encoding="utf-8")

    @staticmethod
    def generate_and_write_manifest(
        data_storage_client: StorageClient,
        manifest_storage_client: StorageClient,
    ) -> None:
        """
        Generates a file metadata manifest.

        The data storage client's base path should be set to the root path for data objects (e.g. ``my-bucket/my-data-prefix``).

        The manifest storage client's base path should be set to the root path for manifest objects (e.g. ``my-bucket/my-manifest-prefix``).

        The following manifest objects will be written with the destination storage client (with the total number of manifest parts being variable)::

           .
           ├── manifest_main.json
           └── parts/
               ├── part01.jsonl
               ├── ...
               └── part99.jsonl

        :param data_storage_client: Storage client for reading data objects.
        :param manifest_storage_client: Storage client for writing manifest objects.
        """
        # Get respective StorageProviders. A StorageClient will always have a StorageProvider
        # TODO: Cleanup by exposing APIs from the client
        data_storage_provider = data_storage_client._storage_provider
        manifest_storage_provider = manifest_storage_client._storage_provider

        # Create a ManifestMetadataProvider for writing manifest, configure manifest storage provider
        # TODO(NGCDP-3018): Opportunity to split up the responsibilities of MetadataProvider
        manifest_metadata_provider = ManifestMetadataProvider(
            storage_provider=manifest_storage_provider, manifest_path="", writable=True
        )

        # For manifest generation we will always assume direct path for listing objects
        for object_metadata in data_storage_provider.list_objects(prefix=""):
            if DEFAULT_MANIFEST_BASE_DIR not in object_metadata.key.split("/"):  # Do not track manifest files
                manifest_metadata_provider.add_file(path=object_metadata.key, metadata=object_metadata)

        manifest_metadata_provider.commit_updates()
