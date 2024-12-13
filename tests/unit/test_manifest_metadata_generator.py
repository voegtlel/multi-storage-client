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
import time
import json
from datetime import datetime, timezone
import os
from random import randint
import tempfile

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.generators import ManifestMetadataGenerator
from multistorageclient.types import ObjectMetadata


def test_file_metadata_generator() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create data folder
        data_parent_folder = os.path.join(tmpdir, 'data_dir')
        data_prefix = 'data'
        os.makedirs(os.path.join(data_parent_folder, data_prefix), exist_ok=True)
        # Create manifest folder
        manifest_prefix = '.msc_manifests'
        os.makedirs(os.path.join(tmpdir, manifest_prefix), exist_ok=True)

        storage_client_config_dict = {
            'profiles': {
                'posix-data': {
                    'storage_provider': {
                        'type': 'file',
                        'options': {
                            'base_path': f'{data_parent_folder}'
                        }
                    }
                },
                'posix-manifest': {
                    'storage_provider': {
                        'type': 'file',
                        'options': {
                            'base_path': f'{tmpdir}'
                        }
                    }
                },
                'posix-data-with-metadata': {
                    'storage_provider': {
                        'type': 'file',
                        'options': {
                            'base_path': f'{data_parent_folder}'
                        }
                    },
                    'metadata_provider': {
                        'type': 'manifest',
                        'options': {
                            'storage_provider_profile': 'posix-manifest',
                            'manifest_path': f'{manifest_prefix}'
                        }
                    }
                }
            }
        }
        # Create data storage client
        data_storage_client = StorageClient(
            StorageClientConfig.from_dict(storage_client_config_dict, profile='posix-data'))
        # Generate objects.
        objects_count = 2
        placeholder_last_modified = datetime.now(tz=timezone.utc)
        expected_object_metadata = {
            key: ObjectMetadata(
                key=key,
                content_length=randint(0, 100),
                last_modified=placeholder_last_modified
            )
            for key in [os.path.join(data_prefix, f'file-{i}.txt') for i in range(objects_count)]
        }
        for key, object_metadatum in expected_object_metadata.items():
            data_storage_client.write(
                path=key,
                body=b'\x00' * object_metadatum.content_length
            )
            object_metadatum.last_modified = datetime.fromtimestamp(
                os.path.getmtime(os.path.join(data_parent_folder, key)), tz=timezone.utc)

        # Create manifest storage client.
        manifest_storage_client = StorageClient(
            StorageClientConfig.from_dict(storage_client_config_dict, profile='posix-manifest'))

        # Generate manifest.
        ManifestMetadataGenerator.generate_and_write_manifest(
            data_storage_client=data_storage_client,
            manifest_storage_client=manifest_storage_client,
        )

        # This is a ceiling division (needs to be done as an upside-down floor division)
        # instead of math.ceil to avoid floats.
        expected_manifest_parts_count = 1

        # Make sure the generation timestamp is parseable and has second-level precision.
        manifest_main_paths = manifest_storage_client.glob(
            os.path.join(manifest_prefix, '*', 'msc_manifest_index.json'))
        assert len(manifest_main_paths) == 1
        manifest_timestamp = datetime.fromisoformat(
            os.path.basename(os.path.dirname(manifest_main_paths[0]))).isoformat(timespec='seconds')

        # Only other file besides manifest parts should be manifest_main.json.
        assert len(
            list(
                manifest_storage_client.list(
                    prefix=os.path.join(manifest_prefix, manifest_timestamp)
                )
            )) - expected_manifest_parts_count == 1
        assert len(
            list(
                manifest_storage_client.list(
                    prefix=os.path.join(manifest_prefix, manifest_timestamp, 'parts')
                )
            )) == expected_manifest_parts_count

        # Create a storage client with the file metadata.
        storage_client_with_metadata = StorageClient(
            StorageClientConfig.from_dict(storage_client_config_dict, profile='posix-data-with-metadata'))

        # Check no unexpected objects included.
        #
        # The manifest should only include data/file-#.txt files and none of the .manifest/* files.
        assert set(expected_object_metadata.keys()) == set(
            [object_metadatum.key for object_metadatum in storage_client_with_metadata.list()])

        # Check expected object metadata.
        for key, expected_object_metadata in expected_object_metadata.items():
            assert expected_object_metadata == storage_client_with_metadata.info(key)

        time.sleep(2)  # Add sleep for 2 seconds so that paths do not conflict
        # Re-generate manifest to test .manifest does not make it into manifests
        ManifestMetadataGenerator.generate_and_write_manifest(
            data_storage_client=data_storage_client,
            manifest_storage_client=manifest_storage_client,
        )
        # Make sure the generation timestamp is parseable and has second-level precision.
        manifest_main_paths = manifest_storage_client.glob(
            os.path.join(manifest_prefix, '*', 'msc_manifest_index.json'))
        assert len(manifest_main_paths) == 2

        manifest_part_paths = sorted(manifest_storage_client.glob(os.path.join(manifest_prefix, '*', 'parts/*jsonl')))
        assert len(manifest_part_paths) == 2

        for p in manifest_part_paths:
            # Open each file and check the contents
            # Since glob returns relative paths, add the base_path
            p = os.path.join(tmpdir, p)
            with open(p, 'r') as file:
                for line in file:
                    # Parse JSON entry
                    entry = json.loads(line)
                    # Check if ".msc_manifests" is in the key
                    assert ".msc_manifests" not in entry["key"]
