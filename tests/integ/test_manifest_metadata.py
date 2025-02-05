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

from copy import deepcopy
from datetime import datetime, timezone
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.generators import ManifestMetadataGenerator
from multistorageclient.providers.manifest_metadata import DEFAULT_MANIFEST_BASE_DIR
from multistorageclient.types import ObjectMetadata
import os
from random import randint
from typing import Any, Dict, Optional

BUCKET = "test-manifest-metadata"


def verify_manifest_metadata(
    storage_provider_config_dict: Dict[str, Any], credentials_provider_config_dict: Optional[Dict[str, Any]] = None
) -> None:
    """
    The manifest metadata and generator unit tests already cover manifest content correctness.

    These tests are only concerned about the manifest metadata and generator working with different storage providers.
    """

    base_path = storage_provider_config_dict["options"]["base_path"]

    data_profile_name = "data"
    data_storage_provider_config_dict = deepcopy(storage_provider_config_dict)
    data_storage_provider_config_dict["options"]["base_path"] = f"{base_path}/data"

    manifest_profile_name = "manifest"
    manifest_storage_provider_config_dict = deepcopy(storage_provider_config_dict)
    manifest_storage_provider_config_dict["options"]["base_path"] = f"{base_path}/manifest"

    data_with_manifest_profile_name = "data-with-manifest"

    storage_client_config_dict = {
        "profiles": {
            data_profile_name: {
                "storage_provider": data_storage_provider_config_dict,
            },
            manifest_profile_name: {
                "storage_provider": manifest_storage_provider_config_dict,
            },
            data_with_manifest_profile_name: {
                "storage_provider": data_storage_provider_config_dict,
                "metadata_provider": {
                    "type": "manifest",
                    "options": {
                        "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                        "storage_provider_profile": manifest_profile_name,
                    },
                },
            },
        }
    }

    if credentials_provider_config_dict is not None:
        for profile in [data_profile_name, manifest_profile_name, data_with_manifest_profile_name]:
            storage_client_config_dict["profiles"][profile]["credentials_provider"] = credentials_provider_config_dict

    # Generate objects.

    data_storage_client = StorageClient(
        StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_profile_name)
    )

    objects_count = 2
    placeholder_last_modified = datetime.now(tz=timezone.utc)
    expected_object_metadata = {
        key: ObjectMetadata(key=key, content_length=randint(0, 100), last_modified=placeholder_last_modified)
        for key in [f"{i}.txt" for i in range(objects_count)]
    }
    for key, object_metadatum in expected_object_metadata.items():
        data_storage_client.write(path=key, body=b"\x00" * object_metadatum.content_length)

    # Generate manifest.

    manifest_storage_client = StorageClient(
        StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=manifest_profile_name)
    )

    ManifestMetadataGenerator.generate_and_write_manifest(
        data_storage_client=data_storage_client, manifest_storage_client=manifest_storage_client
    )

    # Validate object metadata.

    data_with_manifest_storage_client = StorageClient(
        StorageClientConfig.from_dict(config_dict=storage_client_config_dict, profile=data_with_manifest_profile_name)
    )

    for key, expected_object_metadata in expected_object_metadata.items():
        # Not all object metadata is preserved in manifests. Only check preserved fields.
        #
        # Timestamp precision depends on the storage service, so skipping that too.
        actual_object_metadata = data_with_manifest_storage_client.info(path=key)
        assert expected_object_metadata.key == actual_object_metadata.key
        assert expected_object_metadata.type == actual_object_metadata.type
        assert expected_object_metadata.content_length == actual_object_metadata.content_length


def test_s3_local():
    import boto3

    endpoint_url = "http://localhost:9000"
    access_key = "minioadmin"
    secret_key = "minioadmin"

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    # Recreate the bucket.
    try:
        objects = client.list_objects_v2(Bucket=BUCKET).get("Contents", [])
        for obj in objects:
            client.delete_object(Bucket=BUCKET, Key=obj["Key"])
        client.delete_bucket(Bucket=BUCKET)
    except client.exceptions.NoSuchBucket:
        pass
    client.create_bucket(Bucket=BUCKET)

    verify_manifest_metadata(
        storage_provider_config_dict={"type": "s3", "options": {"endpoint_url": endpoint_url, "base_path": BUCKET}},
        credentials_provider_config_dict={
            "type": "S3Credentials",
            "options": {"access_key": access_key, "secret_key": secret_key},
        },
    )


def test_azure_local():
    from azure.storage.blob import BlobServiceClient

    account = "devstoreaccount1"
    endpoint_url = f"http://127.0.0.1:10000/{account}"

    connection_string = ";".join(
        [
            "DefaultEndpointsProtocol=http",
            f"AccountName={account}",
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            f"BlobEndpoint={endpoint_url}",
            f"QueueEndpoint=http://127.0.0.1:10001/{account}",
            f"TableEndpoint=http://127.0.0.1:10002/{account}",
        ]
    )

    client = BlobServiceClient.from_connection_string(connection_string)

    # Recreate the container.
    try:
        container_client = client.get_container_client(BUCKET)
        if container_client.exists():
            for blob in container_client.list_blobs():
                container_client.delete_blob(blob.name)
            client.delete_container(BUCKET)
    except Exception:
        pass
    client.create_container(BUCKET)

    verify_manifest_metadata(
        storage_provider_config_dict={"type": "azure", "options": {"endpoint_url": endpoint_url, "base_path": BUCKET}},
        credentials_provider_config_dict={
            "type": "AzureCredentials",
            "options": {"connection": connection_string},
        },
    )


def test_gcs_local():
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    project_id = "local-project-id"
    endpoint_url = f"http://{'fake-gcs-server' if ('CI' in os.environ) else '127.0.0.1'}:4443"

    bucket = storage.Bucket(
        storage.Client(
            project=project_id, credentials=AnonymousCredentials(), client_options={"api_endpoint": endpoint_url}
        ),
        name=BUCKET,
    )

    # Recreate the bucket.
    try:
        if bucket.exists():
            for blob in bucket.list_blobs():
                if blob.exists():
                    blob.delete()
            bucket.delete()
    except Exception:
        pass
    bucket.create()

    verify_manifest_metadata(
        storage_provider_config_dict={
            "type": "gcs",
            "options": {"project_id": project_id, "endpoint_url": endpoint_url, "base_path": BUCKET},
        },
    )
