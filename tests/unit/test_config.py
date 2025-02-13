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

import os
import pickle
import sys

import pytest
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.config import SimpleProviderBundle
from multistorageclient.providers import (
    ManifestMetadataProvider,
    PosixFileStorageProvider,
    StaticS3CredentialsProvider,
    S3StorageProvider,
)
from multistorageclient.types import StorageProviderConfig


def test_json_config() -> None:
    config = StorageClientConfig.from_json(
        """{
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
    }"""
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_yaml_config() -> None:
    config = StorageClientConfig.from_yaml(
        """
        # YAML Example
        profiles:
          # Profile name
          default:
            # POSIX file
            storage_provider:
              type: file
              options:
                base_path: /
        """
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_override_default_profile() -> None:
    with pytest.raises(ValueError) as ex:
        StorageClientConfig.from_json(
            """{
            "profiles": {
                "default": {
                    "storage_provider": {
                        "type": "s3",
                        "options": {
                            "base_path": "mybucket"
                        }
                    }
                }
            }
        }"""
        )

    assert ex.match('Cannot override "default" profile with storage provider type "s3"; expected "file".')


def test_credentials_provider() -> None:
    os.environ["S3_ACCESS_KEY"] = "my_key"
    os.environ["S3_SECRET_KEY"] = "my_secret"
    json_config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/"
                    }
                },
                "credentials_provider": {
                    "type": "S3Credentials",
                    "options": {
                        "access_key": "${S3_ACCESS_KEY}",
                        "secret_key": "${S3_SECRET_KEY}"
                    }
                }
            }
        }
    }"""
    )

    yaml_config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
            credentials_provider:
              type: S3Credentials
              options:
                access_key: ${S3_ACCESS_KEY}
                secret_key: ${S3_SECRET_KEY}
        """
    )

    assert json_config._config_dict == yaml_config._config_dict

    storage_client = StorageClient(yaml_config)
    assert isinstance(storage_client._credentials_provider, StaticS3CredentialsProvider)
    assert storage_client._credentials_provider._access_key == "my_key"
    assert storage_client._credentials_provider._secret_key == "my_secret"


def test_load_extensions() -> None:
    sys.path.append(os.path.dirname(__file__))
    from mock_module.mocks import TestCredentialsProvider, TestMetadataProvider

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
            credentials_provider:
              type: mock_module.mocks.TestCredentialsProvider
            metadata_provider:
              type: mock_module.mocks.TestMetadataProvider
        """
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, TestCredentialsProvider)
    assert isinstance(storage_client._metadata_provider, TestMetadataProvider)


def test_load_provider_bundle() -> None:
    sys.path.append(os.path.dirname(__file__))
    from mock_module.mocks import TestCredentialsProvider, TestMetadataProvider

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          test-provider-bundle:
            provider_bundle:
              type: mock_module.mocks.TestProviderBundle
        """,
        profile="test-provider-bundle",
    )

    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, TestCredentialsProvider)
    assert isinstance(storage_client._metadata_provider, TestMetadataProvider)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_load_direct_provider_bundle() -> None:
    sys.path.append(os.path.dirname(__file__))
    from mock_module.mocks import TestCredentialsProvider, TestMetadataProvider

    bundle = SimpleProviderBundle(
        storage_provider_config=StorageProviderConfig(type="file", options={"base_path": "/"}),
        credentials_provider=TestCredentialsProvider(),
        metadata_provider=TestMetadataProvider(),
    )
    config = StorageClientConfig.from_provider_bundle(config_dict={}, provider_bundle=bundle)

    storage_client = StorageClient(config)
    assert isinstance(storage_client._credentials_provider, TestCredentialsProvider)
    assert isinstance(storage_client._metadata_provider, TestMetadataProvider)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)

    # Expect an error if pickling this storage_client because it cannot be
    # recreated in another process.
    with pytest.raises(ValueError):
        pickled_client = pickle.dumps(storage_client)
        _ = pickle.loads(pickled_client)


def test_swiftstack_storage_provider() -> None:
    config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "swift_profile": {
                "storage_provider": {
                    "type": "s8k",
                    "options": {
                        "base_path": "/",
                        "endpoint_url": "https://pdx.s8k.io",
                        "region_name": "us-east-1"
                    }
                }
            }
        }
    }""",
        profile="swift_profile",
    )

    assert isinstance(config.storage_provider, S3StorageProvider)


def test_manifest_provider_bundle() -> None:
    sys.path.append(os.path.dirname(__file__))

    json_config = StorageClientConfig.from_json(
        """{
        "profiles": {
            "default": {
                "storage_provider": {
                    "type": "file",
                    "options": {
                        "base_path": "/some_base_path"
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
    }"""
    )

    yaml_config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /some_base_path
            metadata_provider:
              type: manifest
              options:
                manifest_path: .msc_manifests
        """
    )

    assert json_config._config_dict == yaml_config._config_dict

    storage_client = StorageClient(yaml_config)
    assert isinstance(storage_client._metadata_provider, ManifestMetadataProvider)
    assert isinstance(storage_client._storage_provider, PosixFileStorageProvider)


def test_manifest_type_unrecognized() -> None:
    sys.path.append(os.path.dirname(__file__))

    with pytest.raises(ValueError) as e:
        StorageClientConfig.from_yaml(
            """
            profiles:
              default:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_base_path
                metadata_provider:
                  type: file
                  options:
                    manifest_path: .msc_manifests
            """
        )

    assert "Expected a fully qualified class name" in str(e), f"Unexpected error message: {str(e)}"


def test_storage_provider_profile_unrecognized() -> None:
    sys.path.append(os.path.dirname(__file__))

    with pytest.raises(ValueError) as e:
        StorageClientConfig.from_yaml(
            """
            profiles:
              default:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_base_path
                metadata_provider:
                  type: manifest
                  options:
                    manifest_path: .msc_manifests
                    storage_provider_profile: non-existent-profile
            """
        )

    assert "Profile 'non-existent-profile' referenced by storage_provider_profile does not exist" in str(e), (
        f"Unexpected error message: {str(e)}"
    )


def test_storage_provider_profile_with_manifest() -> None:
    sys.path.append(os.path.dirname(__file__))

    with pytest.raises(ValueError) as e:
        StorageClientConfig.from_yaml(
            """
            profiles:
              profile-manifest:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_manifest_base_path
                metadata_provider:
                  type: manifest
                  options:
                    manifest_path: .msc_manifests
              profile-data:
                storage_provider:
                  type: file
                  options:
                    base_path: /some_other_base_path/data
                metadata_provider:
                  type: manifest
                  options:
                    manifest_path: .msc_manifests
                    storage_provider_profile: profile-manifest
            """,
            profile="profile-data",
        )

    assert "Found metadata_provider for profile" in str(e), f"Unexpected error message: {str(e)}"
    assert "not supported" in str(e), f"Unexpected error message: {str(e)}"


def test_load_retry_config() -> None:
    config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
            retry:
              attempts: 4
              delay: 0.5
        """
    )

    storage_client = StorageClient(config)
    assert storage_client._retry_config.attempts == 4
    assert storage_client._retry_config.delay == 0.5

    config = StorageClientConfig.from_yaml(
        """
        profiles:
          default:
            storage_provider:
              type: file
              options:
                base_path: /
        """
    )

    storage_client = StorageClient(config)
    assert storage_client._retry_config.attempts == 3
    assert storage_client._retry_config.delay == 1.0

    with pytest.raises(ValueError) as e:
        config = StorageClientConfig.from_yaml(
            """
            profiles:
              default:
                storage_provider:
                  type: file
                  options:
                    base_path: /
                retry:
                  attempts: 0
                  delay: 0.5
            """
        )

    assert "Attempts must be at least 1." in str(e), f"Unexpected error message: {str(e)}"


def test_s3_storage_provider_on_public_bucket() -> None:
    config = StorageClientConfig.from_yaml(
        """
        profiles:
          s3_public_profile:
            storage_provider:
              type: s3
              options:
                base_path: public-bucket
                region_name: us-west-2
                signature_version: UNSIGNED
        """,
        profile="s3_public_profile",
    )
    assert isinstance(config.storage_provider, S3StorageProvider)
