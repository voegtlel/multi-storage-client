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
import logging
import os
import tempfile
from typing import Any, Dict, Optional

import yaml

from .cache import DEFAULT_CACHE_SIZE_MB, CacheConfig, CacheManager
from .instrumentation import setup_opentelemetry
from .providers import ManifestMetadataProvider
from .schema import validate_config
from .types import (
    DEFAULT_POSIX_PROFILE,
    DEFAULT_POSIX_PROFILE_NAME,
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_DELAY,
    CredentialsProvider,
    MetadataProvider,
    ProviderBundle,
    RetryConfig,
    StorageProvider,
    StorageProviderConfig,
)
from .utils import expand_env_vars, import_class, merge_dictionaries_no_overwrite
from .rclone import read_rclone_config, DEFAULT_RCLONE_CONFIG_FILE_SEARCH_PATHS

STORAGE_PROVIDER_MAPPING = {
    "file": "PosixFileStorageProvider",
    "s3": "S3StorageProvider",
    "gcs": "GoogleStorageProvider",
    "oci": "OracleStorageProvider",
    "azure": "AzureBlobStorageProvider",
    "ais": "AIStoreStorageProvider",
    "s8k": "S8KStorageProvider",
}

CREDENTIALS_PROVIDER_MAPPING = {
    "S3Credentials": "StaticS3CredentialsProvider",
    "AzureCredentials": "StaticAzureCredentialsProvider",
    "AISCredentials": "StaticAISCredentialProvider",
}

DEFAULT_MSC_CONFIG_FILE_SEARCH_PATHS = (
    # Yaml
    "/etc/msc_config.yaml",
    os.path.join(os.getenv("HOME", ""), ".config", "msc", "config.yaml"),
    os.path.join(os.getenv("HOME", ""), ".msc_config.yaml"),
    # Json
    "/etc/msc_config.json",
    os.path.join(os.getenv("HOME", ""), ".config", "msc", "config.json"),
    os.path.join(os.getenv("HOME", ""), ".msc_config.json"),
)

PACKAGE_NAME = "multistorageclient"

logger = logging.Logger(__name__)


class SimpleProviderBundle(ProviderBundle):
    def __init__(
        self,
        storage_provider_config: StorageProviderConfig,
        credentials_provider: Optional[CredentialsProvider] = None,
        metadata_provider: Optional[MetadataProvider] = None,
    ):
        self._storage_provider_config = storage_provider_config
        self._credentials_provider = credentials_provider
        self._metadata_provider = metadata_provider

    @property
    def storage_provider_config(self) -> StorageProviderConfig:
        return self._storage_provider_config

    @property
    def credentials_provider(self) -> Optional[CredentialsProvider]:
        return self._credentials_provider

    @property
    def metadata_provider(self) -> Optional[MetadataProvider]:
        return self._metadata_provider


class StorageClientConfigLoader:
    def __init__(
        self,
        config_dict: Dict[str, Any],
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        provider_bundle: Optional[ProviderBundle] = None,
    ) -> None:
        """
        Initializes a :py:class:`StorageClientConfigLoader` to create a
        StorageClientConfig. Components are built using the ``config_dict`` and
        profile, but a pre-built provider_bundle takes precedence.

        :param config_dict: Dictionary of configuration options.
        :param profile: Name of profile in ``config_dict`` to use to build configuration.
        :param provider_bundle: Optional pre-built :py:class:`multistorageclient.types.ProviderBundle`, takes precedence over ``config_dict``.
        """
        # ProviderBundle takes precedence
        self._provider_bundle = provider_bundle

        # Interpolates all environment variables into actual values.
        config_dict = expand_env_vars(config_dict)

        self._profiles = config_dict.get("profiles", {})

        if DEFAULT_POSIX_PROFILE_NAME not in self._profiles:
            # Assign the default POSIX profile
            self._profiles[DEFAULT_POSIX_PROFILE_NAME] = DEFAULT_POSIX_PROFILE["profiles"][DEFAULT_POSIX_PROFILE_NAME]
        else:
            # Cannot override default POSIX profile
            storage_provider_type = (
                self._profiles[DEFAULT_POSIX_PROFILE_NAME].get("storage_provider", {}).get("type", None)
            )
            if storage_provider_type != "file":
                raise ValueError(
                    f'Cannot override "{DEFAULT_POSIX_PROFILE_NAME}" profile with storage provider type '
                    f'"{storage_provider_type}"; expected "file".'
                )

        profile_dict = self._profiles.get(profile)

        if not profile_dict:
            raise ValueError(f"Profile {profile} not found; available profiles: {list(self._profiles.keys())}")

        self._profile = profile
        self._profile_dict = profile_dict
        self._opentelemetry_dict = config_dict.get("opentelemetry", None)
        self._cache_dict = config_dict.get("cache", None)

    def _build_storage_provider(
        self,
        storage_provider_name: str,
        storage_options: Optional[Dict[str, Any]],
        credentials_provider: Optional[CredentialsProvider] = None,
    ) -> StorageProvider:
        if storage_options is None:
            storage_options = {}
        if storage_provider_name not in STORAGE_PROVIDER_MAPPING:
            raise ValueError(
                f"Storage provider {storage_provider_name} is not supported. "
                f"Supported providers are: {list(STORAGE_PROVIDER_MAPPING.keys())}"
            )
        if credentials_provider:
            storage_options["credentials_provider"] = credentials_provider
        class_name = STORAGE_PROVIDER_MAPPING[storage_provider_name]
        module_name = ".providers"
        cls = import_class(class_name, module_name, PACKAGE_NAME)
        return cls(**storage_options)

    def _build_credentials_provider(
        self, credentials_provider_dict: Optional[Dict[str, Any]]
    ) -> Optional[CredentialsProvider]:
        """
        Initializes the CredentialsProvider based on the provided dictionary.
        """
        if not credentials_provider_dict:
            return None

        if credentials_provider_dict["type"] not in CREDENTIALS_PROVIDER_MAPPING:
            # Fully qualified class path case
            class_type = credentials_provider_dict["type"]
            module_name, class_name = class_type.rsplit(".", 1)
            cls = import_class(class_name, module_name)
        else:
            # Mapped class name case
            class_name = CREDENTIALS_PROVIDER_MAPPING[credentials_provider_dict["type"]]
            module_name = ".providers"
            cls = import_class(class_name, module_name, PACKAGE_NAME)

        options = credentials_provider_dict.get("options", {})
        return cls(**options)

    def _build_provider_bundle_from_config(self, profile_dict: Dict[str, Any]) -> ProviderBundle:
        # Initialize CredentialsProvider
        credentials_provider_dict = profile_dict.get("credentials_provider", None)
        credentials_provider = self._build_credentials_provider(credentials_provider_dict=credentials_provider_dict)

        # Initialize StorageProvider
        storage_provider_dict = profile_dict.get("storage_provider", None)
        if storage_provider_dict:
            storage_provider_name = storage_provider_dict["type"]
            storage_options = storage_provider_dict.get("options", {})
        else:
            raise ValueError("Missing storage_provider in the config.")

        # Initialize MetadataProvider
        metadata_provider_dict = profile_dict.get("metadata_provider", None)
        metadata_provider = None
        if metadata_provider_dict:
            if metadata_provider_dict["type"] == "manifest":
                metadata_options = metadata_provider_dict.get("options", {})
                # If MetadataProvider has a reference to a different storage provider profile
                storage_provider_profile = metadata_options.pop("storage_provider_profile", None)
                if storage_provider_profile:
                    storage_profile_dict = self._profiles.get(storage_provider_profile)
                    if not storage_profile_dict:
                        raise ValueError(
                            f"Profile '{storage_provider_profile}' referenced by "
                            f"storage_provider_profile does not exist."
                        )

                    # Check if metadata provider is configured for this profile
                    # NOTE: The storage profile for manifests does not support metadata provider (at the moment).
                    local_metadata_provider_dict = storage_profile_dict.get("metadata_provider", None)
                    if local_metadata_provider_dict:
                        raise ValueError(
                            f"Found metadata_provider for profile '{storage_provider_profile}'. "
                            f"This is not supported for storage profiles used by manifests.'"
                        )

                    # Initialize CredentialsProvider
                    local_creds_provider_dict = storage_profile_dict.get("credentials_provider", None)
                    local_creds_provider = self._build_credentials_provider(
                        credentials_provider_dict=local_creds_provider_dict
                    )

                    # Initialize StorageProvider
                    local_storage_provider_dict = storage_profile_dict.get("storage_provider", None)
                    if local_storage_provider_dict:
                        local_name = local_storage_provider_dict["type"]
                        local_storage_options = local_storage_provider_dict.get("options", {})
                    else:
                        raise ValueError("Missing storage_provider in the config.")

                    storage_provider = self._build_storage_provider(
                        local_name, local_storage_options, local_creds_provider
                    )
                else:
                    storage_provider = self._build_storage_provider(
                        storage_provider_name, storage_options, credentials_provider
                    )

                metadata_provider = ManifestMetadataProvider(storage_provider, **metadata_options)
            else:
                class_type = metadata_provider_dict["type"]
                if "." not in class_type:
                    raise ValueError(
                        f"Expected a fully qualified class name (e.g., 'module.ClassName'); got '{class_type}'."
                    )
                module_name, class_name = class_type.rsplit(".", 1)
                cls = import_class(class_name, module_name)
                options = metadata_provider_dict.get("options", {})
                metadata_provider = cls(**options)

        return SimpleProviderBundle(
            storage_provider_config=StorageProviderConfig(storage_provider_name, storage_options),
            credentials_provider=credentials_provider,
            metadata_provider=metadata_provider,
        )

    def _build_provider_bundle_from_extension(self, provider_bundle_dict: Dict[str, Any]) -> ProviderBundle:
        class_type = provider_bundle_dict["type"]
        module_name, class_name = class_type.rsplit(".", 1)
        cls = import_class(class_name, module_name)
        options = provider_bundle_dict.get("options", {})
        return cls(**options)

    def _build_provider_bundle(self) -> ProviderBundle:
        if self._provider_bundle:
            return self._provider_bundle  # Return if previously provided.

        # Load 3rd party extension
        provider_bundle_dict = self._profile_dict.get("provider_bundle", None)
        if provider_bundle_dict:
            return self._build_provider_bundle_from_extension(provider_bundle_dict)

        return self._build_provider_bundle_from_config(self._profile_dict)

    def build_config(self) -> "StorageClientConfig":
        bundle = self._build_provider_bundle()
        storage_provider = self._build_storage_provider(
            bundle.storage_provider_config.type, bundle.storage_provider_config.options, bundle.credentials_provider
        )

        # Cache Config
        cache_config: Optional[CacheConfig] = None
        if self._cache_dict is not None:
            tempdir = tempfile.gettempdir()
            default_location = os.path.join(tempdir, ".msc_cache")
            cache_location = self._cache_dict.get("location", default_location)
            size_mb = self._cache_dict.get("size_mb", DEFAULT_CACHE_SIZE_MB)
            os.makedirs(cache_location, exist_ok=True)
            use_etag = self._cache_dict.get("use_etag", False)
            cache_config = CacheConfig(location=cache_location, size_mb=size_mb, use_etag=use_etag)

        # retry options
        retry_config_dict = self._profile_dict.get("retry", None)
        if retry_config_dict:
            attempts = retry_config_dict.get("attempts", DEFAULT_RETRY_ATTEMPTS)
            delay = retry_config_dict.get("delay", DEFAULT_RETRY_DELAY)
            retry_config = RetryConfig(attempts=attempts, delay=delay)
        else:
            retry_config = RetryConfig(attempts=DEFAULT_RETRY_ATTEMPTS, delay=DEFAULT_RETRY_DELAY)

        # set up OpenTelemetry providers once per process
        if self._opentelemetry_dict:
            setup_opentelemetry(self._opentelemetry_dict)

        return StorageClientConfig(
            profile=self._profile,
            storage_provider=storage_provider,
            credentials_provider=bundle.credentials_provider,
            metadata_provider=bundle.metadata_provider,
            cache_config=cache_config,
            retry_config=retry_config,
        )


class StorageClientConfig:
    """
    Configuration class for the :py:class:`multistorageclient.StorageClient`.
    """

    profile: str
    storage_provider: StorageProvider
    credentials_provider: Optional[CredentialsProvider]
    metadata_provider: Optional[MetadataProvider]
    cache_config: Optional[CacheConfig]
    cache_manager: Optional[CacheManager]
    retry_config: Optional[RetryConfig]

    _config_dict: Optional[Dict[str, Any]]

    def __init__(
        self,
        profile: str,
        storage_provider: StorageProvider,
        credentials_provider: Optional[CredentialsProvider] = None,
        metadata_provider: Optional[MetadataProvider] = None,
        cache_config: Optional[CacheConfig] = None,
        retry_config: Optional[RetryConfig] = None,
    ):
        self.profile = profile
        self.storage_provider = storage_provider
        self.credentials_provider = credentials_provider
        self.metadata_provider = metadata_provider
        self.cache_config = cache_config
        self.retry_config = retry_config
        self.cache_manager = CacheManager(profile, cache_config) if cache_config else None

    @staticmethod
    def from_json(config_json: str, profile: str = DEFAULT_POSIX_PROFILE_NAME) -> "StorageClientConfig":
        config_dict = json.loads(config_json)
        return StorageClientConfig.from_dict(config_dict, profile)

    @staticmethod
    def from_yaml(config_yaml: str, profile: str = DEFAULT_POSIX_PROFILE_NAME) -> "StorageClientConfig":
        config_dict = yaml.safe_load(config_yaml)
        return StorageClientConfig.from_dict(config_dict, profile)

    @staticmethod
    def from_dict(config_dict: Dict[str, Any], profile: str = DEFAULT_POSIX_PROFILE_NAME) -> "StorageClientConfig":
        # Validate the config file with predefined JSON schema
        validate_config(config_dict)

        # Load config
        loader = StorageClientConfigLoader(config_dict, profile)
        config = loader.build_config()
        config._config_dict = config_dict

        return config

    @staticmethod
    def from_file(profile: str = DEFAULT_POSIX_PROFILE_NAME) -> "StorageClientConfig":
        msc_config_file = os.getenv("MSC_CONFIG", None)

        # Search config paths
        if msc_config_file is None:
            for filename in DEFAULT_MSC_CONFIG_FILE_SEARCH_PATHS:
                if os.path.exists(filename):
                    msc_config_file = filename
                    break

        msc_config_dict = {}

        # Parse MSC config file.
        if msc_config_file:
            with open(msc_config_file, "r", encoding="utf-8") as f:
                content = f.read()
                if msc_config_file.endswith(".json"):
                    msc_config_dict = json.loads(content)
                else:
                    msc_config_dict = yaml.safe_load(content)

        # Parse rclone config file.
        rclone_config_dict, rclone_config_file = read_rclone_config()

        # If no config file is found, use a default profile.
        if not msc_config_file and not rclone_config_file:
            search_paths = DEFAULT_MSC_CONFIG_FILE_SEARCH_PATHS + DEFAULT_RCLONE_CONFIG_FILE_SEARCH_PATHS
            logger.warning(
                "Cannot find the MSC config or rclone config file in any of the locations: %s",
                search_paths,
            )

            return StorageClientConfig.from_dict(DEFAULT_POSIX_PROFILE, profile=profile)

        # Merge config files.
        merged_config, conflicted_keys = merge_dictionaries_no_overwrite(msc_config_dict, rclone_config_dict)
        if conflicted_keys:
            raise ValueError(
                f'Conflicting keys found in configuration files "{msc_config_file}" and "{rclone_config_file}: {conflicted_keys}'
            )

        return StorageClientConfig.from_dict(merged_config, profile)

    @staticmethod
    def from_provider_bundle(config_dict: Dict[str, Any], provider_bundle: ProviderBundle) -> "StorageClientConfig":
        loader = StorageClientConfigLoader(config_dict, provider_bundle=provider_bundle)
        config = loader.build_config()
        config._config_dict = None  # Explicitly mark as None to avoid confusing pickling errors
        return config

    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        if not state.get("_config_dict"):
            raise ValueError("StorageClientConfig is not serializable")
        del state["credentials_provider"]
        del state["storage_provider"]
        del state["metadata_provider"]
        del state["cache_manager"]
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        loader = StorageClientConfigLoader(state["_config_dict"], state["profile"])
        new_config = loader.build_config()
        self.storage_provider = new_config.storage_provider
        self.credentials_provider = new_config.credentials_provider
        self.metadata_provider = new_config.metadata_provider
        self.cache_config = new_config.cache_config
        self.retry_config = new_config.retry_config
        self.cache_manager = new_config.cache_manager
