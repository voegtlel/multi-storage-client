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
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class CacheBackendType(str, Enum):
    """
    Enum for cache backend types.

    This enum defines the supported types of cache backends that can be used with the cache system.
    """

    FILESYSTEM = "filesystem"

    STORAGE_PROVIDER = "storage_provider"

    @classmethod
    def from_str(cls, value: str) -> "CacheBackendType":
        """
        Create a CacheBackendType from a string.

        :param value: String value to convert to a CacheBackendType.
        :return: The corresponding CacheBackendType enum value.
        :raises ValueError: If the value is not a valid backend type.
        """
        try:
            return cls(value.lower())
        except ValueError:
            valid_values = [e.value for e in cls]
            raise ValueError(f"Invalid cache backend type: {value}. Must be one of {valid_values}")


@dataclass
class EvictionPolicyConfig:
    """
    Configuration for cache eviction policy.

    This class defines the configuration parameters for cache eviction policies,
    including the policy type and refresh interval.
    """

    #: The eviction policy type (LRU, FIFO, RANDOM)
    policy: str
    #: Cache refresh interval in seconds. Default is 300 (5 minutes)
    refresh_interval: int = 300


@dataclass
class CacheBackendConfig:
    """
    Configuration for cache backend.

    This class defines the configuration parameters for cache backends,
    including the cache path and optional storage provider profile.
    """

    #: The path to the cache directory
    cache_path: str
    #: The storage provider profile to use (for S3EXPRESS backend)
    storage_provider_profile: Optional[str] = None


def default_eviction_policy() -> EvictionPolicyConfig:
    """
    Create a default eviction policy configuration. Default is FIFO because it is supported by both backends.

    :return: An EvictionPolicyConfig instance with default values.
    """
    return EvictionPolicyConfig(policy="fifo", refresh_interval=300)


def default_backend_config() -> CacheBackendConfig:
    """
    Create a default backend configuration.

    :return: A CacheBackendConfig instance with default values.
    """
    return CacheBackendConfig(cache_path=os.path.join(tempfile.gettempdir(), "multistorageclient-cache"))


@dataclass
class CacheConfig:
    """
    Configuration for the CacheManager.

    This class defines the complete configuration for the cache system,
    including size limits, etag usage, eviction policy, and backend settings.
    """

    #: The maximum size of the cache in megabytes.
    size: str
    #: Use etag to update the cached files. Default is True.
    use_etag: bool = True
    #: Cache eviction policy configuration. Default is LRU with 300s refresh.
    eviction_policy: EvictionPolicyConfig = field(default_factory=default_eviction_policy)
    #: Cache backend configuration. Default is filesystem.
    backend: CacheBackendConfig = field(default_factory=default_backend_config)

    def size_bytes(self) -> int:
        """
        Convert cache size to bytes.

        :return: The cache size in bytes.
        """
        return self._convert_to_bytes(self.size)

    def get_eviction_policy(self) -> str:
        """
        Get the eviction policy.

        :return: The current eviction policy type.
        """
        return self.eviction_policy.policy

    def get_storage_provider_profile(self) -> Optional[str]:
        """
        Get the storage provider profile.

        :return: The storage provider profile name if set, None otherwise.
        """
        return self.backend.storage_provider_profile

    def _convert_to_bytes(self, size_str: str) -> int:
        """
        Convert size string with unit suffix to bytes.

        :param size_str: Size string with unit suffix (e.g., '200G', '500M', '1T').
        :return: Size in bytes as an integer.
        :raises ValueError: If the size string has an invalid format or unit.

        Examples:
            >>> _convert_to_bytes("200K")  # Returns 204800
            >>> _convert_to_bytes("1.5G")  # Returns 1610612736
        """
        # Extract numeric part and unit
        unit = size_str[-1].upper()
        try:
            numeric_part = size_str[:-1]
            size = float(numeric_part) if "." in numeric_part else int(numeric_part)
        except ValueError:
            raise ValueError(f"Invalid numeric format in size string: {size_str}")

        # Convert to bytes
        conversion_factors = {"M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5, "E": 1024**6}

        if unit not in conversion_factors:
            raise ValueError(f"Invalid size unit: {unit}. Must be one of: M, G, T, P, E")

        return int(size * conversion_factors[unit])
