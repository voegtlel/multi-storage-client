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

from typing import Optional

from .caching.cache_backend import CacheBackend, FileSystemBackend, StorageProviderBackend
from .caching.cache_config import CacheConfig
from .types import StorageProvider

DEFAULT_CACHE_SIZE = "10G"
DEFAULT_CACHE_SIZE_MB = "10000"
DEFAULT_CACHE_REFRESH_INTERVAL = 300  # 5 minutes
DEFAULT_LOCK_TIMEOUT = 600  # 10 minutes


class CacheBackendFactory:
    """Factory class for creating cache backend instances."""

    @staticmethod
    def create(
        profile: str,
        cache_config: CacheConfig,
        storage_provider: Optional[StorageProvider] = None,
    ) -> CacheBackend:
        """Create a cache backend instance based on the cache configuration."""

        # If storage_provider_profile is set, use StorageProviderBackend
        if cache_config.backend.storage_provider_profile:
            if storage_provider is None:
                raise ValueError("Storage provider backend requires a storage provider")

            if str(storage_provider) not in ("s3", "s8k"):
                raise ValueError(
                    "The storage_provider_profile must reference a profile that uses a storage provider of type s3 or s8k"
                )

            return StorageProviderBackend(profile, cache_config, storage_provider)
        # Otherwise, use FileSystemBackend
        else:
            return FileSystemBackend(profile, cache_config)


# CacheManager is an alias for CacheBackend
CacheManager = CacheBackend
