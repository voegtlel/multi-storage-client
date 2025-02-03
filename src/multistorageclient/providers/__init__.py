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

from .manifest_metadata import ManifestMetadataProvider
from .posix_file import PosixFileStorageProvider

try:
    from .azure import AzureBlobStorageProvider, StaticAzureCredentialsProvider
except Exception:
    pass

try:
    from .gcs import GoogleStorageProvider
except Exception:
    pass

try:
    from .oci import OracleStorageProvider
except Exception:
    pass

try:
    from .s3 import S3StorageProvider, StaticS3CredentialsProvider
except Exception:
    pass

try:
    from .s8k import S8KStorageProvider
except Exception:
    pass

try:
    from .ais import AIStoreStorageProvider, StaticAISCredentialProvider
except Exception:
    pass

__all__ = [
    "S3StorageProvider",
    "S8KStorageProvider",
    "StaticS3CredentialsProvider",
    "GoogleStorageProvider",
    "PosixFileStorageProvider",
    "OracleStorageProvider",
    "ManifestMetadataProvider",
    "StaticAzureCredentialsProvider",
    "AzureBlobStorageProvider",
    "AIStoreStorageProvider",
    "StaticAISCredentialProvider",
]
