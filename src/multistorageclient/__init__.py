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

import importlib.util
import sys

from types import ModuleType

from .cache import CacheConfig
from .client import StorageClient, StorageClientConfig
from .shortcuts import (
    download_file,
    glob,
    is_empty,
    is_file,
    open,
    resolve_storage_client,
    upload_file,
)

__all__ = [
    # Classes
    "StorageClient",
    "StorageClientConfig",
    "CacheConfig",
    # Shortcuts
    "download_file",
    "glob",
    "is_empty",
    "is_file",
    "open",
    "resolve_storage_client",
    "upload_file",
]


def lazy_import(name: str) -> ModuleType:
    spec = importlib.util.find_spec(name)
    if spec is None:
        raise ImportError(f"Module {name} not found")
    if spec.loader is None:
        raise ImportError(f"Loader for module {name} not found")
    loader = importlib.util.LazyLoader(spec.loader)
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module


# lazy import for optional dependencies
# full path is needed, relative imports doesn't work here
numpy = lazy_import(f"{__package__}.contrib.numpy")
pickle = lazy_import(f"{__package__}.contrib.pickle")
os = lazy_import(f"{__package__}.contrib.os")
zarr = lazy_import(f"{__package__}.contrib.zarr")
async_fs = lazy_import(f"{__package__}.contrib.async_fs")
xr = lazy_import(f"{__package__}.contrib.xarray")
torch = lazy_import(f"{__package__}.contrib.torch")
