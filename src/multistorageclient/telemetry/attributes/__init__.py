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

from .base import AttributesProvider
from .environment_variables import EnvironmentVariablesAttributesProvider
from .host import HostAttributesProvider
from .msc_config import MSCConfigAttributesProvider
from .process import ProcessAttributesProvider
from .static import StaticAttributesProvider
from .thread import ThreadAttributesProvider

__all__ = [
    "AttributesProvider",
    "EnvironmentVariablesAttributesProvider",
    "HostAttributesProvider",
    "MSCConfigAttributesProvider",
    "ProcessAttributesProvider",
    "StaticAttributesProvider",
    "ThreadAttributesProvider",
]
