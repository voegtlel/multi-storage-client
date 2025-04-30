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
import multiprocessing
import opentelemetry.util.types as api_types
from typing import Mapping


class ProcessAttributesProvider(AttributesProvider):
    """
    Provides :py:type:``api_types.Attributes`` from current process information.
    """

    _PROCESS_ATTRIBUTES: frozenset[str] = frozenset({"pid"})

    #: Attribute key to process attribute key map.
    _attributes: Mapping[str, str]

    def __init__(self, attributes: Mapping[str, str]):
        unsupported_process_attributes = frozenset(attributes.values()) - self._PROCESS_ATTRIBUTES
        if len(unsupported_process_attributes) > 0:
            raise ValueError(f"Unsupported process attributes: {', '.join(unsupported_process_attributes)}")
        self._attributes = attributes

    def attributes(self) -> api_types.Attributes:
        process = multiprocessing.current_process()
        return {
            attribute_key: getattr(process, process_attribute_key)
            for attribute_key, process_attribute_key in self._attributes.items()
            if hasattr(process, process_attribute_key)
        }
