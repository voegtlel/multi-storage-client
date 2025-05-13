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
from collections.abc import Mapping
import opentelemetry.util.types as api_types
import threading


class ThreadAttributesProvider(AttributesProvider):
    """
    Provides :py:type:``api_types.Attributes`` from current thread information.
    """

    _THREAD_ATTRIBUTES: frozenset[str] = frozenset({"ident", "native_id"})

    #: Attribute key to thread attribute key map.
    _attributes: Mapping[str, str]

    def __init__(self, attributes: Mapping[str, str]):
        unsupported_thread_attributes = frozenset(attributes.values()) - self._THREAD_ATTRIBUTES
        if len(unsupported_thread_attributes) > 0:
            raise ValueError(f"Unsupported thread attributes: {', '.join(unsupported_thread_attributes)}")
        self._attributes = attributes

    def attributes(self) -> api_types.Attributes:
        thread = threading.current_thread()
        return {
            attribute_key: getattr(thread, thread_attribute_key)
            for attribute_key, thread_attribute_key in self._attributes.items()
            if hasattr(thread, thread_attribute_key)
        }
