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
import opentelemetry.util.types as api_types
import socket
from typing import Mapping


class HostAttributesProvider(AttributesProvider):
    """
    Provides :py:type:``api_types.Attributes`` from host information.
    """

    _HOST_ATTRIBUTES: frozenset[str] = frozenset({"name"})

    #: Attribute key to host attribute key map.
    _attributes: Mapping[str, str]

    def __init__(self, attributes: Mapping[str, str]):
        unsupported_host_attributes = frozenset(attributes.values()) - self._HOST_ATTRIBUTES
        if len(unsupported_host_attributes) > 0:
            raise ValueError(f"Unsupported host attributes: {', '.join(unsupported_host_attributes)}")
        self._attributes = attributes

    def attributes(self) -> api_types.Attributes:
        return {
            attribute_key: self._host_attribute_value(host_attribute_key=host_attribute_key)
            for attribute_key, host_attribute_key in self._attributes.items()
        }

    def _host_attribute_value(self, host_attribute_key: str) -> api_types.AttributeValue:
        if host_attribute_key == "name":
            return socket.gethostname()
        else:
            raise ValueError(f"Unimplemented host attribute: {host_attribute_key}")
