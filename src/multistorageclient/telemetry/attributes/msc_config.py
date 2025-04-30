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
import copy
import hashlib
import jmespath
import jmespath.functions as jmespath_functions
import opentelemetry.util.types as api_types
from typing import Any, Mapping


class MSCConfigJMESPathFunctions(jmespath_functions.Functions):
    """
    Additional JMESPath functions.
    """

    @jmespath_functions.signature({"types": ["string"]}, {"types": ["string"]})
    def _func_hash(self, algorithm: str, value: str) -> str:
        """
        Return the hexadecimal hash digest of a string.

        :param algorithm: Hash algorithm.
        :param value: Hash value.
        :return: Hexadecimal hash digest.
        """
        value_hash = hashlib.new(algorithm)
        value_hash.update(value.encode())
        return value_hash.hexdigest()


class MSCConfigAttributesProvider(AttributesProvider):
    """
    Provides :py:type:``api_types.Attributes`` from a multi-storage client configuration.
    """

    #: Static attributes.
    _attributes: api_types.Attributes

    def __init__(self, attributes: Mapping[str, Mapping[str, Any]], config_dict: Mapping[str, Any]):
        self._attributes = {
            attribute_key: jmespath.search(
                attribute_value_config_dict["expression"],
                config_dict,
                options=jmespath.Options(custom_functions=MSCConfigJMESPathFunctions()),
            )
            for attribute_key, attribute_value_config_dict in attributes.items()
        }

    def attributes(self) -> api_types.Attributes:
        return copy.deepcopy(self._attributes)
