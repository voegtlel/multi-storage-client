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

import hashlib
from multistorageclient.telemetry.attributes import (
    EnvironmentVariablesAttributesProvider,
    HostAttributesProvider,
    MSCConfigAttributesProvider,
    ProcessAttributesProvider,
    StaticAttributesProvider,
    ThreadAttributesProvider,
)
import os
import pytest


def test_environment_variables_attributes_provider():
    environment_variable_key, environment_variable_value = list(os.environ.items())[0]
    attribute_key = "attribute"
    attributes = EnvironmentVariablesAttributesProvider(
        attributes={attribute_key: environment_variable_key}
    ).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] == environment_variable_value


def test_host_attributes_provider():
    attribute_key = "host"
    attributes = HostAttributesProvider(attributes={attribute_key: "name"}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] is not None

    with pytest.raises(ValueError):
        HostAttributesProvider(attributes={"unsupported": "unsupported"})


def test_msc_config_attributes_provider():
    client_credential_value = "secret"
    client_credential_value_hash_algorithm = "sha3-256"
    client_credential_value_hash = hashlib.new(client_credential_value_hash_algorithm)
    client_credential_value_hash.update(client_credential_value.encode())
    attribute_key = "credentials"
    hashed_attribute_key = "credentials_hash"
    hashed_attribute_value = client_credential_value_hash.hexdigest()
    config_dict_path = "opentelemetry.metrics.exporter.auth.options.client_credential"
    attributes = MSCConfigAttributesProvider(
        attributes={
            attribute_key: {"expression": config_dict_path},
            hashed_attribute_key: {
                "expression": f"hash('{client_credential_value_hash_algorithm}', {config_dict_path})"
            },
        },
        config_dict={
            "opentelemetry": {
                "metrics": {"exporter": {"auth": {"options": {"client_credential": client_credential_value}}}}
            }
        },
    ).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] == client_credential_value
    assert hashed_attribute_key in attributes
    assert attributes[hashed_attribute_key] == hashed_attribute_value


def test_process_attributes_provider():
    attribute_key = "process"
    attributes = ProcessAttributesProvider(attributes={attribute_key: "pid"}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] is not None

    with pytest.raises(ValueError):
        ProcessAttributesProvider(attributes={"unsupported": "unsupported"})


def test_static_attributes_provider():
    attribute_key = "attribute"
    client_credential_value = True
    attributes = StaticAttributesProvider(attributes={attribute_key: client_credential_value}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] == client_credential_value


def test_thread_attributes_provider():
    attribute_key = "thread"
    attributes = ThreadAttributesProvider(attributes={attribute_key: "native_id"}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] is not None

    with pytest.raises(ValueError):
        ThreadAttributesProvider(attributes={"unsupported": "unsupported"})
