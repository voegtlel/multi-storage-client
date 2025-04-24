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

# pytest fixtures.
#
# https://docs.pytest.org/en/stable/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files

import os
import tempfile
import uuid

import pytest

CONFIG_DIR = tempfile.gettempdir()

CONFIG_YAML = """
profiles:
  default:
    storage_provider:
      type: file
      options:
        base_path: /
"""

CONFIG_YAML_WITH_CACHE = """
profiles:
  default:
    storage_provider:
      type: file
      options:
        base_path: /
cache: {}
"""


def setup_config_file(config_json):
    config_filename = os.path.join(CONFIG_DIR, f"msc_config-{uuid.uuid4().hex}.yaml")
    with open(config_filename, "w") as fp:
        fp.write(config_json)

    os.environ["MSC_CONFIG"] = config_filename
    return config_filename


def delete_config_file(config_filename):
    os.unlink(config_filename)


@pytest.fixture
def file_storage_config():
    config_filename = setup_config_file(CONFIG_YAML)
    yield config_filename
    delete_config_file(config_filename)


@pytest.fixture
def file_storage_config_with_cache():
    config_filename = setup_config_file(CONFIG_YAML_WITH_CACHE)
    yield config_filename
    delete_config_file(config_filename)


@pytest.fixture(autouse=True, scope="function")
def reset_globals():
    # Reset the instance cache before each test.
    from multistorageclient import shortcuts

    with shortcuts._cache_lock:
        shortcuts._instance_cache.clear()

    # Reset the environment variables before each test.
    os.environ.clear()

    yield
