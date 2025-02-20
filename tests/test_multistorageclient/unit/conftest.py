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

import pytest

CONFIG_FILENAME = os.path.join(tempfile.gettempdir(), "msc_config.yaml")
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
    with open(CONFIG_FILENAME, "w") as fp:
        fp.write(config_json)

    os.environ["MSC_CONFIG"] = CONFIG_FILENAME


def delete_config_file():
    os.unlink(CONFIG_FILENAME)


@pytest.fixture
def file_storage_config():
    setup_config_file(CONFIG_YAML)
    yield CONFIG_FILENAME
    delete_config_file()


@pytest.fixture
def file_storage_config_with_cache():
    setup_config_file(CONFIG_YAML_WITH_CACHE)
    yield CONFIG_FILENAME
    delete_config_file()
