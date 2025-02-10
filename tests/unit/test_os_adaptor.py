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

import os
import tempfile

import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL


def test_os_path_exist(file_storage_config):
    with tempfile.TemporaryDirectory() as tempdir:
        temp_file_path = os.path.join(tempdir, "existent_file.bin")
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(b"Some test data")

        existent_path = f"{MSC_PROTOCOL}default{tempdir}/existent_file.bin"
        assert msc.os.path.exists(existent_path)

        non_existent_path = f"{MSC_PROTOCOL}default{tempdir}/nonexistent_file.bin"
        assert not msc.os.path.exists(non_existent_path)


def test_os_path_isfile(file_storage_config):
    with tempfile.TemporaryDirectory() as tempdir:
        temp_file_path = os.path.join(tempdir, "existent_file.bin")
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(b"Some test data")

        existent_path = f"{MSC_PROTOCOL}default{tempdir}/existent_file.bin"
        assert msc.os.path.isfile(existent_path)
        assert msc.os.path.isfile(existent_path + "/") is False
        assert msc.os.path.isdir(existent_path) is False


def test_os_path_isdir(file_storage_config):
    with tempfile.TemporaryDirectory() as tempdir:
        existent_path = f"{MSC_PROTOCOL}default{tempdir}"
        assert msc.os.path.isdir(existent_path) == msc.os.path.isdir(existent_path, strict=False)
        assert msc.os.path.isfile(existent_path) is False


def test_makedirs(file_storage_config):
    # Verify local path
    with tempfile.TemporaryDirectory() as tempdir:
        path = os.path.join(tempdir, "test-dir")
        msc.os.makedirs(path)
        assert msc.os.path.isdir(path)

    # Verify msc path (expect no-op)
    with tempfile.TemporaryDirectory() as tempdir:
        path = f"{MSC_PROTOCOL}default{tempdir}/test-dir"
        msc.os.makedirs(path, exist_ok=True)
        msc.os.makedirs(path, exist_ok=False)
        assert msc.os.path.isdir(path) is False
