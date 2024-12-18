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


import mmap
import os
import tempfile

import pytest
from multistorageclient import StorageClient, StorageClientConfig


def verify_open_binary_mode(config: StorageClientConfig):
    storage_client = StorageClient(config)
    body = b"A" * 64 * 1024

    with tempfile.TemporaryDirectory(prefix="binary") as dirname:
        filename = os.path.join(dirname, "testfile.bin")

        fp = storage_client.open(filename, "wb")
        assert not fp.readable()
        assert fp.writable()
        fp.write(body)
        assert 64 * 1024 == fp.tell()

        # verify file is written
        fp.close()
        assert len(list(storage_client.list(dirname))) == 1
        assert storage_client.info(filename).content_length == len(body)

        # verify file is readable
        fp = storage_client.open(filename, "rb")
        assert fp.readable()
        assert not fp.writable()
        assert fp.read(10) == b"A" * 10
        buffer = bytearray(12)
        assert 10 == fp.tell()
        assert 12 == fp.readinto(buffer)
        assert buffer == b"A" * 12
        fp.close()

        storage_client.delete(filename)

        with pytest.raises(FileNotFoundError):
            storage_client.open(os.path.join(dirname, "file-does-not-exist"), "r")


def verify_open_text_mode(config: StorageClientConfig):
    storage_client = StorageClient(config)
    body = '{"text":"✅ Unicode Test ✅"}'

    with tempfile.TemporaryDirectory(prefix="text") as dirname:
        filename = os.path.join(dirname, "testfile.json")

        fp = storage_client.open(filename, "w")
        assert not fp.readable()
        assert fp.writable()
        fp.write(body)

        # verify file is written
        fp.close()
        assert len(list(storage_client.list(dirname))) == 1
        assert storage_client.info(filename).content_length == len(body.encode("utf-8"))

        # verify file is readable
        fp = storage_client.open(filename, "r")
        assert fp.readable()
        assert not fp.writable()
        assert fp.read() == body
        fp.close()

        storage_client.delete(filename)


def verify_open_mmap(config: StorageClientConfig):
    storage_client = StorageClient(config)
    body = b"A" * 64 * 1024

    with tempfile.TemporaryDirectory(prefix="text") as dirname:
        filename = os.path.join(dirname, "testfile.json")

        fp = storage_client.open(filename, "wb")
        assert not fp.readable()
        assert fp.writable()
        fp.write(body)
        fp.close()

        with storage_client.open(filename, "rb") as fp:
            with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
                content = mm[:]
                assert content == body


def test_open_file():
    config = StorageClientConfig.from_dict(
        {
            "profiles": {
                "default": {
                    "storage_provider": {
                        "type": "file",
                        "options": {
                            "base_path": "/",
                        },
                    }
                }
            }
        }
    )

    verify_open_binary_mode(config)
    verify_open_text_mode(config)
