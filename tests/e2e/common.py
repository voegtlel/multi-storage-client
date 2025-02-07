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
import time
import uuid

import multistorageclient as msc

MB = 1024 * 1024


def delete_files(storage_client: msc.StorageClient, prefix: str) -> None:
    for object in storage_client.list(prefix=prefix):
        storage_client.delete(object.key)


def verify_shortcuts(profile: str, prefix: str) -> None:
    body = b"A" * (16 * MB)

    for i in range(10):
        with msc.open(f"msc://{profile}/{prefix}/data-{i}.bin", "wb") as fp:
            fp.write(body)

    results = msc.glob(f"msc://{profile}/{prefix}/**/*.bin")
    assert len(results) == 10

    for res in results:
        with msc.open(res, "rb") as fp:
            assert fp.read(10) == b"A" * 10


def verify_storage_provider(storage_client: msc.StorageClient, prefix: str) -> None:
    body = b"A" * (16 * MB)
    text = '{"text":"✅ Unicode Test ✅"}'

    # write file
    filename = f"{prefix}/testfile.bin"
    storage_client.write(filename, body)
    assert len(list(storage_client.list(prefix))) == 1

    # is file
    assert storage_client.is_file(filename)
    assert not storage_client.is_file(prefix)
    assert not storage_client.is_file("not-exist-prefix")

    # glob
    assert len(storage_client.glob("*.py")) == 0
    assert storage_client.glob(f"{prefix}/*.bin")[0] == filename
    assert len(storage_client.glob(f"{prefix}/*.bin")) == 1

    # verify file is written
    assert storage_client.read(filename) == body
    info = storage_client.info(filename)
    assert info is not None
    assert info.content_length == len(body)
    assert storage_client.is_file(filename)
    assert not storage_client.is_file(prefix)

    # upload
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(body)
    temp_file.seek(0)
    temp_file.flush()
    storage_client.upload_file(filename, temp_file.name)
    os.unlink(temp_file.name)

    # download
    # Create a tmpdir base dir but not the full path to test if storage provider creates the path
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir_name = tmpdir  # Get the filename
        temp_file_path = os.path.join(temp_dir_name, "downloads/data", "downloaded.bin")
        storage_client.download_file(filename, temp_file_path)
        assert os.path.getsize(temp_file_path) == len(body)

    # open file
    with storage_client.open(filename, "wb") as fp:
        fp.write(body)
        assert fp.name == filename
        assert not fp.closed
    assert fp.closed

    assert len(list(storage_client.list(prefix))) == 1

    with storage_client.open(filename, "rb") as fp:
        content = fp.read()
        assert content == body
        assert isinstance(content, bytes)

    # delete file
    storage_client.delete(filename)

    # For CSPs that do not support strong consistency.
    time.sleep(1)
    assert len(list(storage_client.list(prefix))) == 0

    # large file
    body_large = b"*" * (550 * MB)
    with storage_client.open(filename, "wb") as fp:
        fp.write(body_large)

    assert len(list(storage_client.list(prefix))) == 1

    with storage_client.open(filename, "rb") as fp:
        read_size = 128 * MB
        content = fp.read(read_size)
        assert len(content) == read_size

        content += fp.read(read_size)
        assert len(content) == 2 * read_size

        content += fp.read(read_size)
        content += fp.read()
        assert len(content) == len(body_large)
        assert isinstance(content, bytes)

    # delete file
    storage_client.delete(filename)
    assert len(list(storage_client.list(prefix))) == 0

    # unicode file
    filename = f"{prefix}/testfile.txt"
    with storage_client.open(filename, "w") as fp:
        fp.write(text)

    assert len(list(storage_client.list(prefix))) == 1

    with storage_client.open(filename, "r") as fp:
        content = fp.read()
        assert content == text
        assert isinstance(content, str)

    # delete file
    storage_client.delete(filename)

    # For CSPs that do not support strong consistency.
    time.sleep(1)
    assert len(list(storage_client.list(prefix))) == 0


def test_shortcuts(profile: str):
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    prefix = f"files-{uuid.uuid4()}"
    try:
        verify_shortcuts(profile, prefix)
    finally:
        delete_files(client, prefix)


def test_storage_client(profile: str):
    client, _ = msc.resolve_storage_client(f"msc://{profile}/")
    prefix = f"files-{uuid.uuid4()}"
    try:
        verify_storage_provider(client, prefix)
    finally:
        delete_files(client, prefix)
