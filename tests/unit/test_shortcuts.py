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

import json
import os
import tempfile
import threading

import multistorageclient as msc
import pytest
from multistorageclient.types import MSC_PROTOCOL
from utils import file_storage_config

MB = 1024 * 1024


def test_resolve_storage_client(file_storage_config):
    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client(f'{MSC_PROTOCOL}fake/bucket/testfile.bin')

    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client('http://fake/bucket/testfile.bin')

    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client('relative/to/current/path')

    # Verify the three ways to access local filesystem are the same
    sc1, _ = msc.resolve_storage_client('/usr/local/fake/bucket/testfile.bin')
    sc2, _ = msc.resolve_storage_client('file:///usr/local/fake/bucket/testfile.bin')
    sc3, _ = msc.resolve_storage_client('msc://default/usr/local/fake/bucket/testfile.bin')
    assert sc1 == sc2 == sc3

    # Multithreading test to verify the storage_client instance is the same
    def storage_client_thread(results, index):
        tempdir = tempfile.mkdtemp()
        storage_client, path = msc.resolve_storage_client(f'{MSC_PROTOCOL}default{tempdir}/testfile.bin')
        results[index] = (storage_client, path)

    num_threads = 32
    threads = []
    results = [None] * num_threads
    for i in range(num_threads):
        t = threading.Thread(target=storage_client_thread, args=(results, i))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    storage_client, _ = results[0]
    for i in range(1, num_threads):
        assert results[i][0] is storage_client, "All threads should return the same StorageClient instance"


def test_open_url(file_storage_config):
    body = b'A' * 64 * MB
    tempdir = tempfile.mkdtemp()

    fp = msc.open(f'{MSC_PROTOCOL}default{tempdir}/testfile.bin', 'wb')
    fp.write(body)
    fp.close()

    fp = msc.open(f'{MSC_PROTOCOL}default{tempdir}/testfile.bin', 'rb')
    content = fp.read()
    fp.close()
    assert body == content

    results = msc.glob(f'{MSC_PROTOCOL}default{tempdir}/*.bin')
    assert len(results) == 1
    assert results[0] == f'{MSC_PROTOCOL}default{tempdir}/testfile.bin'


def test_download_file(file_storage_config):
    body = b'A' * 64 * MB
    tempdir = tempfile.mkdtemp()

    # Write to a test file
    remote_file_path = f'{MSC_PROTOCOL}default{tempdir}/testfile.bin'
    fp = msc.open(remote_file_path, 'wb')
    fp.write(body)
    fp.close()

    assert msc.is_file(url=remote_file_path)

    local_tempdir = tempfile.mkdtemp()
    local_file_path = f'{local_tempdir}/testfile.bin'
    msc.download_file(url=remote_file_path, local_path=local_file_path)

    fp = msc.open(f'{MSC_PROTOCOL}default{local_file_path}', 'rb')
    content = fp.read()
    fp.close()
    assert body == content

    results = msc.glob(f'{MSC_PROTOCOL}default{local_tempdir}/*.bin')
    assert len(results) == 1
    assert results[0] == f'{MSC_PROTOCOL}default{local_tempdir}/testfile.bin'


def test_glob_include_prefix(file_storage_config):
    data_dir = os.path.join(tempfile.gettempdir(), 'test_data')
    profile_name = 'test_glob_include_prefix'
    config_json = json.dumps({
        'profiles': {
            profile_name: {
                'storage_provider': {
                    'type': 'file',
                    'options': {
                        'base_path': data_dir,
                    }
                }
            }
        }
    })
    previous_config_path = os.getenv('MSC_CONFIG')
    config_filename = os.path.join(tempfile.gettempdir(), '.msc_config.json')

    with open(config_filename, 'w') as fp:
        fp.write(config_json)

    os.environ['MSC_CONFIG'] = config_filename

    body = b'A' * 64 * MB
    sub_prefix = os.path.basename(tempfile.mkdtemp())

    os.makedirs(os.path.join(data_dir, sub_prefix), exist_ok=True)

    # Write to a test file
    remote_file_path = f'{MSC_PROTOCOL}{profile_name}/{sub_prefix}/testfile.bin'
    with msc.open(remote_file_path, 'wb') as fp:
        fp.write(body)

    # NOTE: The URL here does not include the base_path, but profile name and sub-prefix
    results = msc.glob(f'{MSC_PROTOCOL}{profile_name}/{sub_prefix}/**/*.bin')
    assert len(results) == 1

    with msc.open(results[0], 'rb') as fp:
        assert fp.read(10) == b'A' * 10


def test_is_empty(file_storage_config):
    assert msc.is_empty('/usr/bin') is False
    assert msc.is_empty('/tmp/dir/not/exist')

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, 'testfile.bin')
        with msc.open(filepath, 'wb') as fp:
            fp.write(b'TEST')

        assert msc.is_empty(f'{MSC_PROTOCOL}default{tempdir}') is False
