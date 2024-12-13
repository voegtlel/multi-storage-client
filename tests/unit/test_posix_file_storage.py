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

from multistorageclient import StorageClient, StorageClientConfig


def verify_functions(config: StorageClientConfig, prefix: str) -> None:
    storage_client = StorageClient(config)
    body = b'A' * 64 * 1024

    assert len(list(storage_client.list(prefix))) == 0

    # write file
    filename = os.path.join(prefix, 'testfile.bin')
    storage_client.write(filename, body)
    assert len(list(storage_client.list(prefix))) == 1

    # glob
    assert len(storage_client.glob(os.path.join(prefix, "*.py"))) == 0
    assert len(storage_client.glob(os.path.join(prefix, "*.bin"))) == 1

    # verify file is written
    assert storage_client.read(filename) == body
    info = storage_client.info(filename)
    assert info is not None
    assert info.content_length == len(body)
    assert storage_client.is_file(filename)
    assert not storage_client.is_file(prefix)

    # delete file
    storage_client.delete(filename)
    assert len(list(storage_client.list(prefix))) == 0


def verify_list_segment(config: StorageClientConfig, prefix: str) -> None:
    storage_client = StorageClient(config)

    # Create some files.
    for i in range(1, 4):
        key = os.path.join(prefix, f'{i}.txt')
        storage_client.write(key, 'test'.encode())

    # Range over the files.
    for i in range(1, 4):
        assert {f'{i}.txt'} == {
            object_metadatum.key
            for object_metadatum
            in storage_client.list(
                prefix=prefix,
                start_after=f'{i - 1}.txt',
                end_at=f'{i}.txt'
            )
        }


def test_posix_file_storage_provider() -> None:
    base_path = '/'
    config = StorageClientConfig.from_dict({
        'profiles': {
            'default': {
                'storage_provider': {
                    'type': 'file',
                    'options': {
                        'base_path': base_path
                    }
                }
            }
        }
    })

    with tempfile.TemporaryDirectory() as tmpdir:
        verify_functions(config, prefix=os.path.relpath(tmpdir, base_path))
    with tempfile.TemporaryDirectory() as tmpdir:
        verify_list_segment(config, prefix=os.path.relpath(tmpdir, base_path))


def test_posix_file_storage_provider_with_base_path() -> None:
    def _config(base_path: str) -> StorageClientConfig:
        return StorageClientConfig.from_dict({
            'profiles': {
                'default': {
                    'storage_provider': {
                        'type': 'file',
                        'options': {
                            'base_path': base_path
                        }
                    }
                }
            }
        })

    with tempfile.TemporaryDirectory() as tmpdir:
        verify_functions(_config(tmpdir), prefix='')
    with tempfile.TemporaryDirectory() as tmpdir:
        verify_list_segment(_config(tmpdir), prefix='')
