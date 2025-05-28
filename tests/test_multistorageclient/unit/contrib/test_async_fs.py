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

import pytest
import fsspec


@pytest.mark.asyncio
async def test_multi_async_filesystem(file_storage_config_with_cache):
    with tempfile.TemporaryDirectory() as dirname:
        filesystem = fsspec.filesystem("msc")

        # test _pipe_file, _cat_file
        filename = os.path.join(dirname, "test_file.txt")
        test_path = f"default{filename}"
        expected_content = b"Hello World"
        await filesystem._pipe_file(test_path, expected_content)
        content = await filesystem._cat_file(test_path)
        assert content == expected_content, f"Expected {expected_content}, got {content}"

        # test _ls
        dir_path = os.path.join(dirname, "test_directory/")
        test_dir_path = f"default{dir_path}"
        test_file_list = [f"{test_dir_path}file{i}.txt" for i in range(3)]
        for file_path in test_file_list:
            await filesystem._pipe_file(file_path, b"test content")
        listed_files = await filesystem._ls(test_dir_path)
        expected_file_list = sorted([f"default{dir_path}file{i}.txt".lstrip("/") for i in range(3)])
        assert sorted(f["name"] for f in listed_files) == expected_file_list, (
            f"Expected {expected_file_list}, got {sorted(f['name'] for f in listed_files)}"
        )

        # test _info on a file
        info = await filesystem._info(test_path)
        assert info["name"] == test_path, f"Expected name {test_path}, got {info['name']}"
        assert info["type"] == "file"

        # test should ignore the first / in the path
        info = await filesystem._info("/" + test_path)
        assert info["name"] == test_path, f"Expected name {test_path}, got {info['name']}"
        assert info["type"] == "file"

        # test _info on a "directory" both with and without an ending "/"
        info = await filesystem._info(test_dir_path)
        assert info["name"] == test_dir_path, f"Expected name {test_dir_path}, got {info['name']}"
        assert info["type"] == "directory"

        info = await filesystem._info(test_dir_path.removesuffix("/"))
        assert info["name"] == test_dir_path, f"Expected name {test_dir_path}, got {info['name']}"
        assert info["type"] == "directory"

        # test _open
        file_obj = await filesystem._open(test_path, mode="rb")
        opened_content = file_obj.read()
        assert opened_content == expected_content, f"Expected {expected_content}, got {opened_content}"
        file_obj.close()

        # test _rm
        await filesystem._rm(test_path)
        with pytest.raises(FileNotFoundError):
            await filesystem._cat_file(test_path)

        # test _rm on a directory with recursive=True
        await filesystem._rm(test_dir_path, recursive=True)
        test_dir_ls = await filesystem._ls(test_dir_path)
        assert len(test_dir_ls) == 0

        # test _put_file
        local_file_path = os.path.join(dirname, "local_test_file.txt")
        remote_file_path = f"default{os.path.join(dirname, 'remote_test_file.txt')}"
        expected_file_content = b"File for _put_file test"
        with open(local_file_path, "wb") as local_file:
            local_file.write(expected_file_content)

        await filesystem._put_file(local_file_path, remote_file_path, somekwarg1="someval1", somekwarg2="someval2")
        put_file_content = await filesystem._cat_file(remote_file_path)
        assert put_file_content == expected_file_content, f"Expected {expected_file_content}, got {put_file_content}"

        # test _get_file
        downloaded_file_path = os.path.join(dirname, "downloaded_test_file.txt")
        await filesystem._get_file(remote_file_path, downloaded_file_path)

        with open(downloaded_file_path, "rb") as downloaded_file:
            downloaded_content = downloaded_file.read()
        assert downloaded_content == expected_file_content, (
            f"Expected downloaded content to be {expected_file_content}, got {downloaded_content}"
        )

        # test _cp_file
        copy_file_path = f"{remote_file_path}.copy"
        await filesystem._cp_file(remote_file_path, copy_file_path)
        src_info = await filesystem._info(remote_file_path)
        dest_info = await filesystem._info(copy_file_path)
        assert src_info["size"] == dest_info["size"]

        # Clean up remote file after test
        await filesystem._rm(remote_file_path, recursive=True)
