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

import functools
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.file import IN_MEMORY_FILE_SIZE_THRESHOLD
import os
import pytest
import tempfile
from typing import Type
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
@pytest.mark.parametrize(argnames=["with_cache"], argvalues=[[True], [False]])
def test_storage_providers(temp_data_store_type: Type[tempdatastore.TemporaryDataStore], with_cache: bool):
    with temp_data_store_type() as temp_data_store:
        profile = "data"
        config_dict = {"profiles": {profile: temp_data_store.profile_config_dict()}}
        if with_cache:
            config_dict["cache"] = {"size_mb": 5000}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

        file_extension = ".txt"
        file_path_fragments = ["prefix", "infix", f"suffix{file_extension}"]
        file_path = os.path.join(*file_path_fragments)
        file_body_bytes = b"\x00"
        file_body_string = file_body_bytes.decode()

        # Write a file.
        storage_client.write(path=file_path, body=file_body_bytes)

        # Check the file contents.
        assert storage_client.read(path=file_path) == file_body_bytes

        # Check the file metadata.
        file_info = storage_client.info(path=file_path)
        assert file_info is not None
        assert file_info.key.endswith(file_path)
        assert file_info.content_length == len(file_body_bytes)
        assert file_info.type == "file"
        for lead in ["", "/"]:
            assert storage_client.is_file(path=f"{lead}{file_path}")
            assert not storage_client.is_file(path=lead)
            assert not storage_client.is_file(path=f"{lead}{file_path_fragments[0]}-nonexistent")
            assert not storage_client.is_file(path=f"{lead}{file_path_fragments[0]}")

        # List the file.
        assert len(list(storage_client.list(prefix=file_path_fragments[0]))) == 1
        assert len(list(storage_client.list(prefix=os.path.join(*file_path_fragments[:2])))) == 1

        # Glob the file.
        assert len(storage_client.glob(pattern=f"*{file_extension}-nonexistent")) == 0
        assert len(storage_client.glob(pattern=os.path.join("**", f"*{file_extension}-nonexistent"))) == 0
        assert len(storage_client.glob(pattern=os.path.join("**", f"*{file_extension}"))) == 1
        assert storage_client.glob(pattern=os.path.join("**", f"*{file_extension}"))[0] == file_path

        # Check the infix directory metadata.
        for tail in ["", "/"]:
            directory_path = os.path.join(*file_path_fragments[:2])
            directory_info = storage_client.info(path=f"{directory_path}{tail}")
            assert directory_info is not None
            assert directory_info.key.endswith(f"{directory_path}/")
            assert directory_info.type == "directory"

        # List the infix directory.
        assert len(list(storage_client.list(prefix=f"{file_path_fragments[0]}/", include_directories=True))) == 1

        # Delete the file.
        storage_client.delete(path=file_path)

        # Upload + download the file.
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(file_body_bytes)
            temp_file.close()
            storage_client.upload_file(remote_path=file_path, local_path=temp_file.name)
        assert storage_client.is_file(path=file_path)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.close()
            storage_client.download_file(remote_path=file_path, local_path=temp_file.name)
            assert os.path.getsize(temp_file.name) == len(file_body_bytes)

        # Delete the file.
        storage_client.delete(path=file_path)

        # Open the file for writes + reads (bytes).
        with storage_client.open(path=file_path, mode="wb") as file:
            file.write(file_body_bytes)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="rb") as file:
            assert file.read() == file_body_bytes

        # Delete the file.
        storage_client.delete(path=file_path)

        # Open the file for writes + reads (string).
        with storage_client.open(path=file_path, mode="w") as file:
            file.write(file_body_string)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="r") as file:
            assert file.read() == file_body_string

        # Copy the file.
        file_copy_path_fragments = ["copy", *file_path_fragments]
        file_copy_path = os.path.join(*file_copy_path_fragments)
        storage_client.copy(src_path=file_path, dest_path=file_copy_path)
        assert storage_client.read(path=file_copy_path) == file_body_bytes

        # Delete the file and its copy.
        for path in [file_path, file_copy_path]:
            storage_client.delete(path=path)
        assert len(list(storage_client.list(prefix=file_path_fragments[0]))) == 0
        assert len(list(storage_client.list(prefix=file_copy_path_fragments[0]))) == 0

        # Open the file for appends (bytes).
        with storage_client.open(path=file_path, mode="ab") as file:
            file.write(file_body_bytes)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="rb") as file:
            assert file.read() == file_body_bytes

        # Delete the file.
        storage_client.delete(path=file_path)

        # Open the file for appends (string).
        with storage_client.open(path=file_path, mode="a") as file:
            file.write(file_body_string)
        assert storage_client.is_file(path=file_path)
        with storage_client.open(path=file_path, mode="r") as file:
            assert file.read() == file_body_string

        # Delete the file.
        storage_client.delete(path=file_path)

        # The GCS emulator doesn't support large file uploads.
        if temp_data_store_type is not tempdatastore.TemporaryGoogleCloudStorageBucket:
            large_file_body_bytes = b"\x00" * (IN_MEMORY_FILE_SIZE_THRESHOLD + 1)

            # Open the file for writes + reads (bytes).
            with storage_client.open(path=file_path, mode="wb") as file:
                file.write(large_file_body_bytes)
            assert storage_client.is_file(path=file_path)
            with storage_client.open(path=file_path, mode="rb") as file:
                content = b""
                for chunk in iter(functools.partial(file.read, (IN_MEMORY_FILE_SIZE_THRESHOLD // 2)), b""):
                    content += chunk
                assert len(content) == len(large_file_body_bytes)

            # Delete the file.
            storage_client.delete(path=file_path)

        # Write files.
        file_numbers = range(1, 3)
        for i in file_numbers:
            storage_client.write(path=f"{i}{file_extension}", body=file_body_bytes)

        # List the files (paginated).
        for i in file_numbers:
            files = list(
                storage_client.list(prefix="", start_after=f"{i - 1}{file_extension}", end_at=f"{i}{file_extension}")
            )
            assert len(files) == 1
            assert files[0].key.endswith(f"{i}{file_extension}")

        # Delete the files.
        for i in file_numbers:
            storage_client.delete(path=f"{i}{file_extension}")
