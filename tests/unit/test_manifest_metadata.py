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
import random
import string
import tempfile
import pytest
from multistorageclient import StorageClient, StorageClientConfig

BASE_MANIFEST = {
    "version": "1",
}

BASE_MANIFEST_PART_1_LINES = [
    {"key": "dataset_073.tar.idx", "size_bytes": 808, "last_modified": "2024-08-29T22:43:47.675274Z"},
    {"key": "dataset_063.tar.idx", "size_bytes": 808, "last_modified": "2024-08-29T22:43:47.648609Z"},
]

BASE_MANIFEST_PART_2_LINES = [
    {"key": "dataset_066.tar", "size_bytes": 471040, "last_modified": "2024-08-29T22:42:53.556584Z"},
    {"key": "dataset_072.tar", "size_bytes": 471040, "last_modified": "2024-08-29T22:42:53.639584Z"},
]


def create_test_files(data_folderpath, files_metadata) -> None:
    # Create the files specified in the metadata
    for file_meta in files_metadata:
        file_path = os.path.join(data_folderpath, file_meta["key"])
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            f.write("x" * file_meta["size_bytes"])


def test_empty_metadata() -> None:
    # Test starting with an empty manifest, writing some files and then
    # generating a new manifest.
    with tempfile.TemporaryDirectory() as tmpdir:
        data_folderpath = os.path.join(tmpdir, "data")

        config_dict = {
            "profiles": {
                "default": {
                    "storage_provider": {"type": "file", "options": {"base_path": "/"}},
                    "metadata_provider": {"type": "manifest", "options": {"manifest_path": tmpdir, "writable": True}},
                }
            }
        }
        storage_client = StorageClient(StorageClientConfig.from_dict(config_dict))
        assert storage_client._metadata_provider
        assert len(list(storage_client.list(tmpdir))) == 0

        body = b"A" * 8 * 1024 * 1024
        storage_client.write(f"{data_folderpath}/testfile.bin", body)

        # Should not yet show up in listings.
        assert len(storage_client.glob(f"{data_folderpath}/testfile.bin")) == 0

        storage_client.commit_updates()
        assert len(storage_client.glob(f"{data_folderpath}/testfile.bin")) == 1

        # Reload and pick up only persistent changes to ensure the commit persisted.
        print("Recreating storage_client")
        storage_client = StorageClient(StorageClientConfig.from_dict(config_dict))
        assert len(storage_client.glob(f"{data_folderpath}/testfile.bin")) == 1


def test_commit_with_scan():
    # Test building a manifest by instructing the StorageClient to scan a
    # prefix to find the files to add to the manifest.
    with tempfile.TemporaryDirectory() as tmpdir:
        data_folderpath = os.path.join(tmpdir, "data")
        os.makedirs(data_folderpath, exist_ok=True)

        # Write files directly without storage client, so that it must scan to find.
        file_count = 10
        body = b"A" * 8 * 1024 * 1024
        for i in range(file_count):
            random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=11))
            filename = f"shard{i % 3}/testfile_{random_suffix}.bin"
            file_path = os.path.join(data_folderpath, filename)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as file:
                file.write(body)

        # Create StorageClient
        config_dict = {
            "profiles": {
                "default": {
                    "storage_provider": {"type": "file", "options": {"base_path": "/"}},
                    "metadata_provider": {"type": "manifest", "options": {"manifest_path": tmpdir, "writable": True}},
                }
            }
        }

        # No files visible until the commit with prefix.
        storage_client = StorageClient(StorageClientConfig.from_dict(config_dict))
        assert len(list(storage_client.list(""))) == 0

        storage_client.commit_updates(prefix=tmpdir)
        assert len(list(storage_client.list())) == file_count
        assert len(list(storage_client.list(""))) == file_count


def test_file_metadata_and_update():
    # Test creating a manifest and using it with StorageClient. Write a new
    # file and then generate an updated manifest.
    with tempfile.TemporaryDirectory() as tmpdir:
        manifest_folderpath = os.path.join(".msc_manifests", "2024-08-30T00:00:00Z")
        os.makedirs(os.path.join(tmpdir, manifest_folderpath), exist_ok=True)
        manifest_filepath = os.path.join(manifest_folderpath, "msc_manifest_index.json")

        # Relative to the manifest path.
        manifest_parts_folderpath = os.path.join("parts")
        os.makedirs(os.path.join(tmpdir, manifest_folderpath, manifest_parts_folderpath), exist_ok=True)
        manifest_part_1_filepath = os.path.join(manifest_parts_folderpath, "msc_manifest_part000001.jsonl")
        manifest_part_2_filepath = os.path.join(manifest_parts_folderpath, "msc_manifest_part000002.jsonl")

        data_folderpath = tmpdir

        manifest = {**BASE_MANIFEST, "parts": [{"path": manifest_part_1_filepath}, {"path": manifest_part_2_filepath}]}

        with open(os.path.join(tmpdir, manifest_filepath), "w") as file:
            file.write(json.dumps(manifest))

        with open(os.path.join(tmpdir, manifest_folderpath, manifest_part_1_filepath), "w") as file:
            file.write("\n".join([json.dumps(line) for line in BASE_MANIFEST_PART_1_LINES]))

        with open(os.path.join(tmpdir, manifest_folderpath, manifest_part_2_filepath), "w") as file:
            file.write("\n".join([json.dumps(line) for line in BASE_MANIFEST_PART_2_LINES]))

        # Create actual data files in the temporary directory to match the manifest
        create_test_files(data_folderpath, BASE_MANIFEST_PART_1_LINES + BASE_MANIFEST_PART_2_LINES)

        config_dict = {
            "profiles": {
                "default": {
                    "storage_provider": {"type": "file", "options": {"base_path": f"{data_folderpath}"}},
                    "metadata_provider": {
                        "type": "manifest",
                        "options": {"manifest_path": os.path.join(tmpdir, manifest_filepath), "writable": True},
                    },
                }
            }
        }
        storage_client = StorageClient(StorageClientConfig.from_dict(config_dict))
        assert storage_client._metadata_provider
        # Using relative paths for list since base_path is configured with storage provider
        # In this case, the prefix for list is empty
        assert len(list(storage_client.list())) == 4

        # Using relative paths here since base_path is configured with storage provider
        object_metadata = storage_client.info("dataset_072.tar")
        assert object_metadata.content_length == 471040

        # Try calls against non-existent files.
        bad_file_names = ["invalid-filename", tmpdir, "dataset_072.tar.no"]
        for file_name in bad_file_names:
            with pytest.raises(FileNotFoundError):
                storage_client.info(file_name)

            with pytest.raises(FileNotFoundError):
                storage_client.read(file_name)

            with pytest.raises(FileNotFoundError):
                storage_client.delete(file_name)

        # Using relative paths here since base_path is configured with storage provider
        assert len(storage_client.glob("**/*.tar")) == 2
        assert len(storage_client.glob("**/*.idx")) == 2
        assert len(storage_client.glob("**/*.jpg")) == 0

        # Read back the files and validate their content length
        for file_meta in BASE_MANIFEST_PART_1_LINES + BASE_MANIFEST_PART_2_LINES:
            # Using relative paths here since base_path is configured with storage provider
            # The storage provider will resolve the full path
            content = storage_client.read(file_meta["key"])
            assert len(content) == file_meta["size_bytes"]
            assert content == b"x" * file_meta["size_bytes"]

        body = b"A" * 8 * 1024 * 1024
        storage_client.write("testfile.bin", body)

        # Should not yet show up in listings.
        assert len(storage_client.glob("testfile.bin")) == 0

        storage_client.commit_updates()
        assert len(storage_client.glob("testfile.bin")) == 1

        # Reload and pick up only persistent changes. Modify the config to
        # allow storage-client to find the newest manifest.
        config_dict = {
            "profiles": {
                "default": {
                    "storage_provider": {"type": "file", "options": {"base_path": f"{data_folderpath}"}},
                    "metadata_provider": {
                        "type": "manifest",
                        "options": {"manifest_path": ".msc_manifests", "writable": True},
                    },
                }
            }
        }
        storage_client = StorageClient(StorageClientConfig.from_dict(config_dict))
        assert len(storage_client.glob("testfile.bin")) == 1

        # Writing again to the same file should now fail because overwrites not supported.
        with pytest.raises(FileExistsError):
            storage_client.write("testfile.bin", body)
        with pytest.raises(FileExistsError):
            storage_client.upload_file("testfile.bin", "local_path.ignored")

        # Now, delete the file we just created.
        storage_client.delete("testfile.bin")
        storage_client.commit_updates()
        assert len(storage_client.glob("testfile.bin")) == 0


def test_download_file_with_metadata(file_storage_config):
    """
    This test checks if the file metadata provider can correctly resolve the realpath
    when download_file is invoked from shortcut API.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Setup source data
        src_data_folderpath = os.path.join(tmpdir, "src_data")
        os.makedirs(src_data_folderpath, exist_ok=True)

        # TODO(NGCDP-2927): Currently the manifest should be within the same storage provider base_path
        manifest_folderpath = os.path.join(src_data_folderpath, ".msc_manifests", "2024-08-30T00:00:00Z")
        os.makedirs(manifest_folderpath, exist_ok=True)
        manifest_filepath = os.path.join(manifest_folderpath, "msc_manifest_index.json")

        # Relative to the manifest path.
        manifest_parts_folderpath = os.path.join("parts")
        os.makedirs(os.path.join(manifest_folderpath, manifest_parts_folderpath), exist_ok=True)
        manifest_part_1_filepath = os.path.join(manifest_parts_folderpath, "msc_manifest_part000001.jsonl")
        manifest_part_2_filepath = os.path.join(manifest_parts_folderpath, "msc_manifest_part000002.jsonl")

        # Create actual data files in the temporary directory to match the manifest
        create_test_files(src_data_folderpath, BASE_MANIFEST_PART_1_LINES + BASE_MANIFEST_PART_2_LINES)

        # Generate manifest
        manifest = {**BASE_MANIFEST, "parts": [{"path": manifest_part_1_filepath}, {"path": manifest_part_2_filepath}]}

        with open(os.path.join(tmpdir, manifest_filepath), "w") as file:
            file.write(json.dumps(manifest))

        with open(os.path.join(tmpdir, manifest_folderpath, manifest_part_1_filepath), "w") as file:
            file.write("\n".join([json.dumps(line) for line in BASE_MANIFEST_PART_1_LINES]))

        with open(os.path.join(tmpdir, manifest_folderpath, manifest_part_2_filepath), "w") as file:
            file.write("\n".join([json.dumps(line) for line in BASE_MANIFEST_PART_2_LINES]))

        # Create a unique profile name so that we do not re-use a cached instance of StorageClient
        profile_name = test_download_file_with_metadata.__name__
        # msc config with metadata provider
        config_dict = {
            "profiles": {
                f"{profile_name}": {
                    "storage_provider": {"type": "file", "options": {"base_path": src_data_folderpath}},
                    "metadata_provider": {
                        "type": "manifest",
                        "options": {"manifest_path": os.path.join(tmpdir, manifest_filepath), "writable": True},
                    },
                }
            }
        }

        # Overwrite msc config with metadata provider info
        msc_config_path = file_storage_config
        with open(msc_config_path, "w") as conf_file:
            config_str = json.dumps(config_dict)
            conf_file.write(config_str)
        assert os.path.exists(msc_config_path)
        assert os.getenv("MSC_CONFIG") == msc_config_path

        dst_data_folderpath = os.path.join(tmpdir, "dst_data")
        os.makedirs(dst_data_folderpath, exist_ok=True)

        def download_test_files(dst_dir_path: str, files_metadata) -> None:
            import multistorageclient as msc

            # Create the files specified in the metadata
            for file_meta in files_metadata:
                # NOTE: The base_path will be resolved by looking up StorageProvider config
                remote_file_path = f'msc://{profile_name}/{file_meta["key"]}'
                local_file_path = os.path.join(dst_dir_path, file_meta["key"])
                msc.download_file(url=remote_file_path, local_path=local_file_path)

        download_test_files(
            dst_dir_path=dst_data_folderpath, files_metadata=BASE_MANIFEST_PART_1_LINES + BASE_MANIFEST_PART_2_LINES
        )

        storage_client = StorageClient(StorageClientConfig.from_dict(config_dict, profile=profile_name))
        assert storage_client._metadata_provider

        # Read the files from destination and validate their content length
        for file_meta in BASE_MANIFEST_PART_1_LINES + BASE_MANIFEST_PART_2_LINES:
            with open(os.path.join(dst_data_folderpath, file_meta["key"]), "r") as file:
                content = file.read()
                assert len(content) == file_meta["size_bytes"]
                assert content == "x" * file_meta["size_bytes"]
