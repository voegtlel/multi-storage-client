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
import uuid

import multistorageclient as msc
import pytest

BUCKET_NAME = "test-bucket-0002"
ENDPOINT_URL = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
PROFILE_NAME = "s3-local-test-fsspec"

MSC_CONFIG_YAML = f"""
profiles:
  {PROFILE_NAME}:
    storage_provider:
      type: s3
      options:
        endpoint_url: {ENDPOINT_URL}
        region_name: us-east-1
        base_path: {BUCKET_NAME}
    credentials_provider:
      type: S3Credentials
      options:
        access_key: {ACCESS_KEY}
        secret_key: {SECRET_KEY}
"""


def generate_file(fs, path):
    with fs.open(path, "w") as fp:
        fp.write(str(uuid.uuid4()))


def verify_fsspec_implementation():
    fs = msc.async_fs.MultiAsyncFileSystem()

    # Create 15 files in total
    generate_file(fs, f"{PROFILE_NAME}/f1.txt")
    generate_file(fs, f"{PROFILE_NAME}/f2.txt")
    generate_file(fs, f"{PROFILE_NAME}/f3.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/f1.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/f2.txt")
    generate_file(fs, f"{PROFILE_NAME}/b/f3.txt")
    generate_file(fs, f"{PROFILE_NAME}/b/f4.txt")
    generate_file(fs, f"{PROFILE_NAME}/c/f5.txt")
    generate_file(fs, f"{PROFILE_NAME}/c/f6.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/0001/f1.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/0001/f2.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/b/0002/f1.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/b/0002/f2.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/b/c/0003/f1.txt")
    generate_file(fs, f"{PROFILE_NAME}/a/b/c/0003/f2.txt")

    # List files
    assert len(fs.ls(f"{PROFILE_NAME}/")) == 6
    assert len(fs.ls(f"{PROFILE_NAME}/a")) == 4
    assert len(fs.find(f"{PROFILE_NAME}/")) == 15
    assert len(fs.glob(f"{PROFILE_NAME}/*")) == 6
    assert len(fs.glob(f"{PROFILE_NAME}/**")) == 24
    assert len(fs.glob(f"{PROFILE_NAME}/**/*")) == 23

    # Get and Put
    with tempfile.TemporaryDirectory() as tmpdir:
        fs.get(f"{PROFILE_NAME}/f1.txt", os.path.join(tmpdir, "f1.txt"))
        assert os.path.exists(os.path.join(tmpdir, "f1.txt"))
        fs.put(os.path.join(tmpdir, "f1.txt"), f"{PROFILE_NAME}/f1.txt")

    # Cat
    assert len(fs.cat_file(f"{PROFILE_NAME}/a/0001/f1.txt")) == 36

    # Info
    fileinfo = fs.info(f"{PROFILE_NAME}/f1.txt")
    assert fileinfo["size"] == 36

    # Exists
    assert fs.exists(f"{PROFILE_NAME}/a/b/c/0003/f2.txt")
    assert not fs.exists(f"{PROFILE_NAME}/a/b/c/0003/f3.txt")

    # mkdir is no-op for object store
    fs.mkdir(f"{PROFILE_NAME}/a/b/c/d/e/")
    with pytest.raises(FileNotFoundError):
        fs.info(f"{PROFILE_NAME}/a/b/c/d/e/")

    # Pipe and Delete
    fs.pipe_file(f"{PROFILE_NAME}/pipe_file.txt", uuid.uuid4().bytes)
    assert fs.exists(f"{PROFILE_NAME}/pipe_file.txt")
    fs.rm(f"{PROFILE_NAME}/pipe_file.txt")
    assert not fs.exists(f"{PROFILE_NAME}/pipe_file.txt")

    # Move a single file
    fs.pipe_file(f"{PROFILE_NAME}/pipe_file.txt", uuid.uuid4().bytes)
    fs.mv(f"{PROFILE_NAME}/pipe_file.txt", f"{PROFILE_NAME}/pipe_file_rename.txt")
    assert not fs.exists(f"{PROFILE_NAME}/pipe_file.txt")
    assert fs.exists(f"{PROFILE_NAME}/pipe_file_rename.txt")
    fs.rm(f"{PROFILE_NAME}/pipe_file_rename.txt")

    # Delete a directory
    fs.rm(f"{PROFILE_NAME}/a/b/c", recursive=True)
    assert len(fs.glob(f"{PROFILE_NAME}/a/b/c")) == 0

    # Move a directory
    fs.mv(f"{PROFILE_NAME}/a/b", f"{PROFILE_NAME}/dir1", recursive=True)
    assert len(fs.find(f"{PROFILE_NAME}/a/b")) == 0
    assert len(fs.find(f"{PROFILE_NAME}/dir1")) == 2
    assert len(fs.glob(f"{PROFILE_NAME}/dir1/*")) == 1
    assert len(fs.glob(f"{PROFILE_NAME}/dir1/**/*")) == 3


def clear_bucket(client):
    """Delete all objects in the bucket"""
    try:
        objects = client.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", [])
        for obj in objects:
            client.delete_object(Bucket=BUCKET_NAME, Key=obj["Key"])
        client.delete_bucket(Bucket=BUCKET_NAME)
    except client.exceptions.NoSuchBucket:
        pass


def test_fsspec_implementation():
    """
    Setup MinIO for local testing with fsspec implementation.
    """
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )

    clear_bucket(client)

    try:
        client.create_bucket(Bucket=BUCKET_NAME)
    except Exception as e:
        print(f"Failed to create bucket: {e}")
        pass

    # Set MSC_CONFIG
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as fp:
        fp.write(MSC_CONFIG_YAML)
        fp.flush()
        os.environ["MSC_CONFIG"] = fp.name

    verify_fsspec_implementation()

    clear_bucket(client)
