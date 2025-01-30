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
import mmap
import os
import tempfile

import multistorageclient as msc
import numpy as np
from multistorageclient.file import ObjectFile

MB = 1024 * 1024


class TestS3Local:
    def __init__(self, bucket_name: str = "test-bucket-0002"):
        self.bucket_name = bucket_name
        self.minio_access_key = "minioadmin"
        self.minio_secret_key = "minioadmin"
        self.endpoint_url = "http://localhost:9000"

        import boto3

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            region_name="us-east-1",
        )

    def setup_s3_local(self, base_path: str = ""):
        self.clean_bucket()

        try:
            self.client.create_bucket(Bucket=self.bucket_name)
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            pass

        config_json = json.dumps(
            {
                "profiles": {
                    "s3-iad": {
                        "storage_provider": {
                            "type": "s3",
                            "options": {
                                "endpoint_url": self.endpoint_url,
                                "region_name": "us-east-1",
                                "base_path": "" if not base_path else base_path,
                            },
                        },
                        "credentials_provider": {
                            "type": "S3Credentials",
                            "options": {
                                "access_key": self.minio_access_key,
                                "secret_key": self.minio_secret_key,
                            },
                        },
                    }
                },
                "cache": {"size_mb": 5000},
            }
        )

        config_filename = os.path.join(tempfile.gettempdir(), ".msc_config.json")

        with open(config_filename, "w") as fp:
            fp.write(config_json)

        os.environ["MSC_CONFIG"] = config_filename

    def clean_bucket(self):
        """Delete all objects in the bucket"""
        try:
            objects = self.client.list_objects_v2(Bucket=self.bucket_name).get("Contents", [])
            for obj in objects:
                self.client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])
            self.client.delete_bucket(Bucket=self.bucket_name)
        except self.client.exceptions.NoSuchBucket:
            pass


def verify_shortcuts(profile: str, prefix: str):
    prefix = f"{prefix}/data"
    body = b"A" * (64 * MB)

    # open files
    for i in range(10):
        with msc.open(f"msc://{profile}/{prefix}/data-{i}.bin", "wb") as fp:
            fp.write(body)

    # glob
    assert len(msc.glob(f"msc://{profile}/{prefix}/**/*.bin")) == 10

    # upload
    fp = tempfile.NamedTemporaryFile(mode="wb", delete=False)
    fp.write(body)
    fp.close()
    msc.upload_file(f"msc://{profile}/{prefix}/data-11.bin", fp.name)

    file_list = msc.glob(f"msc://{profile}/{prefix}/**/*.bin")
    assert len(file_list) == 11

    for file_url in file_list:
        assert msc.is_file(file_url)

    # download
    filepath = os.path.join(tempfile.gettempdir(), "data-11.bin")
    msc.download_file(f"msc://{profile}/{prefix}/data-11.bin", filepath)
    assert os.path.exists(filepath)

    # numpy
    arr = np.array([1, 2, 3, 4, 5], dtype=np.int32)
    msc.numpy.save(f"msc://{profile}/{prefix}/arr-01.npy", arr)
    assert msc.numpy.load(f"msc://{profile}/{prefix}/arr-01.npy").all() == arr.all()
    assert msc.numpy.memmap(f"msc://{profile}/{prefix}/arr-01.npy", dtype=np.int32, shape=(5,)).all() == arr.all()

    # mmap
    with msc.open(f"msc://{profile}/{prefix}/data-2.bin") as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            content = mm[:]
            assert content == body

    # open file without cache
    with msc.open(f"msc://{profile}/{prefix}/data-2.bin", disable_read_cache=True) as fp:
        assert isinstance(fp, ObjectFile)
        assert fp._cache_manager is None


def test_msc_shortcuts_with_s3() -> None:
    s3_test_helper = TestS3Local()
    s3_test_helper.setup_s3_local(base_path=s3_test_helper.bucket_name)
    try:
        verify_shortcuts(profile="s3-iad", prefix="files")
    finally:
        s3_test_helper.clean_bucket()


def test_msc_shortcuts_with_empty_base_path() -> None:
    s3_test_helper = TestS3Local()
    s3_test_helper.setup_s3_local()
    try:
        verify_shortcuts(profile="s3-iad", prefix=f"{s3_test_helper.bucket_name}/files")
    finally:
        s3_test_helper.clean_bucket()
