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

import numpy as np

import multistorageclient as msc

MB = 1024 * 1024


def verify_shortcuts():
    prefix = 'files'
    body = b'A' * (64 * MB)

    # open files
    for i in range(10):
        with msc.open(f'msc://s3-iad/{prefix}/data-{i}.bin', 'wb') as fp:
            fp.write(body)

    # glob
    assert len(msc.glob('msc://s3-iad/**/*.bin')) == 10

    # upload
    fp = tempfile.NamedTemporaryFile(mode='wb', delete=False)
    fp.write(body)
    fp.close()
    msc.upload_file(f'msc://s3-iad/{prefix}/data-11.bin', fp.name)

    file_list = msc.glob('msc://s3-iad/**/*.bin')
    assert len(file_list) == 11

    for file_url in file_list:
        assert msc.is_file(file_url)

    # download
    filepath = os.path.join(tempfile.gettempdir(), 'data-11.bin')
    msc.download_file(f'msc://s3-iad/{prefix}/data-11.bin', filepath)
    assert os.path.exists(filepath)

    # numpy
    arr = np.array([1, 2, 3, 4, 5], dtype=np.int32)
    msc.numpy.save(f'msc://s3-iad/{prefix}/arr-01.npy', arr)
    assert msc.numpy.load(f'msc://s3-iad/{prefix}/arr-01.npy').all() == arr.all()
    assert msc.numpy.memmap(f'msc://s3-iad/{prefix}/arr-01.npy', dtype=np.int32, shape=(5,)).all() == arr.all()

    # mmap
    with msc.open(f'msc://s3-iad/{prefix}/data-2.bin') as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            content = mm[:]
            assert content == body


def test_s3_local():
    """
    Use MinIO for local S3 storage testing.
    """
    import boto3

    bucket_name = 'test-bucket-0002'

    minio_access_key = 'minioadmin'
    minio_secret_key = 'minioadmin'
    endpoint_url = 'http://localhost:9000'

    client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        region_name='us-east-1'
    )

    clean_bucket(client, bucket_name)

    try:
        client.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Failed to create bucket: {e}")
        pass

    config_json = json.dumps({
        "profiles": {
            "s3-iad": {
                "storage_provider": {
                    "type": "s3",
                    "options": {
                        "endpoint_url": endpoint_url,
                        "region_name": "us-east-1",
                        "base_path": bucket_name,
                    }
                },
                "credentials_provider": {
                    "type": "S3Credentials",
                    "options": {
                        "access_key": minio_access_key,
                        "secret_key": minio_secret_key,
                    }
                }
            }
        },
        "cache": {
            "size_mb": 5000
        }
    })

    config_filename = os.path.join(tempfile.gettempdir(), '.msc_config.json')

    with open(config_filename, 'w') as fp:
        fp.write(config_json)

    os.environ['MSC_CONFIG'] = config_filename

    verify_shortcuts()

    clean_bucket(client, bucket_name)


def clean_bucket(client, bucket_name):
    """ Delete all objects in the bucket """
    try:
        objects = client.list_objects_v2(Bucket=bucket_name).get('Contents', [])
        for obj in objects:
            client.delete_object(Bucket=bucket_name, Key=obj['Key'])
        client.delete_bucket(Bucket=bucket_name)
    except client.exceptions.NoSuchBucket:
        pass
