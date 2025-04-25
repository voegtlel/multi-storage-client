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
from typing import List
import multistorageclient as msc


def create_test_files(num_files: int, size_mb: int) -> List[str]:
    """Create test files of specified size and return their paths."""
    file_paths = []
    for i in range(num_files):
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            # Create a 1MB file with random data
            temp_file.write(os.urandom(size_mb * 1024 * 1024))
            file_paths.append(temp_file.name)
    return file_paths


def test_file_reads_from_cache_with_shortcuts():
    """Test file reads from cache using MSC shortcuts."""
    # Create test files
    num_files = 5
    file_size_mb = 1
    test_files = create_test_files(num_files=num_files, size_mb=file_size_mb)
    profile = "test-s3-iad"
    test_uuid = str(uuid.uuid4())
    base_path = f"msc://{profile}/{test_uuid}"

    # Upload files using shortcuts
    for file_path in test_files:
        msc.upload_file(f"{base_path}/{file_path.lstrip('/')}", file_path)

    # Read files using shortcuts and verify cache hits
    for file_path in test_files:
        # First read should be a cache miss
        with msc.open(f"{base_path}/{file_path.lstrip('/')}", "rb") as f:
            content = f.read()

        # Verify file exists in cache
        assert msc.os.path.exists(f"{base_path}/{file_path.lstrip('/')}")

        # Second read should be a cache hit
        with msc.open(f"{base_path}/{file_path.lstrip('/')}", "rb") as f:
            cached_content = f.read()

        # Verify content matches
        assert content == cached_content

    # Clean up
    # delete all files from test-s3-iad
    iter = msc.list(base_path + "/")
    for item in iter:
        msc.delete(item.key)

    # delete all files from test-s3e
    cache_iter = msc.list("msc://test-s3e/")
    for item in cache_iter:
        msc.delete(item.key)

    for file_path in test_files:
        os.remove(file_path)
