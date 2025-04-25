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
import uuid
import multistorageclient as msc


def test_file_reads_from_cache_with_shortcuts():
    """Test file reads from cache using MSC shortcuts."""
    # Create test files
    num_files = 3
    file_size_mb = 1

    profile = "test-s3-iad"
    s3_express_profile = "test-s3e"
    test_uuid = str(uuid.uuid4())
    base_path = f"msc://{profile}/{test_uuid}"
    cache_path = "tmp/msc_cache"

    # Upload files using shortcuts
    for i in range(num_files):
        with msc.open(f"{base_path}/{i}", "wb") as f:
            f.write(os.urandom(file_size_mb * 1024 * 1024))

    for i in range(num_files):
        file_path = f"{base_path}/{i}"
        with msc.open(file_path, "rb") as f:
            content = f.read()

        # Verify file exists in cache
        assert msc.os.path.exists(f"msc://{s3_express_profile}/{cache_path}/{profile}/{test_uuid}/{i}")

        # Second read should be a cache hit
        with msc.open(file_path, "rb") as f:
            cached_content = f.read()

        # Verify content matches
        assert content == cached_content

    # Clean up
    # delete all files from test-s3-iad
    iter = msc.list(base_path + "/")
    for item in iter:
        msc.delete(item.key)
