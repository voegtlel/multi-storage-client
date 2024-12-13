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
import pytest

from multistorageclient.utils import glob, join_paths, expand_env_vars, extract_prefix_from_glob


def test_basic_glob():
    keys = ['file1.txt', 'file2.txt', 'image1.jpg', 'doc1.pdf']
    pattern = '*.txt'
    expected = ['file1.txt', 'file2.txt']
    assert glob(keys, pattern) == expected


def test_wildcard_glob():
    keys = ['file1.txt', 'file2.txt', 'file3.log', 'file4.txt']
    pattern = 'file?.txt'
    expected = ['file1.txt', 'file2.txt', 'file4.txt']
    assert glob(keys, pattern) == expected


def test_recursive_glob():
    keys = [
        'logs/app1/file1.log',
        'logs/app1/subdir/file2.log',
        'logs/app2/file3.log',
        'logs/app2/subdir/file4.txt'
    ]
    pattern = '**/*.log'
    expected = [
        'logs/app1/file1.log',
        'logs/app1/subdir/file2.log',
        'logs/app2/file3.log'
    ]
    assert glob(keys, pattern) == expected


def test_invalid_glob():
    keys = ['file1.txt', 'file2.txt', 'file3.log', 'file4.txt']
    pattern = '**/***/file/**.txt'
    expected = []
    assert glob(keys, pattern) == expected


def test_join_paths():
    assert "msc://profile/bucket/prefix" == join_paths('msc://profile', 'bucket/prefix')
    assert "msc://profile/bucket/prefix" == join_paths('msc://profile', '/bucket/prefix')
    assert "msc://profile/bucket/prefix" == join_paths('msc://profile/', '/bucket/prefix')


def test_expand_env_vars():
    os.environ['VAR'] = 'value'
    options = {
        'key1': '${VAR}',
        'key2': 42,
        'key3': ['list_item', '$VAR'],
        'key4': {'nested_key': '${VAR}'},
        'key5': 'PREFIX_${VAR}',
    }
    expected = {
        'key1': 'value',
        'key2': 42,
        'key3': ['list_item', 'value'],
        'key4': {'nested_key': 'value'},
        'key5': 'PREFIX_value',
    }
    assert expand_env_vars(options) == expected


def test_expand_env_vars_unresolved_var():
    del os.environ['VAR']
    with pytest.raises(ValueError):
        options = {
            'key1': '${VAR}'
        }

        options = expand_env_vars(options)


def test_extract_prefix_from_glob():
    assert extract_prefix_from_glob("bucket/prefix1/**/*.txt") == "bucket/prefix1"
    assert extract_prefix_from_glob("bucket/prefix1/subprefix2/*my_file") == "bucket/prefix1/subprefix2"
    assert extract_prefix_from_glob("bucket/*.log") == "bucket"
    assert extract_prefix_from_glob("bucket/folder/**/*") == "bucket/folder"
    assert extract_prefix_from_glob("bucket/deep/**/*.csv") == "bucket/deep"
    assert extract_prefix_from_glob("bucket/prefix1") == "bucket/prefix1"
    assert extract_prefix_from_glob("bucket") == "bucket"
    assert extract_prefix_from_glob("**/*.json") == ""
    assert extract_prefix_from_glob("*.pdf") == ""
    # Absolute paths
    assert extract_prefix_from_glob("/") == "/"
    assert extract_prefix_from_glob("/bucket/prefix1/**/*.txt") == "/bucket/prefix1"
    assert extract_prefix_from_glob("") == ""
    # Riva use case
    assert extract_prefix_from_glob("bucket/deep/folder/struct/**/*dataset_info.json") == "bucket/deep/folder/struct"
    # Earth-2
    assert extract_prefix_from_glob("bucket/prefix1/subprefix2/my_file.0.*.mdlus") == "bucket/prefix1/subprefix2"
    assert extract_prefix_from_glob("bucket/prefix1/**/my_file.0.*.mdlus") == "bucket/prefix1"
    assert extract_prefix_from_glob("bucket/**/my_file.0.*.mdlus") == "bucket"
