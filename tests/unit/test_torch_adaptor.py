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

import pytest
import torch

import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL
from utils import file_storage_config


@pytest.fixture
def sample_data(tmp_path):
    # Create a small tensor
    tensor = torch.tensor([1, 2, 3, 4])
    # Create a filepath
    filepath = tmp_path / "test.pt"
    # Save the tensor to the file
    msc.torch.save(tensor, filepath)
    return filepath, tensor


def test_torch_load_with_filepath(file_storage_config, sample_data):
    filepath, expected_tensor = sample_data

    result = msc.torch.load(str(filepath))
    assert torch.equal(result, expected_tensor)


def test_torch_load_with_msc_prefix(file_storage_config, sample_data):
    filepath, expected_tensor = sample_data

    result = msc.torch.load(f"{MSC_PROTOCOL}default{filepath}")
    assert torch.equal(result, expected_tensor)
