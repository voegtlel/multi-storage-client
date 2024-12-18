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
import tempfile
import pytest
import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL


@pytest.fixture
def sample_data():
    import numpy as np

    return np.array([1, 2, 3, 4, 5], dtype=np.int32)


def test_numpy_memmap(file_storage_config_with_cache, sample_data):
    import numpy as np

    with tempfile.NamedTemporaryFile(delete=True, mode="wb") as temp:
        sample_data.tofile(temp.name)  # save as raw binary

        # test file path
        result = msc.numpy.memmap(temp.name, dtype=np.int32, mode="r", shape=(5,))
        assert np.array_equal(result, sample_data)

        # test msc-prefixed path
        result = msc.numpy.memmap(f"{MSC_PROTOCOL}default{temp.name}", dtype=np.int32, mode="r", shape=(5,))
        assert np.array_equal(result, sample_data)

        # test file object
        with open(temp.name) as fp:
            result = msc.numpy.memmap(fp, dtype=np.int32, mode="r", shape=(5,))
            assert np.array_equal(result, sample_data)

        # test default mode
        result = msc.numpy.memmap(temp.name, dtype=np.int32, shape=(5,))
        assert np.array_equal(result, sample_data)

        # test incorrect argument
        with pytest.raises(TypeError):
            _ = msc.numpy.memmap(filename=temp.name, dtype=np.int32, mode="r", shape=(5,))

        # mismatch mode should fail: default mode of memmap function is r+
        with pytest.raises(PermissionError):
            with open(temp.name, mode="r") as fp:
                result = msc.numpy.memmap(fp, dtype=np.int32, shape=(5,))


def test_numpy_load(file_storage_config_with_cache, sample_data):
    import numpy as np

    with tempfile.NamedTemporaryFile(delete=True, mode="wb", suffix=".npy") as temp:
        np.save(temp.name, sample_data)  # save as .npy file

        # test file path
        result = msc.numpy.load(temp.name, allow_pickle=True, mmap_mode="r")
        assert np.array_equal(result, sample_data)

        # test msc-prefixed path
        result = msc.numpy.load(f"{MSC_PROTOCOL}default{temp.name}", allow_pickle=True, mmap_mode="r")
        assert np.array_equal(result, sample_data)

        # test file object
        with open(temp.name, "rb") as fp:
            with pytest.raises(ValueError):
                _ = msc.numpy.load(fp, allow_pickle=True, mmap_mode="r")  # memmap mode is not supported for file handle

            result = msc.numpy.load(fp, allow_pickle=True)
            assert np.array_equal(result, sample_data)


def test_numpy_save(file_storage_config_with_cache, sample_data):
    import numpy as np

    with tempfile.NamedTemporaryFile(delete=True, mode="wb", suffix=".npy") as temp:
        # Test file path
        msc.numpy.save(temp.name, sample_data)

        result = np.load(temp.name)
        assert np.array_equal(result, sample_data)

        # Test msc-prefixed path
        msc_path = f"{MSC_PROTOCOL}default{temp.name}"
        msc.numpy.save(msc_path, sample_data)

        result = np.load(temp.name)
        assert np.array_equal(result, sample_data)

        # test file object
        with open(temp.name, "wb") as fp:
            msc.numpy.save(fp, sample_data)

            result = np.load(temp.name)
            assert np.array_equal(result, sample_data)
