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

from typing import Any, Dict, Union

import numpy as _np

from ..shortcuts import open as msc_open
from ..types import MSC_PROTOCOL


def memmap(*args: Any, **kwargs: Any) -> _np.memmap:
    """
    Adapt ``numpy.memmap``.
    """

    if not args:
        raise TypeError("missing filename argument")
    file = args[0]

    if isinstance(file, str) and file.startswith(MSC_PROTOCOL):
        if "mode" not in kwargs:
            kwargs["mode"] = "r"
        with msc_open(file, mode=str(kwargs.get("mode"))) as fp:
            args = (fp.get_local_path(),) + args[1:]

    return _np.memmap(*args, **kwargs)  # pyright: ignore [reportArgumentType, reportCallIssue]


def load(*args: Any, **kwargs: Any) -> Union[_np.ndarray, Dict[str, _np.ndarray], _np.lib.npyio.NpzFile]:
    """
    Adapt ``numpy.load``.
    """

    file = args[0] if args else kwargs.get("file")
    if isinstance(file, str) and file.startswith(MSC_PROTOCOL):
        with msc_open(file) as fp:
            # For .npy with memmap mode != none, _np.load() will call format.open_memmap() underneath,
            # Which require a file path string
            # Refs:
            # https://github.com/numpy/numpy/blob/main/numpy/lib/_npyio_impl.py#L477
            # https://numpy.org/doc/stable/reference/generated/numpy.lib.format.open_memmap.html
            #
            # For the simplicity of the code, we always pass the file path to _np.load and let it convert the path
            # to file-like object.

            # block until download is completed to ensure local path is available for the open() call within _np.load()
            local_path = fp.get_local_path()
        if not local_path:
            raise ValueError(f"local_path={local_path} for the downloaded file[{file}] is not valid")
        if args:
            args = (local_path,) + args[1:]
        else:
            kwargs["file"] = local_path

    return _np.load(*args, **kwargs)  # pyright: ignore [reportArgumentType, reportCallIssue]


def save(*args: Any, **kwargs: Any) -> None:
    """
    Adapt ``numpy.save``.
    """

    file = args[0] if args else kwargs.get("file")
    if isinstance(file, str) and file.startswith(MSC_PROTOCOL):
        # use context manager to make sure to upload the file once close() is called
        with msc_open(file, mode="wb") as fp:
            if args:
                args = (fp,) + args[1:]
            else:
                kwargs["file"] = fp

            _np.save(*args, **kwargs)  # pyright: ignore [reportArgumentType, reportCallIssue]
    else:
        _np.save(*args, **kwargs)  # pyright: ignore [reportArgumentType, reportCallIssue]
