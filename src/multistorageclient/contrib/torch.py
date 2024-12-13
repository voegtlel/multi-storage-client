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

from typing import Any

import torch as _torch
from torch.serialization import FILE_LIKE

from ..shortcuts import open as msc_open
from ..types import MSC_PROTOCOL


def load(f: FILE_LIKE, *args: Any, **kwargs: Any) -> Any:
    """
    Adapt ``torch.load``.
    """
    if isinstance(f, str) and f.startswith(MSC_PROTOCOL):
        with msc_open(f, "rb") as fp:
            return _torch.load(fp, *args, **kwargs)
    else:
        return _torch.load(f, *args, **kwargs)


def save(obj: object, f: FILE_LIKE, *args: Any, **kwargs: Any) -> Any:
    """
    Adapt ``torch.save``.
    """
    if isinstance(f, str) and f.startswith(MSC_PROTOCOL):
        with msc_open(f, "wb") as fp:
            return _torch.save(obj, fp, *args, **kwargs)
    else:
        return _torch.save(obj, f, *args, **kwargs)
