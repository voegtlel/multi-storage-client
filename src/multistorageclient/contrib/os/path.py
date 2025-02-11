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

import logging
import os.path as _os_path

from ...shortcuts import resolve_storage_client
from ...types import MSC_PROTOCOL

logger = logging.Logger(__name__)


def exists(path: str) -> bool:
    """
    Check if a given path exists.

    :param path: The path to check. It can be a local filesystem path or a path prefixed with a custom protocol (msc://).
    :return: True if the path exists, False otherwise.
    """
    if path.startswith(MSC_PROTOCOL):
        if isfile(path):
            return True
        return isdir(path)
    else:
        return _os_path.exists(path)


def isdir(path: str, strict: bool = True) -> bool:
    """
    Check if a given path is a directory.

    :param path: The path to check. It can be a local filesystem path or a path prefixed with a custom protocol (msc://).
    :param strict: If True, performs additional validation to ensure the directory exists by issuing extra LIST operations
                   on object stores. This can help detect cases where a directory-like path exists but may incur
                   additional latency due to extra API calls. Defaults to True.
    :return: True if the path is a directory, False otherwise.
    """
    if path.startswith(MSC_PROTOCOL):
        if strict:
            storage_client, file_path = resolve_storage_client(path)
            try:
                # Append trailing slash
                if not path.endswith("/"):
                    file_path += "/"
                meta = storage_client.info(file_path)
                return meta.type == "directory"
            except FileNotFoundError:
                return False
            except Exception as e:
                logger.warning("Error occurred while fetching file info at %s, caused by: %s", path, e)
                return False
        else:
            return not isfile(path)
    else:
        return _os_path.isdir(path)


def isfile(path: str) -> bool:
    """
    Check if a given path is a file.

    :param path: The path to check. It can be a local filesystem path or a path prefixed with a custom protocol (msc://).
    :return: True if the path is a file, False otherwise.
    """
    if path.startswith(MSC_PROTOCOL):
        storage_client, file_path = resolve_storage_client(path)
        try:
            if path.endswith("/"):
                return False
            meta = storage_client.info(file_path, strict=False)
            return meta.type == "file"
        except FileNotFoundError:
            return False
        except Exception as e:
            logger.warning("Error occurred while fetching file info at %s, caused by: %s", path, e)
            return False
    else:
        return _os_path.isfile(path)
