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

import fnmatch
import hashlib
import importlib
import os
import re
import shutil
from pathlib import Path
from typing import Any, List, Optional, Tuple, Dict


def split_path(path: str) -> Tuple[str, str]:
    """
    Splits the given path into components: bucket, key

    :param path: The path to split.
    :return: A tuple containing the bucket and key.
    """
    parts = path.lstrip("/").split("/", 1)
    if len(parts) == 2:
        bucket, key = parts
    else:
        bucket = parts[0]
        key = ""
    return bucket, key


def glob(keys: List[str], pattern: str) -> List[str]:
    """
    Matches a list of keys against a Unix-style wildcard pattern, including recursive ``**``.

    :param keys: A list of keys to match against the pattern.
    :param pattern: A Unix-style wildcard pattern (e.g., ``*.txt``, ``**/*.log``).

    :return: A list of keys that match the pattern.
    """
    # Split the pattern into parts
    if "**" in pattern:
        # Handle the recursive case with '**'
        base_pattern = pattern.replace("**/", "")  # Remove the '**/' part for matching
        return [key for key in keys if fnmatch.fnmatch(key, base_pattern)]
    else:
        # Standard fnmatch usage for non-recursive patterns
        return fnmatch.filter(keys, pattern)


def import_class(class_name: str, module_name: str, package_name: Optional[str] = None) -> Any:
    """
    Dynamically imports a class from a given module and package.

    Example:
        cls = import_class('MyClass', 'my_module', 'my_package')
        obj = cls()

    :param class_name: The name of the class to import.
    :param module_name: The name of the module containing the class.
    :param package_name: The package name to resolve relative imports (optional).

    :return: The imported class object.

    :raises AttributeError: If the specified class is not found in the module.
    :raises ImportError: If the specified module cannot be imported.
    """
    if package_name:
        module = importlib.import_module(module_name, package_name)
    else:
        module = importlib.import_module(module_name)

    cls = getattr(module, class_name)
    return cls


def cache_key(path: str) -> str:
    """
    Generate a unique cache key based on the provided file path using a fast hashing algorithm.

    :param path: The file path for which to generate the cache key.

    :return: A hexadecimal string representing the hashed value of the file path, to be used as a cache key.
    """
    md5 = hashlib.md5()
    md5.update(path.encode("utf-8"))
    return md5.hexdigest()


def join_paths(base: str, path: str) -> str:
    """
    Joins two path components, ensuring that no redundant slashes are added.

    This function works for both filesystem paths and custom scheme paths like ``msc://``.
    It removes any trailing slashes from the base and leading slashes from the path before joining them together.
    """
    return os.path.join(base.rstrip("/"), path.lstrip("/"))


def expand_env_vars(data: Any) -> Any:
    """
    Recursively expands environment variables within strings in a data structure.

    This function traverses through a nested data structure (which can be a combination
    of dictionaries, lists, and strings) and replaces any environment variable references
    within strings using the current environment variables.

    Environment variable references can be in the form of:
    - ${VAR_NAME}
    - $VAR_NAME

    If an environment variable is not set, it raises a ValueError indicating the unresolved variables.

    Args:
        data (dict, list, str, or any): The data structure containing strings with possible
            environment variable references.

    Returns:
        The data structure with all environment variables expanded in the strings.

    Raises:
        ValueError: If there are any unresolved environment variables in the strings after expansion.
    """
    if isinstance(data, dict):
        return {key: expand_env_vars(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [expand_env_vars(element) for element in data]
    elif isinstance(data, str):
        expanded = os.path.expandvars(data)
        unresolved_vars = re.findall(r"\$\{[^}]+\}|\$[A-Za-z_][A-Za-z0-9_]*", expanded)
        if unresolved_vars:
            raise ValueError(f"Unresolved environment variables {unresolved_vars} in '{data}'")
        return expanded
    else:
        return data


def extract_prefix_from_glob(s: str) -> str:
    parts = s.split("/")
    prefix_parts = []

    for part in parts:
        # Check if the part contains any glob special characters
        if any(c in part for c in "*?[]{}"):
            break  # Stop if a glob character is found
        prefix_parts.append(part)

    prefix = "/".join(prefix_parts)
    return prefix


def merge_dictionaries_no_overwrite(
    dict1: Optional[Dict[str, Any]] = None,
    dict2: Optional[Dict[str, Any]] = None,
    conflicted_keys: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Recursively merges two dictionaries without overwriting existing keys.

    :param dict1: first dictionary to be merged
    :param dict2: second dictionary to be merged
    :param conflicted_keys: a list that collects any keys for which there is a collision.
        If not provided, a new list is created.

    :return: A tuple of:
            - The merged dictionary from dict1 and dict2 with no overwritten keys.
            - The list of keys that caused conflicts (if any).
    """
    if dict1 is None:
        dict1 = {}

    if dict2 is None:
        dict2 = {}

    if conflicted_keys is None:
        conflicted_keys = []

    for key, value2 in dict2.items():
        if key not in dict1:
            # If the key doesn't exist in dict1, set it.
            dict1[key] = value2
        else:
            # Potential collision
            value1 = dict1[key]

            # If both values are dicts, recurse to check nested fields
            if isinstance(value1, dict) and isinstance(value2, dict):
                merge_dictionaries_no_overwrite(value1, value2, conflicted_keys)
            else:
                conflicted_keys.append(key)

    return dict1, conflicted_keys


def find_executable_path(executable_name: str) -> Optional[Path]:
    """
    Find the path of an executable in the PATH environment variable.

    :param executable_name: Name of the executable to look for in PATH
    :return: A Path object representing the full path of the executable, or None if not found
    """
    executable_path = shutil.which(executable_name)
    if executable_path:
        return Path(executable_path)
    return None
