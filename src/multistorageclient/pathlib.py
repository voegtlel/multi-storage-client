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
import os
from pathlib import Path, PurePosixPath
from typing import List, Union

from .client import StorageClient
from .shortcuts import resolve_storage_client
from .types import MSC_PROTOCOL
from .utils import join_paths

logger = logging.Logger(__name__)


class MultiStoragePath:
    """
    A path object similar to pathlib.Path that supports both local and remote file systems.

    MultiStoragePath provides a unified interface for working with paths across different storage systems,
    including local files, S3, GCS, Azure Blob Storage, and more. It uses the "msc://" protocol
    prefix to identify remote storage paths.

    This implementation is based on Python 3.9's pathlib.Path interface, providing compatible behavior
    for local filesystem operations while extending support to remote storage systems.
    """

    _internal_path: PurePosixPath
    _storage_client: StorageClient
    _path: str

    def __init__(self, path: Union[str, os.PathLike]):
        self._path = str(path)
        self._storage_client, relative_path = resolve_storage_client(self._path)
        self._internal_path = PurePosixPath(relative_path)

        if self._storage_client.is_default_profile():
            if not Path(self._internal_path).is_absolute():
                raise ValueError(f"Path '{self._internal_path}' must be absolute. Relative paths are not supported.")

    def __str__(self) -> str:
        if self._storage_client.is_default_profile():
            return str(self._internal_path)
        return join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path))

    def __repr__(self) -> str:
        return f"MultiStoragePath({str(self)!r})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, MultiStoragePath):
            return False
        return (
            self._storage_client.profile == other._storage_client.profile
            and self._internal_path == other._internal_path
        )

    def __fspath__(self) -> str:
        return str(self)

    def joinpath(self, *pathsegments):
        return self.with_segments(*pathsegments)

    def __truediv__(self, key):
        try:
            return self.joinpath(key)
        except TypeError:
            return NotImplemented

    def __rtruediv__(self, key):
        try:
            return self.with_segments(key, self)
        except TypeError:
            return NotImplemented

    @property
    def anchor(self) -> str:
        return self._internal_path.anchor

    @property
    def name(self) -> str:
        return self._internal_path.name

    @property
    def suffix(self) -> str:
        return self._internal_path.suffix

    @property
    def suffixes(self) -> List[str]:
        return self._internal_path.suffixes

    @property
    def stem(self) -> str:
        return self._internal_path.stem

    @property
    def parent(self) -> "MultiStoragePath":
        parent_path = self._internal_path.parent
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(parent_path))
        return MultiStoragePath(join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(parent_path)))

    @property
    def parents(self) -> List["MultiStoragePath"]:
        if self._storage_client.is_default_profile():
            return [MultiStoragePath(str(p)) for p in self._internal_path.parents]
        else:
            return [
                MultiStoragePath(join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(p)))
                for p in self._internal_path.parents
            ]

    @property
    def parts(self):
        return self._internal_path.parts

    def as_posix(self) -> str:
        if self._storage_client.is_default_profile():
            return self._internal_path.as_posix()
        raise NotImplementedError("MultiStoragePath.as_posix() is unsupported for remote storage paths")

    def is_absolute(self) -> bool:
        # Paths are always absolute
        return True

    def is_relative_to(self, other: "MultiStoragePath") -> bool:
        return isinstance(other, MultiStoragePath) and self._internal_path.is_relative_to(other._internal_path)

    def is_reserved(self) -> bool:
        if self._storage_client.is_default_profile():
            return self._internal_path.is_reserved()
        raise NotImplementedError("MultiStoragePath.is_reserved() is unsupported for remote storage paths")

    def match(self, pattern) -> bool:
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).match(pattern)
        raise NotImplementedError("MultiStoragePath.match() is unsupported for remote storage paths")

    def relative_to(self, other: "MultiStoragePath") -> "MultiStoragePath":
        raise NotImplementedError("MultiStoragePath.relative_to() is unsupported")

    def with_name(self, name: str) -> "MultiStoragePath":
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(self._internal_path.with_name(name)))
        else:
            return MultiStoragePath(
                join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path.with_name(name)))
            )

    def with_stem(self, stem: str) -> "MultiStoragePath":
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(self._internal_path.with_stem(stem)))
        else:
            return MultiStoragePath(
                join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path.with_stem(stem)))
            )

    def with_suffix(self, suffix: str) -> "MultiStoragePath":
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(self._internal_path.with_suffix(suffix)))
        else:
            return MultiStoragePath(
                join_paths(
                    f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path.with_suffix(suffix))
                )
            )

    def with_segments(self, *pathsegments) -> "MultiStoragePath":
        if self._storage_client.is_default_profile():
            new_path = self._internal_path.joinpath(*pathsegments)
            return MultiStoragePath(str(new_path))
        else:
            new_path = self._internal_path.joinpath(*pathsegments)
            return MultiStoragePath(join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(new_path)))

    # Expanding and resolving paths

    @classmethod
    def home(cls):
        return Path.home()

    def expanduser(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).expanduser()
        raise NotImplementedError("MultiStoragePath.expanduser() is unsupported for remote storage paths")

    @classmethod
    def cwd(cls):
        return Path.cwd()

    def absolute(self):
        # Paths are always absolute
        return self

    def resolve(self, strict=False):
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(Path(self._internal_path).resolve(strict=strict)))
        raise NotImplementedError("MultiStoragePath.resolve() is unsupported for remote storage paths")

    def readlink(self):
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(Path(self._internal_path).readlink()))
        raise NotImplementedError("MultiStoragePath.readlink() is unsupported for remote storage paths")

    # Querying file type and status

    def stat(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).stat()
        raise NotImplementedError("MultiStoragePath.stat() is unsupported for remote storage paths")

    def lstat(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).lstat()
        raise NotImplementedError("MultiStoragePath.lstat() is unsupported for remote storage paths")

    def exists(self) -> bool:
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).exists()
        else:
            try:
                self._storage_client.info(str(self._internal_path))
                return True
            except FileNotFoundError:
                return False

    def is_file(self, strict: bool = True) -> bool:
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_file()
        else:
            try:
                # If the path ends with a "/", assume it is a directory.
                path = str(self._internal_path)
                if path.endswith("/"):
                    return False

                meta = self._storage_client.info(path, strict=strict)
                return meta.type == "file"
            except FileNotFoundError:
                return False
            except Exception as e:
                logger.warning("Error occurred while fetching file info at %s, caused by: %s", self._internal_path, e)
                return False

    def is_dir(self, strict: bool = True) -> bool:
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_dir()
        else:
            try:
                # If the path does not end with a "/", append it to ensure the path is a directory.
                path = str(self._internal_path)
                if not path.endswith("/"):
                    path += "/"

                meta = self._storage_client.info(path, strict=strict)
                return meta.type == "directory"
            except FileNotFoundError:
                return False
            except Exception as e:
                logger.warning("Error occurred while fetching file info at %s, caused by: %s", self._internal_path, e)
                return False

    def is_symlink(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_symlink()
        raise NotImplementedError("MultiStoragePath.is_symlink() is unsupported for remote storage paths")

    def is_mount(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_mount()
        raise NotImplementedError("MultiStoragePath.is_mount() is unsupported for remote storage paths")

    def is_socket(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_socket()
        raise NotImplementedError("MultiStoragePath.is_socket() is unsupported for remote storage paths")

    def is_fifo(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_fifo()
        raise NotImplementedError("MultiStoragePath.is_fifo() is unsupported for remote storage paths")

    def is_block_device(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_block_device()
        raise NotImplementedError("MultiStoragePath.is_block_device() is unsupported for remote storage paths")

    def is_char_device(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_char_device()
        raise NotImplementedError("MultiStoragePath.is_char_device() is unsupported for remote storage paths")

    def samefile(self, other_path):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).samefile(other_path)
        return self == other_path

    # Reading and writing files

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        return self._storage_client.open(str(self._internal_path), mode=mode, buffering=buffering, encoding=encoding)

    def read_bytes(self) -> bytes:
        return self._storage_client.read(str(self._internal_path))

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        return self._storage_client.read(str(self._internal_path)).decode(encoding)

    def write_bytes(self, data: bytes) -> None:
        self._storage_client.write(str(self._internal_path), data)

    def write_text(self, data: str, encoding: str = "utf-8", errors: str = "strict") -> None:
        self._storage_client.write(str(self._internal_path), data.encode(encoding))

    # Reading directories

    def iterdir(self):
        if self._storage_client.is_default_profile():
            for item in Path(self._internal_path).iterdir():
                yield MultiStoragePath(str(item))
        else:
            path = str(self._internal_path)
            if not path.endswith("/"):
                path += "/"
            for item in self._storage_client.list(path, include_directories=True, include_url_prefix=True):
                yield MultiStoragePath(item.key)

    def glob(self, pattern):
        if self._storage_client.is_default_profile():
            return [MultiStoragePath(str(p)) for p in Path(self._internal_path).glob(pattern)]
        else:
            return [
                MultiStoragePath(str(p))
                for p in self._storage_client.glob(str(self._internal_path / pattern), include_url_prefix=True)
            ]

    def rglob(self, pattern):
        if self._storage_client.is_default_profile():
            return [MultiStoragePath(str(p)) for p in Path(self._internal_path).rglob(pattern)]
        raise NotImplementedError("MultiStoragePath.rglob() is unsupported for remote storage paths")

    def walk(self, top_down=True, on_error=None, follow_symlinks=False):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).walk(top_down, on_error, follow_symlinks)  # pyright: ignore[reportAttributeAccessIssue]
        raise NotImplementedError("MultiStoragePath.walk() is unsupported for remote storage paths")

    # Creating files and directories

    def touch(self, mode=0o666, exist_ok=False):
        if self._storage_client.is_default_profile():
            Path(self._internal_path).touch(mode, exist_ok)
        else:
            raise NotImplementedError("MultiStoragePath.touch() is unsupported for remote storage paths")

    def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None:
        if self._storage_client.is_default_profile():
            Path(self._internal_path).mkdir(mode, parents, exist_ok)

    def symlink_to(self, target, target_is_directory=False):
        if self._storage_client.is_default_profile():
            Path(self._internal_path).symlink_to(target, target_is_directory)
        else:
            raise NotImplementedError("MultiStoragePath.symlink_to() is unsupported for remote storage paths")

    # Renaming and deleting

    def rename(self, target):
        if self._storage_client.is_default_profile():
            Path(self._internal_path).rename(target)
        else:
            raise NotImplementedError("MultiStoragePath.rename() is unsupported for remote storage paths")

    def replace(self, target):
        if self._storage_client.is_default_profile():
            Path(self._internal_path).replace(target)
        else:
            raise NotImplementedError("MultiStoragePath.replace() is unsupported for remote storage paths")

    def unlink(self, missing_ok: bool = False) -> None:
        if self._storage_client.is_default_profile():
            Path(self._internal_path).unlink(missing_ok=missing_ok)
        else:
            try:
                self._storage_client.delete(str(self._internal_path))
            except FileNotFoundError:
                if not missing_ok:
                    raise

    def rmdir(self) -> None:
        if self._storage_client.is_default_profile():
            Path(self._internal_path).rmdir()
        else:
            raise NotImplementedError("MultiStoragePath.rmdir() is unsupported for remote storage paths")

    # Permissions and ownership

    def owner(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).owner()
        raise NotImplementedError("MultiStoragePath.owner() is unsupported for remote storage paths")

    def group(self):
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).group()
        raise NotImplementedError("MultiStoragePath.group() is unsupported for remote storage paths")

    def chmod(self, mode):
        if self._storage_client.is_default_profile():
            Path(self._internal_path).chmod(mode)
        else:
            raise NotImplementedError("MultiStoragePath.chmod() is unsupported for remote storage paths")

    def lchmod(self, mode):
        if self._storage_client.is_default_profile():
            Path(self._internal_path).lchmod(mode)
        else:
            raise NotImplementedError("MultiStoragePath.lchmod() is unsupported for remote storage paths")
