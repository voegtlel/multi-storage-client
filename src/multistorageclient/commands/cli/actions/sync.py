# SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import argparse
import sys

import multistorageclient as msc

from .action import Action


class SyncAction(Action):
    """Action for synchronizing files to a storage location."""

    def name(self) -> str:
        return "sync"

    def help(self) -> str:
        return "Synchronize files from the source storage to the target storage"

    def setup_parser(self, parser: argparse.ArgumentParser) -> None:
        parser.formatter_class = argparse.RawDescriptionHelpFormatter

        parser.add_argument(
            "--delete-unmatched-files",
            action="store_true",
            help="Delete files at the target that are not present at the source",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose logging",
        )
        parser.add_argument("source_url", help="The path or URL for the source storage (POSIX path or msc:// URL)")
        parser.add_argument("target_url", help="The path or URL for the target storage (POSIX path or msc:// URL)")

        # Add examples as description
        parser.description = """Synchronize files between storage locations. Can be used to:
  1. Upload files from filesystem to object stores
  2. Download files from object stores to filesystem
  3. Transfer files between different object stores
"""

        # Add examples as epilog (appears after argument help)
        parser.epilog = """examples:
  # Upload: filesystem to object store
  msc sync /path/to/dataset msc://profile/prefix

  # Download: object store to filesystem
  msc sync msc://profile/prefix /path/to/dataset

  # Transfer: between object stores
  msc sync msc://profile1/prefix msc://profile2/prefix

  # Sync with cleanup (removes files in target not in source)
  msc sync --delete-unmatched-files msc://source-profile/data msc://target-profile/data
"""

    def run(self, args: argparse.Namespace) -> int:
        if args.verbose:
            print(f"Synchronizing files from {args.source_url} to {args.target_url} ...")
        try:
            msc.sync(args.source_url, args.target_url, args.delete_unmatched_files)
            if args.verbose:
                print("Synchronization completed successfully")
            return 0
        except Exception as e:
            print(f"Error during synchronization: {str(e)}", file=sys.stderr)
            return 1
