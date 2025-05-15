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

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

import multistorageclient as msc


@pytest.fixture
def run_cli():
    """
    Run the CLI as a subprocess with the given arguments.
    """

    def _run_cli(*args, expected_return_code=0):
        cmd = [sys.executable, "-m", "multistorageclient.commands.cli.main"] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True)

        # Print output if return code doesn't match expected
        if result.returncode != expected_return_code:
            print(f"Expected return code {expected_return_code}, got {result.returncode}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        assert result.returncode == expected_return_code
        return result.stdout, result.stderr

    return _run_cli


def test_version_command(run_cli):
    stdout, stderr = run_cli("--version")
    assert f"msc-cli/{msc.__version__}" in stdout
    assert "Python" in stdout


def test_unknown_command(run_cli):
    stdout, stderr = run_cli("unknown_command", expected_return_code=1)
    assert "Unknown command: unknown_command" in stdout
    assert "Run 'msc help'" in stdout


def test_help_command(run_cli):
    stdout, stderr = run_cli("help")
    assert "commands:" in stdout
    assert "help" in stdout


def test_sync_help_command(run_cli):
    stdout, stderr = run_cli("help", "sync")
    assert "Synchronize files" in stdout
    assert "--delete-unmatched-files" in stdout
    assert "--verbose" in stdout
    assert "source_url" in stdout
    assert "target_url" in stdout


def test_sync_command_with_real_files(run_cli):
    with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
        source_file = Path(source_dir) / "test.txt"
        source_file.write_text("Test content")

        # Run the sync command
        stdout, stderr = run_cli("sync", "--verbose", source_dir, target_dir)

        # Verify that the file was copied
        target_file = Path(target_dir) / "test.txt"
        assert target_file.exists()
        assert target_file.read_text() == "Test content"

        assert "Synchronizing files from" in stdout
        assert "Synchronization completed successfully" in stdout
