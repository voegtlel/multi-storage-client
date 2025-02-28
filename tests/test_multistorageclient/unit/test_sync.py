import boto3
import json
import os
import pytest
import shutil
import tempfile
import time

import multistorageclient as msc


class TestSync:
    def __init__(self, bucket_name: str = "test-sync-bucket"):
        self.bucket_name = bucket_name
        self.minio_access_key = "minioadmin"
        self.minio_secret_key = "minioadmin"
        self.endpoint_url = "http://localhost:9000"

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            region_name="us-east-1",
        )

    def setup_s3(self):
        self.clean_bucket()
        try:
            self.client.create_bucket(Bucket=self.bucket_name)
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            pass

        config_json = json.dumps(
            {
                "profiles": {
                    "s3-sync": {
                        "storage_provider": {
                            "type": "s3",
                            "options": {
                                "endpoint_url": self.endpoint_url,
                                "region_name": "us-east-1",
                                "base_path": self.bucket_name,
                            },
                        },
                        "credentials_provider": {
                            "type": "S3Credentials",
                            "options": {
                                "access_key": self.minio_access_key,
                                "secret_key": self.minio_secret_key,
                            },
                        },
                    }
                },
                "cache": {"size_mb": 5000, "use_etag": True},
            }
        )

        config_filename = os.path.join(tempfile.gettempdir(), ".msc_config.json")

        with open(config_filename, "w") as fp:
            fp.write(config_json)

        os.environ["MSC_CONFIG"] = config_filename

    def clean_bucket(self):
        try:
            objects = self.client.list_objects_v2(Bucket=self.bucket_name).get("Contents", [])
            for obj in objects:
                self.client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])
            self.client.delete_bucket(Bucket=self.bucket_name)
        except self.client.exceptions.NoSuchBucket:
            pass

    def get_s3_file_timestamp(self, key: str) -> float:
        response = self.client.head_object(Bucket=self.bucket_name, Key=key)
        return response["LastModified"].timestamp()

    def create_local_test_dataset(self, expected_files: dict, base_path: str = "") -> str:
        """Creates a temporary directory with test files based on expected_files dictionary."""
        base_path = base_path if base_path else tempfile.mkdtemp()

        for rel_path, content in expected_files.items():
            file_path = os.path.join(base_path, rel_path)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(content.encode("utf-8"))

        return base_path

    def verify_sync_and_contents(self, target_url: str, expected_files: dict):
        """Verifies that all expected files exist in the target storage and their contents are correct."""
        for file, expected_content in expected_files.items():
            target_file_url = os.path.join(target_url, file)
            assert msc.is_file(target_file_url), f"Missing file: {target_file_url}"
            actual_content = msc.open(target_file_url).read().decode("utf-8")
            assert actual_content == expected_content, f"Mismatch in file {file}"
        # Ensure there is nothing in target that is not in expected_files
        target_client, target_path = msc.resolve_storage_client(target_url)
        for targetf in target_client.list(prefix=target_path):
            key = targetf.key[len(target_path) :].lstrip("/")
            assert key in expected_files


def test_sync_function():
    test_helper = TestSync()
    test_helper.setup_s3()

    # Create local dataset
    expected_files = {
        "dir1/file0.txt": "a" * 100,
        "dir1/file1.txt": "b" * 100,
        "dir1/file2.txt": "c" * 100,
        "dir2/file0.txt": "d" * 100,
        "dir2/file1.txt": "e" * 100,
        "dir2/file2.txt": "f" * 600 * 1024 * 1024,  # One large file
        "dir3/file0.txt": "g" * 100,
        "dir3/file1.txt": "h" * 100,
        "dir3/file2.txt": "i" * 100,
    }
    source_dir = test_helper.create_local_test_dataset(expected_files)
    second_local_dir = tempfile.mkdtemp()
    # Insert a delay before sync'ing so that timestamps will be clearer.
    time.sleep(1)

    try:
        target_msc_url = "msc://s3-sync/synced-files"
        source_msc_url = os.path.join("msc://default/", source_dir)
        second_msc_url = os.path.join("msc://default/", second_local_dir)

        # Perform first sync
        msc.sync(source_url=source_dir, target_url=target_msc_url)

        # Verify contents on target match expectation.
        test_helper.verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Test deleting some files at target and syncing again
        test_helper.client.delete_object(Bucket=test_helper.bucket_name, Key="synced-files/dir1/file0.txt")
        msc.sync(source_url=source_dir, target_url=target_msc_url)
        test_helper.verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Test syncing twice and verifying timestamps
        timestamps_before = {
            file: test_helper.get_s3_file_timestamp(os.path.join("synced-files", file)) for file in expected_files
        }
        msc.sync(source_url=source_dir, target_url=target_msc_url)
        timestamps_after = {
            file: test_helper.get_s3_file_timestamp(os.path.join("synced-files", file)) for file in expected_files
        }
        assert timestamps_before == timestamps_after, "Timestamps changed on second sync."

        # Test adding new files and syncing again
        new_files = {"dir1/new_file.txt": "n" * 100}
        test_helper.create_local_test_dataset(expected_files=new_files, base_path=source_dir)
        msc.sync(source_url=source_dir, target_url=target_msc_url)
        expected_files.update(new_files)
        test_helper.verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Test modifying one of the source files, but keep size the same, and verify it's copied.
        modified_files = {"dir1/file0.txt": "z" * 100}
        test_helper.create_local_test_dataset(expected_files=modified_files, base_path=source_dir)
        expected_files.update(modified_files)
        msc.sync(source_url=source_dir, target_url=target_msc_url)
        test_helper.verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        with pytest.raises(ValueError):
            msc.sync(source_url="", target_url=target_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=source_dir, target_url="")
        with pytest.raises(ValueError):
            msc.sync(source_url=source_dir, target_url=source_dir)
        with pytest.raises(ValueError):
            msc.sync(source_url=target_msc_url, target_url=target_msc_url)
        with pytest.raises(ValueError):
            msc.sync(source_url=source_msc_url, target_url=os.path.join(source_msc_url, "extra"))

        # Delete all the files at the target and go again.
        for key in expected_files.keys():
            test_helper.client.delete_object(Bucket=test_helper.bucket_name, Key=os.path.join("synced-files", key))

        # Sync using msc for posix file source.
        msc.sync(source_url=source_msc_url, target_url=target_msc_url)
        test_helper.verify_sync_and_contents(target_url=target_msc_url, expected_files=expected_files)

        # Sync from object to a second posix file location
        msc.sync(source_url=target_msc_url, target_url=second_msc_url)
        test_helper.verify_sync_and_contents(target_url=second_msc_url, expected_files=expected_files)

        # Delete all the files at the target and go again.
        for key in expected_files.keys():
            test_helper.client.delete_object(Bucket=test_helper.bucket_name, Key=os.path.join("synced-files", key))

        # Sync using prefixes to just copy one subfolder.
        msc.sync(source_url=os.path.join(source_msc_url, "dir2"), target_url=os.path.join(target_msc_url, "dir2"))
        sub_expected_files = {k: v for k, v in expected_files.items() if k.startswith("dir2")}
        test_helper.verify_sync_and_contents(target_url=target_msc_url, expected_files=sub_expected_files)

    finally:
        test_helper.clean_bucket()
        shutil.rmtree(source_dir, ignore_errors=True)
        shutil.rmtree(second_local_dir, ignore_errors=True)
