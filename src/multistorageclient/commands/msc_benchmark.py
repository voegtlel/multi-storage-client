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

import argparse
import json
import os
import statistics
import time

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, Manager
from multiprocessing.managers import ListProxy
from typing import Any, Dict, List, Union, Optional

from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.schema import BENCHMARK_SCHEMA, validate

# Default configuration
DEFAULT_CONFIG = {
    "processes": [8],
    "threads": [4],
    "tests_mixed": {
        "4MB": 12800,
        "64MB": 800,
    },
}


def load_config(config_path: Optional[str]) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.

    :param config_path: Path to the configuration file
    :return: Configuration dictionary
    """
    if config_path is None:
        print(f"Config file {config_path} not found. Using default configuration.")
        return DEFAULT_CONFIG
    elif os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                config = json.load(f)

            # Validate config against benchmark schema
            validate(instance=config, schema=BENCHMARK_SCHEMA)

            return config
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Error parsing config file {config_path}", e.doc, e.pos)
        except Exception as e:
            raise Exception(f"Encountered an exception loading config file: {e}")
    else:
        raise FileNotFoundError(f"No file found, config_path incorrect {config_path}")


def size_to_bytes(size: str) -> int:
    return int(size[:-2]) * 1024 ** {"KB": 1, "MB": 2, "GB": 3, "TB": 4, "PB": 5}[size[-2:]]


def generate_random_data(tests_mixed: Dict[str, int]) -> Dict[str, bytes]:
    """
    Generate random data for each file size in the tests_mixed dictionary.

    :param tests_mixed: Dictionary mapping size strings to number of files
    :return: Dictionary mapping size strings to random data of that size
    """
    return {k: os.urandom(size_to_bytes(k)) for k in tests_mixed.keys()}


class PerformanceMetrics:
    def __init__(
        self,
        start_times: Union[List[Any], ListProxy],
        end_times: Union[List[Any], ListProxy],
        response_times: Union[List[Any], ListProxy],
        object_sizes: Union[List[Any], ListProxy],
    ) -> None:
        self.start_times = start_times
        self.end_times = end_times
        self.response_times = response_times
        self.object_sizes = object_sizes

    def record(self, start_time: float, end_time: float, size: int) -> None:
        self.start_times.append(start_time)
        self.end_times.append(end_time)
        self.response_times.append(end_time - start_time)
        self.object_sizes.append(size)

    def calculate(self) -> None:
        total_size = sum(self.object_sizes)
        total_time = max(self.end_times) - min(self.start_times)
        avg_response_time = sum(self.response_times) / len(self.response_times)

        response_time_percentiles = {
            "50%": statistics.median(self.response_times),
            "90%": statistics.quantiles(self.response_times, n=10)[-1],
            "99%": statistics.quantiles(self.response_times, n=100)[-1],
        }

        # Results summary
        print(f"Total data transferred: {pretty_print_bytes(total_size)}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Throughput: {pretty_print_bytes(total_size / total_time)}/s")
        print(f"Average response time: {avg_response_time * 1000:.2f} ms")
        print(
            f"Response time percentiles: 50% Median: {response_time_percentiles['50%'] * 1000:.2f} ms, "
            f"90%: {response_time_percentiles['90%'] * 1000:.2f} ms, "
            f"99%: {response_time_percentiles['99%'] * 1000:.2f} ms\n"
        )


def pretty_print_bytes(byte_value: float) -> str:
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while byte_value >= 1024 and i < len(suffixes) - 1:
        byte_value /= 1024.0
        i += 1
    return f"{byte_value:.2f}".rstrip("0").rstrip(".") + " " + suffixes[i]


def upload_object(
    storage_client: StorageClient, size: str, path: str, metrics: PerformanceMetrics, random_data: Dict[str, bytes]
) -> None:
    data = random_data[size]
    start_time = time.time()
    try:
        storage_client.write(path=path, body=data)
    except Exception as e:
        print(f"Error uploading {path}: {e}")
    end_time = time.time()
    metrics.record(start_time, end_time, size_to_bytes(size))


def download_object(storage_client: StorageClient, path: str, metrics: PerformanceMetrics) -> None:
    start_time = time.time()
    size = 0
    try:
        size = len(storage_client.read(path=path))
    except Exception as e:
        print(f"Error downloading {path}: {e}")
    end_time = time.time()
    metrics.record(start_time, end_time, size)


def delete_object(storage_client: StorageClient, path: str) -> None:
    try:
        storage_client.delete(path=path)
    except Exception as e:
        print(f"Error deleting {path}: {e}")


def task(
    storage_client: StorageClient,
    test_type: str,
    bucket: str,
    size: str,
    i: int,
    metrics: PerformanceMetrics,
    random_data: Dict[str, bytes],
) -> None:
    object_name_prefix = f"test-{size}"
    object_name = f"{object_name_prefix}-{i}"
    object_path = os.path.join(bucket, object_name)

    if test_type == "upload":
        upload_object(storage_client, size, object_path, metrics, random_data)
    elif test_type == "download":
        download_object(storage_client, object_path, metrics)
    elif test_type == "delete":
        delete_object(storage_client, object_path)


def process_task(
    storage_client: StorageClient,
    test_type: str,
    bucket: str,
    size: str,
    batch_range: range,
    metrics: PerformanceMetrics,
    threads: int,
    random_data: Dict[str, bytes],
) -> None:
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for i in batch_range:
            executor.submit(task, storage_client, test_type, bucket, size, i, metrics, random_data)


def run_test(
    storage_client: StorageClient,
    test_type: str,
    bucket: str,
    size: str,
    num_objects: int,
    processes: int,
    threads: int,
    random_data: Dict[str, bytes],
) -> None:
    print(
        f"--- Running {test_type} test for {num_objects} x {size} objects with {processes} processes x {threads} threads ---"
    )

    with Manager() as manager:
        # Create shared lists for metrics
        start_times = manager.list()
        end_times = manager.list()
        response_times = manager.list()
        object_sizes = manager.list()

        metrics = PerformanceMetrics(start_times, end_times, response_times, object_sizes)

        # Split files into batches for each process
        batch_size = num_objects // processes + 1
        batches = [range(i, min(i + batch_size, num_objects)) for i in range(0, num_objects, batch_size)]

        with Pool(processes=processes, maxtasksperchild=1) as pool:
            pool.starmap(
                process_task,
                [
                    (storage_client, test_type, bucket, size, batches[i], metrics, threads, random_data)
                    for i in range(len(batches))
                ],
            )

        if test_type != "delete":
            metrics.calculate()
        else:
            print("Delete complete")


def main():
    parser = argparse.ArgumentParser(description="Upload/Download performance tests with Multi-Storage Client")
    parser.add_argument("--prefix", type=str, default="", help="The path prefix to use for the test")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--profile", type=str, required=True, help="MSC profile to use")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    processes = config["processes"]
    threads = config["threads"]
    tests_mixed = config["tests_mixed"]

    # Generate random data for tests
    random_data = generate_random_data(tests_mixed)

    # Initialize storage client
    storage_client_config = StorageClientConfig.from_file(profile=args.profile)
    storage_client = StorageClient(storage_client_config)
    bucket = args.prefix

    # Run tests for each combination of processes and threads
    for num_processes in processes:
        for num_threads in threads:
            for size, num_objects in tests_mixed.items():
                # Upload test
                run_test(storage_client, "upload", bucket, size, num_objects, num_processes, num_threads, random_data)

                # Download test
                run_test(storage_client, "download", bucket, size, num_objects, num_processes, num_threads, random_data)

                # Delete test
                run_test(storage_client, "delete", bucket, size, num_objects, num_processes, num_threads, random_data)


if __name__ == "__main__":
    main()
