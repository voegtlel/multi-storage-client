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
import os
import statistics
import time

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, Manager
from typing import Any, Dict, List

from multistorageclient import StorageClient, StorageClientConfig

PROCESSES: List[int] = [8]
THREADS: List[int] = [4]
TESTS_MIXED: Dict[str, int] = {
    "4MB": 12800,
    "64MB": 800,
}


def size_to_bytes(size: str) -> int:
    return int(size[:-2]) * 1024 ** {"KB": 1, "MB": 2, "GB": 3, "TB": 4, "PB": 5}[size[-2:]]


RANDOM_DATA = {k: os.urandom(size_to_bytes(k)) for k in TESTS_MIXED.keys()}


class PerformanceMetrics:
    def __init__(
        self, start_times: List[Any], end_times: List[Any], response_times: List[Any], object_sizes: List[Any]
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


def upload_object(storage_client: StorageClient, size: str, path: str, metrics: PerformanceMetrics) -> None:
    data = RANDOM_DATA[size]
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
    storage_client: StorageClient, test_type: str, bucket: str, size: str, i: int, metrics: PerformanceMetrics
) -> None:
    object_name_prefix = f"test-{size}"
    object_name = f"{object_name_prefix}-{i}"
    object_path = os.path.join(bucket, object_name)

    if test_type == "upload":
        upload_object(storage_client, size, object_path, metrics)
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
) -> None:
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for i in batch_range:
            executor.submit(task, storage_client, test_type, bucket, size, i, metrics)


def run_test(
    storage_client: StorageClient,
    test_type: str,
    bucket: str,
    size: str,
    num_objects: int,
    processes: int,
    threads: int,
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

        metrics = PerformanceMetrics(start_times, end_times, response_times, object_sizes)  # type: ignore

        # Split files into batches for each process
        batch_size = num_objects // processes + 1
        batches = [range(i, min(i + batch_size, num_objects)) for i in range(0, num_objects, batch_size)]

        with Pool(processes=processes, maxtasksperchild=1) as pool:
            pool.starmap(
                process_task,
                [(storage_client, test_type, bucket, size, batches[i], metrics, threads) for i in range(len(batches))],
            )

        if test_type != "delete":
            metrics.calculate()
        else:
            print("Delete complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload/Download performance tests with nv storage client")
    parser.add_argument("--prefix", type=str, default="", help="The path prefix to use for the test")
    parser.add_argument("--profile", type=str, default="default", help="The storage client profile to use for the test")
    args = parser.parse_args()

    storage_client_config = StorageClientConfig.from_file(profile=args.profile)
    storage_client = StorageClient(storage_client_config)
    prefix = args.prefix

    for size_str, objects in TESTS_MIXED.items():
        for processes in PROCESSES:
            for threads in THREADS:
                run_test(storage_client, "upload", prefix, size_str, objects, processes, threads)
                run_test(storage_client, "download", prefix, size_str, objects, processes, threads)
                run_test(storage_client, "delete", prefix, size_str, objects, processes, threads)


if __name__ == "__main__":
    main()
