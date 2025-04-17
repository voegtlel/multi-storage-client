from datetime import timedelta
import multistorageclient as msc
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.caching.distributed_hint import DistributedHint
import time


def attempt_acquire_lock(hint_prefix, process_id, acquired_count, bucket_config, test_profile=None):
    """Try to acquire the hint and increment shared counter if successful."""
    # Create a new storage client in the child process
    if test_profile is not None:
        storage_client, _ = msc.resolve_storage_client(f"msc://{test_profile}/")
    else:
        profile = "data"
        config_dict = {"profiles": {profile: bucket_config}}
        storage_client = StorageClient(config=StorageClientConfig.from_dict(config_dict=config_dict, profile=profile))

    hint = DistributedHint(
        storage_provider=storage_client._storage_provider,
        hint_prefix=hint_prefix,
        heartbeat_interval=timedelta(seconds=2.0),
        heartbeat_buffer=timedelta(seconds=1.0),
    )
    print(f"Process {process_id} attempting to acquire hint...")
    acquired = hint.acquire()
    if acquired:
        print(f"Process {process_id} successfully acquired the hint")
        with acquired_count.get_lock():
            acquired_count.value += 1
        # Hold the hint for longer than heartbeat lifespan (7 seconds)
        time.sleep(5.0)
        hint.release()
        print(f"Process {process_id} released the hint")
    else:
        print(f"Process {process_id} failed to acquire the hint")
