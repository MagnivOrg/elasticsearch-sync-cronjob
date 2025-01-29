import time
from fetch_data import fetch_data_chunk
from elastic_client import update_elasticsearch
from settings import REDIS_URL
from rq import Queue, Worker
from redis import Redis

redis_conn = Redis.from_url("redis://localhost:6379")
queue = Queue(connection=redis_conn)


def sync_to_elasticsearch():
    """Fetch data in chunks and sync it to Elasticsearch."""
    offset = 0
    chunk_size = 1000000

    while True:
        data_chunk = fetch_data_chunk(offset, chunk_size)
        if not data_chunk:
            break

        update_elasticsearch(data_chunk)
        print(f"Synced {len(data_chunk)} records to Elasticsearch.")

        offset += chunk_size


def worker_loop():
    """Continuously run the worker every 10 minutes."""
    while True:
        print("Starting data sync to Elasticsearch...")
        sync_to_elasticsearch()
        print("Waiting for 10 minutes before the next sync...")
        time.sleep(600)


if __name__ == "__main__":
    with Connection(redis_conn):
        worker = Worker([queue])
        worker.work()

    worker_loop()
