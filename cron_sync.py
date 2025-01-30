import redis
from fetch_data import fetch_data_chunk
from elastic_client import update_elasticsearch
from settings import REDIS_URL

redis_conn = redis.Redis.from_url(REDIS_URL)
LAST_SYNC_KEY = "elasticsearch:last_sync_time"

def get_last_sync_time():
    """Retrieve the last sync timestamp from Redis."""
    last_sync_time = redis_conn.get(LAST_SYNC_KEY)
    return last_sync_time.decode("utf-8") if last_sync_time else "2000-01-01T00:00:00Z"

def set_last_sync_time(timestamp):
    """Update the last sync timestamp in Redis."""
    redis_conn.set(LAST_SYNC_KEY, timestamp)

def sync_to_elasticsearch():
    """Fetch new data and sync to Elasticsearch."""
    last_sync_time = get_last_sync_time()
    chunk_size = 1000000

    while True:
        data_chunk = fetch_data_chunk(last_sync_time, chunk_size)
        if not data_chunk:
            break

        update_elasticsearch(data_chunk)
        last_sync_time = max(record["_source"]["request_start_time"] for record in data_chunk)
        set_last_sync_time(last_sync_time)

        print(f"Synced {len(data_chunk)} new records to Elasticsearch.")

if __name__ == "__main__":
    print("Starting Elasticsearch data sync...")
    sync_to_elasticsearch()
    print("Data sync completed.")
