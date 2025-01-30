import psycopg2
from fetch_data import fetch_data_chunk
from elastic_client import update_elasticsearch
from settings import DB_CONFIG


def get_last_sync_time():
    """Retrieve the latest request_start_time from request_logs."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = """
        SELECT COALESCE(MAX(request_start_time), '2000-01-01T00:00:00Z')
        FROM request_logs;
    """

    cursor.execute(query)
    last_sync_time = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return last_sync_time.isoformat()


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
        print(f"Synced {len(data_chunk)} new records to Elasticsearch.")


if __name__ == "__main__":
    print("Starting Elasticsearch data sync...")
    sync_to_elasticsearch()
    print("Data sync completed.")
