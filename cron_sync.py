from fetch_data import fetch_data_chunk
from elastic_client import update_elasticsearch

CHUNK_SIZE = 1000000

def sync_to_elasticsearch():
    offset = 0

    while True:
        data_chunk = fetch_data_chunk(offset, CHUNK_SIZE)
        if not data_chunk:
            break

        update_elasticsearch(data_chunk)
        print(f"Pushed {len(data_chunk)} records to Elasticsearch.")

        offset += CHUNK_SIZE

if __name__ == "__main__":
    print("Starting Elasticsearch Sync Cron Job...")
    sync_to_elasticsearch()
    print("Cron Job Execution Completed.")
