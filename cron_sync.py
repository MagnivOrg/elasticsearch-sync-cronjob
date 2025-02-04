import psycopg2
from elastic_client import update_elasticsearch
from settings import DB_CONFIG


def sync_to_elasticsearch():
    """Fetch new analytics data, sync to Elasticsearch, and delete once synced."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM analytics_data WHERE synced = FALSE;")
    data_chunk = cursor.fetchall()

    if not data_chunk:
        print("No new data to sync.")
        return

    formatted_data = [
        {
            "_id": row[0],
            "_index": "analytics_index",
            "_source": {
                "id": row[0],
                "workspace_id": row[1],
                "request_log_id": row[2],
                "prompt_id": row[3],
                "prompt_name": row[4],
                "request_start_time": row[5],
                "request_end_time": row[6],
                "price": row[7],
                "tokens": row[8],
                "engine": row[9],
                "tags": row[10] if row[10] else [],
                "metadata": row[11] if row[11] else {},
                "created_at": row[12],
                "updated_at": row[13],
            },
        }
        for row in data_chunk
    ]

    update_elasticsearch(formatted_data)

    cursor.execute(
        "DELETE FROM analytics_data WHERE id = ANY(%s)",
        ([row[0] for row in data_chunk],),
    )
    conn.commit()

    cursor.close()
    conn.close()
    print(f"Synced and deleted {len(data_chunk)} records from analytics_data.")


if __name__ == "__main__":
    print("Starting analytics data sync...")
    sync_to_elasticsearch()
    print("Data sync completed.")
