from datetime import datetime
import psycopg2
from elastic_client import update_elasticsearch
from settings import DB_CONFIG

READ_REPLICA_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['read_host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}?sslmode=require"


def sync_to_elasticsearch():
    """Fetch new analytics data from the read-replica and mark them as synced in the primary DB."""

    read_conn = psycopg2.connect(
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["read_host"],
        port=DB_CONFIG["port"]
    )
    read_cursor = read_conn.cursor()

    read_cursor.execute("""
        SELECT 
            id, request_log_id, workspace_id, prompt_id, prompt_name,
            request_start_time, request_end_time, price, tokens, engine, 
            tags, analytics_metadata, created_at, updated_at
        FROM analytics_data 
        WHERE synced = FALSE
        LIMIT 100;
    """)

    data_chunk = read_cursor.fetchall()
    read_cursor.close()
    read_conn.close()

    if not data_chunk:
        print("No new data to sync.")
        return

    formatted_data = []
    for row in data_chunk:
        formatted_data.append({
            "_id": row[0],
            "_index": "analytics_index",
            "_source": {
                "id": row[0],
                "request_log_id": row[1],
                "workspace_id": row[2],
                "prompt_id": row[3],
                "prompt_name": row[4] if row[4] else "",
                "request_start_time": row[5].isoformat() if isinstance(row[5], datetime) else "1970-01-01T00:00:00",
                "request_end_time": row[6].isoformat() if isinstance(row[6], datetime) else "1970-01-01T00:00:00",
                "price": float(row[7]) if row[7] is not None else 0.0,
                "tokens": int(row[8]) if row[8] is not None else 0,
                "engine": row[9] if row[9] else "",
                "tags": row[10] if isinstance(row[10], list) else [],
                "analytics_metadata": row[11] if isinstance(row[11], dict) else {},
                "created_at": row[12].isoformat() if isinstance(row[12], datetime) else "1970-01-01T00:00:00",
                "updated_at": row[13].isoformat() if isinstance(row[13], datetime) else "1970-01-01T00:00:00",
            },
        })

    update_elasticsearch(formatted_data)

    write_db_config = {k: DB_CONFIG[k] for k in ["dbname", "user", "password", "host", "port"] if k in DB_CONFIG}

    write_conn = psycopg2.connect(**write_db_config)
    write_cursor = write_conn.cursor()

    try:
        write_cursor.execute("""
            UPDATE analytics_data 
            SET synced = TRUE 
            WHERE id = ANY(%s);
        """, ([row[0] for row in data_chunk],))

        write_conn.commit()
        print(f"Synced and updated {len(data_chunk)} records in analytics_data.")

    except Exception as e:
        write_conn.rollback()
        print(f"Error updating records: {e}")

    finally:
        write_cursor.close()
        write_conn.close()


if __name__ == "__main__":
    print("Starting analytics data sync...")
    sync_to_elasticsearch()
    print("Data sync completed.")
