import psycopg2
from settings import DB_CONFIG

def fetch_data_chunk(last_sync_time, limit):
    """Fetch only new records from PostgreSQL based on created_at timestamp."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    query = f"""
        SELECT
            rl.id, rl.workspace_id, rl.request_start_time, rl.request_end_time,
            rl.price, rl.tokens, rl.engine, 
            ARRAY_AGG(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL) AS tags,
            jsonb_object_agg(mf.name, mv.value) FILTER (WHERE mv.value IS NOT NULL) AS metadata
        FROM request_logs AS rl
        LEFT JOIN requests_tags AS rt ON rl.id = rt.request_id
        LEFT JOIN tags AS t ON rt.tag_id = t.id
        LEFT JOIN metadata_value AS mv ON rl.id = mv.request_id
        LEFT JOIN metadata_field AS mf ON mv.metadata_field_id = mf.id
        WHERE rl.created_at > %s
        GROUP BY rl.id, rl.workspace_id, rl.request_start_time, rl.request_end_time, rl.price, rl.tokens, rl.engine
        ORDER BY rl.created_at ASC
        LIMIT %s;
    """
    cursor.execute(query, (last_sync_time, limit))
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [
        {
            "_id": row[0],
            "_index": "request_logs_index",
            "_source": {
                "id": row[0],
                "workspace_id": row[1],
                "request_start_time": row[2],
                "request_end_time": row[3],
                "price": row[4],
                "tokens": row[5],
                "engine": row[6],
                "tags": row[7] if row[7] else [],
                "metadata": row[8] if row[8] else {},
            },
        }
        for row in rows
    ]
