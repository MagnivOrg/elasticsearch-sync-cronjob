from elasticsearch import Elasticsearch, helpers
from settings import ES_HOST

es = Elasticsearch([ES_HOST])

def update_elasticsearch(data_chunk):
    """Push precomputed data to Elasticsearch in batches and catch errors."""
    if data_chunk:
        try:
            helpers.bulk(es, data_chunk)
            print(f"Pushed {len(data_chunk)} records to Elasticsearch.")
        except helpers.BulkIndexError as e:
            print("⚠️ Bulk index error:", e.errors)
            for error in e.errors:
                print("Failed document:", error)
