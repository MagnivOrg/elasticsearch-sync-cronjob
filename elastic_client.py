from elasticsearch import Elasticsearch, helpers

from main import POSTGRES_HOST

es = Elasticsearch([POSTGRES_HOST])

def update_elasticsearch(data_chunk):
    """Push data to Elasticsearch in batches."""
    if data_chunk:
        helpers.bulk(es, data_chunk)
        print(f"Pushed {len(data_chunk)} records to Elasticsearch.")
