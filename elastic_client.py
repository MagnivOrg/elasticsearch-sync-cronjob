from elasticsearch import Elasticsearch, helpers
from settings import ES_HOST

es = Elasticsearch([ES_HOST])

def update_elasticsearch(data_chunk):
    """Push data to Elasticsearch in batches."""
    if data_chunk:
        helpers.bulk(es, data_chunk)
        print(f"Pushed {len(data_chunk)} records to Elasticsearch.")
