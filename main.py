import redis
from rq import Worker, Queue, Connection

from settings import REDIS_URL

redis_conn = redis.from_url(REDIS_URL)
queue = Queue("elastic_sync", connection=redis_conn)


def start_worker():
    """Start the RQ worker to listen for jobs in the elastic_sync queue."""
    with Connection(redis_conn):
        worker = Worker(["elastic_sync"])
        print("Worker is listening for jobs...")
        worker.work()


if __name__ == "__main__":
    print("Starting Elasticsearch Sync Worker...")
    start_worker()
