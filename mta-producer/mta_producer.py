import logging
import json
import time
import requests
import signal
from config import settings
from google.transit import gtfs_realtime_pb2
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MTA-Producer")

conf = {
    'bootstrap.servers': settings.kafka_bootstrap_servers,
    'client.id': 'mta-producer',
    'acks': 'all',
    'retries': 5,
    'linger.ms': 10,
    'batch.num.messages': 100,
    'compression.type': 'gzip'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch():
    try:
        response = requests.get(settings.mta_feed_url, timeout=10)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        if feed.entity:
            return feed.entity[0]
        else:
            logger.warning("No entities found in MTA feed")
            return None
    except requests.RequestException as e:
        logger.error(f"Error fetching MTA feed: {e}")
        return None

def serialize_entity_to_payload(entity):
    if entity.HasField('trip_update'):
        payload = {
            "trip_id": entity.trip_update.trip.trip_id,
            "route_id": entity.trip_update.trip.route_id,
            "timestamp": entity.trip_update.timestamp,
            "stop_time_updates": [
                {
                    "stop_id": u.stop_id,
                    "arrival_delay": u.arrival.delay if u.HasField('arrival') else 0,
                    "arrival_time": u.arrival.time if u.HasField('arrival') else None
                } for u in entity.trip_update.stop_time_update
            ]
        }
        yield payload

def produce_to_kafka(payload):
    try:
        producer.produce(
            settings.kafka_topic_name,
            key=payload['trip_id'],
            value=json.dumps(payload),
            callback=delivery_report)
        producer.flush()
        logger.info(f"Successfully polled and produced payload for trip_id {payload['trip_id']}")
    except Exception as e:
        logger.error(f"Error producing to Kafka: {e}")

running = True

def signal_handler(sig, frame):
    global running
    print("Signal received, shutting down")
    running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def run_producer():
    while running:
        entity = fetch()
        if entity:
            for payload in serialize_entity_to_payload(entity):
                produce_to_kafka(payload)
        time.sleep(settings.poll_interval)

        for _ in range(settings.poll_interval):
            if not running: break
            time.sleep(1)

    print("Flushing remaining messages...")
    producer.flush(timeout=10)
    print("Shutdown complete.")

if __name__ == "__main__":
    run_producer()