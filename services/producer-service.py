import logging
import os
import json 
import time
import requests
from google.transit import gtfs_realtime_pb2
from confluent_kafka import Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("MTA-Producer")

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'client.id': 'mta-producer',
    'acks': 'all', # Guarantee data is written to all replicas
    'retries': 5, # Retry up to 5 times on failure
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
        response = requests.get(os.getenv('MTA_FEED_URL'), timeout=10)
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
            os.getenv('TOPIC_NAME'), 
            key=payload['trip_id'],
            value=json.dumps(payload), 
            callback=delivery_report)
        producer.flush()
        logger.info(f"Successfully polled and produced payload for trip_id {payload['trip_id']}")
    except Exception as e:
        logger.error(f"Error producing to Kafka: {e}")

if __name__ == "__main__":
    load_dotenv()

    while True:
        entity = fetch()
        if entity:
            for payload in serialize_entity_to_payload(entity):
                produce_to_kafka(payload)
        time.sleep(30)