import json
from confluent_kafka import Producer, KafkaError

class MtaKafkaClient:
    def __init__(self, server: str):
        self._producer = Producer(
            {
                'bootstrap.servers': server,
                'client.id': 'mta-producer',
                'acks': 'all',
                'retries': 5,
                'linger.ms': 10,
                'batch.num.messages': 100,
                'compression.type': 'gzip'
            }
        )

    def send_event(self, topic: str, key, value):
        self._producer.produce(
            topic,
            key=key,
            value=json.dumps(value),
            callback=self._delivery_report)

    def _delivery_report(self, err, msg):
        if err: print(f"Error: {err}")

    def flush(self):
        self._producer.flush()