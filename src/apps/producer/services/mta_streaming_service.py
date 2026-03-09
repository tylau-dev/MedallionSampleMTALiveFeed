import asyncio
import logging
from apps.producer.clients.mta_http_client import MTAHttpClient
from apps.producer.clients.mta_kafka_client import MtaKafkaClient
from apps.producer.constants import GTFS_FEED_URLS, GTFS_LINES
from shared.config import Config

logger = logging.getLogger(__name__)

class MTAStreamingService:
    def __init__(self, mta_client: MTAHttpClient, kafka_client: MtaKafkaClient, config: Config):
        self._mta_client = mta_client
        self._kafka_client = kafka_client
        self._kafka_topic_name = config.kafka_topic_name

    async def run_cycle(self):
        tasks = []
        for line in GTFS_LINES:
            tasks.append(self._fetch_and_produce(GTFS_FEED_URLS.get(line), self._kafka_topic_name))

        await asyncio.gather(*tasks)
        logger.info(f"Cycle complete")

    async def _fetch_and_produce(self, url, topic):
        entities = await self._mta_client.get_feed(url)

        if entities is None:
            return

        for entity in entities:
            mta_payload = self._serialize_mta_entity(entity)
            if mta_payload:
                self._kafka_client.send_event(topic, mta_payload["trip_id"], mta_payload)
                logger.info(f"Event sent to Kafka topic {topic}, key {mta_payload['trip_id']} for trip_id {mta_payload['trip_id']}")

    def _serialize_mta_entity(self, entity):
        if entity.HasField('trip_update'):
            return {
                "trip_id": entity.trip_update.trip.trip_id,
                "route_id": entity.trip_update.trip.route_id,
                "stop_time_updates": [
                    {
                        "stop_id": u.stop_id,
                        "delay": u.arrival.delay if u.HasField('arrival') else 0
                    } for u in entity.trip_update.stop_time_update
                ]
            }
