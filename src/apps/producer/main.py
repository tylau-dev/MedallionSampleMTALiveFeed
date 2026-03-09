import asyncio
import logging
import signal
from apps.producer.clients.mta_kafka_client import MtaKafkaClient
from apps.producer.clients.mta_http_client import MTAHttpClient
from apps.producer.services.mta_streaming_service import MTAStreamingService
from shared.config import settings

logger = logging.getLogger(__name__)

async def main():
    async with MTAHttpClient() as mta_client:
        mta_kafka_client = MtaKafkaClient(settings.kafka_bootstrap_servers)

        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        for s in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, stop_event.set)

        logger.info("Starting MTA Producer...")
        try:
            while not stop_event.is_set():
                mta_streaming_service = MTAStreamingService(mta_client, mta_kafka_client, settings)
                await mta_streaming_service.run_cycle()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=settings.poll_interval)
                except asyncio.TimeoutError:
                    continue
        finally:
            logger.info("Shutting down...")
            mta_kafka_client.flush()

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

if __name__ == "__main__":
    setup_logging()
    asyncio.run(main())