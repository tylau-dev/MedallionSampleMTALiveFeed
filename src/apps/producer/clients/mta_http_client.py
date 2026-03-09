import httpx
import logging
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)

class MTAHttpClient:
    def __init__(self):
        self._client = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=10)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.aclose()

    async def get_feed(self, feed_url: str) -> gtfs_realtime_pb2.FeedMessage:
        try:
            logger.info(f"Fetching MTA feed from {feed_url}")
            response = await self._client.get(feed_url)
            response.raise_for_status()

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)

            return feed.entity if feed.entity else None
        except httpx.RequestError as e:
            logger.error(f"Error fetching MTA feed: {e}")
            return None