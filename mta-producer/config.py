from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    poll_interval: int = 30
    mta_feed_url: str = ""
    topic_name: str = "mta_live_updates"

    model_config = SettingsConfigDict(env_file=".env")

settings = Config()