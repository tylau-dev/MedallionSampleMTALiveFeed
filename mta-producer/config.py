from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic_name: str = "mta_live_updates"
    poll_interval: int = 30
    mta_feed_url: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

settings = Config()