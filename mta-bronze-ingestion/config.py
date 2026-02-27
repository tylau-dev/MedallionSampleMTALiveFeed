from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:29092"
    kafka_topic: str = "mta_live_updates"

    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    checkpoint_location: str = "s3a://mta-bronze/checkpoints/"
    output_path: str = "s3a://mta-bronze/data/mta_trips"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

settings = Config()