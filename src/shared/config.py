from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_name: str = "mta_subway"

    poll_interval: int = 30

    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = ""
    s3_secret_key: str = ""

    bronze_checkpoint_location: str = "s3a://mta-bronze/checkpoints/"
    bronze_output_path: str = "s3a://mta-bronze/data/mta_trips"

    silver_checkpoint_location: str = "s3a://mta-silver/checkpoints/"
    silver_output_path: str = "s3a://mta-silver/data/mta_trips"

    gold_checkpoint_location: str = "s3a://mta-gold/checkpoints/"
    gold_output_path: str = "s3a://mta-gold/data/mta_trips"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

settings = Config()