from pydantic_settings import BaseSettings, SettingsConfigDict

class Config(BaseSettings):
    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = ""
    s3_secret_key: str = ""

    bronze_checkpoint_location: str = "s3a://mta-bronze/checkpoints/"
    bronze_output_path: str = "s3a://mta-bronze/data/mta_trips"

    silver_checkpoint_location: str = "s3a://mta-silver/checkpoints/"
    silver_output_path: str = "s3a://mta-silver/data/mta_trips"

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"
    )

settings = Config()