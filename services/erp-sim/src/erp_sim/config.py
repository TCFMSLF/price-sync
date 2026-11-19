from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    erp_database_url: str = "postgresql+psycopg://price_sync:price_sync@localhost:5433/erp"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_price_updates: str = "price-updates"
    outbox_batch_size: int = 100
    outbox_poll_interval_seconds: float = 1.0
    outbox_retry_max_seconds: float = 30.0
    seed_file: str = "data/products.json"


settings = Settings()
