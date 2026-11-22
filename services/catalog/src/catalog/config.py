from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    catalog_database_url: str = "postgresql+psycopg://price_sync:price_sync@localhost:5433/catalog"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_price_updates: str = "price-updates"
    kafka_consumer_group: str = "catalog-price-sync"
    catalog_seed_file: str = "data/products.json"
    consumer_max_attempts: int = 3
    consumer_retry_delay_seconds: float = 1.0
    consumer_poll_timeout_seconds: float = 0.05
    consumer_stall_timeout_seconds: float = 60.0
    latency_window_size: int = 100


settings = Settings()
