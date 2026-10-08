from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TO_", extra="ignore")

    database_url: str = "postgresql://observability:observability@postgres:5432/observability"
    redis_url: str = "redis://redis:6379/0"
    data_url: str = "http://data:8000"
    executor_host: str = "executor"
    executor_port: int = 8000
    executor_capacity: int = Field(default=8, ge=1)
    executor_replicas: int = Field(default=1, ge=1)
    batch_size: int = Field(default=256, ge=1, le=1024)
    queue_capacity: int = Field(default=8192, ge=1024)
    max_pending_measurements: int = Field(default=256, ge=1)
    kafka_brokers: str = "kafka:9092"
    kafka_topic: str = "experiment"
    kafka_group: str = "group15-temperature-v1"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_ca_file: str = ""
    kafka_keystore_file: str = ""
    kafka_keystore_password_file: str = ""
    kafka_endpoint_identification: str = "https"
    notification_url: str = "http://notification-sink:8000/api/notify"
    notification_token_file: str = ""
    notification_interval: float = Field(default=0.1, gt=0, le=5)
    notification_concurrency: int = Field(default=32, ge=1, le=256)
    notification_timeout: float = Field(default=2, gt=0)
    notification_lease: float = Field(default=15, gt=0)
    cache_ttl: int = Field(default=30, ge=1)
    vm_hourly_cost_usd: float = Field(default=0, ge=0)
    storage_gb_monthly_cost_usd: float = Field(default=0, ge=0)

    @model_validator(mode="after")
    def check_lease(self):
        if self.notification_lease < self.notification_timeout * 2 + 2:
            raise ValueError("notification lease must exceed twice the HTTP timeout plus 2 seconds")
        return self

    def kafka_config(self) -> dict:
        config = {
            "bootstrap.servers": self.kafka_brokers,
            "security.protocol": self.kafka_security_protocol,
        }
        if self.kafka_security_protocol == "SSL":
            config.update(
                {
                    "ssl.ca.location": self.kafka_ca_file,
                    "ssl.keystore.location": self.kafka_keystore_file,
                    "ssl.endpoint.identification.algorithm": self.kafka_endpoint_identification,
                }
            )
            if self.kafka_keystore_password_file:
                config["ssl.keystore.password"] = (
                    Path(self.kafka_keystore_password_file).read_text().strip()
                )
        return config
