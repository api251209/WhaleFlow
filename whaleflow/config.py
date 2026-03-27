from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # FinMind API
    finmind_api_token: str = ""
    finmind_base_url: str = "https://api.finmindtrade.com/api/v4/data"

    # Database
    db_path: Path = Path("data/whaleflow.db")

    # Rate limiting
    rate_limit_per_day: int = 580  # buffer under 600
    rate_limit_per_second: float = 2.0

    # Fetch settings
    fetch_retry_times: int = 3
    fetch_retry_delay: float = 2.0  # seconds

    # Logging
    log_level: str = "INFO"


settings = Settings()
