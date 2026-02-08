from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings and environment variables.
    Pydantic automatically maps uppercase ENV variables to these attributes.
    """
    # IBKR Credentials
    IBKR_TOKEN: str = Field(..., alias="IBKR_TOKEN")
    IBKR_QUERY_ID: str = Field(..., alias="IBKR_QUERY_ID")

    # Database Configuration
    # Defaults to local DuckDB, but can be overridden for Online/Postgres
    DATABASE_URL: str = Field("data/trading_data.duckdb", alias="DATABASE_URL")
    IS_OFFLINE: bool = True

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


# Singleton instance for use across the app
settings = Settings()