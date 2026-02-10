from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings and environment variables.
    """
    # IBKR Credentials
    IBKR_TOKEN: str = Field(..., alias="IBKR_TOKEN")
    IBKR_QUERY_ID: str = Field(..., alias="IBKR_QUERY_ID")

    # Database Configuration
    # Defaults to local DuckDB, but can be overridden for Online/Postgres
    DATABASE_URL: str = Field("data/trading_data.duckdb", alias="DATABASE_URL")

    # MotherDuck Token (Optional - triggers Cloud Storage if present)
    MOTHERDUCK_TOKEN: Optional[str] = Field(None, alias="MOTHERDUCK_TOKEN")

    IS_OFFLINE: bool = True

    # Allow extra fields in .env without error
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra='ignore')


# Singleton instance for use across the app
settings = Settings()