from pathlib import Path

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class ApiConfig(BaseModel):
    key: str
    secret: str


class WebsocketConfig(BaseModel):
    url: str
    symbol: str
    max_bars_wait: int = 12
    retest_pct: float = 0.003
    mode: str = "replay"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=f"{BASE_DIR}/.env",
        env_file_encoding="utf-8",
        env_prefix="APP__",
        env_nested_delimiter="__",
    )

    api: ApiConfig
    ws: WebsocketConfig


settings = Settings()
