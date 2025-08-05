from pathlib import Path
from typing import Optional

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
    order_percent: float = 0.4
    trailing_pct: float = 0.01
    take_profit_pct: Optional[float] = None
    balance_drawdown_limit_pct: float = 0.05
    min_atr_1h: Optional[float] = None

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
