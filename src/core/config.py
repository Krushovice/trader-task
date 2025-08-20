from pathlib import Path
from typing import Optional, Literal

from pydantic import BaseModel, Field, field_validator
from pydantic_core.core_schema import FieldValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class ApiConfig(BaseModel):
    key: str = Field(min_length=5)
    secret: str = Field(min_length=10)
    testnet: bool = True


class WebsocketConfig(BaseModel):
    # базовые параметры
    url: str
    symbol: str
    timeframe: Literal["1m", "3m", "5m", "15m", "1h"] = "1m"

    # поведение
    mode: Literal["replay", "live"] = "replay"
    reconnect_delay: int = 5  # сек

    # торговые настройки
    max_bars_wait: int = 12
    retest_pct: float = 0.003
    order_percent: float = 0.40
    trailing_pct: float = 0.01
    take_profit_pct: Optional[float] = None
    balance_drawdown_limit_pct: float = 0.05
    min_atr_1h: Optional[float] = None
    max_order_cost_usdt: Optional[float] = None

    # хотим, чтобы повторная установка значений тоже валидировалась
    model_config = {"validate_assignment": True}

    # ---- validators ----
    @field_validator("url")
    @classmethod
    def validate_ws_url(cls, url: str) -> str:
        if not (url.startswith("ws://") or url.startswith("wss://")):
            raise ValueError("url должен начинаться с ws:// или wss://")
        return url

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, symbol: str) -> str:
        return symbol.upper().strip()

    @field_validator("max_bars_wait")
    @classmethod
    def validate_max_bars_wait(cls, max_bars_wait: int) -> int:
        if max_bars_wait < 1:
            raise ValueError("max_bars_wait должен быть >= 1")
        return max_bars_wait

    @field_validator(
        "order_percent",
        "trailing_pct",
        "balance_drawdown_limit_pct",
        "retest_pct",
        "take_profit_pct",
    )
    @classmethod
    def validate_percent_range(
        cls, percent_value: Optional[float], info: FieldValidationInfo
    ) -> Optional[float]:
        if percent_value is None:
            return None
        if not (0 < percent_value <= 1):
            raise ValueError(f"{info.field_name} должен быть в диапазоне (0, 1]")
        return percent_value

    @field_validator("min_atr_1h")
    @classmethod
    def validate_min_atr(cls, min_atr_1h: Optional[float]) -> Optional[float]:
        if min_atr_1h is None:
            return None
        if min_atr_1h <= 0:
            raise ValueError("min_atr_1h должен быть > 0")
        return min_atr_1h

    @field_validator("reconnect_delay")
    @classmethod
    def validate_reconnect_delay(cls, reconnect_delay: int) -> int:
        if reconnect_delay < 0:
            raise ValueError("reconnect_delay должен быть >= 0")
        return reconnect_delay


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        env_prefix="APP__",
        env_nested_delimiter="__",
        extra="ignore",
        validate_assignment=True,
    )

    api: ApiConfig
    ws: WebsocketConfig


settings = Settings()
