from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Protocol, Any

import pandas as pd
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator


class OHLCVClient(Protocol):
    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, limit: int
    ) -> list[list[Any]]: ...


def _latest_number(
    df: Optional[pd.DataFrame],
    column: str,
) -> Optional[float]:
    """Безопасно достаёт последнее числовое значение столбца (или None)."""
    if df is None or df.empty or column not in df.columns:
        return None
    s = pd.to_numeric(df[column], errors="coerce").dropna()
    return float(s.iloc[-1]) if not s.empty else None


@dataclass(slots=True)
class HTFCache:
    symbol: str
    rest: OHLCVClient
    _df_1h: Optional[pd.DataFrame] = field(
        default=None,
        init=False,
    )
    _df_1d: Optional[pd.DataFrame] = field(
        default=None,
        init=False,
    )
    _last_hour_key: Optional[tuple[int, int, int, int]] = field(
        default=None, init=False
    )
    _last_day_key: Optional[tuple[int, int, int]] = field(
        default=None,
        init=False,
    )

    async def _fetch_df(self, timeframe: str, limit: int) -> pd.DataFrame:
        ohlcv = await self.rest.fetch_ohlcv(
            self.symbol.upper(),
            timeframe,
            limit=limit,
        )
        df = pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])
        if timeframe == "1h":
            df["ema60"] = EMAIndicator(
                close=df["c"], window=60, fillna=False
            ).ema_indicator()
        elif timeframe == "1d":
            df["rsi"] = RSIIndicator(
                close=df["c"],
                window=14,
                fillna=False,
            ).rsi()
        return df

    async def get(
        self,
        current_dt_utc: datetime,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Вернёт актуальные df_1h и df_1d, обновляя их только при смене часа/дня."""
        hour_key = (
            current_dt_utc.year,
            current_dt_utc.month,
            current_dt_utc.day,
            current_dt_utc.hour,
        )
        day_key = (
            current_dt_utc.year,
            current_dt_utc.month,
            current_dt_utc.day,
        )

        if self._df_1h is None or hour_key != self._last_hour_key:
            self._df_1h = await self._fetch_df(
                "1h",
                limit=200,
            )
            self._last_hour_key = hour_key

        if self._df_1d is None or day_key != self._last_day_key:
            self._df_1d = await self._fetch_df(
                "1d",
                limit=200,
            )
            self._last_day_key = day_key

        return self._df_1h, self._df_1d

    # Удобные геттеры (если где-то нужно)
    def ema1h(self) -> Optional[float]:
        return _latest_number(self._df_1h, "ema60")

    def rsi1d(self) -> Optional[float]:
        return _latest_number(self._df_1d, "rsi")
