from typing import Optional
import pandas as pd
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange


class Indicators:
    @staticmethod
    def ema(
        series: pd.Series,
        window: int,
    ) -> Optional[float]:
        if series is None or series.empty or len(series) < window:
            return None
        s = pd.to_numeric(series, errors="coerce")
        values = (
            EMAIndicator(
                close=s,
                window=window,
                fillna=False,
            )
            .ema_indicator()
            .dropna()
        )
        return float(values.iloc[-1]) if not values.empty else None

    @staticmethod
    def rsi(
        series: pd.Series,
        window: int,
    ) -> Optional[float]:
        if series is None or series.empty or len(series) < window + 1:
            return None
        s = pd.to_numeric(series, errors="coerce")
        values = (
            RSIIndicator(
                close=s,
                window=window,
                fillna=False,
            )
            .rsi()
            .dropna()
        )
        return float(values.iloc[-1]) if not values.empty else None

    @staticmethod
    def atr(
        df: pd.DataFrame,
        window: int = 14,
    ) -> Optional[float]:
        required = {"h", "l", "c"}
        if df is None or df.empty or not required.issubset(df.columns):
            return None
        h = pd.to_numeric(df["h"], errors="coerce")
        l = pd.to_numeric(df["l"], errors="coerce")
        c = pd.to_numeric(df["c"], errors="coerce")
        if min(len(h), len(l), len(c)) < window + 1:
            return None
        atr_ind = AverageTrueRange(
            high=h,
            low=l,
            close=c,
            window=window,
            fillna=False,
        )
        values = atr_ind.average_true_range().dropna()
        return float(values.iloc[-1]) if not values.empty else None
