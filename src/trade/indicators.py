import pandas as pd
from ta.trend import ema_indicator
from ta.momentum import rsi
from ta.volatility import AverageTrueRange


class Indicators:
    @staticmethod
    def ema(series: pd.Series, window: int) -> float:
        return ema_indicator(
            series,
            window=window,
        ).iloc[-1]

    @staticmethod
    def rsi(series: pd.Series, window: int) -> float:
        return rsi(
            series,
            window=window,
        ).iloc[-1]

    @staticmethod
    def atr(df: pd.DataFrame, window: int = 14) -> float:
        atr = AverageTrueRange(
            high=df["h"],
            low=df["l"],
            close=df["c"],
            window=window,
        )
        return atr.average_true_range().iloc[-1]
