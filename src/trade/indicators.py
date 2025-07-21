import pandas as pd
from ta.trend import ema_indicator
from ta.momentum import rsi


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
