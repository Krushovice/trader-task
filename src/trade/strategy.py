from collections import deque
import pandas as pd
from core.config import settings


class StrategyState:
    def __init__(self):
        self.breakout_ts = None
        self.retested = False
        self.prices = deque(maxlen=settings.ws.max_bars_wait + 1)
        self.retest_pct = settings.ws.retest_pct

    def on_new_bar(
        self,
        kline: dict,
        df_5: pd.DataFrame,
        df_1h: pd.DataFrame,
        df_1d: pd.DataFrame,
    ) -> tuple[bool, bool]:
        price = float(kline["close"])
        self.prices.append(price)

        # индикаторы
        ema1h = df_1h["ema60"].iat[-1]
        rsi1d = df_1d["rsi"].iat[-1]
        price5 = df_5["c"].iat[-1]
        ema60_5 = df_5["ema60_5"].iat[-1]
        ema163_5 = df_5["ema163_5"].iat[-1]

        # пробой
        if len(self.prices) > 1:
            prev = self.prices[-2]
            if prev <= ema1h < price:
                self.breakout_ts = kline["start_at"]
                self.retested = False

        # тайм-аут ретеста
        if self.breakout_ts and len(self.prices) - 1 > settings.ws.max_bars_wait:
            self.breakout_ts = None
            self.retested = False

        # ретест
        if self.breakout_ts and not self.retested:
            if abs(price - ema1h) / ema1h <= self.retest_pct:
                self.retested = True

        # условия
        bounced = self.retested and abs(price - ema1h) / ema1h <= self.retest_pct
        mtf_long = price5 > ema60_5 and price5 > ema163_5
        mtf_short = price5 < ema60_5 and price5 < ema163_5
        rsi_long_ok = rsi1d <= 45
        rsi_short_ok = rsi1d >= 55

        long_bounce = price > ema1h and price <= ema1h * 1.007
        short_bounce = price < ema1h and price >= ema1h * 0.993

        long_signal = bounced and long_bounce and mtf_long and rsi_long_ok
        short_signal = bounced and short_bounce and mtf_short and rsi_short_ok
        return long_signal, short_signal
