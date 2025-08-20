import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import ccxt.async_support as ccxt

from core.config import settings
from trade.data_ws import DataWS
from trade.indicators import Indicators
from trade.strategy import StrategyState
from trade.execution import Executor
from trade.buffer import BarBuffer
from trade.htf_cache import HTFCache
from trade.utils import normalize_kline


# ---- logging ----
LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("main")


class TradingApp:
    def __init__(self) -> None:
        # config
        self.symbol: str = settings.ws.symbol
        self.mode: str = settings.ws.mode
        self.base_timeframe: str = settings.ws.timeframe

        # REST client (public/private)
        public_api_url = "https://api.bybit.com"
        self.rest = ccxt.bybit(
            {
                "enableRateLimit": True,
                "urls": {
                    "api": {
                        "public": public_api_url,
                        "private": public_api_url,
                    },
                },
            }
        )

        # executor (ccxt + trailing stops)
        self.executor = Executor()

        # state & buffers
        self.state = StrategyState()
        self.base_tf_buffer = BarBuffer(maxlen=2000)  # enough for EMA163 warmup
        self.htf_cache = HTFCache(
            symbol=self.symbol,
            rest=self.rest,
        )

        # ws (created in run_live)
        self.ws_client: Optional[DataWS] = None

    # -------- helpers --------

    @staticmethod
    def latest_num(
        df: Optional[pd.DataFrame],
        col: str,
    ) -> Optional[float]:
        if df is None or df.empty or col not in df.columns:
            return None
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        return float(s.iloc[-1]) if not s.empty else None

    async def fetch_df(
        self,
        timeframe: str,
        limit: int,
    ) -> pd.DataFrame:
        """Generic OHLCV loader for replay and warmups."""
        ohlcv = await self.rest.fetch_ohlcv(
            self.symbol.upper(),
            timeframe,
            limit=limit,
        )
        df = pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])
        if timeframe == "1h":
            # full column for caching/ATR/EMA access
            from ta.trend import EMAIndicator

            df["ema60"] = EMAIndicator(
                close=df["c"],
                window=60,
                fillna=False,
            ).ema_indicator()
        elif timeframe == "1d":
            from ta.momentum import RSIIndicator

            df["rsi"] = RSIIndicator(
                close=df["c"],
                window=14,
                fillna=False,
            ).rsi()
        return df

    # -------- core handlers --------

    async def handle_kline(self, raw_kline: dict) -> None:
        """Main per-bar handler for both live (WS) and replay."""
        kline = normalize_kline(raw_kline)
        bar_time = datetime.fromtimestamp(kline["start_at"] / 1000, timezone.utc)
        price = float(kline["close"])

        # 1) update base timeframe buffer and compute EMAs on real history (not a single bar)
        self.base_tf_buffer.add(kline)
        df_base = self.base_tf_buffer.to_df()
        if df_base.empty:
            return

        ema60_5 = Indicators.ema(df_base["c"], window=60)
        ema163_5 = Indicators.ema(df_base["c"], window=163)
        if ema60_5 is None or ema163_5 is None:
            logger.debug("Warmup EMA in progress; skip bar")
            return

        # 2) hourly/daily caches (no globals; updated on boundaries)
        df_1h, df_1d = await self.htf_cache.get(bar_time)
        ema1h = self.latest_num(df_1h, "ema60")
        rsi1d = self.latest_num(df_1d, "rsi")
        if ema1h is None or rsi1d is None:
            logger.debug("Warmup HTF in progress; skip bar")
            return

        # 3) ATR filter (optional)
        atr_1h = Indicators.atr(df_1h)
        if atr_1h is not None and atr_1h <= 1e-9:
            atr_1h = None  # считаем как «нет ATR»
        min_atr = settings.ws.min_atr_1h
        if min_atr is not None and (atr_1h is None or atr_1h < min_atr):
            logger.info(
                "[SKIP] ATR too low (%s < %s) — skipping trade",
                atr_1h,
                min_atr,
            )
            return

        # 4) feed StrategyState (expects compact dfs)
        df_for_strategy_5 = pd.DataFrame(
            [
                {
                    "c": float(df_base["c"].iat[-1]),
                    "ema60_5": ema60_5,
                    "ema163_5": ema163_5,
                }
            ]
        )
        df_for_strategy_1h = pd.DataFrame([{"ema60": ema1h}])
        df_for_strategy_1d = pd.DataFrame([{"rsi": rsi1d}])

        long_signal, short_signal = self.state.on_new_bar(
            {"start_at": kline["start_at"], "close": kline["close"]},
            df_for_strategy_5,
            df_for_strategy_1h,
            df_for_strategy_1d,
        )

        logger.info(
            "[SIGNAL] Long=%s | Short=%s | price=%.6f | ema1h=%.6f | rsi=%.2f",
            long_signal,
            short_signal,
            price,
            ema1h,
            rsi1d,
        )

        # 5) balance & drawdown control
        bal = await self.executor.exchange.fetch_balance()
        usdt_total = (bal.get("total") or {}).get("USDT")
        if usdt_total is None:
            logger.warning("No USDT balance info, skip bar")
            return
        balance = float(usdt_total)

        if self.executor.start_balance == 0.0:
            self.executor.start_balance = balance
            logger.info(
                "[BALANCE] Start balance: %.4f USDT",
                balance,
            )

        drawdown_limit = self.executor.start_balance * (
            1 - settings.ws.balance_drawdown_limit_pct
        )
        if balance < drawdown_limit:
            if not self.executor.is_stopped_due_to_drawdown:
                logger.critical(
                    "[STOP] Balance drawdown triggered! Balance: %.4f < Limit: %.4f",
                    balance,
                    drawdown_limit,
                )
                self.executor.is_stopped_due_to_drawdown = True
                with open("stopped_due_to_drawdown.lock", "w") as f:
                    f.write(f"Stopped at {datetime.now(timezone.utc).isoformat()}\n")
                    f.write(f"Balance: {balance:.4f} USDT\n")
                    f.write(f"Drawdown limit: {drawdown_limit:.4f} USDT\n")
            return

        if (
            self.executor.is_stopped_due_to_drawdown
            and balance >= self.executor.start_balance
        ):
            logger.info("[RESUME] Balance recovered. Resuming trading.")
            self.executor.is_stopped_due_to_drawdown = False
            try:
                os.remove("stopped_due_to_drawdown.lock")
                logger.info("[FILE] Lock file removed")
            except FileNotFoundError:
                pass

        if self.executor.cooldown_bars > 0:
            self.executor.cooldown_bars -= 1
            logger.info(
                "[PAUSE] Cooldown active (%d bars left)",
                self.executor.cooldown_bars,
            )
            return

        # 6) trade actions
        if self.mode == "live":
            if long_signal:
                await self.executor.order(
                    "long",
                    price,
                    balance,
                )
            if short_signal:
                await self.executor.order(
                    "short",
                    price,
                    balance,
                )
            await self.executor.check_trailing_stops(price)
        else:
            if long_signal or short_signal:
                logger.info(
                    "[DRY-RUN] Would place %s at %.6f",
                    "long" if long_signal else "short",
                    price,
                )
            await self.executor.check_trailing_stops(price)

    # -------- run modes --------

    async def run_live(self) -> None:
        self.ws_client = DataWS(self.handle_kline)
        task = asyncio.create_task(self.ws_client.start())
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            if self.ws_client:
                await self.ws_client.stop()
            await self.executor.close()
            await self.rest.close()
            logger.info("Live stopped")

    async def run_replay(self) -> None:
        logger.info(
            "Starting replay mode (TF=%s)",
            self.base_timeframe,
        )
        df_base = await self.fetch_df(
            self.base_timeframe,
            limit=1000,
        )
        for _, row in df_base.iterrows():
            k = {
                "ts": row["ts"],
                "o": row["o"],
                "h": row["h"],
                "l": row["l"],
                "c": row["c"],
                "v": row["v"],
            }
            await self.handle_kline(k)
        await self.executor.close()
        await self.rest.close()
        logger.info("Replay finished")

    async def run(self) -> None:
        logger.info(
            "Bot started in %s mode (%s) for %s",
            self.mode,
            "TESTNET" if settings.api.testnet else "MAINNET",
            self.symbol,
        )
        if self.mode == "live":
            await self.run_live()
        else:
            await self.run_replay()


if __name__ == "__main__":
    asyncio.run(TradingApp().run())
