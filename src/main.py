import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import ccxt.async_support as ccxt
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator

from core.config import settings
from trade.data_ws import DataWS
from trade.indicators import Indicators
from trade.strategy import StrategyState
from trade.execution import Executor
from trade.buffer import BarBuffer
from trade.utils import normalize_kline, aggregate_ohlcv


LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger("main")


class TradingApp:
    def __init__(self) -> None:
        self.symbol: str = settings.ws.symbol
        self.mode: str = settings.ws.mode
        self.base_timeframe: str = settings.ws.timeframe  # ожидаем "5m"

        # Приватный клиент для торговли (переключается testnet/prod внутри Executor)
        self.executor = Executor()

        # Публичный REST только для REPLAY (история 5m) — всегда prod, чтобы была история
        self.public_rest = ccxt.bybit(
            {
                "enableRateLimit": True,
                "urls": {
                    "api": {
                        "public": "https://api.bybit.com",
                        "private": "https://api.bybit.com",
                    }
                },
            }
        )

        # Состояние/буферы
        self.state = StrategyState()
        self.base_tf_buffer = BarBuffer(maxlen=3000)  # хватит на прогрев EMA163

        self.ws_client: Optional[DataWS] = None

    @staticmethod
    def latest_num(df: Optional[pd.DataFrame], col: str) -> Optional[float]:
        if df is None or df.empty or col not in df.columns:
            return None
        s = pd.to_numeric(df[col], errors="coerce").dropna()
        return float(s.iloc[-1]) if not s.empty else None

    async def fetch_df(self, timeframe: str, limit: int) -> pd.DataFrame:
        """История для REPLAY (prod public REST), колонки: ts,o,h,l,c,v."""
        ohlcv = await self.public_rest.fetch_ohlcv(
            self.symbol.upper(), timeframe, limit=limit
        )
        return pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])

    async def handle_kline(self, raw_kline: dict) -> None:
        kline = normalize_kline(raw_kline)
        bar_time = datetime.fromtimestamp(kline["start_at"] / 1000, timezone.utc)
        price = float(kline["close"])

        # 1) обновляем буфер базового ТФ и считаем EMA60/163 на истории
        self.base_tf_buffer.add(kline)
        df_base = self.base_tf_buffer.to_df()
        if df_base.empty:
            return

        ema60_5 = Indicators.ema(df_base["c"], window=60)
        ema163_5 = Indicators.ema(df_base["c"], window=163)
        if ema60_5 is None or ema163_5 is None:
            logger.debug("Warmup EMA in progress; skip bar")
            return

        # 2) HTF: агрегируем из 5m (универсально для live/replay)
        df_1h = aggregate_ohlcv(df_base, "1H")
        df_1d = aggregate_ohlcv(df_base, "1D")

        # индикаторы HTF
        if df_1h is None or df_1h.empty or len(df_1h) < 60:
            ema1h = None
        else:
            _ema = EMAIndicator(
                close=df_1h["c"],
                window=60,
                fillna=False,
            ).ema_indicator()
            _ema = _ema.dropna()
            ema1h = float(_ema.iloc[-1]) if not _ema.empty else None

        # RSI14@1d
        if df_1d is None or df_1d.empty or len(df_1d) < 14:
            rsi1d = None
        else:
            _rsi = RSIIndicator(
                close=df_1d["c"],
                window=14,
                fillna=False,
            ).rsi()
            _rsi = _rsi.dropna()
            rsi1d = float(_rsi.iloc[-1]) if not _rsi.empty else None

        if ema1h is None or rsi1d is None:
            logger.debug("Warmup HTF (agg) in progress; skip bar")
            return

        # ATR@1h
        atr_1h = Indicators.atr(df_1h)
        if atr_1h is not None and atr_1h <= 1e-9:
            atr_1h = None
        min_atr = settings.ws.min_atr_1h
        if min_atr is not None and (atr_1h is None or atr_1h < min_atr):
            logger.info(
                "[SKIP] ATR too low (%s < %s) — skipping trade", atr_1h, min_atr
            )
            return

        # 3) StrategyState — компактные df
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

        # 4) Торговые действия / баланс — только в LIVE (никаких приватных вызовов в REPLAY)
        if self.mode == "live":
            bal = await self.executor.exchange.fetch_balance()
            usdt_total = (bal.get("total") or {}).get("USDT")
            if usdt_total is None:
                logger.warning("No USDT balance info, skip bar")
                return
            balance = float(usdt_total)

            if self.executor.start_balance == 0.0:
                self.executor.start_balance = balance
                logger.info("[BALANCE] Start balance: %.4f USDT", balance)

            drawdown_limit = self.executor.start_balance * (
                1 - settings.ws.balance_drawdown_limit_pct
            )
            if balance < drawdown_limit:
                if not self.executor.is_stopped_due_to_drawdown:
                    logger.critical(
                        "[STOP] Balance drawdown! %.4f < %.4f", balance, drawdown_limit
                    )
                    self.executor.is_stopped_due_to_drawdown = True
                    lock_path = os.getenv("LOCK_PATH", "stopped_due_to_drawdown.lock")
                    with open(lock_path, "w") as f:
                        f.write(
                            f"Stopped at {datetime.now(timezone.utc).isoformat()}\n"
                        )
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
                    os.remove(os.getenv("LOCK_PATH", "stopped_due_to_drawdown.lock"))
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

            if long_signal:
                await self.executor.order("long", price, balance)
            if short_signal:
                await self.executor.order("short", price, balance)
            await self.executor.check_trailing_stops(price)
        else:
            # REPLAY: только логируем намерения, никаких приватных запросов
            if long_signal or short_signal:
                logger.info(
                    "[DRY-RUN] Would place %s at %.6f",
                    "long" if long_signal else "short",
                    price,
                )
            await self.executor.check_trailing_stops(price)

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
            await self.public_rest.close()
            logger.info("Live stopped")

    async def run_replay(self) -> None:
        logger.info(
            "Starting replay mode (TF=%s)",
            self.base_timeframe,
        )
        try:
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
        finally:
            # закрываем всегда, даже если в цикле было исключение
            await self.executor.close()
            await self.public_rest.close()
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
