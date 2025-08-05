import asyncio
import logging
import os

import pandas as pd
import ccxt.async_support as ccxt
from datetime import datetime, timezone

from core.config import settings
from trade.data_ws import DataWS
from trade.indicators import Indicators
from trade.strategy import StrategyState
from trade.execution import Executor

# Логирование
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

# Инициализация
state = StrategyState()
executor = Executor()
rest = ccxt.bybit({"enableRateLimit": True})

SYMBOL = settings.ws.symbol
MODE = settings.ws.mode
RETEST_PCT = settings.ws.retest_pct


async def fetch_df(
    symbol: str,
    timeframe: str,
    limit: int,
):
    ohlcv = await rest.fetch_ohlcv(
        symbol.upper(),
        timeframe,
        limit=limit,
    )
    df = pd.DataFrame(ohlcv, columns=["ts", "o", "h", "l", "c", "v"])
    if timeframe == "1h":
        df["ema60"] = Indicators.ema(
            df["c"],
            window=60,
        )
    if timeframe == "1d":
        df["rsi"] = Indicators.rsi(
            df["c"],
            window=14,
        )
    return df


async def handle_kline(kline):
    # Дата бара
    bar_time = datetime.fromtimestamp(
        kline["start_at"] / 1000,
        timezone.utc,
    )
    logger.debug("5m bar at %s", bar_time.isoformat())

    # Собираем df_5
    df_5 = pd.DataFrame(
        [
            [
                kline["start_at"],
                kline["open"],
                kline["high"],
                kline["low"],
                kline["close"],
                kline["volume"],
            ]
        ],
        columns=["ts", "o", "h", "l", "c", "v"],
    )
    df_5["ema60_5"] = Indicators.ema(df_5["c"], window=60)
    df_5["ema163_5"] = Indicators.ema(df_5["c"], window=163)

    # Старшие TF
    df_1h = await fetch_df(SYMBOL, "1h", limit=200)
    df_1d = await fetch_df(SYMBOL, "1d", limit=50)

    # Промежуточные значения
    price = float(kline["close"])
    ema1h = df_1h["ema60"].iat[-1]
    rsi1d = df_1d["rsi"].iat[-1]
    price5 = df_5["c"].iat[-1]
    ema60_5 = df_5["ema60_5"].iat[-1]
    ema163_5 = df_5["ema163_5"].iat[-1]

    logger.debug(
        "State: breakout_ts=%s, retested=%s",
        state.breakout_ts,
        state.retested,
    )
    logger.debug(
        "Values: price=%.6f, ema1h=%.6f, rsi1d=%.2f",
        price,
        ema1h,
        rsi1d,
    )

    # ATR фильтр (если задан)
    atr_1h = Indicators.atr(df_1h)
    if settings.ws.min_atr_1h and atr_1h < settings.ws.min_atr_1h:
        logger.warning(
            "[SKIP] ATR too low (%.4f < %.4f) — skipping trade",
            atr_1h,
            settings.ws.min_atr_1h,
        )
        return

    # Сигналы
    long_signal, short_signal = state.on_new_bar(
        kline,
        df_5,
        df_1h,
        df_1d,
    )

    logger.info(
        "[SIGNAL] Long=%s | Short=%s | price=%.4f | ema1h=%.4f | rsi=%.2f",
        long_signal,
        short_signal,
        price,
        ema1h,
        rsi1d,
    )

    # Баланс
    balance = (await executor.exchange.fetch_balance())["total"]["USDT"]
    logger.debug(
        "Balance: %.6f USDT, price: %.6f",
        balance,
        price,
    )

    # Инициализация стартового баланса
    if executor.start_balance == 0.0:
        executor.start_balance = balance
        logger.info(
            "[BALANCE] Start balance: %.4f USDT",
            balance,
        )

    # Проверка на просадку
    drawdown_limit = executor.start_balance * (
        1 - settings.ws.balance_drawdown_limit_pct
    )
    if balance < drawdown_limit:
        if not executor.is_stopped_due_to_drawdown:
            logger.critical(
                "[STOP] Balance drawdown triggered! Balance: %.4f < Limit: %.4f",
                balance,
                drawdown_limit,
            )
            executor.is_stopped_due_to_drawdown = True
            with open("stopped_due_to_drawdown.lock", "w") as f:
                f.write(f"Stopped at {datetime.now().isoformat()} UTC\n")
                f.write(f"Balance: {balance:.4f} USDT\n")
                f.write(f"Drawdown limit: {drawdown_limit:.4f} USDT\n")
        return

    # Восстановление торговли при росте баланса
    if executor.is_stopped_due_to_drawdown and balance >= executor.start_balance:
        logger.info("[RESUME] Balance recovered. Resuming trading.")
        executor.is_stopped_due_to_drawdown = False
        try:
            os.remove("stopped_due_to_drawdown.lock")
            logger.info("[FILE] Lock file removed")
        except FileNotFoundError:
            pass

    # Проверка на паузу после серии убытков
    if executor.cooldown_bars > 0:
        executor.cooldown_bars -= 1
        logger.info(
            "[PAUSE] Cooldown active (%d bars left)",
            executor.cooldown_bars,
        )
        return

    # Основная логика
    if MODE == "live":
        if long_signal:
            await executor.order(
                "long",
                price,
                balance,
            )
        if short_signal:
            await executor.order(
                "short",
                price,
                balance,
            )
        await executor.check_trailing_stops(price)
    else:
        if long_signal or short_signal:
            logger.info(
                "[DRY-RUN] Would place %s at %.6f",
                "long" if long_signal else "short",
                price,
            )
        await executor.check_trailing_stops(price)


async def run_live():
    ws_client = DataWS(handle_kline)
    task = asyncio.create_task(ws_client.start())
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        await ws_client.stop()
        await executor.close()
        await rest.close()
        logger.info("Live stopped")


async def run_replay():
    logger.info("Starting replay mode")
    df5 = await fetch_df(
        SYMBOL,
        "5m",
        limit=500,
    )
    df1h = await fetch_df(
        SYMBOL,
        "1h",
        limit=200,
    )
    df1d = await fetch_df(
        SYMBOL,
        "1d",
        limit=50,
    )
    for _, row in df5.iterrows():
        k = {
            "start_at": row["ts"],
            "open": row["o"],
            "high": row["h"],
            "low": row["l"],
            "close": row["c"],
            "volume": row["v"],
        }
        await handle_kline(k)
    await executor.close()
    await rest.close()
    logger.info("Replay finished")


async def main():
    logger.info("Bot started in %s mode", MODE)
    if MODE == "live":
        await run_live()
    else:
        await run_replay()


if __name__ == "__main__":
    asyncio.run(main())
