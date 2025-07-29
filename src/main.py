import asyncio
import logging
import os

import pandas as pd
import ccxt.async_support as ccxt

from core.config import settings
from trade import (
    DataWS,
    Indicators,
    StrategyState,
    Executor,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
)
logger = logging.getLogger(__name__)

# Инициализация стратегии и исполнителя
state = StrategyState()
executor = Executor()

SYMBOl = settings.ws.symbol
MODE = settings.ws.mode

# REST-клиент Bybit для H1 и D
rest = ccxt.bybit({"enableRateLimit": True})


async def fetch_df(
    symbol: str,
    timeframe: str,
    limit: int,
) -> pd.DataFrame:
    """
    Получаем исторические данные и рассчитываем индикаторы:
      - EMA60 для 1h
      - RSI14 для 1d
    """
    ohlcv = await rest.fetch_ohlcv(
        symbol.upper(),
        timeframe,
        limit=limit,
    )
    df = pd.DataFrame(
        ohlcv,
        columns=["ts", "o", "h", "l", "c", "v"],
    )
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


async def handle_kline(kline: dict):
    logger.info("Received new 5m bar at %s", kline["start_at"])
    # Формируем df_5 для последнего бара
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
    df_5["ema60_5"] = Indicators.ema(
        df_5["c"],
        window=60,
    )
    df_5["ema163_5"] = Indicators.ema(
        df_5["c"],
        window=163,
    )

    # Получаем старшие TF
    df_1h = await fetch_df(
        SYMBOl,
        "1h",
        limit=200,
    )
    df_1d = await fetch_df(
        SYMBOl,
        "1d",
        limit=50,
    )

    # Генерируем сигналы
    long_signal, short_signal = state.on_new_bar(
        kline,
        df_5,
        df_1h,
        df_1d,
    )
    logger.info(
        "Signals — Long: %s, Short: %s",
        long_signal,
        short_signal,
    )

    # Баланс и цена
    balance = (await executor.exchange.fetch_balance())["total"]["USDT"]
    price = float(kline["close"])
    logger.info(
        "Current balance: %f USDT, price: %f",
        balance,
        price,
    )

    # Исполнение ордера
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
    else:
        # replay mode: dry-run
        if long_signal or short_signal:
            logger.info(
                "[DRY-RUN] Would place %s order at %f",
                "long" if long_signal else "short",
                price,
            )


async def replay_loop():
    # история 5m, 1h, 1d
    logger.info("Starting replay mode for historical data")
    df5 = await fetch_df(
        SYMBOl,
        "5m",
        limit=1000,
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
        await asyncio.sleep(0)  # без задержки
    await executor.close()
    logger.info("Replay finished")


async def main():
    logger.info(
        "Starting Bybit LCUSDT.P trading bot in %s mode",
        MODE,
    )
    if MODE == "live":
        ws = DataWS(handle_kline)
        await ws.start()
        await executor.close()
        logger.info("Live mode stopped")
    else:
        await replay_loop()


if __name__ == "__main__":
    asyncio.run(main())
