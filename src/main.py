import asyncio
import pandas as pd
import ccxt.async_support as ccxt

from core.config import settings
from trade import (
    DataWS,
    Indicators,
    StrategyState,
    Executor,
)


# Инициализация стратегии и исполнителя
state = StrategyState()
executor = Executor()

symbol = settings.ws.symbol


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
    """
    При каждом подтверждённом 5m-баре:
      1. Собираем последний бар в df_5 и считаем EMA60/EMA163.
      2. Загружаем H1 и D данные для EMA60 и RSI.
      3. Получаем сигналы.
      4. Выставляем лимитный ордер под текущую цену.
    """
    # 1) 5m бар
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

    # 2) H1 и D
    df_1h = await fetch_df(
        symbol,
        "1h",
        limit=200,
    )
    df_1d = await fetch_df(
        symbol,
        "1d",
        limit=50,
    )

    # 3) Сигналы
    long_signal, short_signal = state.on_new_bar(
        kline,
        df_5,
        df_1h,
        df_1d,
    )

    # 4) Ордер
    balance = (await executor.exchange.fetch_balance())["total"]["USDT"]
    price = float(kline["close"])
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


async def main():
    # Запускаем WebSocket-клиент
    ws = DataWS(handle_kline)
    await ws.start()
    await executor.close()


if __name__ == "__main__":
    asyncio.run(main())
