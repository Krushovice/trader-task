import asyncio
import logging
import ccxt.async_support as ccxt
from datetime import datetime, timezone

from core.config import settings
from main import handle_kline  # Импортируй как есть!

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

# rest = ccxt.bybit(
#     {
#         "enableRateLimit": True,
#         "options": {"defaultType": "linear"},
#         "apiKey": settings.api.key,
#         "secret": settings.api.secret,
#         "urls": {
#             "api": {
#                 "public": "https://api-testnet.bybit.com",
#                 "private": "https://api-testnet.bybit.com",
#             }
#         },
#     }
# )
#
# SYMBOL = settings.ws.symbol
#
#
# async def polling_loop():
#     last_bar_time = None
#     while True:
#         try:
#             ohlcv = await rest.fetch_ohlcv(SYMBOL, "5m", limit=2)
#             bar = ohlcv[-1]  # Последняя закрытая 5m свеча
#             bar_time = bar[0]
#             if last_bar_time is not None and bar_time == last_bar_time:
#                 await asyncio.sleep(15)
#                 continue
#             last_bar_time = bar_time
#
#             kline = {
#                 "start_at": bar[0],
#                 "open": bar[1],
#                 "high": bar[2],
#                 "low": bar[3],
#                 "close": bar[4],
#                 "volume": bar[5],
#             }
#             logger.info(
#                 "New closed 5m bar: %s",
#                 datetime.fromtimestamp(bar[0] / 1000, timezone.utc),
#             )
#             await handle_kline(kline)
#         except Exception as e:
#             logger.error("Polling error: %s", e)
#             await asyncio.sleep(30)
#         await asyncio.sleep(15)  # Можно увеличить до 60-300 секунд

import ccxt

bybit = ccxt.bybit(
    {
        "apiKey": "ТОТ_ЖЕ_КЛЮЧ",
        "secret": "ТОТ_ЖЕ_СЕКРЕТ",
        "options": {"defaultType": "linear"},
        "urls": {
            "api": {
                "public": "https://api-testnet.bybit.com",
                "private": "https://api-testnet.bybit.com",
            }
        },
    }
)


if __name__ == "__main__":
    logger.info(f"API KEY (repr): {repr(settings.api.key)}")
    logger.info(f"API SECRET (repr): {repr(settings.api.secret)}")

    print(bybit.fetch_balance())
    # asyncio.run(polling_loop())
