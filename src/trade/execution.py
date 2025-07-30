import logging

import ccxt.async_support as ccxt

from core.config import settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

SYMBOL = settings.ws.symbol


class Executor:
    def __init__(self):
        self.exchange = ccxt.bybit(
            {
                "apiKey": settings.api.key,
                "secret": settings.api.secret,
                "enableRateLimit": True,
                "options": {"defaultType": "swap"},
            }
        )
        logger.info(
            "Executor initialized for symbol %s",
        )

    async def order(
        self,
        action: str,
        price: float,
        balance: float,
    ):
        raw_qty = balance * 0.4 / price
        markets = await self.exchange.fetch_markets()
        market = next(m for m in markets if m["symbol"] == SYMBOL)
        step = float(market["info"]["lotSizeFilter"]["qtyStep"])
        qty = (raw_qty // step) * step
        side = "Buy" if action == "long" else "Sell"

        logger.info(
            "Placing %s limit order: qty=%f at price=%f",
            side,
            qty,
            price,
        )
        order = await self.exchange.create_order(
            SYMBOL,
            "limit",
            side,
            qty,
            price,
            {"timeInForce": "PostOnly"},
        )
        logger.info("Order placed: %s", order)
        return order

    async def close(self):
        await self.exchange.close()
        logger.info("CCXT client closed")
