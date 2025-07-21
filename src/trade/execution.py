import ccxt.async_support as ccxt
from core.config import settings


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

    async def order(
        self,
        action: str,
        price: float,
        balance: float,
    ):
        symbol = settings.ws.symbol
        raw_qty = balance * 0.4 / price
        markets = await self.exchange.fetch_markets()
        market = next(m for m in markets if m["symbol"] == symbol)
        step = float(market["info"]["lotSizeFilter"]["qtyStep"])
        qty = (raw_qty // step) * step
        side = "buy" if action == "long" else "sell"
        # Выставляем лимитный ордер по цене текущего закрытия
        await self.exchange.create_order(
            symbol,
            "limit",
            side,
            qty,
            price,
            {"timeInForce": "PostOnly"},
        )

    async def close(self):
        await self.exchange.close()
