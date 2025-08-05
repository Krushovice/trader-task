import logging

import ccxt.async_support as ccxt

from core.config import settings
from .trailing import TrailingStopManager

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
                "options": {
                    "defaultType": "linear",
                    "urls": {
                        "api": {
                            "public": "https://api-testnet.bybit.com",
                            "private": "https://api-testnet.bybit.com",
                        }
                    },
                },
            }
        )
        self.order_percent = getattr(
            settings.ws,
            "order_percent",
            0.4,
        )
        self.market = None
        self.step = None
        self.min_notional = None
        self.consecutive_losses = 0
        self.max_consecutive_losses = 3
        self.entry_prices: dict[str, float] = {"long": 0.0, "short": 0.0}
        self.cooldown_bars = 0  # пауза в барах после потерь
        self.start_balance: float = 0.0
        self.is_stopped_due_to_drawdown = False
        tp_pct = settings.ws.take_profit_pct or None

        self.trailing_long = TrailingStopManager(
            "long",
            settings.ws.trailing_pct,
            tp_pct,
        )
        self.trailing_short = TrailingStopManager(
            "short",
            settings.ws.trailing_pct,
            tp_pct,
        )

        logger.info(
            "Executor initialized for symbol %s",
            SYMBOL,
        )

    async def _load_market(self):
        if self.market is None:
            markets = await self.exchange.fetch_markets()
            self.market = next(m for m in markets if m["symbol"] == SYMBOL)
            f = self.market["info"]["lotSizeFilter"]
            self.step = float(f["qtyStep"])
            self.min_notional = float(f.get("minNotional", f.get("min_trading_qty", 0)))

    async def order(
        self,
        action: str,
        price: float,
        balance: float,
    ):
        await self._load_market()
        raw_qty = balance * self.order_percent / price
        qty = (raw_qty // self.step) * self.step
        notional = qty * price
        side = "Buy" if action == "long" else "Sell"

        if notional < self.min_notional:
            logger.warning(
                "[ENTRY FAILED] side=%s | notional=%.4f < min_notional=%.4f",
                side,
                notional,
                self.min_notional,
            )
            return None

        logger.info(
            "[ENTRY] side=%s | qty=%.4f | price=%.4f | notional=%.4f",
            side,
            qty,
            price,
            notional,
        )

        try:
            order = await self.exchange.create_order(
                SYMBOL,
                "limit",
                side,
                qty,
                price,
                {"timeInForce": "PostOnly"},
            )
            logger.info(
                "Order placed: %s",
                order,
            )

            # активируем трейлинг
            if action == "long":
                self.trailing_long.activate(price)
            else:
                self.trailing_short.activate(price)
            self.entry_prices[action] = price

            return order
        except Exception as e:
            logger.error(
                "Failed to place order: %s",
                e,
            )
            return None

    async def check_trailing_stops(
        self,
        price: float,
    ):
        """Вызывается при каждом баре"""
        for manager, side in [
            (self.trailing_long, "long"),
            (self.trailing_short, "short"),
        ]:
            if manager.active:
                manager.update_price(price)
                exit_reason = manager.should_exit(price)
                if exit_reason:
                    logger.warning(
                        "[EXIT] Trailing stop triggered | side=%s | price=%.4f | reason=%s",
                        side,
                        price,
                        exit_reason,
                    )
                    await self.close_position(side, price)
                    manager.clear()
                    # анализируем PnL
                    entry_price = self.entry_prices.get(side)
                    if entry_price:
                        pnl = (
                            (price - entry_price)
                            if side == "long"
                            else (entry_price - price)
                        )
                        if pnl > 0:
                            self.consecutive_losses = 0
                        else:
                            self.consecutive_losses += 1
                            logger.warning(
                                "[LOSS] Consecutive losses: %d",
                                self.consecutive_losses,
                            )
                            if self.consecutive_losses >= self.max_consecutive_losses:
                                self.cooldown_bars = 10  # пауза на 10 баров
                                logger.error(
                                    "[PAUSE] Max losses reached. Skipping new trades for 10 bars."
                                )

    async def close_position(
        self,
        side: str,
        price: float,
    ):
        await self._load_market()
        try:
            positions = await self.exchange.fetch_positions([SYMBOL])
            position = next(p for p in positions if p["symbol"] == SYMBOL)
            qty = abs(float(position["contracts"]))
            if qty == 0:
                logger.info(
                    "No open position to close for %s",
                    side,
                )
                return

            close_side = "Sell" if side == "long" else "Buy"
            logger.info(
                "Closing %s position: qty=%.6f at market",
                side,
                qty,
            )

            await self.exchange.create_market_order(
                SYMBOL,
                close_side,
                qty,
            )
        except Exception as e:
            logger.error(
                "Failed to close %s position: %s",
                side,
                e,
            )

    async def close(self):
        await self.exchange.close()
        logger.info("CCXT client closed")
