import logging
from decimal import Decimal, ROUND_DOWN
from typing import Optional

import ccxt.async_support as ccxt
from core.config import settings
from trade.trailing import TrailingStopManager  # твой модуль с классом

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")

SYMBOL = settings.ws.symbol


def floor_to_step(quantity: float, step: float) -> float:
    if step <= 0:
        return quantity
    q = Decimal(str(quantity))
    s = Decimal(str(step))
    return float((q / s).to_integral_value(rounding=ROUND_DOWN) * s)


def round_to_tick(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    p = Decimal(str(price))
    t = Decimal(str(tick_size))
    return float((p / t).to_integral_value(rounding=ROUND_DOWN) * t)


class Executor:
    def __init__(self):
        api_url = (
            "https://api-testnet.bybit.com"
            if settings.api.testnet
            else "https://api.bybit.com"
        )
        self.exchange = ccxt.bybit(
            {
                "apiKey": settings.api.key,
                "secret": settings.api.secret,
                "enableRateLimit": True,
                "options": {"defaultType": "linear"},
                "urls": {"api": {"public": api_url, "private": api_url}},
            }
        )
        self.order_percent = getattr(settings.ws, "order_percent", 0.4)
        self.max_order_cost: Optional[float] = getattr(
            settings.ws, "max_order_cost_usdt", None
        )

        self.market = None
        self.step = 0.0
        self.tick_size = 0.0
        self.min_notional = 0.0

        self.consecutive_losses = 0
        self.max_consecutive_losses = 3
        self.entry_prices: dict[str, float] = {"long": 0.0, "short": 0.0}
        self.cooldown_bars = 0
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
            "Executor initialized for %s (%s)",
            SYMBOL,
            "TESTNET" if settings.api.testnet else "MAINNET",
        )

    async def _load_market(self):
        if self.market is None:
            markets = await self.exchange.fetch_markets()
            self.market = next(m for m in markets if m["symbol"] == SYMBOL)
            info = self.market.get("info", {})
            lot = info.get("lotSizeFilter", {})
            price_f = info.get("priceFilter", {})
            self.step = float(lot.get("qtyStep", lot.get("stepSize", 0)) or 0)
            self.tick_size = float(price_f.get("tickSize", 0) or 0)
            self.min_notional = float(
                lot.get("minNotional", lot.get("min_trading_qty", 0)) or 0
            )

    async def order(
        self,
        action: str,
        price: float,
        balance: float,
    ):
        await self._load_market()
        raw_qty = balance * self.order_percent / price
        qty = floor_to_step(raw_qty, self.step)

        if qty <= 0:
            logger.warning("[ENTRY FAILED] qty <= 0 after step floor")
            return None

        notional = qty * price
        if self.min_notional and notional < self.min_notional:
            logger.warning(
                "[ENTRY FAILED] notional=%.4f < min_notional=%.4f",
                notional,
                self.min_notional,
            )
            return None
        if self.max_order_cost is not None and notional > self.max_order_cost:
            logger.warning(
                "[ENTRY BLOCKED] notional %.4f > max_order_cost %.4f",
                notional,
                self.max_order_cost,
            )
            return None

        side = "Buy" if action == "long" else "Sell"
        limit_price = round_to_tick(price, self.tick_size)
        if side == "Buy":
            limit_price = max(
                self.tick_size or 0.0, limit_price - (self.tick_size or 0.0)
            )
        else:
            limit_price = limit_price + (self.tick_size or 0.0)

        logger.info(
            "[ENTRY] side=%s | qty=%.6f | price=%.6f | notional=%.4f",
            side,
            qty,
            limit_price,
            notional,
        )

        try:
            order = await self.exchange.create_order(
                SYMBOL,
                "limit",
                side,
                qty,
                limit_price,
                {"timeInForce": "PostOnly"},
            )
            logger.info("Order placed: %s", order)

            if action == "long":
                self.trailing_long.activate(limit_price)
            else:
                self.trailing_short.activate(limit_price)
            self.entry_prices[action] = limit_price

            return order
        except Exception as e:
            logger.error("Failed to place order: %s", e)
            return None

    async def check_trailing_stops(
        self,
        price: float,
    ):
        for manager, side in [
            (self.trailing_long, "long"),
            (self.trailing_short, "short"),
        ]:
            if manager.active:
                manager.update_price(price)
                exit_reason = manager.should_exit(price)
                if exit_reason:
                    logger.warning(
                        "[EXIT] Trailing stop | side=%s | price=%.6f | reason=%s",
                        side,
                        price,
                        exit_reason,
                    )
                    await self.close_position(side, price)
                    manager.clear()

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
                                self.cooldown_bars = 10
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
            pos = next(
                (
                    p
                    for p in positions
                    if p.get("symbol") == SYMBOL and p.get("side") == side
                ),
                None,
            )
            if not pos:
                logger.info(
                    "No open '%s' position to close",
                    side,
                )
                return
            qty = abs(float(pos.get("contracts", 0)))
            if qty == 0:
                logger.info(
                    "No contracts to close for %s",
                    side,
                )
                return

            close_side = "Sell" if side == "long" else "Buy"
            logger.info(
                "Closing %s position: qty=%.6f at market (reduceOnly)",
                side,
                qty,
            )

            await self.exchange.create_market_order(
                SYMBOL,
                close_side,
                qty,
                params={"reduceOnly": True},
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
