import logging
from decimal import Decimal, ROUND_DOWN
from typing import Optional, Dict

import ccxt.async_support as ccxt

from core.config import settings
from trade.trailing import TrailingStopManager

logger = logging.getLogger(__name__)


# ---------- helpers ----------


def ws_to_ccxt_linear(symbol_ws: str) -> str:
    """
    Convert a WS symbol like 'LTCUSDT' to CCXT linear-perp symbol 'LTC/USDT:USDT'.
    (If позже понадобится USDC — поменяешь хвост на ':USDC').
    """
    s = symbol_ws.strip().upper()
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT:USDT"
    raise ValueError(f"Unsupported WS symbol for CCXT: {symbol_ws}")


def floor_to_step(quantity: float, step: float) -> float:
    """Floor quantity to the exchange step (qtyStep)."""
    if step <= 0:
        return float(quantity)
    q = Decimal(str(quantity))
    s = Decimal(str(step))
    return float((q / s).to_integral_value(rounding=ROUND_DOWN) * s)


def round_to_tick(price: float, tick_size: float) -> float:
    """Floor price to the exchange tick size."""
    if tick_size <= 0:
        return float(price)
    p = Decimal(str(price))
    t = Decimal(str(tick_size))
    return float((p / t).to_integral_value(rounding=ROUND_DOWN) * t)


# ---------- executor ----------


class Executor:
    def __init__(self) -> None:
        # Symbols for WS (data) and CCXT (trading)
        self.symbol_ws: str = settings.ws.symbol  # e.g. "LTCUSDT"
        self.symbol_cx: str = ws_to_ccxt_linear(self.symbol_ws)  # e.g. "LTC/USDT:USDT"

        # CCXT (private REST) — switches testnet/mainnet via API flag
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
                "options": {"defaultType": "linear"},  # linear USDT perps
                "urls": {"api": {"public": api_url, "private": api_url}},
            }
        )

        # Sizing & risk
        self.order_percent: float = getattr(settings.ws, "order_percent", 0.4)
        self.max_order_cost: Optional[float] = getattr(
            settings.ws, "max_order_cost_usdt", None
        )

        # Market filters (lazy-loaded)
        self.market: Optional[Dict] = None
        self.qty_step: float = 0.0
        self.tick_size: float = 0.0
        self.min_notional: float = 0.0

        # PnL / cooldown state
        self.consecutive_losses: int = 0
        self.max_consecutive_losses: int = 3
        self.entry_prices: Dict[str, float] = {"long": 0.0, "short": 0.0}
        self.cooldown_bars: int = 0
        self.start_balance: float = 0.0
        self.is_stopped_due_to_drawdown: bool = False

        # Trailing stops
        tp_pct = settings.ws.take_profit_pct or None
        self.trailing_long = TrailingStopManager(
            "long", settings.ws.trailing_pct, tp_pct
        )
        self.trailing_short = TrailingStopManager(
            "short", settings.ws.trailing_pct, tp_pct
        )

        logger.info(
            "Executor initialized for %s (TESTNET=%s)",
            self.symbol_ws,
            str(settings.api.testnet),
        )
        logger.info("Symbols: WS=%s, CCXT=%s", self.symbol_ws, self.symbol_cx)

    # ---------- market metadata ----------

    async def _load_market(self) -> None:
        if self.market is not None:
            return

        markets = await self.exchange.fetch_markets()
        m = next((m for m in markets if m.get("symbol") == self.symbol_cx), None)
        if not m:
            raise RuntimeError(f"Market not found for {self.symbol_cx}")

        self.market = m
        info = m.get("info", {}) or {}
        lot = info.get("lotSizeFilter", {}) or info.get("lot_size_filter", {}) or {}
        price_f = info.get("priceFilter", {}) or info.get("price_filter", {}) or {}

        # Quantity step & tick size
        self.qty_step = float(lot.get("qtyStep") or lot.get("stepSize") or 0)
        self.tick_size = float(price_f.get("tickSize") or 0)

        # Min notional (Bybit may expose under different keys; fallback to 0)
        self.min_notional = float(
            lot.get("minNotional")
            or lot.get("minOrderAmt")
            or lot.get("min_trading_qty")
            or 0
        )

    # ---------- orders ----------

    async def order(self, action: str, price: float, balance: float):
        """
        Place a PostOnly limit order sized as order_percent of balance / price.
        action: "long" -> Buy, "short" -> Sell
        """
        await self._load_market()

        # size
        raw_qty = (balance * self.order_percent) / max(price, 1e-12)
        qty = floor_to_step(raw_qty, self.qty_step)
        if qty <= 0:
            logger.warning(
                "[ENTRY FAILED] qty <= 0 after step floor (raw=%.12f, step=%s)",
                raw_qty,
                self.qty_step,
            )
            return None

        # notional checks
        notional = qty * price
        if self.min_notional and notional < self.min_notional:
            logger.warning(
                "[ENTRY FAILED] notional=%.6f < min_notional=%.6f",
                notional,
                self.min_notional,
            )
            return None
        if self.max_order_cost is not None and notional > self.max_order_cost:
            logger.warning(
                "[ENTRY BLOCKED] notional %.6f > max_order_cost %.6f",
                notional,
                self.max_order_cost,
            )
            return None

        side = "Buy" if action == "long" else "Sell"

        # price: round to tick & make passive (PostOnly)
        lp = round_to_tick(price, self.tick_size)
        if side == "Buy":
            limit_price = max(self.tick_size or 0.0, lp - (self.tick_size or 0.0))
        else:
            limit_price = lp + (self.tick_size or 0.0)

        logger.info(
            "[ENTRY] side=%s | qty=%.6f | price=%.6f | notional=%.6f",
            side,
            qty,
            limit_price,
            notional,
        )

        try:
            order = await self.exchange.create_order(
                self.symbol_cx,
                "limit",
                side,
                qty,
                limit_price,
                {"timeInForce": "PostOnly", "postOnly": True},
            )
            logger.info("Order placed: %s", order)

            # activate trailing based on entry limit price
            if action == "long":
                self.trailing_long.activate(limit_price)
            else:
                self.trailing_short.activate(limit_price)
            self.entry_prices[action] = limit_price

            return order
        except Exception as e:
            logger.error("Failed to place order: %s", e)
            return None

    # ---------- trailing / exits ----------

    async def check_trailing_stops(self, price: float) -> None:
        """Call each bar/tick to update trailing stops and exit if needed."""
        for manager, side in (
            (self.trailing_long, "long"),
            (self.trailing_short, "short"),
        ):
            if not manager.active:
                continue

            manager.update_price(price)
            exit_reason = manager.should_exit(price)
            if not exit_reason:
                continue

            logger.warning(
                "[EXIT] Trailing stop | side=%s | price=%.6f | reason=%s",
                side,
                price,
                exit_reason,
            )
            await self.close_position(side, price)
            manager.clear()

            entry_price = self.entry_prices.get(side) or 0.0
            if entry_price > 0:
                pnl = (price - entry_price) if side == "long" else (entry_price - price)
                if pnl > 0:
                    self.consecutive_losses = 0
                else:
                    self.consecutive_losses += 1
                    logger.warning(
                        "[LOSS] Consecutive losses: %d", self.consecutive_losses
                    )
                    if self.consecutive_losses >= self.max_consecutive_losses:
                        self.cooldown_bars = 10
                        logger.error(
                            "[PAUSE] Max losses reached. Skipping new trades for 10 bars."
                        )

    async def close_position(self, side: str, price: float) -> None:
        """Close an open position on the given side using a reduceOnly market order."""
        await self._load_market()
        try:
            positions = await self.exchange.fetch_positions([self.symbol_cx])
            pos = None

            # Prefer explicit side match if available
            for p in positions:
                if p.get("symbol") != self.symbol_cx:
                    continue
                contracts = abs(float(p.get("contracts") or 0))
                if contracts <= 0:
                    continue
                p_side = (p.get("side") or "").lower()  # 'long'/'short' or ''
                if p_side == side:
                    pos = p
                    break
                # Fallback: if side unknown, accept any non-zero position for the symbol
                if not p_side:
                    pos = p
                    break

            if not pos:
                logger.info("No open '%s' position to close", side)
                return

            qty = abs(float(pos.get("contracts") or 0))
            if qty <= 0:
                logger.info("No contracts to close for %s", side)
                return

            close_side = "Sell" if side == "long" else "Buy"
            logger.info(
                "Closing %s position: qty=%.6f at market (reduceOnly)", side, qty
            )

            await self.exchange.create_market_order(
                self.symbol_cx,
                close_side,
                qty,
                params={"reduceOnly": True},
            )
        except Exception as e:
            logger.error("Failed to close %s position: %s", side, e)

    # ---------- teardown ----------

    async def close(self) -> None:
        try:
            await self.exchange.close()
        except Exception:
            pass
        logger.info("CCXT client closed")
