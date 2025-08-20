import asyncio
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Mapping, Iterator
from aiohttp import ClientSession, WSMsgType, ClientError

from core.config import settings

logger = logging.getLogger(__name__)


class DataWS:
    def __init__(self, handler):
        self.url: str = settings.ws.url
        self.symbol: str = settings.ws.symbol
        self.timeframe: str = settings.ws.timeframe  # "1m","5m","1h"
        self.topic: str = self._make_topic(
            self.url,
            self.timeframe,
            self.symbol,
        )
        self.handler = handler
        self.reconnect_delay: int = settings.ws.reconnect_delay
        self._session: Optional[ClientSession] = None
        self._running: bool = False

    @staticmethod
    def _make_topic(
        url: str,
        timeframe: str,
        symbol: str,
    ) -> str:
        if "/v5/" in url:
            tf_map = {
                "1m": "1",
                "3m": "3",
                "5m": "5",
                "15m": "15",
                "1h": "60",
            }
            interval = tf_map.get(timeframe)
            if not interval:
                raise ValueError(f"Unsupported timeframe for v5: {timeframe}")
            return f"kline.{interval}.{symbol}"
        return f"klineV2.5.{symbol}"

    async def start(self):
        if self._running:
            return
        self._running = True
        self._session = ClientSession()

        while self._running:
            try:
                logger.info(
                    "Connecting to WS %s …",
                    self.url,
                )
                async with self._session.ws_connect(
                    self.url,
                    heartbeat=30,
                    timeout=60,
                ) as ws:
                    await ws.send_json(
                        {"op": "subscribe", "args": [self.topic]},
                    )
                    logger.info("Subscribed to topic %s", self.topic)

                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == WSMsgType.TEXT:
                            try:
                                message = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue
                            if message.get("op") in {"subscribe", "pong"}:
                                continue
                            if message.get("topic") != self.topic:
                                continue
                            payload = message.get("data")
                            if payload is None:
                                continue
                            for candle in self._iter_confirmed_candles(payload):
                                # приводим к start_at/open/high/low/close/volume
                                await self.handler(candle)

                        elif msg.type in (
                            WSMsgType.CLOSED,
                            WSMsgType.ERROR,
                        ):
                            logger.warning("WS closed/error, reconnecting")
                            break

            except asyncio.CancelledError:
                logger.info("WS task cancelled; shutting down")
                break
            except ClientError as err:
                logger.warning("WS client error: %s", err)
            except Exception as err:
                logger.exception("WS unexpected error: %s", err)

            if self._running:
                logger.info("Reconnecting in %ds …", self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)

        await self._close_session()

    async def stop(self):
        self._running = False
        await self._close_session()
        logger.info("WS stopped")

    async def _close_session(self):
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = None

    @staticmethod
    def _iter_confirmed_candles(
        payload: Any,
    ) -> Iterator[Dict[str, float]]:
        if isinstance(payload, Mapping):
            items: Sequence[Mapping[str, Any]] = [payload]
        elif isinstance(payload, Sequence):
            items = payload  # type: ignore[assignment]
        else:
            return  # корректно для генератора

        for raw in items:
            if not isinstance(raw, Mapping):
                continue
            if not (raw.get("is_confirmed") or raw.get("confirm")):
                continue
            try:
                yield {
                    "start_at": int(raw["start"]),
                    "open": float(raw["open"]),
                    "high": float(raw["high"]),
                    "low": float(raw["low"]),
                    "close": float(raw["close"]),
                    "volume": float(raw.get("volume", 0.0)),
                }
            except (KeyError, TypeError, ValueError):
                continue
