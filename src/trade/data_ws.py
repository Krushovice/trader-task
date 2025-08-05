import asyncio
import json
import logging

from aiohttp import ClientSession, WSMsgType

from core.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger(__name__)


class DataWS:
    def __init__(self, handler):
        self.url = settings.ws.url
        self.topic = f"kline.5.{settings.ws.symbol.upper()}"
        self.handler = handler
        self.reconnect_delay = settings.ws.reconnect_delay
        self.session = None
        self._running = False

    async def start(self):
        self._running = True
        # создаём единую сессию
        self.session = ClientSession()
        while self._running:
            try:
                logger.info("Connecting to WS %s …", self.url)
                async with self.session.ws_connect(
                    self.url, heartbeat=30, timeout=60
                ) as ws:
                    # подписка
                    await ws.send_json({"topic": self.topic, "event": "sub"})
                    logger.info("Subscribed to topic %s", self.topic)
                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            # можно проверить: if data.get('event') == 'subscribed'
                            if data.get("topic") == self.topic and data.get("data"):
                                k = data["data"]["kline"]
                                if k.get("is_confirmed"):
                                    await self.handler(k)
                        elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                            logger.warning("WS closed/error, reconnecting")
                            break
            except Exception as e:
                logger.error("WS error: %s", e)
            if self._running:
                logger.info("Reconnecting in %ds …", self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)

    async def stop(self):
        self._running = False
        # закрыть WS‑коннекшн и сессию
        if self.session:
            await self.session.close()
            logger.info("ClientSession closed")
