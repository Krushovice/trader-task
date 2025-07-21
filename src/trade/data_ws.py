import asyncio
import json

from aiohttp import ClientSession, WSMsgType

from core.config import settings


class DataWS:
    def __init__(self, handler):
        # Подписка на 5m kline для SYMBOL
        self.url = settings.ws.url
        self.topic = f"kline.5.{settings.ws.symbol}"
        self.handler = handler

    async def start(self):
        # Бесконечный цикл переподключений
        while True:
            try:
                async with ClientSession() as session:
                    async with session.ws_connect(self.url) as ws:
                        # отправляем подписку
                        await ws.send_json({"topic": self.topic, "event": "sub"})
                        # слушаем сообщения
                        async for msg in ws:
                            if msg.type == WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if data.get("topic") == self.topic and data.get("data"):
                                    k = data["data"]["kline"]
                                    if k.get("is_confirmed"):  # закрытый бар
                                        await self.handler(k)
                            elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                                break
            except Exception as e:
                print(f"WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)
