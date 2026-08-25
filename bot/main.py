# bot/main.py
"""Discord bot: pushes market-price-alerts price jumps to one channel.

Own consumer group (discord-bot-consumer) so it stays up when the web gateway is
down — and so Grafana shows its lag as a separate curve. Pure display consumer:
the median+MAD 判定 belongs to the Flink job, never to this process.

Run:          python -m bot.main
Self-check:   python -m bot.main --selfcheck   (no Discord token needed)
"""
import asyncio
import logging
import os
import sys
import time

from utils.alerts import PRICE_TOPIC, format_price_alert, parse_price_alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP  = "discord-bot-consumer"   # Grafana lag 面板靠這個名字認人
COOLDOWN_SECS   = int(os.environ.get("ALERT_COOLDOWN_SECS", "60"))


def should_push(last_push: dict, alert: dict, now: float) -> bool:
    """Per-symbol push throttle. Mutates last_push when it lets one through.

    冷卻是「推播節流」不是「訊號過濾」：被擋掉的告警仍然在 topic 與網站面板上。
    key 帶 source_type，避免不同來源的同名標的互相蓋掉冷卻。
    """
    key = (alert["source_type"], alert["symbol"])
    if now - last_push.get(key, float("-inf")) < COOLDOWN_SECS:
        return False
    last_push[key] = now
    return True


async def _pump(channel):
    from confluent_kafka import Consumer   # 放函式內，--selfcheck 才能不裝任何依賴就跑

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([PRICE_TOPIC])
    logger.info(f"[BOT] 🟢 consuming {PRICE_TOPIC} as {CONSUMER_GROUP} | 冷卻 {COOLDOWN_SECS}s")

    last_push: dict = {}
    pushed = skipped = 0
    try:
        while True:
            # confluent_kafka 的 poll 是阻塞的 → 丟到 thread，別卡住 discord 的心跳
            msg = await asyncio.to_thread(consumer.poll, 1.0)
            if msg is None:
                continue
            if msg.error():
                logger.warning(f"[BOT] kafka error: {msg.error()}")
                continue

            alert = parse_price_alert(msg.value())
            if alert is None:
                continue

            if should_push(last_push, alert, time.monotonic()):
                await channel.send(format_price_alert(alert))
                pushed += 1
                logger.info(f"[BOT] 📤 pushed {alert['symbol']} | pushed={pushed} skipped={skipped}")
            else:
                skipped += 1
    finally:
        consumer.close()


def _selfcheck():
    a = {"source_type": "BINANCE_CRYPTO", "symbol": "BTCUSDT"}
    b = {"source_type": "BINANCE_CRYPTO", "symbol": "ETHUSDT"}
    c = {"source_type": "ALPACA_STOCK", "symbol": "BTCUSDT"}   # 同名不同來源
    last: dict = {}

    assert should_push(last, a, 1000.0)                     # 第一筆一定推
    assert not should_push(last, a, 1000.0 + COOLDOWN_SECS - 1)   # 冷卻窗內擋掉
    assert should_push(last, b, 1000.0)                     # 其他 symbol 不受影響
    assert should_push(last, c, 1000.0)                     # 來源不同也不受影響
    assert should_push(last, a, 1000.0 + COOLDOWN_SECS)     # 冷卻結束後放行
    print("✅ bot cooldown self-check passed")


def main():
    import discord
    from dotenv import load_dotenv

    load_dotenv()
    token = os.environ.get("DISCORD_TOKEN", "")
    channel_id = os.environ.get("DISCORD_CHANNEL_ID", "")
    # compose 是 restart: unless-stopped，設定沒補會無限重啟——把原因講清楚，別丟 traceback
    if not token or not channel_id.isdigit():
        raise SystemExit("[BOT] 缺少設定：請在 .env 補上 DISCORD_TOKEN 與 DISCORD_CHANNEL_ID（頻道數字 ID）")
    channel_id = int(channel_id)

    class AlertBot(discord.Client):
        async def setup_hook(self):
            # 用 setup_hook 而不是 on_ready：on_ready 每次重連都會再觸發一次，
            # 會開出第二個 consumer 與第二個推播迴圈。
            self.loop.create_task(self._run_pump())

        async def _run_pump(self):
            await self.wait_until_ready()
            logger.info(f"[BOT] 已登入 {self.user}")
            await _pump(await self.fetch_channel(channel_id))

    # 只推播、不讀訊息，不需要任何 privileged intent
    AlertBot(intents=discord.Intents.none()).run(token)


if __name__ == "__main__":
    if "--selfcheck" in sys.argv:
        _selfcheck()
    else:
        main()
