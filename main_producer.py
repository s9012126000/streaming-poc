# main_producer.py
import asyncio
import logging
import os
from dotenv import load_dotenv
from utils.market_hours import is_market_open
from producers.alpaca_producer import AlpacaProducer
from producers.binance_producer import BinanceProducer

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

ALPACA_API_KEY    = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]
KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP",   "localhost:9092")
SCHEMA_REGISTRY   = os.environ.get("SCHEMA_REGISTRY",   "http://localhost:8081")

# 從 Schema Registry 讀取已註冊的 Schema（或直接嵌入 Schema 字串）
SCHEMA_STR = open("schemas/market_trade.avsc").read()

CHECK_INTERVAL_SECS = 60   # 每 60 秒檢查一次，斷掉的 producer 就重開


async def _stop(name: str, producer, task: asyncio.Task):
    logger.info(f"⏹  停掉 {name} producer")
    stop = getattr(producer, "stop", None)
    if stop is not None:
        await asyncio.to_thread(stop)   # SDK 的 stop() 是同步且會等最多 5 秒
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def run():
    """同一時間只跑一條 producer：美股盤中跑 ALPACA，其餘時間跑 BINANCE。

    盤中 = 美東 Mon–Fri 09:30–16:00（夏令時即 UTC+8 21:30 到隔天 04:00），
    見 utils/market_hours.py。切換最慢會延遲 CHECK_INTERVAL_SECS。
    """
    producers = {
        "BINANCE": BinanceProducer(KAFKA_BOOTSTRAP, SCHEMA_REGISTRY, SCHEMA_STR),
        "ALPACA":  AlpacaProducer(ALPACA_API_KEY, ALPACA_SECRET_KEY,
                                  KAFKA_BOOTSTRAP, SCHEMA_REGISTRY, SCHEMA_STR),
    }
    tasks: dict[str, asyncio.Task] = {}

    while True:
        active = "ALPACA" if is_market_open() else "BINANCE"

        for name, producer in producers.items():
            task = tasks.get(name)

            if name != active:
                if task is not None:
                    if not task.done():
                        await _stop(name, producer, task)
                    tasks.pop(name)
                continue

            if task is None or task.done():
                # done() 也涵蓋「連線掉了、任務丟出例外」——把原因記下來再重開，
                # 不然掛掉之後只是靜悄悄地沒有資料
                if task is not None and not task.cancelled() and task.exception():
                    logger.warning(f"⚠  {name} producer 掛了：{task.exception()!r}，重啟")
                logger.info(f"▶  啟動 {name} producer"
                            f"{'（美股交易時段）' if name == 'ALPACA' else ''}")
                tasks[name] = asyncio.create_task(producer.run())

        await asyncio.sleep(CHECK_INTERVAL_SECS)

if __name__ == "__main__":
    asyncio.run(run())