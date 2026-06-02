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

CHECK_INTERVAL_SECS = 60   # 每 60 秒重新判斷是否切換資料源


async def run():
    alpaca_producer  = AlpacaProducer(ALPACA_API_KEY, ALPACA_SECRET_KEY,
                                      KAFKA_BOOTSTRAP, SCHEMA_REGISTRY, SCHEMA_STR)
    binance_producer = BinanceProducer(KAFKA_BOOTSTRAP, SCHEMA_REGISTRY, SCHEMA_STR)

    current_task = None
    current_source = None

    while True:
        market_open   = is_market_open()
        target_source = "ALPACA" if market_open else "BINANCE"

        if target_source != current_source:
            # 取消舊 Task
            if current_task and not current_task.done():
                logger.info(f"⏹  停止 {current_source} Producer")
                current_task.cancel()
                try:
                    await current_task
                except asyncio.CancelledError:
                    pass

            # 啟動新 Task
            if target_source == "ALPACA":
                logger.info("▶  切換到 ALPACA（美股交易時段）")
                current_task = asyncio.create_task(alpaca_producer.run())
            else:
                logger.info("▶  切換到 BINANCE（非交易時段 / 24/7 壓測）")
                current_task = asyncio.create_task(binance_producer.run())

            current_source = target_source

        await asyncio.sleep(CHECK_INTERVAL_SECS)

if __name__ == "__main__":
    asyncio.run(run())