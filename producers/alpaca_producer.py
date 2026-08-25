# producers/alpaca_producer.py
import asyncio
import logging
from alpaca.data.live import StockDataStream
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = logging.getLogger(__name__)

NASDAQ_TOP30 = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA",
    "AMD",  "INTC", "QCOM", "AVGO",  "MU",   "AMAT",
    "CRM",  "ORCL", "ADBE", "NOW",   "SNOW", "PLTR",
    "AMGN", "GILD", "REGN", "MRNA",
    "NFLX", "CSCO", "COST", "PEP",   "SBUX", "PYPL", "ZM"
]

class AlpacaProducer:
    def __init__(self, api_key: str, secret_key: str,
                 kafka_bootstrap: str, schema_registry_url: str,
                 schema_str: str):
        self.api_key    = api_key
        self.secret_key = secret_key

        schema_registry = SchemaRegistryClient({"url": schema_registry_url})
        # schema 由 scripts/register_schema.sh 明確註冊，producer 不自動註冊：
        # 否則改到 market_trade.avsc 就會在下次送資料時無聲地產生新版本。
        self.serializer = AvroSerializer(schema_registry, schema_str,
                                         conf={"auto.register.schemas": False})
        self.producer   = Producer({"bootstrap.servers": kafka_bootstrap})
        self.count      = 0
        self.wss        = None

    async def handle_trade(self, trade):
        record = {
            "source_type":    "ALPACA_STOCK",
            "asset_class":    "STOCK",
            "symbol":         trade.symbol,
            "price":          float(trade.price),
            "size":           float(trade.size),
            "trade_time":     int(trade.timestamp.timestamp() * 1000),
            "exchange":       trade.exchange or "IEX",
            "tape":           trade.tape or "",
            "is_buyer_maker": None,
            "agg_trade_id":   None
        }
        serialized = self.serializer(
            record,
            SerializationContext("market-trades", MessageField.VALUE)
        )
        self.producer.produce("market-trades", key=trade.symbol, value=serialized)
        self.producer.poll(0)

        self.count += 1
        if self.count % 100 == 0:
            logger.info(f"[ALPACA] Produced {self.count} msgs | {trade.symbol} @ ${trade.price}")

    async def run(self):
        self.wss = StockDataStream(self.api_key, self.secret_key)
        self.wss.subscribe_trades(self.handle_trade, *NASDAQ_TOP30)
        logger.info(f"[ALPACA] 🟢 訂閱 {len(NASDAQ_TOP30)} 支 NASDAQ 股票")
        await asyncio.to_thread(self.wss.run)

    def stop(self):
        """收盤時關掉 stream。

        run() 跑在 to_thread 裡，取消 task 停不掉那條執行緒的 websocket，
        一定要走 SDK 自己的 stop()（thread-safe，內部最多等 5 秒）。
        """
        if self.wss is not None:
            self.wss.stop()
            self.wss = None