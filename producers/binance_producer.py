import asyncio
import json
import logging
import websockets
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = logging.getLogger(__name__)

BINANCE_TOP20 = [
    "btcusdt", "ethusdt", "bnbusdt",  "solusdt", "xrpusdt",
    "dogeusdt","adausdt", "avaxusdt", "dotusdt",  "maticusdt",
    "linkusdt","ltcusdt", "uniusdt",  "atomusdt", "nearusdt",
    "aptusdt", "opusdt",  "arbusdt",  "suiusdt",  "injusdt"
]


class BinanceProducer:
    def __init__(self, kafka_bootstrap: str, schema_registry_url: str, schema_str: str):
        schema_registry = SchemaRegistryClient({"url": schema_registry_url})
        # schema 由 scripts/register_schema.sh 明確註冊，producer 不自動註冊：
        # 否則改到 market_trade.avsc 就會在下次送資料時無聲地產生新版本。
        self.serializer = AvroSerializer(schema_registry, schema_str,
                                         conf={"auto.register.schemas": False})
        self.producer   = Producer({"bootstrap.servers": kafka_bootstrap})
        self.count      = 0

    def _build_streams_url(self) -> str:
        streams = [f"{sym}@aggTrade" for sym in BINANCE_TOP20]
        return f"wss://stream.binance.com:9443/stream?streams={'/'.join(streams)}"

    async def run(self):
        url = self._build_streams_url()
        logger.info(f"[BINANCE] 🟡 連線到 Binance，訂閱 {len(BINANCE_TOP20)} 個幣種 aggTrade")

        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            while True:
                try:
                    raw  = await ws.recv()
                    data = json.loads(raw)
                    trade = data.get("data", {})

                    if trade.get("e") != "aggTrade":
                        continue

                    symbol_upper = trade["s"]  # e.g. BTCUSDT
                    record = {
                        "source_type":    "BINANCE_CRYPTO",
                        "asset_class":    "CRYPTO",
                        "symbol":         symbol_upper,
                        "price":          float(trade["p"]),
                        "size":           float(trade["q"]),
                        "trade_time":     int(trade["T"]),
                        "exchange":       None,
                        "tape":           None,
                        "is_buyer_maker": bool(trade["m"]),
                        "agg_trade_id":   int(trade["a"])
                    }
                    serialized = self.serializer(
                        record,
                        SerializationContext("market-trades", MessageField.VALUE)
                    )
                    self.producer.produce("market-trades", key=symbol_upper, value=serialized)
                    self.producer.poll(0)

                    self.count += 1
                    if self.count % 500 == 0:
                        logger.info(f"[BINANCE] Produced {self.count} msgs | {symbol_upper} @ {trade['p']}")

                except websockets.ConnectionClosedError as e:
                    logger.warning(f"[BINANCE] WebSocket 斷線，5 秒後重連: {e}")
                    await asyncio.sleep(5)
                    break  # 外層 while 重新 connect