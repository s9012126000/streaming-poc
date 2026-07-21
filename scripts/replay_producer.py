"""Load-test producer (Week 4): flood market-trades to trigger backpressure.

Reuses the market_trade Avro schema + AvroSerializer (same path as the real
producer, so schema is naturally correct). Synthesizes trades across the top-20
symbols as fast as possible. Watch Grafana lag / checkpoint duration + Flink UI
backpressure, then tune parallelism/slots and confirm lag drains.

Run:  python scripts/replay_producer.py [target_total]   # default 5_000_000
"""
import os
import random
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
           "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
           "LINKUSDT", "LTCUSDT", "UNIUSDT", "ATOMUSDT", "NEARUSDT",
           "APTUSDT", "OPUSDT", "ARBUSDT", "SUIUSDT", "INJUSDT"]

target = int(sys.argv[1]) if len(sys.argv) > 1 else 5_000_000
schema_str = open("schemas/market_trade.avsc").read()
serializer = AvroSerializer(SchemaRegistryClient({"url": SCHEMA_REGISTRY}), schema_str,
                            conf={"auto.register.schemas": False})
# linger + big batches = throughput over latency, the point is to overwhelm Flink
producer = Producer({"bootstrap.servers": BOOTSTRAP,
                     "linger.ms": 50, "batch.size": 1 << 20, "acks": "1"})

ctx = SerializationContext("market-trades", MessageField.VALUE)
start = time.time()
for i in range(target):
    sym = SYMBOLS[i % len(SYMBOLS)]
    rec = {"source_type": "BINANCE_CRYPTO", "asset_class": "CRYPTO", "symbol": sym,
           "price": round(random.uniform(1, 60000), 2), "size": round(random.uniform(0.001, 5), 4),
           "trade_time": int(time.time() * 1000), "exchange": None, "tape": None,
           "is_buyer_maker": bool(i & 1), "agg_trade_id": i}
    producer.produce("market-trades", key=sym, value=serializer(rec, ctx))
    if i % 10000 == 0:
        producer.poll(0)
        rate = i / max(time.time() - start, 1e-6)
        print(f"\r{i:,} sent | {rate:,.0f} msg/s", end="", flush=True)

producer.flush()
print(f"\ndone: {target:,} in {time.time() - start:.1f}s")
