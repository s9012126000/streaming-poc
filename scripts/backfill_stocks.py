"""Seed bronze.trades with the last US session so the watchlist has stocks before the open.

Alpaca 的即時 stream 只有開盤時才有成交，週末或盤前打開畫面，觀察清單的美股會
整個是空的。這支把最近幾個交易日的 1 分鐘 bar 灌進 market-trades——一根 bar 當成
一筆成交（price=該分鐘收盤價、size=該分鐘成交量），走的是跟真實 producer 完全
一樣的 topic 與 Avro schema，所以 Flink / Iceberg / gateway / UI 都不用動。

ponytail: bar 不是逐筆成交。要真的逐筆就改抓 get_stock_trades，代價是同樣時間
範圍的訊息量差三個數量級（30 檔一天約 30 萬筆起跳）。exchange 標成 BARS_1M，
倉庫裡分得出哪些 row 是合成的。

Run:  python -m scripts.backfill_stocks [days]      # 預設 1 個交易日
"""
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

from producers.alpaca_producer import NASDAQ_TOP30
from utils.market_hours import US_EASTERN, MARKET_OPEN, MARKET_CLOSE

load_dotenv()

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY = os.environ.get("SCHEMA_REGISTRY", "http://localhost:8081")
LOOKBACK_DAYS = 10          # 往回撈得夠遠才蓋得到假期＋週末，實際保留幾天由 days 決定


def _in_session(ts: datetime) -> bool:
    """只留常規交易時段：day_open 才會是真正的開盤價，不是盤前第一筆。"""
    et = ts.astimezone(US_EASTERN)
    return et.weekday() < 5 and MARKET_OPEN <= et.time() < MARKET_CLOSE


def main(days: int = 1) -> int:
    client = StockHistoricalDataClient(os.environ["ALPACA_API_KEY"], os.environ["ALPACA_SECRET_KEY"])
    bars = client.get_stock_bars(StockBarsRequest(
        symbol_or_symbols=NASDAQ_TOP30, timeframe=TimeFrame.Minute,
        start=datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS))).data

    # 先挑出最近 N 個「真的有成交」的交易日，再照這個日期集合過濾
    by_day: dict[str, list] = defaultdict(list)
    for symbol, symbol_bars in bars.items():
        for bar in symbol_bars:
            if _in_session(bar.timestamp):
                by_day[bar.timestamp.astimezone(US_EASTERN).strftime("%Y-%m-%d")].append((symbol, bar))
    sessions = sorted(by_day)[-days:]
    if not sessions:
        print("no session bars returned — 免費方案有 15 分鐘延遲，或這段期間全是假日")
        return 0

    serializer = AvroSerializer(SchemaRegistryClient({"url": SCHEMA_REGISTRY}),
                                open("schemas/market_trade.avsc").read(),
                                conf={"auto.register.schemas": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "batch.size": 1 << 20})
    ctx = SerializationContext("market-trades", MessageField.VALUE)

    sent = 0
    for day in sessions:
        for symbol, bar in sorted(by_day[day], key=lambda sb: sb[1].timestamp):
            rec = {"source_type": "ALPACA_STOCK", "asset_class": "STOCK", "symbol": symbol,
                   "price": float(bar.close), "size": float(bar.volume),
                   "trade_time": int(bar.timestamp.timestamp() * 1000),
                   "exchange": "BARS_1M", "tape": None,
                   "is_buyer_maker": None, "agg_trade_id": None}
            producer.produce("market-trades", key=symbol, value=serializer(rec, ctx))
            sent += 1
            if sent % 5000 == 0:
                producer.poll(0)
    producer.flush()
    print(f"✅ {sent:,} bar-trades sent | sessions={sessions} | symbols={len(bars)}")
    return sent


if __name__ == "__main__":
    n = main(int(sys.argv[1]) if len(sys.argv) > 1 else 1)
    # 30 檔 × 390 分鐘 ≈ 11,700；數量級不對就是時段過濾或 API 回傳有問題
    assert n == 0 or n > 1000, f"produced suspiciously few rows: {n}"
    assert not _in_session(datetime(2026, 8, 1, 18, 0, tzinfo=timezone.utc)), "週六不該算在時段內"
