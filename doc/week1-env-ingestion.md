# 📅 Week 1 — 環境建置 & 雙資料源攝取驗證
> **主題**：讓完整 Stack 在本機跑起來，實作 Alpaca（交易時段）+ Binance（非交易時段）雙資料源自動切換 Producer  
> **目標**：7 個容器健康、Producer 能自動依照市場時間切換資料源、Control Center 可觀察 24/7 不間斷資料流

---

## 🎯 雙資料源策略

```
交易時段（ET 09:30～16:00，週一到週五非假日）
    └─▶ Alpaca IEX WebSocket（NASDAQ Top 30 真實股市資料）
            ↓ 訊息量：數十～數百 msg/s
            ↓ 特色：真實美股 VWAP/OHLCV，業務邏輯驗證

非交易時段（收盤後、週末、美股假日）
    └─▶ Binance WebSocket（Top 20 加密貨幣，24/7 免費）
            ↓ 訊息量：500～3,000 msg/s
            ↓ 特色：高吞吐量壓測，Pipeline 全天不中斷
```

**兩者統一寫入相同的 Kafka Topic（`market-trades`）**，透過 `source_type` 欄位區分。  
Flink 的處理邏輯完全相同，只有 BI 層查詢時會依 `source_type` 分開展示。

---

## 🎯 本週交付目標

- [ ] Docker Compose 啟動 7 個核心服務
- [ ] MinIO `warehouse` bucket 建立完成
- [ ] Kafka Topics 建立（market-trades / market-quotes / market-alerts）
- [ ] 統一 Avro Schema（同時相容股市與加密貨幣欄位）成功註冊
- [ ] 雙資料源 Producer 正常運行：交易時段跑 Alpaca，非交易時段自動切換 Binance
- [ ] Control Center 確認 24 小時都有訊息進入（包含美股收盤後的 Binance 資料）

---

## Day 1～2：Docker Compose 環境建置

### 啟動指令

```bash
git clone https://github.com/your-org/nasdaq-streaming-poc.git
cd nasdaq-streaming-poc
docker-compose up -d

# 確認全部服務健康
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### docker-compose.yml（Week 1 最小可行版）

```yaml
version: "3.8"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    hostname: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:7.5.0
    hostname: broker
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 48
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    depends_on: [broker]
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: broker:29092

  control-center:
    image: confluentinc/cp-enterprise-control-center:7.5.0
    depends_on: [broker, schema-registry]
    ports:
      - "9021:9021"
    environment:
      CONTROL_CENTER_BOOTSTRAP_SERVERS: broker:29092
      CONTROL_CENTER_SCHEMA_REGISTRY_URL: http://schema-registry:8081

  minio:
    image: quay.io/minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data

  iceberg-rest:
    image: tabulario/iceberg-rest:latest
    ports:
      - "8181:8181"
    environment:
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
      AWS_REGION: us-east-1
      CATALOG_WAREHOUSE: s3://warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"

  flink-jobmanager:
    image: apache/flink:2.0-scala_2.12
    ports:
      - "8082:8081"
    command: jobmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        state.backend: rocksdb
        state.checkpoints.dir: s3://warehouse/checkpoints
        s3.endpoint: http://minio:9000
        s3.access-key: minioadmin
        s3.secret-key: minioadmin
        table.exec.source.idle-timeout: 30s

  flink-taskmanager:
    image: apache/flink:2.0-scala_2.12
    depends_on: [flink-jobmanager]
    command: taskmanager
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4

volumes:
  minio_data:
```

> **新增**：`table.exec.source.idle-timeout: 30s`  
> 解決股市收盤後 Watermark 停止推進、Flink Window 不關閉的問題。  
> 當資料源超過 30 秒沒有新訊息，Flink 自動推進 Watermark。

---

## Day 3～4：建立 Kafka Topics & 統一 Schema

### 建立 Topics（統一命名，不區分資料源）

```bash
# 主交易流：同時承載股市與加密貨幣資料
docker exec broker kafka-topics --create \
  --bootstrap-server broker:29092 \
  --topic market-trades \
  --partitions 16 \
  --replication-factor 1

# 報價流（後續擴充用）
docker exec broker kafka-topics --create \
  --bootstrap-server broker:29092 \
  --topic market-quotes \
  --partitions 16 \
  --replication-factor 1

# 異常告警
docker exec broker kafka-topics --create \
  --bootstrap-server broker:29092 \
  --topic market-alerts \
  --partitions 4 \
  --replication-factor 1

# Dead Letter Queue（資料品質問題）
docker exec broker kafka-topics --create \
  --bootstrap-server broker:29092 \
  --topic market-dlq \
  --partitions 4 \
  --replication-factor 1

# 驗證
docker exec broker kafka-topics --list --bootstrap-server broker:29092
```

### 統一 Avro Schema 設計

兩個資料源共用同一個 Schema，用 `source_type` 欄位區分來源，  
`asset_class` 區分資產類型。對各資料源沒有的欄位，填入 `null`（nullable field）。

```json
{
  "type": "record",
  "name": "MarketTrade",
  "namespace": "com.poc.market",
  "doc": "統一市場交易事件，涵蓋股票（Alpaca）與加密貨幣（Binance）",
  "fields": [
    {
      "name": "source_type",
      "type": {"type": "enum", "name": "SourceType", "symbols": ["ALPACA_STOCK", "BINANCE_CRYPTO"]},
      "doc": "資料來源識別符"
    },
    {
      "name": "asset_class",
      "type": {"type": "enum", "name": "AssetClass", "symbols": ["STOCK", "CRYPTO"]},
      "doc": "資產類別"
    },
    {"name": "symbol",       "type": "string",           "doc": "交易標的代號，例如 AAPL 或 BTCUSDT"},
    {"name": "price",        "type": "double",           "doc": "成交價格"},
    {"name": "size",         "type": "double",           "doc": "成交數量（股數或加密貨幣數量）"},
    {"name": "trade_time",   "type": "long",             "doc": "Event Time，毫秒 Unix Timestamp"},
    {"name": "exchange",     "type": ["null", "string"], "default": null, "doc": "交易所（股票適用，加密貨幣為 null）"},
    {"name": "tape",         "type": ["null", "string"], "default": null, "doc": "Tape 識別（股票適用，A/B/C）"},
    {"name": "is_buyer_maker","type": ["null", "boolean"],"default": null, "doc": "是否由 Maker 方買入（加密貨幣適用）"},
    {"name": "agg_trade_id", "type": ["null", "long"],   "default": null, "doc": "Binance 聚合成交 ID（加密貨幣適用）"}
  ]
}
```

### 向 Schema Registry 註冊

```bash
curl -X POST http://localhost:8081/subjects/market-trades-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @schemas/market_trade.avsc.json   # 將上方 JSON 存為此檔

# 確認註冊成功
curl http://localhost:8081/subjects
# 預期：["market-trades-value"]
```

---

## Day 5～7：雙資料源 Producer 實作

### 安裝依賴

```bash
pip install alpaca-py websockets confluent-kafka "confluent-kafka[avro]" pytz
```

### 核心工具：市場時間判斷器

```python
# utils/market_hours.py
from datetime import datetime, time
import pytz

US_EASTERN = pytz.timezone("America/New_York")

# 2026 美股假日（NYSE Holiday Calendar）
NYSE_HOLIDAYS_2026 = {
    datetime(2026, 1, 1).date(),   # New Year's Day
    datetime(2026, 1, 19).date(),  # MLK Day
    datetime(2026, 2, 16).date(),  # Presidents' Day
    datetime(2026, 4, 3).date(),   # Good Friday
    datetime(2026, 5, 25).date(),  # Memorial Day
    datetime(2026, 7, 3).date(),   # Independence Day (observed)
    datetime(2026, 9, 7).date(),   # Labor Day
    datetime(2026, 11, 26).date(), # Thanksgiving Day
    datetime(2026, 12, 25).date(), # Christmas Day
}

MARKET_OPEN  = time(9, 30, 0)
MARKET_CLOSE = time(16, 0, 0)

def is_market_open() -> bool:
    """判斷美股目前是否在交易時段"""
    now_et = datetime.now(US_EASTERN)

    # 週末
    if now_et.weekday() >= 5:
        return False

    # 假日
    if now_et.date() in NYSE_HOLIDAYS_2026:
        return False

    # 交易時間
    current_time = now_et.time()
    return MARKET_OPEN <= current_time < MARKET_CLOSE

def seconds_until_market_open() -> int:
    """計算距離下一個開盤還有幾秒"""
    now_et = datetime.now(US_EASTERN)
    # 簡化計算，實際可加入假日邏輯
    next_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_et >= next_open:
        from datetime import timedelta
        next_open += timedelta(days=1)
    delta = next_open - now_et
    return int(delta.total_seconds())
```

### Alpaca Producer（交易時段）

```python
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
        self.serializer = AvroSerializer(schema_registry, schema_str)
        self.producer   = Producer({"bootstrap.servers": kafka_bootstrap})
        self.count      = 0

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
        wss = StockDataStream(self.api_key, self.secret_key)
        wss.subscribe_trades(self.handle_trade, *NASDAQ_TOP30)
        logger.info(f"[ALPACA] 🟢 訂閱 {len(NASDAQ_TOP30)} 支 NASDAQ 股票")
        await wss.run()
```

### Binance Producer（非交易時段）

```python
# producers/binance_producer.py
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
        self.serializer = AvroSerializer(schema_registry, schema_str)
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
```

### 主入口：自動切換邏輯

```python
# main_producer.py
import asyncio
import logging
import os
from utils.market_hours import is_market_open
from producers.alpaca_producer import AlpacaProducer
from producers.binance_producer import BinanceProducer

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
```

### 啟動方式

```bash
export ALPACA_API_KEY="your_key"
export ALPACA_SECRET_KEY="your_secret"
export KAFKA_BOOTSTRAP="localhost:9092"
export SCHEMA_REGISTRY="http://localhost:8081"

python main_producer.py
```

**切換行為預覽**：
```
08:29 AM CST  [INFO] ▶  切換到 BINANCE（非交易時段 / 24/7 壓測）
08:31 AM CST  [INFO] [BINANCE] Produced 500 msgs | BTCUSDT @ 98234.5
...
09:31 AM CST  [INFO] ⏹  停止 BINANCE Producer
09:31 AM CST  [INFO] ▶  切換到 ALPACA（美股交易時段）
09:31 AM CST  [INFO] [ALPACA] 🟢 訂閱 30 支 NASDAQ 股票
09:31 AM CST  [INFO] [ALPACA] Produced 100 msgs | NVDA @ $875.30
...
16:01 PM CST  [INFO] ⏹  停止 ALPACA Producer
16:01 PM CST  [INFO] ▶  切換到 BINANCE（非交易時段 / 24/7 壓測）
```

---

## Day 7：驗證雙資料源切換

### Control Center 驗證

開啟 `http://localhost:9021` → Topics → `market-trades` → Messages

在訊息列表中確認兩種資料源都有進入：

```bash
# CLI 消費並觀察 source_type 欄位
docker exec broker kafka-console-consumer \
  --bootstrap-server broker:29092 \
  --topic market-trades \
  --from-beginning \
  --max-messages 10
```

### 手動觸發切換測試

```python
# 測試用：強制模擬非交易時段（修改 utils/market_hours.py）
# 將 is_market_open() 暫時 return False → 觀察切換到 Binance
# 再改回 return True → 觀察切換回 Alpaca
```

---

## ✅ Week 1 驗收清單

| 項目 | 驗證指令 / 方式 | 預期結果 |
|---|---|---|
| 所有容器健康 | `docker ps` | 7 個容器均為 `Up` |
| MinIO Bucket | `http://localhost:9001` | `warehouse` bucket 存在 |
| Kafka Topics | `kafka-topics --list` | 看到 4 個 topics（含 DLQ）|
| Schema 已註冊 | `curl http://localhost:8081/subjects` | `["market-trades-value"]` |
| Binance Producer | 終端機 log（非交易時段）| 每 500 筆輸出一次計數 |
| Alpaca Producer | 終端機 log（交易時段）| 每 100 筆輸出一次計數 |
| 自動切換 | 修改時間判斷強制切換 | Log 顯示 ⏹ 停止 + ▶ 切換 |
| 統一 Schema | Control Center Messages | source_type 欄位存在（ALPACA_STOCK 或 BINANCE_CRYPTO）|
| Watermark Idle | 停止 Producer 後等 30 秒 | Flink Job 不卡住（下週驗證）|

---

## 🌐 服務 URL 總覽

| 服務 | URL | 帳號密碼 |
|---|---|---|
| Kafka Control Center | http://localhost:9021 | 無需登入 |
| Schema Registry API | http://localhost:8081 | 無需登入 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Flink Dashboard | http://localhost:8082 | 無需登入 |
| Iceberg REST API | http://localhost:8181 | API |
