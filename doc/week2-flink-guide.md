# Week 2 — Flink 串流處理實戰指南

> 本文件包含從環境準備到 Bronze/Silver/Anomaly 三個 Flink Job 的完整操作步驟，  
> 並整合所有已知的版本修正與 Gotcha。

---

## 目錄

1. [前置：Dockerfile 安裝 Connector JARs](#1-前置dockerfile-安裝-connector-jars)
2. [前置：SQL Client 初始化檔案](#2-前置sql-client-初始化檔案)
3. [Day 8～9：環境確認](#3-day-89環境確認)
4. [Day 9～10：Flink Job 1 — Bronze Writer](#4-day-910flink-job-1--bronze-writer)
5. [Day 11～12：Flink Job 2 — VWAP Calculator](#5-day-1112flink-job-2--vwap-calculator)
6. [Day 12～13：Flink Job 3 — Anomaly Detector](#6-day-1213flink-job-3--anomaly-detector)
7. [Day 14：Checkpoint 崩潰恢復測試](#7-day-14checkpoint-崩潰恢復測試)
8. [Week 2 驗收清單](#8-week-2-驗收清單)
9. [已知 Gotcha 與修正摘要](#9-已知-gotcha-與修正摘要)

---

## 1. 前置：Dockerfile 安裝 Connector JARs

> ⚠️ **重要**：Flink 官方 image 不含 Kafka/Iceberg connector，必須預先打包。  
> 不要在 `docker-compose.yml` 的 `command` 裡用 `wget` 下載，會因為網路或版本問題失敗。  
> 正確做法是本機先下載 JAR，再用 `COPY` 打進 image。

### 步驟一：本機下載 JAR（正確版本）

```bash
mkdir -p docker/flink/jars

# Kafka Connector（注意：版本是 4.0.0-2.0，不是 3.3.0-2.0）
curl -L -o docker/flink/jars/flink-sql-connector-kafka.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar

# Iceberg Runtime（注意：版本是 1.10.1，不是 1.9.0）
curl -L -o docker/flink/jars/iceberg-flink-runtime.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.0/1.10.1/iceberg-flink-runtime-2.0-1.10.1.jar

# Avro + Schema Registry
curl -L -o docker/flink/jars/flink-avro-confluent-registry.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/2.0.0/flink-avro-confluent-registry-2.0.0.jar

# AWS SDK（MinIO S3 存取）
curl -L -o docker/flink/jars/aws-bundle.jar \
  https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.51/bundle-2.29.51.jar
  
curl -L -o docker/flink/jars/hadoop-common.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar

# 確認 4 個檔案，各應 > 1MB
ls -lh docker/flink/jars/
```

**預期輸出**：

```
aws-bundle.jar                       ~500MB
flink-avro-confluent-registry.jar    ~5MB
flink-sql-connector-kafka.jar        ~10MB
iceberg-flink-runtime.jar            ~50MB
```

### 步驟二：建立 Dockerfile

```bash
cat > docker/flink/Dockerfile << 'EOF'
FROM apache/flink:2.0-scala_2.12

# 從本機複製，不依賴 build 時網路
COPY jars/*.jar /opt/flink/lib/

RUN echo "Installed JARs:" && ls /opt/flink/lib/ | grep -E "kafka|iceberg|avro|bundle"
EOF
```

### 步驟三：更新 docker-compose.yml 的 Flink 部分

將 `image:` 改為 `build:`，`command` 改回簡單形式：

```yaml
flink-jobmanager:
  build:
    context: ./docker/flink
    dockerfile: Dockerfile
  hostname: flink-jobmanager
  container_name: flink-jobmanager
  command: jobmanager          # ← 簡單的 command，不需要 bash -c
  depends_on:
    minio:
      condition: service_healthy
  ports:
    - "8082:8081"
  environment:
    FLINK_PROPERTIES: |
      jobmanager.rpc.address: flink-jobmanager
      jobmanager.memory.process.size: 1600m
      state.backend: rocksdb
      state.checkpoints.dir: s3://warehouse/checkpoints
      s3.endpoint: http://minio:9000
      s3.access-key: minioadmin
      s3.secret-key: minioadmin
      s3.path.style.access: true
      execution.checkpointing.interval: 30s
      execution.checkpointing.mode: EXACTLY_ONCE
      table.exec.source.idle-timeout: 30s

flink-taskmanager:
  build:
    context: ./docker/flink
    dockerfile: Dockerfile
  hostname: flink-taskmanager
  container_name: flink-taskmanager
  command: taskmanager
  depends_on:
    - flink-jobmanager
  environment:
    FLINK_PROPERTIES: |
      jobmanager.rpc.address: flink-jobmanager
      taskmanager.memory.process.size: 2048m
      taskmanager.numberOfTaskSlots: 4
      state.backend: rocksdb
      state.checkpoints.dir: s3://warehouse/checkpoints
      s3.endpoint: http://minio:9000
      s3.access-key: minioadmin
      s3.secret-key: minioadmin
      s3.path.style.access: true
```

### 步驟四：Build 並啟動

```bash
# Build（第一次約 2～3 分鐘）
docker-compose build flink-jobmanager flink-taskmanager

# 確認 JAR 有打進 image
docker run --rm streaming-poc-flink-jobmanager ls /opt/flink/lib/ | grep -E "kafka|iceberg|avro|bundle|hadoop"

# 啟動
docker-compose up -d flink-jobmanager flink-taskmanager
```

**預期看到 4 行**：

```
bundle-2.29.51.jar
flink-avro-confluent-registry-2.0.0.jar
flink-sql-connector-kafka-4.0.0-2.0.jar
hadoop-common.jar
iceberg-flink-runtime-2.0-1.10.1.jar
```

---

## 2. 前置：SQL Client 初始化檔案

> ⚠️ **重要**：Flink SQL Client 的 Catalog 是 **Session 級別**，不會自動持久化。  
> 每次重開 SQL Client 都必須重新 `CREATE CATALOG`。  
> 解法：建立 `flink-init.sql`，啟動時自動載入。

### 建立初始化檔案

```bash
cat > flink-init.sql << 'EOF'
-- Iceberg Catalog（每次啟動 SQL Client 必須先建立）
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://iceberg-rest:8181',
  'warehouse'            = 's3://warehouse/',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://minio:9000',
  's3.access-key-id'     = 'minioadmin',
  's3.secret-access-key' = 'minioadmin',
  's3.path-style-access' = 'true'
);

-- 全域設定
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';
SET 'table.exec.source.idle-timeout'   = '30s';
EOF

# 複製到容器內
docker cp flink-init.sql flink-jobmanager:/opt/flink/flink-init.sql
```

### 之後每次進入 SQL Client

```bash
# 帶 init 檔啟動，Catalog 自動建好
docker exec -it flink-jobmanager ./bin/sql-client.sh -i /opt/flink/flink-init.sql
```

---

## 3. Day 8～9：環境確認

```bash
# 確認 JobManager 和 TaskManager 都在運行
docker ps | grep flink

# 開啟 Flink Dashboard
open http://localhost:8082
# 預期：Available Task Slots: 4，Running Jobs: 0

# 確認 JAR 在容器內
docker exec flink-jobmanager ls /opt/flink/lib/ | grep -E "kafka|iceberg|avro|bundle"
```

---

## 4. Day 9～10：Flink Job 1 — Bronze Writer

**目的**：把 Kafka 每一筆原始交易訊息落地到 Iceberg，保留原始資料（不做任何轉換）。

```bash
# 進入 SQL Client（帶 init 檔）
docker exec -it flink-jobmanager ./bin/sql-client.sh -i /opt/flink/flink-init.sql
```

### Step 1：建立資料庫

```sql
CREATE DATABASE IF NOT EXISTS iceberg_catalog.bronze;
CREATE DATABASE IF NOT EXISTS iceberg_catalog.silver;
```

### Step 2：建立 Kafka Source Table

> ⚠️ **Gotcha**：Flink 2.0 的 `INTERVAL` 不支援 `MILLISECOND`，需改用 `SECOND`。

```sql
CREATE TABLE kafka_market_trades (
  source_type    STRING,
  asset_class    STRING,
  symbol         STRING,
  price          DOUBLE,
  size           DOUBLE,
  trade_time     BIGINT,
  exchange       STRING,
  tape           STRING,
  is_buyer_maker BOOLEAN,
  agg_trade_id   BIGINT,
  event_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
  -- ✅ 正確：用 SECOND，不能用 MILLISECOND
  WATERMARK FOR event_ts AS event_ts - INTERVAL '1' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'market-trades',
  'properties.bootstrap.servers' = 'broker:29092',
  'properties.group.id'          = 'flink-bronze-consumer',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = 'http://schema-registry:8081',
  'scan.startup.mode'            = 'latest-offset'
);
```

### Step 3：建立 Iceberg Bronze Sink Table

> ⚠️ **Gotcha**：`PARTITIONED BY` 不能用 `DATE_FORMAT()` 函式，只能用欄位名稱。  
> 解法：新增 `trade_date STRING` 欄位，在 INSERT 的 SELECT 裡計算日期。

```sql
CREATE TABLE iceberg_catalog.bronze.market_trades (
  source_type    STRING,
  asset_class    STRING,
  symbol         STRING,
  price          DOUBLE,
  size           DOUBLE,
  trade_time     BIGINT,
  exchange       STRING,
  tape           STRING,
  is_buyer_maker BOOLEAN,
  agg_trade_id   BIGINT,
  event_ts       TIMESTAMP(3),
  ingest_ts      TIMESTAMP(3),
  trade_date     STRING          -- ✅ 分區用的日期欄位，格式 'yyyy-MM-dd'
) PARTITIONED BY (source_type, trade_date)   -- ✅ 只用欄位名稱
WITH (
  'write.format.default'         = 'parquet',
  'write.target-file-size-bytes' = '67108864'
);
```

### Step 4：啟動 Bronze Job

```sql
-- DATE_FORMAT 在 SELECT 裡計算，不在 DDL 裡
INSERT INTO iceberg_catalog.bronze.market_trades
SELECT
  source_type, asset_class, symbol, price, size,
  trade_time, exchange, tape, is_buyer_maker, agg_trade_id,
  event_ts,
  CURRENT_TIMESTAMP                    AS ingest_ts,
  DATE_FORMAT(event_ts, 'yyyy-MM-dd')  AS trade_date
FROM kafka_market_trades
WHERE price > 0 AND size > 0;
```

### Step 5：驗證 Bronze Job

```bash
# 另開 Terminal，等 1～2 分鐘後查詢
docker exec -it trino trino --execute "
  SELECT source_type, COUNT(*) AS cnt, MAX(event_ts) AS latest
  FROM iceberg.bronze.market_trades
  GROUP BY source_type
"
```

**預期輸出**：

```
BINANCE_CRYPTO | 15432 | 2026-05-21 17:05:23.123
```

**MinIO 分區結構**：

```
warehouse/
└── bronze/
    └── market_trades/
        ├── source_type=BINANCE_CRYPTO/
        │   └── trade_date=2026-05-21/
        │       └── part-00001.parquet
        └── source_type=ALPACA_STOCK/
            └── trade_date=2026-05-21/
                └── part-00002.parquet
```

---

## 5. Day 11～12：Flink Job 2 — VWAP Calculator

**目的**：每 1 分鐘，對每個 symbol 計算 VWAP（成交量加權均價）。

**公式**：`VWAP = Σ(price × size) / Σ(size)`

### Step 1：建立 Silver VWAP Sink Table

```sql
CREATE TABLE iceberg_catalog.silver.vwap_1min (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  source_type   STRING,
  asset_class   STRING,
  symbol        STRING,
  vwap          DOUBLE,
  total_volume  DOUBLE,
  trade_count   BIGINT,
  price_high    DOUBLE,
  price_low     DOUBLE,
  trade_date    STRING
) PARTITIONED BY (source_type, trade_date);
```

### Step 2：啟動 VWAP Job（Tumbling Window）

```sql
INSERT INTO iceberg_catalog.silver.vwap_1min
SELECT
  TUMBLE_START(event_ts, INTERVAL '1' MINUTE)        AS window_start,
  TUMBLE_END(event_ts,   INTERVAL '1' MINUTE)        AS window_end,
  source_type,
  asset_class,
  symbol,
  SUM(price * size) / NULLIF(SUM(size), 0)           AS vwap,
  SUM(size)                                          AS total_volume,
  COUNT(*)                                           AS trade_count,
  MAX(price)                                         AS price_high,
  MIN(price)                                         AS price_low,
  DATE_FORMAT(TUMBLE_START(event_ts, INTERVAL '1' MINUTE), 'yyyy-MM-dd') AS trade_date
FROM kafka_market_trades
GROUP BY
  TUMBLE(event_ts, INTERVAL '1' MINUTE),
  source_type, asset_class, symbol;
```

> **Tumbling Window 行為說明**：  
> 視窗在 Watermark 推進後才關閉，所以 17:00 視窗的結果會在 17:01:01 左右才出現在 Iceberg。  
> 這是正常的，不是 Bug。

### Step 3：驗證 VWAP

```sql
-- Trino 查詢
SELECT window_start, symbol, ROUND(vwap, 2) AS vwap,
       ROUND(total_volume, 4) AS vol, trade_count
FROM iceberg.silver.vwap_1min
WHERE source_type = 'BINANCE_CRYPTO'
  AND symbol = 'BTCUSDT'
ORDER BY window_start DESC
LIMIT 5;
```

---

## 6. Day 12～13：Flink Job 3 — Anomaly Detector（PyFlink）

**目的**：維護每個 symbol 的歷史成交量統計，當成交量超過 `mean + 3σ` 時發出告警。

### anomaly_detector.py

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor
from pyflink.common.typeinfo import Types
import json, math

class VolumeAnomalyDetector(KeyedProcessFunction):
    """為每個 symbol 維護最近 100 筆成交量狀態，超過 3σ 時輸出告警"""

    def open(self, runtime_context):
        self.volume_history = runtime_context.get_list_state(
            ListStateDescriptor("volume_history", Types.DOUBLE())
        )

    def process_element(self, record, ctx):
        vol = record["size"]
        history = list(self.volume_history.get() or [])
        history.append(vol)

        if len(history) > 100:
            history = history[-100:]
        self.volume_history.update(history)

        # 至少 20 筆才偵測，避免冷啟動誤報
        if len(history) < 20:
            return

        mean   = sum(history) / len(history)
        stddev = math.sqrt(sum((x - mean) ** 2 for x in history) / len(history))
        threshold = mean + 3 * stddev

        if vol > threshold and stddev > 0:
            alert = {
                "symbol":      record["symbol"],
                "source_type": record["source_type"],
                "volume":      vol,
                "mean":        round(mean, 4),
                "stddev":      round(stddev, 4),
                "sigma":       round((vol - mean) / stddev, 2),
                "alert_ts":    ctx.timestamp()
            }
            yield json.dumps(alert).encode()


env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(30000)

# ... Kafka Source / Sink 配置（參考 Week 2 詳細流程）

env.execute("Market Volume Anomaly Detector")
```

**狀態管理說明**：

```
Flink 為每個 symbol 維護獨立的 ListState：

BTCUSDT 的狀態：
  最近 100 筆成交量 → mean=0.05, stddev=0.01
  閾值 = 0.05 + 3×0.01 = 0.08

當突然來一筆 [BTCUSDT x2.5 BTC]：
  2.5 >> 0.08 → 觸發 Alert → 寫入 market-alerts Topic
```

---

## 7. Day 14：Checkpoint 崩潰恢復測試

```bash
# Step 1：記錄現在 Bronze 的筆數
docker exec -it trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.market_trades"
# 假設：12,500 筆

# Step 2：強制殺掉 TaskManager（模擬崩潰）
docker kill flink-taskmanager

# Step 3：觀察 Flink Dashboard http://localhost:8082
# Job 狀態：RUNNING → RESTARTING → RUNNING（約 30 秒）

# Step 4：重新查詢筆數
docker exec -it trino trino --execute \
  "SELECT COUNT(*) FROM iceberg.bronze.market_trades"
# 預期：12,500 + 崩潰期間新增的筆數

# Step 5：驗證無重複資料（Exactly-Once 保證）
docker exec -it trino trino --execute "
  SELECT symbol, trade_time, COUNT(*) AS dup_count
  FROM iceberg.bronze.market_trades
  GROUP BY symbol, trade_time
  HAVING COUNT(*) > 1
  LIMIT 5
"
# 預期：0 rows
```

---

## 8. Week 2 驗收清單

| 項目 | 驗證方式 | 預期結果 |
|---|---|---|
| JAR 已安裝 | `docker exec flink-jobmanager ls /opt/flink/lib/` | 4 個 JAR 存在 |
| Flink Dashboard | `http://localhost:8082` | 2～3 個 Jobs 狀態 `RUNNING` |
| Bronze 有資料 | Trino `SELECT COUNT(*)` | 筆數持續增加 |
| VWAP 每分鐘更新 | Trino 查 Silver | 每分鐘一筆，數值合理 |
| 兩個來源分開儲存 | MinIO Console `http://localhost:9001` | `BINANCE_CRYPTO/` 和 `ALPACA_STOCK/` 兩個分區 |
| Checkpoint 恢復 | 殺 TaskManager 後重查 | 無重複、無遺失 |
| 告警正常觸發 | Control Center market-alerts Topic | 看到 JSON 格式的告警訊息 |

---

## 9. 已知 Gotcha 與修正摘要

| 問題 | 錯誤寫法 | 正確修正 |
|---|---|---|
| Kafka connector 找不到 | `docker-compose command` 裡 wget | 本機下載 → Dockerfile COPY |
| Kafka JAR 版本 404 | `3.3.0-2.0` | **`4.0.0-2.0`** |
| Iceberg JAR 版本 404 | `1.9.0` | **`1.10.1`** |
| WATERMARK 語法錯誤 | `INTERVAL '500' MILLISECOND` | **`INTERVAL '1' SECOND`** |
| PARTITIONED BY 語法錯誤 | `DATE_FORMAT(event_ts, 'yyyy-MM-dd')` | 新增 `trade_date STRING` 欄位，DATE_FORMAT 放 SELECT |
| Catalog 消失 | 每次手動 CREATE CATALOG | 建立 `flink-init.sql`，用 `-i` 參數啟動 SQL Client |
| `Could not find factory 'kafka'` | JAR 未安裝或容器未重啟 | build image 後重啟容器 |

---

*文件更新：Week 2 Flink 實戰手冊 | 2026-05-22*
