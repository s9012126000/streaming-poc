# 📅 Week 2 — Flink 串流處理核心實作
> **主題**：Flink 消費統一 `market-trades` Topic，依 `source_type` 分別計算 VWAP / 異常偵測，結果寫入 Iceberg Bronze & Silver  
> **目標**：2 個 Flink Jobs 持續運行，Binance 與 Alpaca 的資料同時被正確處理且可分開查詢

---

## 🎯 本週交付目標

- [ ] Flink Job 1：Bronze 寫入（`market-trades` → Iceberg raw）持續運行
- [ ] Flink Job 2：VWAP 計算（Silver Layer）每分鐘有資料，Binance 與 Alpaca 分區存放
- [ ] Flink 異常偵測：成交量 3σ 突增時觸發 `market-alerts` Topic
- [ ] Trino 可分別查詢股票 vs 加密貨幣的 VWAP
- [ ] Flink Checkpoint 成功，崩潰後可自動恢復
- [ ] 非交易時段（Binance 高頻）Pipeline 吞吐量驗證

---

## Day 8～9：Flink Kafka Source + Iceberg Bronze Sink

### 概念說明：為何 Bronze 要區分 source_type 分區

Binance 和 Alpaca 的資料最終都落在 Bronze，但：
- Binance 非交易時段可達 **3,000 msg/s**，Parquet 小檔案問題更嚴重
- 業務查詢幾乎都會 filter `source_type`，分區可大幅減少掃描量
- 分開分區讓 Compaction 可以只針對特定來源執行，不影響另一個

### flink_bronze.sql

```sql
-- ===================================================
-- Step 1: 定義 Kafka Source（統一 market-trades Topic）
-- ===================================================
CREATE TABLE kafka_market_trades (
  source_type   STRING,    -- 'ALPACA_STOCK' 或 'BINANCE_CRYPTO'
  asset_class   STRING,    -- 'STOCK' 或 'CRYPTO'
  symbol        STRING,
  price         DOUBLE,
  size          DOUBLE,
  trade_time    BIGINT,    -- 毫秒 Timestamp，轉為 TIMESTAMP(3) 後用於 Watermark
  exchange      STRING,
  tape          STRING,
  is_buyer_maker BOOLEAN,
  agg_trade_id  BIGINT,
  -- 將 Bigint 毫秒 Timestamp 轉換為 Flink TIMESTAMP 型態
  event_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
  -- Watermark：允許 500ms 遲到（Binance aggTrade 通常 < 100ms，股市資料可能稍大）
  WATERMARK FOR event_ts AS event_ts - INTERVAL '500' MILLISECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'market-trades',
  'properties.bootstrap.servers' = 'broker:29092',
  'properties.group.id'          = 'flink-bronze-consumer',
  'format'                       = 'avro-confluent',
  'avro-confluent.url'           = 'http://schema-registry:8081',
  'scan.startup.mode'            = 'latest-offset'
);

-- ===================================================
-- Step 2: 定義 Iceberg Bronze Sink
-- 分區：(source_type, asset_class, trade_date)
-- 方便後續只掃描特定資料源的特定日期
-- ===================================================
CREATE TABLE iceberg_bronze_trades (
  source_type   STRING,
  asset_class   STRING,
  symbol        STRING,
  price         DOUBLE,
  size          DOUBLE,
  trade_time    BIGINT,
  exchange      STRING,
  tape          STRING,
  is_buyer_maker BOOLEAN,
  agg_trade_id  BIGINT,
  event_ts      TIMESTAMP(3),
  ingest_ts     TIMESTAMP(3)
) PARTITIONED BY (
  source_type,
  asset_class,
  DATE_FORMAT(event_ts, 'yyyy-MM-dd')
) WITH (
  'connector'              = 'iceberg',
  'catalog-name'           = 'iceberg_catalog',
  'catalog-type'           = 'rest',
  'uri'                    = 'http://iceberg-rest:8181',
  'warehouse'              = 's3://warehouse/',
  'database-name'          = 'bronze',
  'table-name'             = 'market_trades',
  'write.format.default'   = 'parquet',
  'write.target-file-size-bytes' = '67108864'
);

-- ===================================================
-- Step 3: 啟動 Bronze 寫入 Job
-- ===================================================
INSERT INTO iceberg_bronze_trades
SELECT
  source_type, asset_class, symbol, price, size,
  trade_time, exchange, tape, is_buyer_maker, agg_trade_id,
  event_ts,
  CURRENT_TIMESTAMP AS ingest_ts
FROM kafka_market_trades
WHERE price > 0 AND size > 0;
```

---

## Day 10～11：VWAP 計算（Silver Layer）

### Flink SQL 關鍵設計：按 source_type 分組

VWAP 的業務意義在股票和加密貨幣中相同（成交量加權均價），但需要分開計算，  
避免不同資產類別的資料互相干擾，查詢時也方便 `WHERE source_type = 'ALPACA_STOCK'`。

### flink_vwap.sql

```sql
-- Silver Layer VWAP：1 分鐘 Tumbling Window
-- 包含 source_type 欄位，方便 BI 層分開展示

CREATE TABLE iceberg_silver_vwap (
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3),
  source_type    STRING,      -- 新增：資料來源識別符
  asset_class    STRING,      -- 新增：資產類別
  symbol         STRING,
  vwap           DOUBLE,
  total_volume   DOUBLE,      -- 注意：加密貨幣 size 是浮點數（如 0.0012 BTC），用 DOUBLE
  trade_count    BIGINT,
  price_high     DOUBLE,
  price_low      DOUBLE,
  PRIMARY KEY (window_start, source_type, symbol) NOT ENFORCED
) WITH (
  'connector'       = 'iceberg',
  'catalog-name'    = 'iceberg_catalog',
  'catalog-type'    = 'rest',
  'uri'             = 'http://iceberg-rest:8181',
  'warehouse'       = 's3://warehouse/',
  'database-name'   = 'silver',
  'table-name'      = 'vwap_1min'
);

INSERT INTO iceberg_silver_vwap
SELECT
  TUMBLE_START(event_ts, INTERVAL '1' MINUTE)   AS window_start,
  TUMBLE_END(event_ts,   INTERVAL '1' MINUTE)   AS window_end,
  source_type,
  asset_class,
  symbol,
  SUM(price * size) / NULLIF(SUM(size), 0)       AS vwap,
  SUM(size)                                      AS total_volume,
  COUNT(*)                                       AS trade_count,
  MAX(price)                                     AS price_high,
  MIN(price)                                     AS price_low
FROM kafka_market_trades
GROUP BY
  TUMBLE(event_ts, INTERVAL '1' MINUTE),
  source_type,
  asset_class,
  symbol;
```

### Trino 驗證：分開查詢兩個資料源

```sql
-- 查詢美股 NVDA（交易時段）
SELECT window_start, symbol, ROUND(vwap, 2) AS vwap_price, total_volume
FROM iceberg.silver.vwap_1min
WHERE source_type = 'ALPACA_STOCK' AND symbol = 'NVDA'
ORDER BY window_start DESC LIMIT 5;

-- 查詢加密貨幣 BTCUSDT（非交易時段）
SELECT window_start, symbol, ROUND(vwap, 6) AS vwap_price, ROUND(total_volume, 4) AS vol_btc
FROM iceberg.silver.vwap_1min
WHERE source_type = 'BINANCE_CRYPTO' AND symbol = 'BTCUSDT'
ORDER BY window_start DESC LIMIT 5;

-- 確認兩個資料源的訊息都有進入 Bronze
SELECT source_type, asset_class, COUNT(*) AS cnt, MAX(event_ts) AS latest
FROM iceberg.bronze.market_trades
GROUP BY source_type, asset_class;
```

---

## Day 12～13：異常偵測（成交量 3σ 突增）

### 更新：異常偵測同時覆蓋股市與加密貨幣

**PyFlink VolumeAnomalyDetector** 邏輯不需修改，  
因為 State 是以 `symbol` 為 Key，Binance 的 BTCUSDT 和 Alpaca 的 AAPL 是獨立的狀態，不會互相干擾。

唯一需要調整的是 **Alert 訊息格式**，加入 `source_type` 欄位：

```python
# 在 VolumeAnomalyDetector.process_element 中更新 alert 結構
if vol > threshold and vol > 0:
    alert = {
        "symbol":      record["symbol"],
        "source_type": record["source_type"],   # 新增
        "asset_class": record["asset_class"],   # 新增
        "volume":      vol,
        "mean":        round(mean, 4),
        "stddev":      round(stddev, 4),
        "threshold":   round(threshold, 4),
        "sigma":       round((vol - mean) / max(stddev, 1e-10), 2),
        "alert_ts":    ctx.timestamp()
    }
    yield alert
```

### 調整告警閾值建議

| 資料源 | 建議最小絕對量過濾 | 原因 |
|---|---|---|
| Alpaca（股票）| `vol > 1,000`（股數）| 低流動性股票成交量本就很小 |
| Binance（加密）| `vol > 0.5`（BTC數量）或 `val > $10,000`（估算市值）| 加密貨幣 size 單位不同，用金額判斷更有意義 |

---

## Day 14：吞吐量壓測（Binance 高頻驗證）

非交易時段是壓測 Pipeline 的最佳時機（Binance 500～3,000 msg/s）。

```bash
# Step 1：觀察 Flink 處理速率（Flink Dashboard → Running Jobs → 點擊 Job → Metrics）
# 觀察指標：numRecordsInPerSecond / numRecordsOutPerSecond

# Step 2：確認 Consumer Lag 在高頻時仍穩定（Control Center → Consumer Groups）
# 預期：Lag 不持續累積（表示 Flink 跟得上 Binance 的速率）

# Step 3：Bronze 寫入速率確認
docker exec trino trino --execute "
  SELECT
    source_type,
    DATE_FORMAT(event_ts, 'HH:mm') AS minute,
    COUNT(*) AS records_this_minute
  FROM iceberg.bronze.market_trades
  WHERE event_ts >= NOW() - INTERVAL '5' MINUTE
  GROUP BY source_type, DATE_FORMAT(event_ts, 'HH:mm')
  ORDER BY minute DESC, source_type
"

# 預期輸出（非交易時段全 Binance）：
# source_type      minute  records_this_minute
# BINANCE_CRYPTO   14:30   18450
# BINANCE_CRYPTO   14:29   17823
```

### Checkpoint 崩潰恢復測試（同 Week 1 計畫）

```bash
# 殺掉 TaskManager 後重啟，確認 Exactly-Once
docker kill nasdaq-streaming-poc_flink-taskmanager_1
# 等待自動恢復（約 30 秒）
# 重新查詢確認無重複資料
docker exec trino trino --execute "
  SELECT source_type, symbol, trade_time, COUNT(*) AS dup
  FROM iceberg.bronze.market_trades
  GROUP BY source_type, symbol, trade_time
  HAVING COUNT(*) > 1
  LIMIT 5
"
# 預期：0 rows
```

---

## ✅ Week 2 驗收清單

| 項目 | 驗證指令 / 方式 | 預期結果 |
|---|---|---|
| Flink Jobs 運行 | `http://localhost:8082` | 2 個 Jobs 狀態 `RUNNING` |
| Bronze 按來源分區 | MinIO Console → warehouse/bronze/ | 看到 `ALPACA_STOCK/` 和 `BINANCE_CRYPTO/` 兩個分區目錄 |
| 美股 VWAP 正確 | Trino 查詢 NVDA VWAP | 數值合理（交易時段） |
| 加密貨幣 VWAP 正確 | Trino 查詢 BTCUSDT VWAP | 數值合理（非交易時段）|
| 告警含 source_type | Control Center `market-alerts` | Alert 訊息有 source_type 欄位 |
| 高頻壓測通過 | Flink Dashboard Metrics | Binance 高頻時 Consumer Lag 不累積 |
| Checkpoint 恢復 | 殺掉 TaskManager 後重查 | 無重複資料，筆數正確 |
| Watermark Idle | 停止 Producer 後 30 秒 | Flink Window 正常關閉（idle-timeout 生效）|
