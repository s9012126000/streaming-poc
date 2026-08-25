# 🧭 一筆資料的歷程 — 從 Binance 到 MinIO

> **主題**：追蹤單一筆成交，從交易所 WebSocket 一路到 MinIO 上的 Parquet 物件，並說明各站之間的關係
> **對象**：`market-trades` → `bronze_ingest.py` → `iceberg.bronze.trades` → `s3://warehouse/`

> 本文描述的是**實際執行中的行為**，數字取自 2026-07-16 的實測，非設計草稿。

---

## 📍 全景

```
Binance WS
   │  JSON
   ▼
binance_producer.py ──── 查詢 ───▶ Schema Registry (id=1)
   │  [magic][schema id][Avro body]
   ▼
Kafka: market-trades  (16 partitions, RF=3)
   │  BYTES（Flink 刻意不解碼）
   ▼
bronze_ingest.py :: Decode ──── 解不開 ───▶ market-dlq
   │  解得開
   ├──────────────▶ market-trades-clean   (at-least-once)
   └──────────────▶ Iceberg Sink          (exactly-once)
                        │  寫 Parquet → 但還看不見
                        ▼
                    Checkpoint (每 30s) → snapshot commit
                        │
                        ▼
                    MinIO: s3://warehouse/bronze/trades/...
```

---

## 1️⃣ Producer — 從 JSON 變成有 schema 的 bytes

Binance WebSocket 推來一筆 `aggTrade` JSON，`binance_producer.py` 整形成統一 record（`source_type`、`symbol`、`price`、`trade_time`…）。

接著 `AvroSerializer` 做兩件事：

1. 拿 `schemas/market_trade.avsc` 去問 Schema Registry「這份 schema 的 id 是多少」。因為 `auto.register.schemas=False`，它**只能查、不能寫** — 查不到就直接 404 失敗。查到的答案是 **id 1**。
2. 產出 Confluent wire format：

```
[0x00][00 00 00 01][Avro binary body...]
 magic   schema id=1    只有值，沒有欄位名
```

> 💡 **這 5 個 byte 是整條 pipeline 的關鍵。**
> Avro body 裡只有值、沒有欄位名（這是它比 JSON 省的原因），所以脫離 schema 就是一堆無意義的 bytes。那個 id 是它與 Registry 之間唯一的繩子。

---

## 2️⃣ Kafka — 決定順序與位置

```python
producer.produce("market-trades", key="BTCUSDT", value=<bytes>)
```

**Key 決定 partition**：`murmur2("BTCUSDT") % 16`。

這帶來一個保證 — **同一個 symbol 的成交永遠落在同一個 partition，因此永遠保持順序**。跨 symbol 則沒有任何順序保證，這正是後面需要 watermark 的原因。

寫入時 `min.insync.replicas=2`、`RF=3`：3 份副本中至少 2 份確認才算成功。訊息在 partition log 中拿到一個**單調遞增的 offset** — 這個號碼會一路影響到最後。

---

## 3️⃣ Flink Source — 刻意不解碼

`kafka_trades_raw` 用 `'format'='raw'`，欄位型別是 `BYTES`。

這是刻意的設計：**Flink 在這裡完全不理解資料內容**，只把 bytes 原封不動撈進來。

> ⚠️ 一旦讓 Kafka connector 自己解 Avro，遇到壞訊息就會直接拋例外、job crash-loop — 你連把它送進 DLQ 的機會都沒有。

同時記下 `partition` / `offset` / `topic` 三個 metadata 欄位，以及讀到哪裡（`scan.startup.mode='group-offsets'`）。

---

## 4️⃣ Decode — 分岔點

`Decode` 這個 `ProcessFunction` 剝掉前 5 個 byte、用 schema 解開 Avro body。

這是整條 pipeline 唯一的分岔：

| 結果 | 去向 |
|---|---|
| 解得開 | 主流 |
| 解不開 | 打上 `DLQ_TAG` → side output → `market-dlq`，**job 不中斷** |

---

## 5️⃣ 扇出成三條路

同一份解好的資料，用一個 `StatementSet` 同時寫進三個 sink。三者讀的是**同一個 stream**，不是三次獨立計算：

| Sink | 去哪 | 保證 |
|---|---|---|
| `iceberg_catalog.bronze.trades` | MinIO | **exactly-once**（靠 checkpoint） |
| `kafka_trades_clean` | Kafka | at-least-once |
| `kafka_dlq_sink` | Kafka | at-least-once |

---

## 6️⃣ Iceberg Sink — 寫了，但看不見

Sink 算出這筆資料的分區值 `(source_type='BINANCE_CRYPTO', trade_date='2026-07-16')`，累積進對應的 Parquet writer，再透過 `S3FileIO` PUT 到 MinIO：

```
s3://warehouse/bronze/trades/data/
  source_type=BINANCE_CRYPTO/trade_date=2026-07-16/
    00000-0-<uuid>-00001.parquet
```

> ⚠️ **檔案已經實體存在於 MinIO 上了，但 Trino 查不到。**
> Iceberg 的 table 內容不是「掃描目錄看有什麼檔案」，而是由 metadata 明確列出的 — 沒被 metadata 認領的檔案就等於不存在。

---

## 7️⃣ Checkpoint — 讓它變得看得見

每 30 秒（牆上時鐘，**跟資料無關**），JobManager 發出 barrier。Sink 收到後把手上的 Parquet 檔關閉、flush，交出「我寫了這些檔案」的清單，但**先不 commit**。

等**所有** operator 都完成、checkpoint 整體成功，committer 才呼叫 Iceberg REST Catalog，產生新的 **snapshot**：manifest 認領這批新檔案，`metadata.json` 指向新 snapshot。

**這一刻，資料才對 Trino 可見。** 這是個 two-phase commit。

MinIO 上的證據很直接：

```
16:41:17  4.7KiB  ...-00001.parquet
16:41:47  4.7KiB  ...-00002.parquet     ← 30 秒
16:42:17   16KiB  ...-00003.parquet     ← 30 秒
```

**每個檔案間隔正好 30 秒 = checkpoint interval。** 實測 78 個 snapshot，就是 78 次成功的 checkpoint。

> 這也解釋了一個曾經踩過的坑：`flink-init` 沒有 `FLINK_PROPERTIES`，bronze job 的 checkpoint 根本沒開 → Iceberg 永遠 0 列，但 Parquet 檔一直在寫、DLQ 和 clean topic 也一直有輸出，三個 job 全都 RUNNING、監控一片綠。

---

## 🔗 它們之間的關係

真正該記住的是這幾條線把各站串起來的方式。

### Schema id 是 producer 與 decoder 之間唯一的契約

那 5 個 byte 讓 Kafka 裡的資料自我描述。這也是為什麼「關掉 auto-register」重要 — 否則 producer 可以在你不知情的狀況下，單方面改寫這份契約。

### Kafka partition ≠ Iceberg partition

名字撞在一起，但**完全無關**：

| | 依據 | 決定什麼 |
|---|---|---|
| Kafka partition | `murmur2(symbol) % 16` | 平行度與順序 |
| Iceberg partition | `(source_type, trade_date)` | 查詢時能跳過多少檔案 |

### Offset 是 replay 的座標，checkpoint 是它的存檔點

Offset 在 checkpoint 時才 commit — 所以「重啟後從哪裡開始」和「Iceberg commit 到哪裡」是**同一個 checkpoint 決定的**。

這正是 exactly-once 的來源：資料寫進 Iceberg 和「我讀到這裡了」是原子性地一起生效的。任一失敗，兩者一起退回上一個 checkpoint。

### Watermark 完全沒出現在這條路上

Bronze 是純 append，不需要知道「時間走到哪」。Watermark 只在 `vwap_1min` 那條線上才登場 — 那裡要決定 window 何時算帳。

這就是為什麼 watermark 卡在 `Long.MIN_VALUE` 時，bronze 照常落地、silver 卻是 0 列。

**一句話：watermark 決定「有沒有結果產生」，checkpoint 決定「產生的東西什麼時候看得見」。**

---

## ⚠️ 順帶浮出的問題：小檔案

**4.7KiB 的檔案，每 30 秒一個。**

78 個 snapshot 對應著幾百個碎檔，Trino 每次查詢都得逐個開啟。`scripts/iceberg_maintenance.sql` 正是要解決這個。
