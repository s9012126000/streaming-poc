# 技術選型手冊

> 這個專案實際用到的每一項技術：它在這裡做什麼、為什麼是它、代價是什麼。
>
> 跟其他文件的分工：
> - [`doc/knowledge/`](knowledge/) — **概念**：這個東西怎麼運作
> - 本文件 — **選型**：為什麼選它、放棄了什麼
>
> 每一節的「實際踩到的」都是這個專案真的發生過的事，不是通則。

---

## 版本清單

全部釘住版本。`latest` 只用在自建 image（配 `imagePullPolicy: Always`），
因為那些是我們自己 build 的，內容可控。

| 元件 | 版本 | 備註 |
|---|---|---|
| Kafka | `confluentinc/cp-kafka:7.9.0` | KRaft 模式，無 ZooKeeper |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.0` | |
| Flink | `apache/flink:2.0.2-scala_2.12` | + Iceberg 1.10.1、Kafka connector 4.0.1 |
| Iceberg REST Catalog | `tabulario/iceberg-rest:1.6.0` | 後端接 Postgres |
| Postgres | `postgres:17-alpine` | 只當 Iceberg catalog 的後端 |
| MinIO | `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z` | S3 相容物件儲存 |
| Trino | `trinodb/trino:476` | |
| Prometheus | `prom/prometheus:v3.1.0` | |
| Alertmanager | `prom/alertmanager:v0.28.1` | |
| Grafana | `grafana/grafana:11.5.1` | |
| kafka-exporter | `danielqsj/kafka-exporter:v1.7.0` | |
| nginx | `nginx:1.27-alpine` | 前端靜態檔 + 反向代理 |
| Python | `python:3.12-slim` | producer / gateway / bot |
| Node | `node:22-alpine` | 只在 build 階段 |

工具型 image（只出現在 initContainer 與一次性 Job）：
`curlimages/curl:8.11.1`（等待依賴就緒）、
`quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z`（建 bucket）。
這兩個也釘版本——**initContainer 壞掉的症狀是 pod 卡在 `Init:0/1`，
比主容器壞掉更難察覺**。

---

# 一、攝取層

## Binance / Alpaca WebSocket

**做什麼**：兩個外部行情來源。`main_producer.py` 依美股交易時段切換——
盤中跑 Alpaca（NASDAQ 前 30 支），其餘時間跑 Binance（加密貨幣，24 小時）。

**為什麼是 WebSocket 而不是 REST 輪詢**：行情是 push 型的資料。
輪詢要嘛太慢（漏掉成交）要嘛太吵（大部分請求回空）。WebSocket 一條長連線，
交易所有資料就推過來——這是 streaming 專案的起點，也是「為什麼需要 Kafka」的理由：
**上游的速率你控制不了**。

**實際踩到的**：Binance 對某些地區有 geo-block。專案原本在 `us-central1`，
producer 連不上，最後把整個叢集搬到 `asia-east1`（台灣）。
> 資料來源的地理限制會反過來決定叢集的位置——這在規劃階段完全沒想到。

## `confluent-kafka` (Python)

**做什麼**：producer / gateway / bot 都用它。底層是 C 寫的 `librdkafka`，
比純 Python 的 `kafka-python` 快得多，而且 Avro 與 Schema Registry 的整合是官方的。

**核心原理**：`produce()` 是**非同步**的——它只是把訊息丟進本地緩衝區就返回。
真正的送出由背景執行緒批次處理。所以：

```python
p.produce(topic, value=data)   # 不會阻塞，也不保證送到
p.flush()                      # 這裡才等待
```

**實際踩到的兩件事**：

`acks` 預設就是 `all`（librdkafka v1.0.0 起），計畫裡列的「要設 acks=all」是多慮的。
但 **`enable.idempotence` 預設是 `false`**——網路抖動觸發重試就可能寫入重複訊息，
而 Spot 環境的抖動不會少。這是專案目前**還沒補上**的缺口。

`confluent-kafka[avro,schemaregistry]` 的 extras 要寫對，少一個就 import 失敗。

## Avro + Schema Registry

**做什麼**：`schemas/market_trade.avsc` 定義成交的欄位。producer 送出前用它序列化，
Flink 讀取時用它反序列化。

**核心原理 —— Confluent wire format**：

```
[0x00][4-byte schema id][Avro binary payload]
  ↑        ↑
  魔術位元組  註冊表裡的 schema ID
```

訊息本身**不帶 schema**，只帶一個 ID。這是它比 JSON 省的地方——
欄位名不會在每一則訊息裡重複一次。

**為什麼需要 Registry**：schema 演進。生產者加了一個欄位，
舊的消費者不該因此爆掉。Registry 在註冊時做相容性檢查（預設 `BACKWARD`），
不相容就直接拒絕註冊——**把「會不會爆」從執行期挪到部署期**。

**取捨**：多一個必須先啟動的服務，而且 producer 和 consumer 都要連得到它。
換來的是「資料格式」變成一個有版本、可驗證的東西，而不是口頭約定。

**雲端對應**：AWS Glue Schema Registry、Confluent Cloud、GCP 沒有原生對應
（通常直接用 Confluent Cloud 或自架）。

---

# 二、匯流排

## Apache Kafka（KRaft 模式）

**做什麼**：整個系統的資料匯流排。六個 topic：

| topic | partition | 用途 |
|---|---|---|
| `market-trades` | 16 | 原始成交 |
| `market-trades-clean` | 16 | 清洗後 |
| `market-quotes` | 16 | 報價 |
| `market-alerts` | 4 | 量能異常 |
| `market-price-alerts` | 4 | 價格跳動 |
| `market-dlq` | 4 | 死信 |

全部 `replication-factor 3`。

**核心原理**：Kafka 是一個**分散式的、可重讀的 append-only log**。
這句話裡每個詞都重要：

- **append-only**：只能往後加，不能改。所以寫入極快（順序 I/O）
- **可重讀**：消費不是「取走」，是「移動自己的 offset」。
  同一批資料可以被多個 consumer group 各自從頭讀一次
- **分散式**：topic 切成 partition 分散在多個 broker，
  **partition 是平行度的單位**——16 個 partition 最多支撐 16 個平行消費者

**為什麼這個專案需要它**：三個理由，缺一個都可以不用 Kafka。

1. **削峰**：上游速率你控制不了，下游處理速度有限。Kafka 當緩衝
2. **扇出**：同一份成交資料，Flink 要算 VWAP、bot 要看告警、
   未來還可能有別的消費者。**一份寫入、多方獨立消費**
3. **可重放**：Flink job 掛掉重跑時，從 checkpoint 記錄的 offset 接續，
   不用回頭跟交易所要資料（也要不到）

**KRaft vs ZooKeeper**：Kafka 3.3 起 KRaft 生產可用，3.5 起 ZooKeeper 標記棄用。
KRaft 把 metadata 存進 Kafka 自己的一個內部 topic，由 controller quorum 管理——
**少養一個叢集，少一個要監控、要備份、要升級的東西**。
本專案直接用 KRaft，沒碰過 ZooKeeper。

**實際踩到的**：

**`advertised.listeners` 是搬上 K8s 最容易卡的一關。** Kafka 的 client
第一次連上任一 broker 之後，會拿到一份「所有 broker 的位址」清單，
之後直接連對應的 partition leader。所以那份清單裡的位址**必須是 client 連得到的**：

```bash
FQDN="${POD_NAME}.kafka.streaming.svc.cluster.local"
export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${FQDN}:9092"
```

寫成 `0.0.0.0` 會讓 client 拿到一個無意義的位址。
這也是 Kafka 必須用 StatefulSet 的原因——**pod 名字要穩定**。

**quorum 需要奇數且過半。** 三個 broker 的 KRaft quorum，容忍一台掛掉。
兩台的話任何一台掛掉就失去多數——這是本專案把 node 從 2 台加到 3 台的理由，
**節點數是為 quorum 決定的，不是為算力**。

詳見 [`doc/knowledge/kb-kafka.md`](knowledge/kb-kafka.md)。

**雲端對應**：AWS MSK、GCP Managed Service for Kafka、Azure Event Hubs（Kafka 相容）、
Confluent Cloud。托管版省掉的主要是「broker 的維運」，
而 partition 設計、consumer group、offset 管理這些**還是你的責任**。

## kafka-exporter

**做什麼**：把 Kafka 的內部狀態翻譯成 Prometheus 指標。
Kafka 自己只吐 JMX，Prometheus 讀不懂。

**實際踩到的**：這是 Week 8 漏掉的一塊。沒有它，
`ConsumerLagGrowing` 和 `DLQNotEmpty` 兩條告警規則引用的指標根本不存在——
而 **Prometheus 對不存在的指標不報錯，只回空集合**。
告警永遠不觸發，`/alerts` 顯示綠色，跟「一切正常」長得一模一樣。

> **靜默失效的告警比沒有告警更危險。**

---

# 三、運算

## Apache Flink 2.0

**做什麼**：四個常駐 streaming job。

| job | 寫法 | 做什麼 |
|---|---|---|
| `insert_bronze_trades` | SQL | Kafka → Iceberg bronze（原樣落地） |
| `vwap-1min-silver` | SQL | 1 分鐘視窗 VWAP → Iceberg silver |
| `volume-anomaly-mad` | PyFlink | 量能異常（MAD）→ Kafka alerts |
| `price-anomaly-jump` | PyFlink | 價格跳動 → Kafka alerts |

**為什麼是 Flink 而不是 Spark Streaming**：

| | Flink | Spark Structured Streaming |
|---|---|---|
| 模型 | 真正的逐筆流式 | micro-batch |
| 延遲 | 毫秒級 | 秒級（受 batch interval 限制） |
| 事件時間 | 一等公民（watermark 內建） | 支援但較晚成熟 |
| 狀態 | RocksDB，可增量 checkpoint | 有，但設計偏批次 |

行情資料的異常偵測要的是「這一筆進來時就判斷」，不是「等 5 秒湊一批」。
**如果需求是每小時跑一次報表，Spark 會是更省事的選擇。**

**核心原理 —— 三個必須理解的東西**：

**① 事件時間與 watermark。** 資料可能亂序到達（網路、多來源）。
watermark 是一個「我認為 T 之前的資料都到齊了」的宣告，
視窗靠它決定何時關閉並輸出。

```
table.exec.source.idle-timeout: 30s
```

這行很關鍵：**沒有資料流入的 partition 會擋住 watermark 前進**，
導致視窗永遠不觸發。16 個 partition 裡有幾個冷門標的沒成交，
整個 VWAP 就卡住了。設 30 秒把閒置的 partition 排除在 watermark 計算之外。

**② Checkpoint 與 exactly-once。** 每 60 秒對所有運算元的狀態做一次一致性快照，
寫到 MinIO。掛掉時從最近的 checkpoint 恢復，Kafka offset 也一併回滾——
所以是「重放但不重複」。

```
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
state.checkpoints.dir: s3://warehouse/checkpoints
```

**狀態寫在物件儲存而不是本地磁碟**，這是 TaskManager 能當無狀態 Deployment
的原因——被 Spot 回收後新 pod 從 S3 恢復即可。

**③ JobManager 與 TaskManager。** JM 是大腦（排程、協調 checkpoint），
TM 是手腳（實際運算）。**只有 JM 有狀態**（job graph 在記憶體），
所以只有它需要 HA。

**實際踩到的**：

**standalone 模式沒有 HA 的話，JM 一重建 = 所有 job 永久消失。**
日誌會說 `Successfully recovered 0 persisted job graphs`——
它不是報錯，是誠實地說「沒東西可以撿」。而 MinIO 裡的 checkpoint 還在，
變成沒人認領的孤兒。**存檔還在，但遊戲忘了你有存過檔。**

解法是 Kubernetes HA：job graph 本體寫 S3、指標寫 K8s ConfigMap、
leader 選舉用 Lease。四行設定 + 一份 RBAC。
詳見 [`doc/knowledge/distributed-coordination.md`](knowledge/distributed-coordination.md)。

**記憶體要留餘裕。** `jobmanager.memory.process.size: 1400m` 配
`limits.memory: 2Gi`——差的那 600MB 給 RocksDB 的堆外記憶體和 JVM 開銷。
設太接近就是週期性 OOMKilled，**而且日誌不會說是記憶體問題**。

**為什麼沒用 Flink Kubernetes Operator**：Operator 把「提交 job」變成
apply 一個 CRD，聲明式、也更接近生產做法。沒用是因為
**這個專案的目的是學會原生物件怎麼串**——用 Operator 就跳過了
JM/TM 部署、HA 設定、RBAC 這些。這是刻意的取捨，不是 Operator 不好。

**雲端對應**：AWS Managed Service for Apache Flink（原 Kinesis Data Analytics）、
GCP Dataflow（Beam 模型，非 Flink API）、Azure HDInsight、Ververica Platform。

## PyFlink vs Flink SQL

專案兩種都用。分界很清楚：

| | 用在哪 | 為什麼 |
|---|---|---|
| **SQL** | bronze 落地、VWAP 聚合 | 純粹的投影與視窗聚合，SQL 表達得最短 |
| **PyFlink** | MAD 異常、價格跳動 | 需要自訂邏輯與狀態，SQL 寫不出來（或寫出來沒人看得懂） |

SQL 的優勢是**改一行不用重 build image**（掛在 ConfigMap 裡）。
PyFlink 的代價是 Python 與 JVM 之間的序列化開銷。

---

# 四、湖倉

## Apache Iceberg

**做什麼**：bronze / silver 兩層表，檔案存在 MinIO。

**核心原理 —— 為什麼不是「一堆 Parquet 檔案」**：

```
catalog             metadata.json         manifest list → manifest → data files
「表在哪」    →    「表長什麼樣、        「這個快照包含哪些檔案」
                    有哪些快照」
```

多這幾層換到三件事：

**① 原子性 commit。** 寫入不是「把檔案放進目錄」，是「產生新的 metadata.json
並用 CAS 更新 catalog 的指標」。要嘛全部可見，要嘛完全不可見——
**沒有「讀到寫到一半」的狀態**。

**② 時間旅行與快照隔離。** 每次 commit 產生一個 snapshot。
讀取時鎖定一個 snapshot，即使同時有人在寫也不受影響。

**③ schema 演進不用重寫資料。** 欄位有獨立 ID，改名/增刪只動 metadata。

**vs Delta Lake / Hudi**：三者解決同一類問題。選 Iceberg 是因為它
**引擎中立**——Flink 寫、Trino 讀、未來 Spark 也能讀，沒有綁定。
Delta 早期跟 Databricks 綁得比較緊（現在開放很多），Hudi 偏向 upsert 場景。

**實際踩到的 —— 這個專案最貴的一課**：

**catalog 和資料是兩層，而 catalog 也需要持久化。**
`tabulario/iceberg-rest` 沒設 `CATALOG_URI` 時，預設是記憶體裡的 SQLite。
pod 一重啟，「有哪些表」全部歸零——Parquet 檔全都變成孤兒。

```
Trino: Schema 'bronze' does not exist      ← catalog 沒了
MinIO: 18M /data/warehouse/bronze          ← 資料還在
```

> **書還在書架上，被燒掉的是索引卡。**

而換成檔案型 SQLite 之後又炸了第二次：**SQLite 是單寫者模型**，
兩個 Flink job 同時 commit 就有一個直接失敗（`SQLITE_BUSY`），
被包成 HTTP 500，Flink 收到 `CommitStateUnknownException` 進入無法自癒的重啟迴圈。

最終方案是 Postgres。完整經過見
[`doc/knowledge/iceberg-catalog.md`](knowledge/iceberg-catalog.md)。

**雲端對應**：AWS Glue Data Catalog + S3、GCP BigLake Metastore + GCS、
Snowflake / Databricks 的 Iceberg 支援、Tabular（已被 Databricks 收購）。

## MinIO

**做什麼**：S3 相容的物件儲存。放 Iceberg 的 Parquet、Flink 的 checkpoint 與 HA 狀態。

**為什麼**：本機開發要一個 S3。MinIO 的 API 相容度高到
**程式碼完全不用改就能換成真的 S3/GCS**——只換 endpoint 和憑證。

**取捨**：在 K8s 上自架物件儲存本身就是一件要維運的事（PVC、備份、擴容）。
生產環境沒有理由不用託管的 GCS/S3。這裡用它是為了「整套能在本機跑」。

**實際踩到的**：`s3.path-style-access: true` 必設。
MinIO 只支援 `endpoint/bucket/key`，不支援 AWS 的 `bucket.endpoint/key`。

**雲端對應**：直接換成 GCS（本專案的下一步）、S3、Azure Blob。

## PostgreSQL

**做什麼**：**只當 Iceberg catalog 的後端**，不存業務資料。

裡面只有兩張表：

```sql
iceberg_tables                    -- namespace / table / metadata_location
iceberg_namespace_properties
```

**為什麼需要它**：見上面 Iceberg 那段。簡短版：catalog 要支援
**並行寫入**和**CAS**，SQLite 兩者都不行。

**實際踩到的**：`PGDATA` 必須指向子目錄。GKE 掛載 ext4 的 PVC 時
根目錄會有 `lost+found`，而 `initdb` 堅持要一個完全空的目錄：

```
initdb: error: directory "/var/lib/postgresql/data" exists but is not empty
```

**雲端對應**：Cloud SQL、RDS、Aurora。catalog 的寫入量極小
（每次 checkpoint 更新幾列），最小規格就夠。

---

# 五、查詢

## Trino

**做什麼**：查 Iceberg 表。gateway 的 `/api/klines` 和 `/api/symbols` 都走它。

**核心原理**：MPP（大規模平行處理）查詢引擎，**只做運算不存資料**。
它透過 connector 讀外部資料源，把查詢切成 stage 分散執行，全程在記憶體裡。

**vs Spark SQL**：Trino 為**互動式查詢**設計（秒級回應），
Spark 為**批次 ETL** 設計（容錯、可跑幾小時）。
Trino 的一個 stage 掛掉整個查詢就失敗——它賭的是「查詢夠短，重跑成本低」。

**為什麼這個專案用它**：K 線圖要「使用者一點就出來」。
Spark 起一個 job 的時間就超過使用者的耐心了。

**實際踩到的**：

Trino 476 的 Iceberg connector 設定是 `fs.native-s3.enabled=true`，
**不是** `fs.s3.enabled`。網路上的答案版本差異很大，最後是去翻對應版本的文件才確定。

**原本規劃「按需啟動」以省 1 vCPU，後來推翻**——因為 Web 前端本來就依賴它。
`replicas: 0` 的話圖表直接壞掉。這是設計上的矛盾，
在「Trino 只是分析工具」的假設下才成立，而那個假設在加了 Web 之後就不成立了。

**雲端對應**：AWS Athena（就是託管的 Trino/Presto）、
GCP BigQuery（架構不同但定位相同）、Starburst（Trino 的商業版）。

---

# 六、應用層

## FastAPI + SSE

**做什麼**：gateway。兩種資料路徑：

```
即時：Kafka consumer → 記憶體 → SSE 推給瀏覽器
歷史：Trino 查詢 → JSON
```

**為什麼是 SSE 不是 WebSocket**：行情圖表是**單向**的——伺服器推、瀏覽器收，
沒有反向通訊需求。SSE 是純 HTTP，自動重連內建，
**經過 nginx / 負載平衡器的行為比 WebSocket 單純**（不需要 upgrade 協商）。

WebSocket 的優勢在雙向互動（聊天、協作），這裡用不到。

**實際踩到的**：gateway 只能 `replicas: 1`。它在記憶體裡維護
`_alert_history` / `_price_history` / `_quotes`，新連上的 client 會先收到這份快照。
多副本的話每個副本各自累積不同的歷史，使用者重新整理可能連到另一個副本、
**看到不一樣的資料**。要多副本得先把狀態外推（例如 Redis），那是另一個工程。

nginx 的反向代理也有坑：

```nginx
location /api/stream {
    proxy_pass http://gateway.streaming.svc.cluster.local:8000;  # 不能有結尾斜線
    proxy_buffering off;      # ← 沒這行，SSE 會被 nginx 緩衝住
    proxy_read_timeout 24h;   # ← 沒這行，長連線會被切斷
}
```

## React + lightweight-charts + Vite

**做什麼**：K 線圖前端。`lightweight-charts` 是 TradingView 開源的圖表庫，
專為金融圖表設計（K 棒、成交量、十字準星），體積比 ECharts / Chart.js 小很多。

**實際踩到的**：compose 版本是開發模式（`npm run dev`），
搬上 K8s 時**改寫成兩階段 build**：

```dockerfile
FROM node:22-alpine AS build
RUN npm ci && npm run build      # 產出靜態檔
FROM nginx:1.27-alpine           # 只帶靜態檔，image 約 50MB
```

不該在容器啟動時 `npm install`——慢、不可重現、而且要對外網路。
**這是「compose 的開發模式不能直接搬」的典型例子。**

nginx 監聽 8080 而不是 80，因為以非 root 執行時綁不了特權埠。

## discord.py

**做什麼**：bot 消費 `market-price-alerts`，推播到 Discord 頻道。

**為什麼 `replicas: 1` 且 `strategy: Recreate`**：兩個副本會讓同一則告警送兩次
——它們不是分工消費，是各自讀全部的告警。滾動更新期間新舊並存也一樣，
所以用 `Recreate`。

**實際踩到的**：bot 目前**沒有任何健康端點**。
原本有一個 `pgrep -f 'bot.main'` 的 liveness probe，兩個問題：

1. `python:3.12-slim` 不含 procps，根本沒有 `pgrep` → probe 永遠失敗 → 殺了 26 次
2. 更根本的是：`CMD` 用 exec form，python 就是 PID 1，
   **PID 1 死掉容器本來就會結束**，這個 probe 在設計上不可能提供資訊

已整段移除。**「聊勝於無」在監控上是錯的判斷**——
弱訊號的 probe 沒有上行空間，只有下行風險。

---

# 七、平台

## Kubernetes / GKE

**做什麼**：跑上面所有東西。GKE Standard、3 × e2-standard-4 Spot、
手寫 raw YAML（不用 Helm）。

**核心原理 —— 一句話**：
**K8s 沒有「執行」你的 YAML，它只是把 YAML 存進 etcd**，
然後各種 controller 不斷把「實際狀態」拉向「宣告的狀態」。

理解這件事之後很多行為就順了：`apply` 很快回來不代表做完了；
刪掉 pod 它會長回來；`kubectl get` 是在讀 etcd。

**為什麼這個專案要上 K8s**：不是為了「雲原生」這個詞。
具體換到三件 compose 給不了的：

1. **自癒**：Spot node 被回收，pod 自動在別台重建
2. **宣告式的資源管理**：requests/limits、PDB、topologySpread
3. **真正的多副本與滾動更新**

代價是**一整層新的失敗模式**：RBAC、uid/fsGroup、Job 不可變、
ConfigMap 掛載會蓋掉整個目錄……這個專案四週踩的坑幾乎都在這一層。

**為什麼不用 Helm**：專案的目的是學會原生物件怎麼串。
先用 chart 就跳過了那一段。**Kafka 是第一個「值得重新考慮要不要用 Operator」
的地方**（Strimzi），但那也意味著跳過 StatefulSet / advertised listener 這些。

詳見 [`doc/knowledge/k8s-objects.md`](knowledge/k8s-objects.md)。

## Spot VM

**做什麼**：三台 node 全部是 Spot，價格約隨需的三分之一。

**代價**：GCP 可以隨時回收，只給 30 秒通知。所以整套系統的設計前提是
**「任何一個 pod 隨時可能消失」**——這反而逼出了正確的架構：

- Flink 的狀態在 S3 不在本地磁碟
- TaskManager 是無狀態 Deployment
- Kafka 用 StatefulSet + PDB + topologySpread

**實際踩到的**：`ZONE_RESOURCE_POOL_EXHAUSTED`——Spot 容量不足時
叢集會建起來但一台 node 都沒有。而且 `gcloud container clusters list-errors`
顯示的是**歷史**錯誤，當前狀態要看 `instances list` 和 MIG 的 `IS_STABLE`。

## Artifact Registry

**做什麼**：存五個自建 image（producer / flink / gateway / web / bot）。

**實際踩到的兩件事**：

**`roles/container.defaultNodeServiceAccount` 不包含 `artifactregistry.reader`。**
GKE 的提示訊息會叫你授予前者，但那不夠——症狀是 ImagePullBackOff 403。

**arm64 Mac 要指定平台**：

```bash
docker buildx build --platform linux/amd64 ...
```

不指定的話 build 出來的是 arm64 image，在 e2 node 上跑不起來，
而錯誤訊息（`exec format error`）跟平台完全沒關係。

---

# 八、監控

## Prometheus

**做什麼**：抓 8 個 target 的指標，評估 4 條 SLO 規則。

**核心原理 —— pull 而不是 push**：Prometheus 主動去抓，
而不是等服務推過來。好處是「目標活著嗎」本身就是一個免費的指標
（抓不到 = down），而且不用擔心被大量 push 打爆。

**這個專案最大的轉換**：`static_configs` → `kubernetes_sd_configs`。

```
compose：Prometheus 這一側列舉所有位址
K8s：   各服務用 pod annotation 宣告「我要被抓」
```

結果是整份 `prometheus.yml` 裡**沒有任何位址**。
TaskManager 從 2 個擴到 4 個會自動納入。

詳見 [`doc/knowledge/observability.md`](knowledge/observability.md)。

**雲端對應**：Google Managed Service for Prometheus、
Amazon Managed Prometheus、Datadog / New Relic（商業，模型不同）。

## Alertmanager

**做什麼**：收 Prometheus 推來的告警，分組、去重、送到 Discord。

**分工要分清楚**：Prometheus 決定「什麼時候該告警」，
Alertmanager 決定「怎麼通知」。所以規則寫在 Prometheus，收件人寫在這裡。

**實際踩到的**：**Alertmanager 不展開環境變數。**
寫 `${DISCORD_WEBHOOK}` 只會被當字面字串送出去，Discord 回 404，
而日誌只說「通知失敗」。解法是 `webhook_url_file` 指向掛載的 Secret 檔案。

## Grafana

**做什麼**：四個 panel——consumer lag、checkpoint duration/size、
watermark lag、records/s。

**provisioning 而不是手動建**：設定進 git，不會發生
「看板只存在某人瀏覽器裡點出來的狀態」。代價是 UI 上改了不會存回檔案，
重啟就還原——要改就改 YAML 再 apply。

**實際踩到的**：資料來源的 `uid` 要固定（我們寫死 `PROM`）。
讓 Grafana 自動產生的話每次重建都不同，看板會找不到資料來源，
每個 panel 顯示 `Datasource PROM was not found`。

---

# 沒有用到的技術，以及為什麼

舊版文件涵蓋了幾項最後沒用上的東西，一併記錄理由：

| 技術 | 為什麼沒用 |
|---|---|
| **ZooKeeper** | Kafka 3.3+ 的 KRaft 已生產可用。少養一個叢集 |
| **Apache Airflow** | 這是**串流**專案，沒有需要排程的批次 DAG。Iceberg 表維護（compaction / expire snapshot）未來可能需要，但一個 CronJob 就夠 |
| **Apache Superset** | 前端是自己寫的 K 線圖，不需要通用 BI。Grafana 已覆蓋監控面板 |
| **OpenLineage + Marquez** | 資料血緣在只有 bronze/silver 兩層、四個 job 的規模下，價值低於維運成本 |
| **Flink Kubernetes Operator** | 用 CRD 提交 job 更接近生產做法，但會跳過 JM/TM 部署、HA 設定、RBAC——而那正是本專案要學的 |
| **Strimzi（Kafka Operator）** | 同上。手寫 StatefulSet 才會遇到 advertised listener 這一關 |
| **Helm** | 同上。先用 chart 就跳過了原生物件怎麼串 |
| **NetworkPolicy** | 應該要有，只是還沒做。目前 namespace 內誰都連得到誰 |
| **Secret Manager** | 憑證仍在 K8s Secret（etcd 裡只是 base64）。已列為待辦 |

前六項是**刻意的取捨**——它們都會讓系統更好，但會讓學習少一層。
後兩項是**還沒做**，不是不該做。

---

# 雲端對應總表

| 本專案 | AWS | GCP | Azure |
|---|---|---|---|
| Kafka (KRaft) | MSK | Managed Service for Kafka | Event Hubs (Kafka API) |
| Schema Registry | Glue Schema Registry | —（用 Confluent Cloud） | Schema Registry (Event Hubs) |
| Flink | Managed Service for Apache Flink | Dataflow (Beam) | HDInsight |
| Iceberg catalog | Glue Data Catalog | BigLake Metastore | Purview |
| MinIO | S3 | GCS | Blob Storage |
| Postgres | RDS / Aurora | Cloud SQL | Database for PostgreSQL |
| Trino | Athena | BigQuery（架構不同） | Synapse |
| Kubernetes | EKS | **GKE**（本專案） | AKS |
| Prometheus | Amazon Managed Prometheus | Managed Service for Prometheus | Azure Monitor |
| Grafana | Amazon Managed Grafana | —（自架） | Azure Managed Grafana |

**托管服務省掉的是「維運」，不是「設計」。**
partition 怎麼切、watermark 怎麼設、catalog 放哪、告警閾值多少——
這些換到雲上還是你的責任，而它們才是這個專案真正在練的東西。
