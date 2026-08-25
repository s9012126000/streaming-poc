# 專案架構總覽 — 服務之間的關係

> 即時市場資料管線：交易所 → Kafka → Flink → Iceberg 湖倉 → 查詢與圖表 → 告警。
> 這份講的是**服務之間怎麼串**，各服務的部署方式見 `doc/updated_plan/`。

---

## 資料流全圖

```
                    ┌─────────────┐
                    │  Binance    │  外部交易所
                    │  Alpaca     │
                    └──────┬──────┘
                           │ WebSocket 拉即時成交
                           ▼
                    ┌─────────────┐      ┌──────────────────┐
                    │  Producer   │◄────►│ Schema Registry  │ 檢查資料格式
                    └──────┬──────┘      └────────┬─────────┘
                           │ 寫入                  │ schema 存在
                           ▼                       │ Kafka topic 裡
                    ┌─────────────────────────────▼──┐
                    │  Kafka  broker-0/1/2（RF=3）    │  ← 資料匯流排
                    │  topics: trades / alerts / dlq  │
                    └───┬─────────────────────┬──────┘
                        │ 讀                   │ 讀 alerts
                        ▼                     ▼
                 ┌─────────────┐        ┌──────────┐
                 │    Flink    │        │  Discord │
                 │  VWAP/異常   │        │   Bot    │──► Discord
                 └──────┬──────┘        └──────────┘
                        │ 寫入 Iceberg 表
          ┌─────────────┴──────────────┐
          ▼                            ▼
   ┌──────────────┐            ┌──────────────┐
   │ iceberg-rest │            │    MinIO     │
   │  「檔案在哪」  │            │ 「檔案本體」   │
   │   Catalog    │            │  Parquet     │
   └──────▲───────┘            └──────▲───────┘
          │ 問位置                     │ 讀檔案
          └────────┬───────────────────┘
                   │
             ┌─────┴─────┐
             │   Trino   │  SQL 查詢引擎
             └─────┬─────┘
                   │
             ┌─────▼──────┐
             │  Gateway   │──► Web 前端（K 線圖）
             └────────────┘
```

---

## 關鍵關係

### Kafka 是中心

所有東西都繞著它 —— Producer 寫進去、Flink 和 Bot 讀出來。
它讓上下游解耦：**Flink 掛掉，Producer 照常寫，資料不會掉**。
這是整個架構最重要的一個性質，也是 RF=3 值得花力氣做的理由。

### 儲存層拆成兩半（最不直覺的一點）

| | 存什麼 | 類比 |
|---|---|---|
| **iceberg-rest** | 中繼資料 —— 有哪些表、欄位是什麼、資料檔在哪 | 圖書館的**目錄** |
| **MinIO** | 真正的 Parquet 檔案 | 圖書館的**書架** |

Trino 查詢時的順序：

```
1. 問 iceberg-rest：「trades 表的檔案在哪？」
2. 拿到一串 S3 路徑
3. 去 MinIO 讀那些 Parquet 檔
```

**兩者缺一不可。** 只有 MinIO 沒有 catalog，查詢引擎不知道檔案的結構與版本；
只有 catalog 沒有 MinIO，那是一份指向空氣的目錄。

### Schema Registry 有點特別

它自己是**無狀態**的 —— schema 存在 Kafka 的一個內部 topic 裡。

所以：**它依賴 Kafka，但 Kafka 不依賴它。**
Kafka 可以單獨跑起來，Schema Registry 一定要等 Kafka 先好。

### Gateway 同時吃兩邊

| 資料 | 來源 |
|---|---|
| 即時報價（SSE 推播） | Kafka |
| 歷史 K 線 | Trino → Iceberg |

這也是為什麼 Week 8 的 Gateway 要等 Kafka 和 Trino 都就緒才有意義。

---

## 依賴順序 = 部署順序

### Week 7：基礎設施（互相之間沒有循環依賴）

```
1. MinIO           沒有依賴          ← 最單純的有狀態服務，先驗證 PVC
2. Kafka           沒有依賴          ← 本階段最難
3. Schema Registry 要先有 Kafka
4. iceberg-rest    要先有 MinIO
```

### Week 8：真正用到這些基礎設施的東西

```
5. Flink           要 Kafka + iceberg-rest + MinIO
6. Producer        要 Kafka + Schema Registry
7. Trino           要 iceberg-rest + MinIO
8. Gateway         要 Kafka + Trino
9. Web / Bot       要 Gateway / Kafka
```

整條鏈要到 Week 8 結束才會通。Week 7 只會看到「基礎設施都活著」，
看不到資料流動 —— 這是預期的，不是做錯了。

---

## 有狀態 vs 無狀態

判斷準則不是「有沒有資料」，而是**副本之間可不可以互換**。

| 服務 | K8s 物件 | 為什麼 |
|---|---|---|
| Kafka broker × 3 | **StatefulSet** | 每個持有不同分區、要互相認人選 leader |
| MinIO | **StatefulSet** | 資料要跟著 pod 走 |
| Schema Registry | Deployment | 狀態存在 Kafka topic 裡 |
| iceberg-rest | Deployment | 狀態存在 MinIO |
| Flink TaskManager | Deployment | 狀態在 checkpoint（雲端儲存） |
| Trino | Deployment / Job | 查詢引擎，無狀態 |
| Producer / Gateway / Web / Bot | Deployment | 完全無狀態 |

**只有兩個服務需要 StatefulSet。** 這個比例是健康的 ——
大部分服務應該想辦法把狀態外推，能用 Deployment 就用 Deployment。

---

## Compose → K8s 的對應

| docker-compose | K8s | 備註 |
|---|---|---|
| 一個 service | Deployment / StatefulSet | 見上表 |
| `depends_on: service_healthy` | initContainer 輪詢 + readinessProbe | K8s 沒有原生的啟動順序 |
| `environment:` | ConfigMap | |
| `.env` | Secret（從 `.env` 現場生成，不進 git） | |
| `volumes:` | PVC / volumeClaimTemplates | **PVC 存在就一直計費** |
| `ports:` | Service | 先 ClusterIP + port-forward |
| `broker-1:29092` | `broker-0.kafka.streaming.svc.cluster.local:9092` | advertised listener 是最容易卡的地方 |
| 一次性容器（`kafka-init`） | Job | 要能重跑不出錯（冪等） |
