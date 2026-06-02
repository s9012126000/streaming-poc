# 🔬 技術深度手冊 — 所有使用技術的背景知識、原理、優勢與雲端對應服務

> 本文件涵蓋 NASDAQ Streaming POC 中所有使用到的開源技術，  
> 包含：背景歷史、核心原理、設計優勢、使用限制與三大雲端的對應服務。

---

## 目錄

0. [Apache ZooKeeper](#0-apache-zookeeper)
1. [Apache Kafka](#1-apache-kafka)
2. [Confluent Schema Registry](#2-confluent-schema-registry)
3. [Apache Flink 2.0](#3-apache-flink-20)
4. [Apache Iceberg](#4-apache-iceberg)
5. [MinIO](#5-minio)
6. [Apache Airflow](#6-apache-airflow)
7. [Trino](#7-trino)
8. [Apache Superset](#8-apache-superset)
9. [Prometheus + Grafana](#9-prometheus--grafana)
10. [OpenLineage + Marquez](#10-openlineage--marquez)
11. [Flink Kubernetes Operator](#11-flink-kubernetes-operator)
12. [Alpaca Markets WebSocket](#12-alpaca-markets-websocket)

---

## 0. Apache ZooKeeper

### 背景

ZooKeeper 由 Yahoo! 工程師於 2007 年開發，2008 年正式開源並捐贈給 Apache 基金會，2011 年成為 Apache 頂級專案。名稱來自一個比喻：「分散式系統就像一個動物園（Zoo），管理它們需要一位專業的動物園管理員（Keeper）」。

最初為了解決 Yahoo! 內部 Hadoop 叢集的分散式協調問題而誕生，後來被 Apache Kafka、HBase、Hadoop YARN、Apache Flink 等幾乎所有主流分散式系統採用。

### 核心原理

ZooKeeper 本質上是一個**分散式協調服務（Distributed Coordination Service）**，提供一個類似檔案系統的階層式 Key-Value 樹狀結構（稱為 ZNode），並在此基礎上實現分散式系統最需要的協調原語（Primitive）。

```
ZooKeeper ZNode 樹狀結構（以 Kafka 為例）：

/
├── /kafka
│   ├── /kafka/brokers
│   │   ├── /kafka/brokers/ids
│   │   │   ├── /kafka/brokers/ids/1   ← Broker 1 存活（Ephemeral Node）
│   │   │   └── /kafka/brokers/ids/2   ← Broker 2 存活（Ephemeral Node）
│   │   └── /kafka/brokers/topics
│   │       └── /kafka/brokers/topics/market-trades/partitions/...
│   ├── /kafka/controller             ← 當前 Controller Broker（Ephemeral）
│   └── /kafka/config
│       └── /kafka/config/topics/...  ← Topic 設定（Persistent Node）
```

**ZNode 類型**：

| 類型 | 說明 | Kafka 使用場景 |
|---|---|---|
| **Persistent Node** | 永久存在，需手動刪除 | Topic 設定、ACL、分區配置 |
| **Ephemeral Node** | 建立者連線斷開後自動刪除 | Broker 心跳、Controller 選舉 |
| **Sequential Node** | 自動附加遞增編號 | 分散式鎖的實現 |
| **Ephemeral Sequential** | 以上兩者結合 | Leader 選舉演算法 |

**ZooKeeper 提供的四大協調原語**：

1. **Leader Election（Leader 選舉）**
   多個節點同時嘗試建立同一個 Ephemeral Sequential ZNode，序號最小者成為 Leader。若 Leader 死亡，其 Ephemeral Node 消失，其他節點收到通知重新選舉。
   → Kafka 用此選出 Controller Broker，由 Controller 管理 Partition Leader 分配。

2. **Service Discovery（服務發現）**
   每個 Broker 啟動時在 `/kafka/brokers/ids/` 建立 Ephemeral Node，寫入自己的 host:port 資訊。Broker 崩潰時，ZooKeeper 自動刪除該節點。
   → Producer/Consumer 透過 ZooKeeper 知道哪些 Broker 目前存活。

3. **Distributed Lock（分散式鎖）**
   利用 Ephemeral Sequential ZNode 實現公平鎖（Fair Lock）：每個競爭者建立一個帶序號的節點，序號最小者持有鎖，釋放鎖即刪除節點。

4. **Watcher / Event Notification（事件通知）**
   客戶端可以對任意 ZNode 設定 Watcher，當節點發生變化（建立/刪除/修改）時，ZooKeeper 立即向客戶端發送通知（one-time trigger）。
   → Kafka Consumer Rebalance 的觸發機制。

**ZooKeeper 的一致性保證（ZAB 協議）**：

ZooKeeper 使用 **ZAB（ZooKeeper Atomic Broadcast）** 協議確保資料一致性，原理類似 Raft：

```
ZooKeeper Ensemble（奇數個節點，通常 3 或 5 個）

Client Write Request
        │
        ▼
  ┌─────────────┐
  │   Leader    │  ← 所有寫入都經過 Leader
  └──────┬──────┘
         │ 廣播 PROPOSAL
    ┌────▼─────┐    ┌──────────┐
    │Follower A│    │Follower B│
    └────┬─────┘    └─────┬────┘
         │ ACK             │ ACK
         └────────┬────────┘
                  ▼
          超過半數 ACK → Leader 發送 COMMIT → 回應 Client
```

- **Quorum（法定人數）**：超過半數節點同意才能提交寫入
- **3 節點 Ensemble**：允許 1 個節點故障
- **5 節點 Ensemble**：允許 2 個節點故障
- **強一致性**：任意節點讀到的資料一定是已提交的最新版本

### ZooKeeper 在 Kafka 中的角色

| ZooKeeper 管理的 Kafka 元數據 | 說明 |
|---|---|
| Broker 的存活狀態 | Broker 心跳透過 Ephemeral Node 維護 |
| Controller 選舉 | 決定哪個 Broker 是 Controller |
| Partition Leader 資訊 | 每個 Partition 目前的 Leader 是哪個 Broker |
| Topic 設定 | Topic 的分區數、replication factor、保留策略 |
| ISR（In-Sync Replicas）| 追上 Leader 的副本列表 |

### ZooKeeper 的限制與 KRaft 的崛起

| 問題 | 說明 |
|---|---|
| **Controller 瓶頸** | 重啟時需從 ZooKeeper 全量重載元數據，10 萬 Partition 可能要幾十秒 |
| **雙系統維運負擔** | 需同時維運 Kafka 和 ZooKeeper 兩套叢集 |
| **Partition 數量上限** | 生產環境建議不超過 20 萬個 Partition |
| **強依賴外部系統** | ZooKeeper 掛掉，整個 Kafka 叢集停擺 |

**KRaft 模式（Kafka 4.0 預設，正式移除 ZooKeeper 依賴）**：

```
KRaft 架構 — 不再需要獨立 ZooKeeper

┌────────────────────────────────────────┐
│         Kafka KRaft Cluster            │
│                                        │
│  Controller Quorum（3 節點，用 Raft）  │
│  元數據存於 __cluster_metadata Topic   │
│                                        │
│  Broker 1   Broker 2   Broker 3        │
│（Broker 可同時擔任 Controller 角色）   │
└────────────────────────────────────────┘

✅ 移除 ZooKeeper，單一系統維運
✅ Partition 上限從 20 萬 → 百萬級別
✅ Controller 重啟從分鐘級 → 秒級
```

**本 POC 使用 ZooKeeper 的原因**：使用 `confluentinc/cp-kafka:7.5.0`，此版本預設仍採用 ZooKeeper 模式。升級到 Confluent Platform 8.x 或 Apache Kafka 4.x 後，可切換 KRaft 模式直接移除 ZooKeeper 容器。

### 優勢

- **高可用**：Ensemble 設計，部分節點故障不影響服務
- **強一致性**：ZAB 協議保證所有客戶端看到相同的資料狀態
- **通用協調原語**：Leader 選舉、分散式鎖、事件通知，任何分散式系統都可使用
- **成熟穩定**：超過 15 年的大規模生產驗證

### 雲端對應服務

ZooKeeper 作為基礎協調服務，在雲端通常被各託管服務隱藏，不需使用者自行管理：

| 雲端 | 對應方式 | 說明 |
|---|---|---|
| **AWS** | Amazon MSK 內建 | MSK 自動管理 ZooKeeper 或 KRaft，使用者完全不需接觸 |
| **GCP** | Cloud Pub/Sub 架構內建 | 完全不需要 ZooKeeper |
| **Azure** | Azure Event Hubs 內建 | 完全不需要 ZooKeeper |
| **自管 K8s** | Strimzi Kafka Operator | 自動部署並管理 ZooKeeper 或 KRaft 模式的 Kafka |
| **協調服務替代** | **etcd**（K8s 使用）、**Consul**（HashiCorp）| 現代系統多改用 etcd 或 Consul 作為協調服務 |

---

## 1. Apache Kafka

### 背景

Kafka 於 2010 年由 LinkedIn 工程師 Jay Kreps 等人開發，2011 年開源捐贈給 Apache 基金會。  
最初設計目的是解決 LinkedIn 內部海量行為事件（瀏覽、點擊）的即時傳輸問題。  
2014 年 Jay Kreps 等人創辦 Confluent 公司，圍繞 Kafka 建構完整的企業級串流平台。

### 核心原理

Kafka 的本質是一個**分散式、持久化的日誌系統（Distributed Commit Log）**，而非傳統意義上的訊息佇列。

```
Topic: nasdaq-trades
│
├── Partition 0: [msg@offset:0] [msg@offset:1] [msg@offset:2] ...
├── Partition 1: [msg@offset:0] [msg@offset:1] ...
├── Partition 2: [msg@offset:0] [msg@offset:1] ...
└── ...（共 16 Partitions）

Consumer Group: flink-bronze-consumer
├── Consumer A  → 讀取 Partition 0, 1, 2, 3
├── Consumer B  → 讀取 Partition 4, 5, 6, 7
├── Consumer C  → 讀取 Partition 8, 9, 10, 11
└── Consumer D  → 讀取 Partition 12, 13, 14, 15
```

**關鍵設計決策**：
- **Partition（分區）**：Topic 的水平分片單位，決定了最大並行消費度。Partition 數量 = 最大 Consumer 並行數。
- **Offset（偏移量）**：訊息在 Partition 中的唯一序號。Consumer 可以重播任意 Offset 的訊息。
- **Replication（副本）**：每個 Partition 有一個 Leader 和多個 Follower。Leader 崩潰時，Follower 自動晉升，實現高可用。
- **Retention（保留）**：訊息不會在被消費後刪除，而是根據時間或大小閾值刪除（預設 7 天）。
- **Log Compaction（日誌壓縮）**：特殊模式，保留每個 Key 的最新一條訊息，適合 CDC 事件。

### 傳遞語意

| 語意 | 說明 | 適用場景 |
|---|---|---|
| At-Most-Once | 訊息可能遺失，但不會重複 | 日誌、指標（允許少量遺失） |
| At-Least-Once | 訊息不會遺失，但可能重複 | 一般業務事件（下游要做去重） |
| Exactly-Once | 不遺失也不重複（需 Kafka Transactions） | 金融交易、帳務系統 |

本 POC 使用 **Exactly-Once**，搭配 Flink Checkpoint 和 Iceberg Two-Phase Commit 實現端到端保證。

### 優勢

- **極高吞吐量**：單 broker 可達每秒數百萬訊息（取決於硬體與訊息大小）
- **持久化**：訊息寫入磁碟，重啟後不遺失
- **解耦**：Producer 和 Consumer 完全獨立，Consumer 可以獨立控制消費速度
- **可重播（Replayability）**：Consumer 可以隨時回到任意 Offset 重新消費

### 限制

- 運維複雜度高（Broker、Zookeeper/KRaft、Partition Rebalance）
- Partition 數量設定後難以縮減（只能增加）
- 不原生支援延遲訊息或優先級佇列

### 雲端對應服務

| 雲端 | 服務名稱 | 特色 | 選擇建議 |
|---|---|---|---|
| **AWS** | **Amazon MSK**（Managed Streaming for Apache Kafka） | 完全相容 Kafka API，支援 KRaft 模式，Multi-AZ 自動 HA | AWS 原生優先選擇；另有 MSK Serverless（按用量計費）|
| **AWS** | **Amazon Kinesis Data Streams** | 非 Kafka 協議，與 AWS 生態系深度整合，Serverless | 不需遷移現有 Kafka 代碼時考慮 |
| **GCP** | **Google Cloud Pub/Sub** | 全託管、全球分散式、自動擴縮，但不相容 Kafka 協議 | GCP 原生應用，不需 Kafka 生態系時選用 |
| **Azure** | **Azure Event Hubs** | 提供 Kafka 協議端點（Kafka API 相容），但有部分限制 | Azure 生態系，且現有 Kafka 代碼需遷移時 |
| **Multi-Cloud** | **Confluent Cloud** | 完整 Kafka + Flink + Schema Registry 一體化平台，三雲均支援 | 需要完整 Confluent 平台能力或跨雲部署 |

---

## 2. Confluent Schema Registry

### 背景

Schema Registry 是 Confluent 在 2015 年開源的元件，解決 Kafka 訊息格式管理問題。  
在沒有 Schema Registry 的情況下，Producer 和 Consumer 對訊息格式的理解完全依賴「口頭約定」，一旦欄位更改，Consumer 就會崩潰。

### 核心原理

```
Producer                Schema Registry              Consumer
  │                           │                          │
  ├──"我要寫 StockTrade"──────▶│                          │
  │◀──Schema ID: 42 ──────────│                          │
  │                           │                          │
  ├──[Magic Byte][Schema ID 42][Avro Payload]────────────▶│
  │                           │◀── "Schema 42 是什麼？" ─┤
  │                           │──────── Schema 定義 ─────▶│
  │                           │                          │
```

每個 Avro 訊息的前 5 個 Byte 包含：
- `0x00`（Magic Byte）：標識這是 Schema Registry 訊息
- `schema_id`（4 Bytes）：對應 Schema Registry 中的版本 ID

**相容性策略**：

| 策略 | 說明 | 適用場景 |
|---|---|---|
| `BACKWARD` | 新 Schema 可以讀取舊資料（預設）| 新增 optional 欄位 |
| `FORWARD` | 舊 Schema 可以讀取新資料 | 刪除 optional 欄位 |
| `FULL` | 雙向相容 | 嚴格 Schema 管控 |
| `BACKWARD_TRANSITIVE` | 新 Schema 可讀取所有歷史版本 | 本 POC 推薦設定 |

### 雲端對應服務

| 雲端 | 服務 | 說明 |
|---|---|---|
| **AWS** | AWS Glue Schema Registry | 免費，支援 Avro/JSON/Protobuf，與 MSK 整合 |
| **GCP** | Confluent Cloud Schema Registry | Pub/Sub 本身無原生 Schema Registry，需用 Confluent Cloud |
| **Azure** | Azure Schema Registry（Event Hubs） | 支援 Avro/JSON，與 Event Hubs 深度整合 |

---

## 3. Apache Flink 2.0

### 背景

Flink 源於 2010 年的柏林工業大學研究計畫「Stratosphere」，2014 年加入 Apache 基金會。  
2016 年後被 Alibaba 大規模採用（雙十一即時資料處理），成為全球最高吞吐量的串流引擎之一。  
2025 年 3 月 Apache Flink 2.0 正式發布，帶來 Materialized Table、全新的批流統一 API 等重大更新。  
Netflix、Uber、Airbnb、Stripe 等頂尖科技公司均在生產環境大規模使用 Flink。

### 核心原理

Flink 將一切資料都視為**流（Stream）**：有界批次資料是「有限長度的流」，無界串流是「無限長度的流」，兩者統一使用同一套 API 和執行引擎。

```
                 Flink 執行模型

Source → [Operator 1] → [Operator 2] → [Operator 3] → Sink
            ↕ State         ↕ State
         (RocksDB)       (RocksDB)
            ↕ Checkpoint
         (S3/MinIO)
```

**時間語意**：

| 時間類型 | 說明 | 使用場景 |
|---|---|---|
| **Event Time** | 事件發生的實際時間（資料中的 `trade_ts`）| 準確計算，需要 Watermark 處理遲到事件 |
| **Processing Time** | Flink 處理事件的當前系統時間 | 低延遲，但結果不可重現 |
| **Ingestion Time** | 事件進入 Flink 的時間 | 介於兩者之間，較少使用 |

本 POC 使用 **Event Time**，配合 Watermark 處理 IEX 資料可能的亂序情況。

**Watermark 機制**：

```
Event Stream: [e@10:00:01] [e@10:00:03] [e@10:00:02 ← 遲到] [e@10:00:05]
                                                      ↑
Watermark 策略: trade_ts - 200ms                 這筆在 Watermark 推進後到達
                                                  若已過容忍期限，該事件被丟棄或路由到 DLQ
```

**Checkpoint 機制（Exactly-Once 的基礎）**：

Flink 使用 **Chandy-Lamport 分散式快照演算法** 定期為所有 Operator 的狀態打快照：
1. JobManager 向所有 Source 插入 Checkpoint Barrier 訊號
2. Barrier 像「水位標記」一樣在資料流中流動
3. 每個 Operator 收到 Barrier 時，將自身狀態寫入 Checkpoint Storage（MinIO/S3）
4. 全部 Operator 完成後，JobManager 確認此 Checkpoint 有效
5. 崩潰時，所有 Operator 從最新 Checkpoint 恢復，Kafka Offset 也回退到對應位置

### Flink 2.0 新特性

- **Materialized Table**：宣告式資料管道，自動決定 Streaming 或 Batch 執行模式
- **Dynamic Iceberg Sink**：自動偵測 Schema 變化，動態新增 Iceberg 欄位
- **Disaggregated State Management**：State Backend 從 TaskManager 本地分離，支援更大的狀態

### 優勢

- **真正低延遲**：event-by-event 處理，延遲可達毫秒級（Spark Streaming 是微批，延遲秒級）
- **精確的 Exactly-Once**：端到端 Exactly-Once 語意，不需下游去重
- **Stateful Processing**：強大的狀態管理，天然支援跨事件的聚合、Join、Pattern Detection
- **批流統一**：同一套代碼可以跑 Batch 和 Streaming，簡化維護

### 雲端對應服務

| 雲端 | 服務 | 說明 |
|---|---|---|
| **AWS** | **Amazon Managed Service for Apache Flink** | 2023 年從 Kinesis Data Analytics 改名，完全託管的 Flink 服務，支援 Java/Python/SQL API，與 Kinesis/MSK 深度整合，自動擴縮 |
| **GCP** | **Google Cloud Dataflow** | 基於 Apache Beam 的託管串流服務；也可透過 Confluent Cloud for Flink 在 GCP 使用完整 Flink |
| **Azure** | **Azure HDInsight on AKS（Flink）** | 在 K8s 上的託管 Flink；也可使用 Azure Stream Analytics（非 Flink 相容） |
| **Multi-Cloud** | **Confluent Cloud for Apache Flink** | 2024 年 GA，三雲均可使用，與 Confluent Kafka 深度整合，全 Serverless |

---

## 4. Apache Iceberg

### 背景

Iceberg 由 Netflix 工程師 Ryan Blue 等人於 2017 年設計，2018 年開源捐贈給 Apache 基金會，  
2020 年成為 Apache 頂級專案。設計目的是解決傳統 Hive 表格格式在大規模資料湖中的諸多問題：  
無法原子性更新、Schema 版本管理混亂、Partition 規則無法修改、並發讀寫衝突等。

### 核心原理

Iceberg 不是一個資料庫，而是**一種定義如何管理資料集合的開放規範（Table Format Specification）**。

```
Iceberg Table Architecture

Catalog（元數據入口）
    └── Table Metadata (JSON)
           ├── Current Schema
           ├── Partition Spec
           ├── Snapshot List
           └── Current Snapshot
                    └── Manifest List
                           ├── Manifest File A
                           │    ├── data/part-0001.parquet
                           │    └── data/part-0002.parquet
                           └── Manifest File B
                                ├── data/part-0003.parquet
                                └── data/part-0004.parquet
```

**ACID 的實現方式**：  
Iceberg 使用**樂觀並發控制（Optimistic Concurrency Control）**：
- 每次寫入都建立新的 Snapshot（快照），不覆蓋舊資料
- 提交時用 Compare-And-Swap 確保原子性
- 讀取永遠看到一個一致的快照版本，不會讀到「寫到一半」的資料

**Time Travel 的實現**：  
因為每個 Snapshot 都完整保存，查詢時只需指定要讀取哪個 Snapshot 的 Manifest，即可「穿越時間」：
```sql
SELECT * FROM iceberg.silver.vwap_1min
FOR TIMESTAMP AS OF TIMESTAMP '2026-05-08 09:30:00';
```

**Schema Evolution（無需重寫資料）**：
- 新增欄位：舊檔案讀取時，新欄位自動填 NULL
- 刪除欄位：舊檔案中的欄位被忽略
- 重命名欄位：透過欄位 ID（而非名稱）追蹤，重命名後舊檔案仍可正確讀取

### 優勢

- **ACID 事務**：支援原子性的 INSERT/UPDATE/DELETE，確保資料一致性
- **Time Travel**：可查詢任意歷史時間點，支援資料審計與回滾
- **Schema Evolution**：欄位增刪改不需重寫資料，Flink Job 不需重啟
- **Partition Evolution**：分區策略可以在線修改（從按月改為按天），舊資料仍可查詢
- **Multi-Engine 支援**：Flink、Spark、Trino、Hive 等均可讀寫同一張 Iceberg 表

### 雲端對應服務

| 雲端 | Iceberg 整合方式 | Catalog 服務 | 說明 |
|---|---|---|---|
| **AWS** | S3 儲存 + AWS Glue Catalog | **AWS Glue Data Catalog**（Iceberg REST 端點） | Glue 5.0 支援 Iceberg 1.6.1；也可用 Amazon S3 Tables（2024 新服務，內建 Iceberg） |
| **GCP** | GCS 儲存 + BigLake Managed Tables | **BigLake / BigQuery Iceberg** | GCP 透過 BigLake Managed Tables 支援 Iceberg；BigQuery 可直接查詢 Iceberg 外部表 |
| **Azure** | ADLS Gen2 儲存 + Polaris Catalog | **Azure Fabric OneLake + Polaris** | Microsoft Fabric 透過 OneLake 支援 Iceberg；可搭配 Polaris（Snowflake 開源的 Iceberg Catalog） |

---

## 5. MinIO

### 背景

MinIO 創立於 2014 年，是一個高性能的開源物件儲存系統，完全相容 AWS S3 API。  
設計目標是成為「私有雲 / 本地環境的 S3」，讓企業在自有硬體上實現雲端等級的物件儲存能力。

### 核心原理

MinIO 採用 **Erasure Coding（糾刪碼）** 而非傳統的 3 副本複製：
- 將一個物件拆分成 K 個資料片段 + M 個校驗片段
- 任意 M 個片段損壞都可以重建完整物件
- 相較於 3 副本，空間效率提升 50% 以上

在本 POC 中，MinIO 扮演 **S3 的本地替代品**，是 Iceberg Parquet 檔案的實際儲存後端。

### 雲端對應服務

| 雲端 | 對應服務 | 說明 |
|---|---|---|
| **AWS** | **Amazon S3** | 業界標準物件儲存，MinIO 完全相容其 API，生產環境直接替換 |
| **GCP** | **Google Cloud Storage (GCS)** | 需調整 Flink/Spark 的 `fs.defaultFS` 配置 |
| **Azure** | **Azure Data Lake Storage Gen2 (ADLS)** | 支援 Hierarchical Namespace，適合大規模分析工作負載 |

---

## 6. Apache Airflow

### 背景

Airflow 由 Airbnb 工程師 Maxime Beauchemin 於 2014 年創建，2016 年加入 Apache 孵化器，  
2019 年成為 Apache 頂級專案。Airflow 的核心理念是「**Configuration as Code**」——用 Python 程式碼定義工作流程（DAG）。

### 核心原理

Airflow 圍繞 **DAG（Directed Acyclic Graph，有向無環圖）** 組織工作流程：

```
DAG: nasdaq_iceberg_maintenance
│
├── compact_bronze_trades
│       │
│       ▼
├── merge_silver_daily_ohlcv
│       │
│       ▼
├── build_gold_top_movers
│       │
│       ▼
└── expire_old_snapshots
```

**核心元件**：
- **Scheduler**：解析 DAG 定義，按排程觸發 Task 執行
- **Executor**：決定 Task 在哪裡執行（本地、Celery 分散式、Kubernetes 上的 Pod）
- **Metadata DB**：儲存 DAG 定義、Task 執行歷史、狀態
- **Webserver**：提供 UI 介面，查看 DAG 執行狀態、Log、觸發手動執行

**在本 POC 中的角色**：  
Airflow 不參與 Streaming 的即時處理，而是負責「Streaming 的維護工作」——  
每小時執行 Iceberg Compaction、Bronze → Silver MERGE、清理過期 Snapshots。  
這是「Streaming + Batch Hybrid」架構中 Airflow 的典型角色。

### 優勢

- **Python 原生**：DAG 用 Python 撰寫，可利用所有 Python 生態系（判斷式、迴圈、動態生成 Task）
- **豐富的 Operator**：內建 200+ Operator，支援 Spark、Flink、Kubernetes、SQL、S3 等
- **可視化監控**：DAG 執行圖、每個 Task 的執行時間、成功/失敗歷史一目了然
- **Backfill**：可以重新執行歷史日期的 DAG，彌補遺漏的執行

### 雲端對應服務

| 雲端 | 服務 | 說明 |
|---|---|---|
| **AWS** | **Amazon MWAA**（Managed Workflows for Apache Airflow） | 完全託管的 Airflow，與 S3/Glue/EMR/Redshift 深度整合 |
| **GCP** | **Cloud Composer** | 基於 GKE 的託管 Airflow，與 BigQuery/GCS/Dataflow 深度整合 |
| **Azure** | **Azure Data Factory + Airflow Integration Runtime** | ADF 原生支援 Airflow DAG 執行；也可用 AKS 自託管 |
| **SaaS** | **Astronomer** | 商業版 Airflow SaaS，三雲均支援，企業級功能 |

---

## 7. Trino

### 背景

Trino（前身 PrestoSQL）源自 Facebook 於 2012 年開發的 Presto 分散式 SQL 引擎。  
2019 年原始開發者 Martin Traverso 等人離開 Facebook，以 Trino 名稱繼續開發開源版本。  
Trino 的設計目標是「**聯邦查詢（Federated Query）**」——用單一 SQL 查詢跨越多個異質資料來源（S3、RDBMS、Iceberg、Hive 等）。

### 核心原理

```
Client SQL
    │
    ▼
Trino Coordinator（查詢計畫、優化、排程）
    │
    ├── Worker 1 → 讀取 Iceberg Parquet on MinIO（Partition 0, 1）
    ├── Worker 2 → 讀取 Iceberg Parquet on MinIO（Partition 2, 3）
    └── Worker 3 → 讀取 Iceberg Parquet on MinIO（Partition 4, 5）
    │
    ▼
聚合結果 → 返回 Client
```

Trino 不儲存任何資料，完全是「查詢層」。它透過 **Connector** 機制支援不同資料來源，  
Iceberg Connector 直接讀取 Parquet 檔案並下推 Filter（Predicate Pushdown），避免全表掃描。

### 優勢

- **No-ETL 查詢**：直接查詢 Iceberg Parquet，不需先把資料載入資料庫
- **多源聯邦查詢**：一個 SQL 可以 JOIN Iceberg 表和 PostgreSQL 表
- **標準 SQL**：完整支援 ANSI SQL，Superset / BI 工具可直接連接
- **Iceberg 原生支援**：支援 Time Travel、Schema Evolution 查詢

### 雲端對應服務

| 雲端 | 服務 | 說明 |
|---|---|---|
| **AWS** | **Amazon Athena** | Serverless SQL，直接查詢 S3 上的 Parquet/Iceberg，按掃描量計費 |
| **GCP** | **BigQuery** | Serverless 資料倉儲，原生支援讀取 GCS 上的 Iceberg 表（BigLake External Tables） |
| **Azure** | **Azure Synapse Analytics** | 支援 Synapse Serverless SQL Pool 直接查詢 ADLS 上的 Parquet/Delta/Iceberg |

---

## 8. Apache Superset

### 背景

Superset 由 Airbnb 工程師 Maxime Beauchemin（同時也是 Airflow 作者）於 2015 年創建，  
2017 年捐贈給 Apache 基金會，2021 年成為頂級專案。  
設計目標是提供一個輕量、易用、可嵌入的開源 BI 工具。

### 核心原理

Superset 是一個純 Query Layer 的 BI 工具，本身**不儲存任何業務資料**：
- 透過 SQLAlchemy 連接資料庫（Trino、PostgreSQL、BigQuery、Redshift 等）
- 在 Superset 的 Metadata DB 中儲存 Dashboard 定義、Chart 配置
- 每次瀏覽 Dashboard 時，動態向後端資料庫發送 SQL 查詢

本 POC 中：Superset → Trino → Iceberg Parquet on MinIO

### 雲端對應服務

| 雲端 | 服務 | 說明 |
|---|---|---|
| **AWS** | Amazon QuickSight | 原生 SaaS BI，可連接 Athena/Redshift/S3 |
| **GCP** | Looker / Looker Studio | Looker 是企業版，Looker Studio 免費，可連接 BigQuery |
| **Azure** | Power BI | Microsoft 生態系標準 BI，深度整合 Azure Synapse / ADLS |
| **Multi-Cloud** | Grafana（資料面板）/ Metabase（輕量 BI） | 開源替代方案 |

---

## 9. Prometheus + Grafana

### Prometheus 背景與原理

Prometheus 由 SoundCloud 於 2012 年開發，2016 年成為 CNCF 第二個畢業專案（僅次於 Kubernetes）。  
採用**Pull 模型**：Prometheus 主動定期抓取（Scrape）各服務暴露的 `/metrics` HTTP 端點，  
而非由各服務主動推送。這讓 Prometheus 天然支援服務探索和動態擴縮。

資料以**時間序列（Time Series）**格式儲存，每條時間序列由指標名稱和 Label 組合唯一標識：
```
kafka_consumer_group_lag{topic="nasdaq-trades", partition="3", group="flink-consumer"} 42
                         ↑──────── Labels ────────────────────────────────────────── ↑ Value
```

### Grafana 背景與原理

Grafana 於 2014 年由 Torkel Ödegaard 開發，是最廣泛使用的指標視覺化工具。  
Grafana 本身只是「視覺化層」，它連接各種資料來源（Prometheus、InfluxDB、Loki、Tempo 等），  
將時間序列資料渲染為折線圖、熱力圖、Gauge 等視覺化元素。

### 雲端對應服務

| 雲端 | 服務 | 說明 |
|---|---|---|
| **AWS** | Amazon CloudWatch | 完全託管的監控服務，原生整合所有 AWS 服務；可透過 Container Insights 監控 EKS |
| **GCP** | Google Cloud Monitoring（Stackdriver） | 支援 Prometheus 格式指標，可用 Grafana 連接 |
| **Azure** | Azure Monitor + Azure Managed Grafana | 2022 年推出 Azure Managed Grafana，完全託管，整合 Azure Monitor |
| **SaaS** | Grafana Cloud | 商業版 Grafana，免費 Tier 可用，整合 Prometheus/Loki/Tempo |

---

## 10. OpenLineage + Marquez

### 背景

OpenLineage 於 2021 年由 Astronomer 公司發起，目標是為資料工程建立一個**開放的資料血緣標準（Open Standard for Data Lineage）**。  
類似 Kafka 之於訊息串流，OpenLineage 定義了一套統一的事件格式，讓 Flink / Spark / Airflow / dbt 等不同工具都能用同一語言「說清楚」資料的來龍去脈。

Marquez 是 OpenLineage 的參考實作（Reference Implementation），提供收集、儲存、查詢、視覺化血緣資料的完整後端。

### 核心概念

```
OpenLineage Event 的基本結構：

{
  "eventType": "COMPLETE",
  "job": {
    "namespace": "nasdaq-poc",
    "name": "nasdaq_vwap_calculator"    ← 哪個 Job
  },
  "inputs": [{
    "namespace": "kafka",
    "name": "nasdaq-trades"             ← 讀取哪些資料
  }],
  "outputs": [{
    "namespace": "iceberg",
    "name": "silver.vwap_1min"          ← 寫入哪些資料
  }],
  "run": {
    "runId": "uuid",
    "facets": { "processing_engine": {...} }
  }
}
```

### 重要性（Senior DE 的視角）

資料血緣讓你能回答兩個關鍵問題：
1. **上游影響分析（Impact Analysis）**：「Kafka Topic `nasdaq-trades` 壞了，哪些下游 Dashboard 會受影響？」
2. **下游追蹤（Data Tracing）**：「Silver VWAP 表的數值異常，是哪個 Flink Job / Kafka Topic 造成的？」

### 雲端對應服務

| 雲端 | 服務 | 說明 |
|---|---|---|
| **AWS** | **Amazon DataZone** | 含資料血緣、資料目錄、治理功能的整合平台 |
| **GCP** | **Google Dataplex + Data Catalog** | 自動抓取 BigQuery/GCS 的資料血緣，可視化展示 |
| **Azure** | **Microsoft Purview** | 企業級資料治理平台，支援跨雲自動血緣爬取 |

---

## 11. Flink Kubernetes Operator

### 背景

Flink Kubernetes Operator 是 Apache Flink 社群於 2022 年推出的官方 K8s 擴展，  
2023 年達到 Production Ready 狀態。它讓工程師用 **Kubernetes CRD（Custom Resource Definition）**  
以宣告式方式管理 Flink Job 的完整生命週期，而不需寫複雜的 Shell Script。

### 核心能力

| 功能 | 說明 |
|---|---|
| **宣告式部署** | 用 YAML 定義 Flink Job，Operator 負責實現（類似 Deployment 管理 Pod）|
| **自動重啟** | Job 崩潰時，Operator 從最新 Checkpoint 自動恢復，無需人工介入 |
| **零停機升級** | `upgradeMode: savepoint` — 升級前自動建立 Savepoint，升級後從 Savepoint 恢復，資料零遺失 |
| **Savepoint 管理** | 可排程定期觸發 Savepoint（如每 6 小時），作為長期備份 |
| **Job 暫停/恢復** | 用 `spec.job.state: suspended` 暫停 Job（釋放資源），再恢復時從最新 Savepoint 繼續 |

### 雲端對應服務

| 雲端 | 方式 | 說明 |
|---|---|---|
| **AWS** | Amazon EKS + Flink K8s Operator | 在 EKS 上安裝 Operator，State 存 S3；或使用 Amazon Managed Service for Apache Flink（全托管）|
| **GCP** | Google GKE + Flink K8s Operator | 在 GKE 上安裝 Operator，State 存 GCS |
| **Azure** | Azure AKS + Flink K8s Operator | 在 AKS 上安裝 Operator，State 存 ADLS |

---

## 12. Alpaca Markets WebSocket

### 背景

Alpaca 是一家成立於 2015 年的 FinTech 公司，提供針對程式化交易的 API-First 券商服務。  
2019 年推出零手續費股票交易 API，快速成為 Algo Trading 社群最受歡迎的資料來源之一。  
Alpaca 的市場資料 API 基於 **WebSocket** 協議，支援即時 Trades、Quotes、Bars 串流。

### 免費方案限制

| 項目 | Free（IEX）| 說明 |
|---|---|---|
| 資料來源 | IEX Exchange | IEX 約佔全美股市成交量的幾個百分點，非全市場代表 |
| WebSocket 訂閱 | 最多 30 個 symbols | 超過需升級付費方案 |
| 歷史資料 | 2016 年至今，15 分鐘延遲 | 即時 WebSocket 無延遲，但 REST API 歷史查詢有延遲 |

### 資料類型

| Stream | 說明 | 典型頻率（熱門股開盤時）|
|---|---|---|
| `subscribe_trades` | 每筆實際成交（price, size, exchange, tape）| 每秒 1～10 筆 |
| `subscribe_quotes` | 即時最優 bid/ask（NBBO）更新 | 每秒 10～50 筆 |
| `subscribe_bars` | 1 分鐘 OHLCV 已聚合棒 | 每分鐘 1 筆 |

### 升級路徑（當需要更大流量時）

| 方案 | 費用 | 資料來源 | 訂閱數 |
|---|---|---|---|
| Free | $0 | IEX | 30 symbols |
| Algo Trader Plus | $99/月 | SIP（全市場）| 無限制 |
| 更高流量需求 | - | Polygon.io / Databento | - |

---

## 技術選型對照總表（本地 POC vs 雲端生產）

| 層級 | 本地 POC 技術 | AWS | GCP | Azure |
|---|---|---|---|---|
| **分散式協調** | Apache ZooKeeper | 內建於 MSK（不需管理）| 不需要 | 不需要 |
| **訊息佇列** | Apache Kafka | Amazon MSK | Cloud Pub/Sub | Azure Event Hubs |
| **Schema 管控** | Confluent Schema Registry | AWS Glue Schema Registry | Confluent Cloud SR | Azure Schema Registry |
| **串流處理** | Apache Flink 2.0 | Amazon Managed Flink | Cloud Dataflow / Confluent Flink | HDInsight Flink / Confluent Flink |
| **表格格式** | Apache Iceberg | S3 + AWS Glue Catalog | GCS + BigLake | ADLS + Polaris |
| **物件儲存** | MinIO | Amazon S3 | Google Cloud Storage | Azure Data Lake Storage Gen2 |
| **Batch 協調** | Apache Airflow | Amazon MWAA | Cloud Composer | Azure Data Factory + Airflow |
| **SQL 查詢引擎** | Trino | Amazon Athena | BigQuery | Azure Synapse Serverless |
| **BI 視覺化** | Apache Superset | Amazon QuickSight | Looker / Looker Studio | Power BI |
| **指標監控** | Prometheus + Grafana | CloudWatch + Managed Grafana | Cloud Monitoring + Grafana | Azure Monitor + Azure Managed Grafana |
| **資料血緣** | OpenLineage + Marquez | Amazon DataZone | Google Dataplex | Microsoft Purview |
| **容器編排** | Kubernetes + Flink Operator | Amazon EKS | Google GKE | Azure AKS |

---

*文件整理：NASDAQ Streaming POC 技術深度手冊 | 2026-05*
