# Kafka 知識庫

> 這個專案用到的 Kafka 概念整理。指令都以本專案的環境為準
> （namespace `streaming`、StatefulSet `broker`、bootstrap `kafka-bootstrap:9092`）。
>
> 部署相關的決策與踩坑見 `doc/updated_plan/week7-decisions.md`。

---

## 目錄

1. [核心概念地圖](#1-核心概念地圖)
2. [Topic / Partition / Replica](#2-topic--partition--replica)
3. [Leader 與寫入路徑](#3-leader-與寫入路徑)
4. [ISR — 判定與意義](#4-isr--判定與意義)
5. [acks 與 min.insync.replicas](#5-acks-與-mininsyncreplicas)
6. [KRaft 與 Quorum](#6-kraft-與-quorum)
7. [Listener 模型](#7-listener-模型)
8. [Retention 與磁碟規劃](#8-retention-與磁碟規劃)
9. [常用指令速查](#9-常用指令速查)
10. [監控該盯什麼](#10-監控該盯什麼)

---

## 1. 核心概念地圖

```
Topic（market-trades）
  └── Partition 0 ─┬─ Replica on broker-0  ← Leader（唯一處理讀寫）
  │                ├─ Replica on broker-1  ← Follower（只複製）
  │                └─ Replica on broker-2  ← Follower
  ├── Partition 1 ─┬─ Replica on broker-1  ← Leader
  │                └─ ...
  └── ... 共 16 個分區
```

三個層次不要搞混：

| 層次 | 是什麼 | 決定什麼 |
|---|---|---|
| **Topic** | 邏輯上的資料流名稱 | 資料怎麼分類 |
| **Partition** | Topic 的水平切分 | **平行度上限** |
| **Replica** | Partition 的副本 | **容錯能力** |

---

## 2. Topic / Partition / Replica

### `--describe` 輸出怎麼讀

```
Topic: market-trades  TopicId: 6bNu3S1PSMKlHSIlHz_adg
PartitionCount: 16    ReplicationFactor: 3
Configs: min.insync.replicas=2,segment.bytes=67108864,retention.bytes=268435456

  Partition: 0  Leader: 0  Replicas: 0,1,2  Isr: 0,1,2  Elr:  LastKnownElr:
  Partition: 1  Leader: 1  Replicas: 1,2,0  Isr: 0,1,2  Elr:  LastKnownElr:
  Partition: 2  Leader: 2  Replicas: 2,0,1  Isr: 0,1,2  Elr:  LastKnownElr:
```

| 欄位 | 意義 |
|---|---|
| `TopicId` | Kafka 內部 UUID。KRaft 用它識別 topic，不是用名字。**刪掉重建同名 topic 會拿到不同 id**，避免 client 誤混新舊資料 |
| `PartitionCount` | 平行度上限 —— 最多這麼多 consumer 同時讀同一個 consumer group |
| `Leader: 0` | 這個分區的讀寫都只經過 broker-0 |
| `Replicas: 0,1,2` | 指派的副本清單。**順序有意義**，第一個是 preferred leader |
| `Isr: 0,1,2` | 目前跟得上的副本 —— **最需要盯的一欄** |
| `Elr` / `LastKnownElr` | Eligible Leader Replicas（KIP-966）。健康時是空的 |

### Replicas 順序為什麼要輪替

```
Partition 0 → Replicas: 0,1,2   → preferred leader = broker-0
Partition 1 → Replicas: 1,2,0   → preferred leader = broker-1
Partition 2 → Replicas: 2,0,1   → preferred leader = broker-2
```

Kafka 刻意輪替，讓 leader 平均分佈在三台。
**負載平衡靠的是分區，不是副本** —— follower 不服務流量。

故障恢復後，Kafka 會自動把 leader 換回 preferred leader（preferred leader election）。

### Elr（Eligible Leader Replicas）

處理一個舊有的兩難：ISR 裡的副本全掛時，要不要讓落後的副本當 leader？

- 讓 → 可能遺失資料（unclean leader election）
- 不讓 → 分區完全不可用

ELR 追蹤「暫時不在 ISR、但資料其實完整」的副本，讓 Kafka 在災難時做更好的選擇。
**健康時為空是正常的。**

---

## 3. Leader 與寫入路徑

```
① Producer 決定分區
     有 key   → hash(key) % partitionCount
     沒有 key → 輪流分配
              ↓
② 查 metadata：「partition 3 的 leader 是誰？」→ broker-1
              ↓
③ 直接連 broker-1.kafka.streaming.svc.cluster.local:9092
   （不經過代理——這就是 advertised listener 的用途）
              ↓
④ leader append 到自己的 log
              ↓
⑤ follower 主動「拉」新資料（pull，不是 push）
              ↓
⑥ 依 acks 設定，等到足夠副本寫入後回 ack
```

### 兩個容易誤解的點

**複製是「拉」不是「推」。**
Follower 像 consumer 一樣主動去 leader 拉。所以 follower 慢的時候只是「拉得慢」，
不會拖垮 leader。ISR 判定的就是「你上次拉到哪、落後多久」。

**讀取也只找 leader，而且只讀得到 high watermark 以前。**

```
leader log:  [1][2][3][4][5]
                       ↑
                  high watermark（已被足夠副本確認的位置）

[4][5] 已寫進 leader 但還沒被足夠副本確認 → consumer 看不到
```

避免「讀到後來因 leader 故障而消失的資料」。

> 例外：KIP-392 允許 consumer 從最近的 follower 讀，用來省跨區流量費。
> 本專案單 zone 用不到，但面試被問「follower 除了容錯還有什麼用」可以提。

---

## 4. ISR — 判定與意義

### 判定規則（只有一條）

> follower 在最近 `replica.lag.time.max.ms`（預設 30 秒）內，
> 曾經追上 leader 的 Log End Offset → 留在 ISR
> 超過這段時間沒追上 → 踢出 ISR

leader 為每個 follower 記一個「上次完全追上」的時間戳：

```
leader LEO = 1000
follower A 拉到 1000 → 時間戳更新
follower B 拉到  950 → 時間戳不動
                     ↓ 30 秒後仍沒追上
              leader 把 B 踢出 ISR
```

追回來會自動加回去，不需人工介入。

### 為什麼用「時間」不用「落後幾筆」

早期有 `replica.lag.max.messages`（落後 N 筆就踢出），**後來廢除**，因為流量突增時誤判：

```
平常每秒 100 筆   → follower 落後 50 筆，正常
突然湧入 10000 筆 → follower 瞬間落後 5000 筆
                  → 被誤判成掛掉、踢出 ISR
                  → ISR 剩 1 個 → 寫入被拒
                  → 明明只是忙碌，卻變成故障
```

時間判準沒這問題 —— follower 只要持續在追，時間戳就持續更新。

### 誰記錄 ISR

```
leader 發現 follower 落後 → 更新 ISR → 通知 KRaft controller
  → controller 寫進叢集 metadata → kafka-topics --describe 讀到的就是這份
```

**ISR 是動態的**，每次 describe 可能不同。

---

## 5. acks 與 min.insync.replicas

### `acks`（Producer 端設定）

| `acks` | leader 何時回 ack | 風險 |
|---|---|---|
| `0` | 送出就算成功 | 最快，可能整批遺失 |
| `1` | leader 自己寫入即可 | leader 立刻掛掉 → 資料沒了 |
| **`all`** | ISR 裡的副本都寫入 | 最慢但最安全 |

### `min.insync.replicas`（Broker/Topic 端設定）

只有在 `acks=all` 時才生效。兩者搭配：

```
acks=all + min.insync.replicas=2

ISR 有 3 個 → 等 3 個都寫入 → ack
ISR 剩 2 個 → 等 2 個都寫入 → ack（仍可運作）
ISR 剩 1 個 → 2 > 1 → ❌ 拒絕寫入，回 NOT_ENOUGH_REPLICAS
```

**寧可拒絕寫入，也不接受「只有一份副本」的資料** —— 那一份掛了資料就永久消失。

> ⚠️ **Producer 沒設 `acks=all` 的話，RF=3 和 `min.insync.replicas=2` 全是白做的。**
> 這是 Week 8 接 Producer 時最容易漏掉的一行。

### 本專案的設定

```
RF = 3
min.insync.replicas = 2
→ 可容忍 1 個 broker 故障，仍能正常寫入
→ 掉 2 個就會拒絕寫入（刻意的：保護資料而非維持可用性）
```

---

## 6. KRaft 與 Quorum

### Quorum = 多數決

用來防止 **split-brain（腦裂）**：

```
網路斷成兩半：
  broker-0（孤立）  |  broker-1, broker-2

3 個節點 → 多數 = 2
  broker-0 那邊：1 < 2 → 停止服務 ✋
  另一邊：      2 ≥ 2 → 繼續運作 ✅
```

「超過半數」的集合不可能同時存在兩個，所以永遠只有一邊能動。

### 為什麼用奇數

| 總數 | 多數 | 可容忍故障 |
|---|---|---|
| 3 | 2 | **1** |
| 4 | 3 | 1 ← 沒有變好，卻多付一台的錢 |
| 5 | 3 | 2 |

### 狀態怎麼看

```bash
kubectl exec -n streaming broker-0 -- \
  kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status
```

```
LeaderId:       0            ← 被選出的 controller
LeaderEpoch:    46           ← 第 46 屆（每次改選 +1）
CurrentVoters:  [0, 1, 2]    ← 有投票權的成員
HighWatermark:  3068         ← 已被多數確認的位置
MaxFollowerLag: 0            ← 落後 0，完全同步
```

`LeaderEpoch` 偏高代表改選頻繁 —— 在 Spot 環境很正常（每次 broker 重建就要重選）。

### ⚠️ 別和 ISR 搞混

| | KRaft Quorum | ISR |
|---|---|---|
| 管什麼 | **叢集 metadata**（有哪些 topic、誰是 leader） | **你的訊息資料** |
| 設定 | `controller.quorum.voters` | `min.insync.replicas` |
| 前身 | ZooKeeper | 一直都是 Kafka 自己 |

兩者都設 2 是巧合（都因為 3 節點的多數是 2），機制完全獨立。

---

## 7. Listener 模型

### 兩個設定的差別

> **`listeners`** = 我在哪裡接聽（綁定位址）
> **`advertised.listeners`** = 我告訴 client「請用這個位址找我」

必須分開的原因：**broker 自己看到的位址，不等於 client 能用的位址**（NAT、容器、埠對應、雲端）。

### 為什麼 client 必須連到特定 broker

```
① client 連 bootstrap（任一 broker）
② client 問：「這個 topic 的分區資訊？」
③ broker 回：「broker-0 → X:9092, broker-1 → Y:9092 ...
              partition 0 的 leader 是 broker-1」
              ↑ 這些位址來自 advertised.listeners
④ client 要寫 partition 0 → 直接連 broker-1 的那個位址
```

**client 無條件相信第 ③ 步回傳的位址。** 那個位址在 client 的網路裡連不到，第 ④ 步就死。

### 三種寫錯的方式

| 寫成 | 症狀 |
|---|---|
| `0.0.0.0` | client 拿到 0.0.0.0，連不到 |
| pod IP | 當下能用，pod 重建 IP 就變 |
| ClusterIP Service 名 | **最陰險**——三台都說「我是 kafka:9092」，client 想找 broker-1 卻被隨機轉到 broker-2，協定壞掉 |

### 本專案的正確設定

```bash
KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://broker-0.kafka.streaming.svc.cluster.local:9092"
KAFKA_LISTENERS="PLAINTEXT://0.0.0.0:9092,CONTROLLER://broker-0.kafka.streaming.svc.cluster.local:9093"
```

三個條件缺一不可：**每台不同**（pod 序號算出）、**解析得到**（headless Service）、
**重建後不變**（StatefulSet 固定 pod 名）。

> ⚠️ CONTROLLER listener 必須綁 FQDN 不能綁 `0.0.0.0`——
> 詳細原因見 `week7-decisions.md` 的「坑 2」。

---

## 8. Retention 與磁碟規劃

### `log.retention.bytes` 是「每個分區」的上限

最容易低估的一個參數。實際要這樣算：

```
6 個 topic 共 60 個分區（16×3 + 4×3）
× RF=3          = 180 個分區副本
÷ 3 個 broker   = 每台承載 60 個分區副本

每分區 5GB   → 每台 300GB   ❌
每分區 1GB   → 每台  60GB   ❌
每分區 256MB → 每台  15GB   ✅（磁碟 20Gi，留 5GB 餘裕）
```

### `log.segment.bytes` 要明顯小於 retention.bytes

Kafka 是**整個 segment 一起刪**的：

```
segment = retention → 不到上限不刪，一刪就清空（粒度太粗）
segment = retention/4 → 每個分區 4 個 segment，刪除平順
```

本專案：retention 256MB / segment 64MB。

### 兩種 retention 同時生效

```
log.retention.hours = 168   （7 天）
log.retention.bytes = 256MB （每分區）
→ 先達到哪個就先刪哪個
```

---

## 9. 常用指令速查

```bash
K="kubectl exec -n streaming broker-0 --"
BS="--bootstrap-server localhost:9092"
```

### 健康檢查

```bash
# 最常用：空輸出 = 健康
$K kafka-topics $BS --describe --under-replicated-partitions

# quorum 狀態
$K kafka-metadata-quorum $BS describe --status

# 從 client 角度看得到幾個 broker（驗證 advertised listener）
$K kafka-broker-api-versions --bootstrap-server kafka-bootstrap:9092 | grep 'id: '
```

### Topic 操作

```bash
$K kafka-topics $BS --list
$K kafka-topics $BS --describe --topic market-trades

# 建立（--if-not-exists 是冪等的關鍵）
$K kafka-topics $BS --create --if-not-exists \
   --topic NAME --partitions 16 --replication-factor 3

# 改設定（分區只能增加不能減少）
$K kafka-topics $BS --alter --topic NAME --partitions 32
$K kafka-configs $BS --alter --entity-type topics --entity-name NAME \
   --add-config retention.ms=3600000
```

### 讀寫測試

```bash
# 寫（Ctrl+D 結束）
$K kafka-console-producer $BS --topic market-trades

# 讀（從頭開始）
$K kafka-console-consumer $BS --topic market-trades --from-beginning --max-messages 10

# 看 consumer group 的落後量
$K kafka-consumer-groups $BS --describe --group GROUP_NAME
```

### 看設定

```bash
$K kafka-configs $BS --describe --entity-type brokers --entity-name 0 --all
$K grep -h "^log.retention" /etc/kafka/kafka.properties
```

---

## 10. 監控該盯什麼

Week 9 的 Prometheus 要抓的核心指標：

| 指標 | 意義 | 告警門檻建議 |
|---|---|---|
| `UnderMinIsrPartitionCount` | ISR < min.insync.replicas | **> 0 立刻告警**（已經在拒絕寫入） |
| `UnderReplicatedPartitions` | ISR < Replicas | 持續 > 0 超過 5 分鐘 |
| `OfflinePartitionsCount` | 沒有 leader 的分區 | **> 0 立刻告警**（該分區完全不可用） |
| `IsrShrinksPerSec` / `IsrExpandsPerSec` | ISR 震盪頻率 | 頻繁震盪 = broker 不穩或磁碟太慢 |
| `ActiveControllerCount` | 全叢集加總應該剛好 = 1 | ≠ 1 表示腦裂或無 controller |
| consumer group lag | consumer 落後量 | 持續成長 = 消費不及 |

**第一個和第三個是最該設告警的** —— 它們直接對應「資料寫不進去」和「資料讀不到」。

> Spot 環境下 `IsrShrinks` 會很熱鬧（每次 broker 被回收都會震盪一次），
> 設告警時門檻要放寬，不然會被自己的成本策略洗版。

---

## 附錄：本專案的 topic 清單

| Topic | 分區 | RF | 用途 |
|---|---|---|---|
| `market-trades` | 16 | 3 | 原始成交資料（bronze） |
| `market-trades-clean` | 16 | 3 | 清洗後 |
| `market-quotes` | 16 | 3 | 報價 |
| `market-alerts` | 4 | 3 | 成交量異常告警 |
| `market-price-alerts` | 4 | 3 | 價格異常告警 |
| `market-dlq` | 4 | 3 | 死信佇列（poison message） |
