# 分散式協調：leader election 與 fencing

> 起因：「單副本為什麼要選 leader？」

---

## HA 這個詞在 Flink 裡指的不是直覺的那個

一般講 High Availability 會想到「多開幾台，掛一台還有備援」。
**Flink 的 "HA services" 主要不是那個意思。**

| | 要什麼 | 解決什麼 |
|---|---|---|
| `replicas: 2+` | 熱備援 | 主的掛了秒級接手，**停機時間**變短 |
| **HA services** | 狀態持久化 + leader election | pod 重建後**把 job 撿回來** |

本專案要的是第二個。我們的 JobManager 仍然是 `replicas: 1`——
掛了還是會停約 60 秒，但**停完之後 job 會回來**。

沒有 HA 時，JM 的日誌會出現這句：

```
Successfully recovered 0 persisted job graphs.
```

它不是報錯，是在誠實地說「我去看了，沒東西可以撿」。
而 MinIO 裡的 checkpoint 還在——**狀態資料好好的，只是沒有任何東西
記得這些 checkpoint 屬於哪個 job**。存檔還在，但遊戲忘了你有存過檔。

---

## `replicas: 1` 不保證只有一個

這是回答「單副本為什麼要選 leader」的關鍵。

K8s 保證的是**最終**只有一個，不是**任何瞬間**都只有一個。至少三種情況會並存：

| 情況 | 說明 |
|---|---|
| **網路分區** | node 失聯 → K8s 認為 pod 死了 → 在別處建新的 → **但舊的還活著** |
| Spot 回收 | 搶佔通知後有 30 秒寬限期，新舊可能短暫重疊 |
| 手滑 | `kubectl scale --replicas=2` |

第一種最危險。這種情況下 `replicas: 1` 的宣告仍然成立，實際卻有兩個 JM。

### 兩個 JM 同時活著會怎樣

```
JM-A 從 checkpoint-42 復原 4 個 job  →  寫 bronze.trades
JM-B 從 checkpoint-42 復原 4 個 job  →  也寫 bronze.trades
```

同一批 Kafka 訊息處理兩次、寫進同一張表。**`EXACTLY_ONCE` 直接崩掉**，
而且不會有任何錯誤訊息——資料就是多了一倍，要對帳才會發現。

> 沒有 HA 的時候反而不會有這個問題：job graph 只在記憶體，新 JM 起來是空的。
> **所以「加了 HA 儲存」和「必須有 leader election」是綁在一起的**——
> 一個可共享、可變的狀態儲存，沒有所有權仲裁的話比沒有還糟。

---

## 這不是共識演算法

常見的誤解。**Flink 自己不跑共識演算法，它把共識外包給 etcd。**

| | 共識演算法 | 租約 / 鎖 |
|---|---|---|
| 例子 | Raft、Paxos、ZAB | Flink 的 leader election |
| 參與者 | 彼此投票，沒有仲裁者 | 都去問同一個仲裁者 |
| 需要 | 多數決（quorum） | 一個線性一致的儲存 |
| 難度 | 很難寫對 | 很簡單 |

Flink 的 JM 之間**不互相通訊、不投票**。它們只是各自對 etcd 做 CAS：

> 「如果這把鎖現在沒人拿、或已過期，就記成我」

etcd 保證同時只有一個人成功——而 **etcd 內部**用 Raft 在它自己的副本
之間達成共識。共識確實發生了，只是不在 Flink 這一層。

### 本專案剛好兩種都有

**Kafka 的 controller —— 真的在跑共識。**
KRaft 就是 Raft，三個 broker 互相投票選 controller，需要 quorum（2/3）。
**Kafka 沒有外部仲裁者，所以必須自己來**——這也是 node 從 2 台加到 3 台的原因。

**Flink 的 JM —— 沒有。**
它有 etcd 可以問。這也是為什麼 `replicas: 1` 沒問題——
沒有「多數決」的概念，不需要奇數個。

```
Kafka：  broker-0 ⟷ broker-1 ⟷ broker-2      互相投票
Flink：  JM-A ─┐
              ├─▶ etcd（內部跑 Raft）         各自去問
         JM-B ─┘
```

同樣的道理，ZooKeeper 模式也是外包，只是外包給 ZAB。
**在 K8s 上你已經有 etcd 了，再裝 ZK 是重複投資。**

---

## 為什麼鎖本身不夠：fencing token

經典的反例（Martin Kleppmann 那篇）：

```
client-1 拿到鎖（token=33）
client-1 GC 暫停 20 秒          ← 它自己完全不知道
    租約過期
    client-2 拿到鎖（token=34），開始寫
client-1 醒來，以為自己還是 leader，也去寫
    → 兩個一起寫
```

**問題在於 client-1 從來沒被通知過自己失效了。**
鎖是「取得時」檢查一次，但寫入發生在幾秒後——中間那段沒有保護。
加長租約只能降低機率，不能消除。

### 解法：三步

```
1. 仲裁者每次把鎖交出去，同時發一個「單調遞增」的號碼
2. 持有者對受保護資源的每次寫入，都要帶上這個號碼
3. 資源端記住看過的最大號碼，比它小的一律拒絕
```

**最關鍵的一句：檢查必須發生在資源端，不是客戶端。**
如果是 client 自己寫之前先問「我還是 leader 嗎」，問完到寫入之間仍有空隙。
只有「資源拒絕舊 token」才是原子的。

這也意味著**資源本身必須支援這件事**——純粹的檔案系統做不到，
因為它不記得任何版本。

---

## Flink 的實作

`leaderSessionID` 是 UUID，**不是遞增數字**，所以沒辦法比大小。
Flink 用兩個等價機制：

**① JM ↔ etcd**——靠 K8s 物件的 `resourceVersion`。
每次更新 HA ConfigMap 都是 CAS：「如果版本還是 N 就改」。
舊 JM 手上的版本早已過期，寫入失敗，它就知道自己不是 leader，
然後**主動結束自己的 process**（Flink 稱為 revoke leadership）。

> 不是被回收，是自我了斷。沒有任何外力去殺它——是它自己發現的。

**② TM ↔ JM**——TaskManager 從 HA 服務得知「當前 leader 的 session 是 X」，
之後只接受帶這個 id 的 RPC。舊 JM 喊出去的指令 id 對不上，直接丟棄。
這是「比對當前值」而非「比大小」，能成立是因為 HA 服務是唯一真相來源。

### 日誌長這樣

```
KubernetesLeaderElectionDriver{configMapName='streaming-poc-flink-cluster-config-map'}
New leader elected 20a1f993-... for streaming-poc-flink-cluster-config-map
was granted leadership with leaderSessionID=c08393f5-b092-4b46-9194-1f6609125455
```

**最快的判斷方式：看 `leaderSessionID` 是不是全 0。**
沒開 HA 時它是 `00000000-0000-0000-0000-000000000000`，開了之後是隨機 UUID。

---

## 為什麼本體要放 S3、指標放 ConfigMap

```
ConfigMap (etcd)  ──指標──▶  s3://warehouse/flink-ha/  (job graph 本體)
     小，但強一致                    大，但只是檔案
```

兩個理由：

1. **ConfigMap 有 1MB 上限**，塞不下 job graph 本體
2. **只有 etcd 能仲裁誰是 leader**——S3 沒有 CAS

**兩者缺一都無法復原。** 這也是為什麼清理時必須同時清：
S3 清了但 ConfigMap 留著，JM 會拿著指標去找一個不存在的檔案，
啟動後不斷 RESTARTING。

---

## 本專案設定了什麼

```yaml
# k8s/30-compute/flink-rbac.yaml —— ServiceAccount + Role(configmaps/leases/pods) + RoleBinding

# JM 與 TM 都要
spec:
  serviceAccountName: flink

FLINK_PROPERTIES: |
  high-availability.type: kubernetes
  high-availability.storageDir: s3://warehouse/flink-ha
  kubernetes.cluster-id: streaming-poc-flink
  kubernetes.namespace: streaming
```

| 設定 | 作用 | 不設會怎樣 |
|---|---|---|
| `type: kubernetes` | 開關（同時啟用持久化與選舉，兩者是一組的） | HA 不啟用 |
| `storageDir` | job graph 本體寫哪 | 啟動失敗，必填 |
| `cluster-id` | **鎖的名字 = 叢集身分證** | 啟動失敗 |
| `namespace` | 預設是 `"default"` | 跑去別的 namespace 找，然後沒權限 |

⚠️ **`cluster-id` 要當成不可變的識別碼。** 改掉它 = 開一把新鎖 =
Flink 認為這是全新的叢集，舊 job 找不回來。

⚠️ **TM 也要 `serviceAccountName`。** 開了 HA 之後 TM 找 JM 的方式從
「連固定位址」變成「問 HA 服務誰是 leader」。只改 JM 的話，TM 會起來
但註冊不上，`slots-total` 永遠是 0。

---

## 驗證 HA 真的生效

```bash
# ① HA 物件存在（每個 job 一個 + 一個 cluster）
kubectl get cm -n streaming | grep streaming-poc-flink

# ② session id 是隨機 UUID，不是全 0
kubectl logs -n streaming deploy/flink-jobmanager -c jobmanager | grep -i "granted leadership"

# ③ 本體真的寫進 S3
kubectl exec -n streaming minio-0 -- ls -R /data/warehouse/flink-ha
#   → submittedExecutionPlan × N   job graph 本體
#   → completedCheckpoint × N      checkpoint 中繼資料
```

**但最終的證明只有一個：重啟之後那句話變成 `recovered 4 persisted job graphs`。**
本專案實測到的是一次非計畫性的 pod 重建（很可能是 Spot 回收）自動復原了
4 個 job——比刻意演練更有說服力。

---

## 順帶：這個結構還會再出現一次

| | 本體 | 指標 | 指標存在哪 |
|---|---|---|---|
| Flink HA | S3 的 job graph | ConfigMap | etcd |
| Iceberg catalog | MinIO 的 Parquet / metadata.json | `metadata_location` 欄位 | Postgres |

**兩次故障的本質完全相同：本體好好的，指標沒了。**
見 [iceberg-catalog.md](iceberg-catalog.md)。
