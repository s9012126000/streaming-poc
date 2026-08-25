# K8s 的物件模型

> 起因：「ConfigMap 是像 docker volume 的地方嗎」

---

## 先講一件顛覆直覺的事

**K8s 沒有「執行」你的 YAML，它只是把 YAML 存起來。**

```
你 kubectl apply 一個 Deployment
   → API Server 寫進 etcd：「應該要有 2 個 web pod」
   → Deployment controller 一直盯著 etcd
   → 發現實際只有 1 個 → 去建第 2 個        ← 這才是「執行」
```

etcd 存的是**你要什麼**（`spec`）和**現在是什麼**（`status`），
中間那段靠各種 controller 不斷把兩者拉近。這就是「宣告式」的本質。

理解這件事之後，很多行為就順了：

- `kubectl apply` 很快回來，不代表事情做完了
- 刪掉 pod 它會自己長回來——因為 etcd 裡的 spec 沒變
- `kubectl get` 是在讀 etcd，不是去問 pod

---

## etcd

分散式的鍵值資料庫，K8s 拿它當**唯一**的儲存。叢集裡的一切狀態都在裡面：

```
/registry/pods/streaming/broker-0
/registry/services/streaming/kafka
/registry/configmaps/streaming/flink-sql
/registry/secrets/streaming/streaming-secrets
```

它用 **Raft** 在自己的副本之間達成共識，寫入要多數同意才算數。
結果是它提供「比較並交換」（CAS）：

> 「如果這筆資料的版本還是 5，就改成我；否則失敗」

**這個能力是 leader election 的基礎**——見 [distributed-coordination.md](distributed-coordination.md)。

### 在 GKE 上你看不到它

```
control plane（Google 管）      你的 node pool（你付錢）
├── API Server                  ├── gke-...-t0sn
├── etcd        ← 在這裡        ├── gke-...-xk0l
├── Scheduler                   └── gke-...-gvtt
└── Controller Manager
```

自架 K8s 的話 etcd 就是你的責任，而且是整個叢集最脆弱的一環——
**etcd 沒了叢集就沒了**，即使所有 node 都活得好好的。

---

## 三種「把東西塞給 pod」的方式

| | 存在哪 | 大小上限 | 容器能寫嗎 | 誰產生內容 |
|---|---|---|---|---|
| **ConfigMap** | etcd | < 1MB | ❌ 唯讀 | **你**，`kubectl apply` |
| **Secret** | etcd（只 base64，**不是加密**）| < 1MB | ❌ 唯讀 | 你 |
| **PVC** | 真實磁碟（GCE PD）| GB 起跳 | ✅ | **程式自己**，運行時 |

### 判斷準則：問「這個內容是誰寫的」

```
你寫的、想進 git 的、開機時讀一次的   →  ConfigMap
程式自己寫的、你不會去看的           →  PVC
```

本專案的實例：

```
ConfigMap                      PVC
─────────────────────────      ─────────────────────
kafka-config    broker 設定    broker-0/1/2  log segment
app-config      連線位址        minio         Parquet 檔
trino-config    catalog 定義    catalog-db    Postgres 資料
flink-sql       SQL 腳本        prometheus    TSDB
flink-jobs      PyFlink 原始碼
```

> ⚠️ 這個判斷曾經做錯過。Iceberg catalog 一開始被當成「設定」，
> 實際上它是「程式持續寫入的資料庫」。詳見 [iceberg-catalog.md](iceberg-catalog.md)。

### ConfigMap ≠ docker volume

掛進容器的**體驗**很像，但存的地方完全不同。
compose 裡兩種 volume 用法各自對應到不同的 K8s 物件：

```yaml
volumes:
  - ./docker/flink/sql:/opt/flink/sql   # bind mount，唯讀設定 → ConfigMap
  - minio_data:/data                    # named volume，可寫資料 → PVC
```

三個會咬人的差異：

**1. 掛進來是唯讀的。** 程式想寫回去會失敗。

**2. 內容會自己變——但只有掛成檔案時。**

```yaml
volumeMounts: [{ name: flink-sql, mountPath: /opt/flink/sql }]  # 約 1 分鐘自動更新
envFrom:      [{ configMapRef: { name: app-config } }]           # 永遠不會更新
```

改了 `app-config` 之後 gateway 和 bot 要 `kubectl rollout restart` 才會生效。

**3. 掛載會蓋掉整個目錄。** 這一項在遷移時最容易漏，因為兩邊「看起來」
都是把檔案放進容器。本專案踩到兩次：

- Grafana 的 provider 設定和看板 JSON 不能放同一層，看板只好改掛 `/var/lib/grafana/dashboards`
- Alertmanager 的設定檔用 `subPath` 掛單一檔案，避免蓋掉 `secrets/` 子目錄

### 實作細節：symlink farm

ConfigMap 掛進來其實是一堆 symlink：

```
/opt/flink/sql/flink-init.sql -> ..data/flink-init.sql
/opt/flink/sql/..data -> ..2026_08_25_09_14_22.123456789
```

kubelet 更新時是「建新目錄 → 換 `..data` 這個 symlink」，原子性地切換。
副作用是**有些程式監看檔案變化時偵測不到**（inode 沒變，是 symlink 指向變了）。

---

## Namespace / Deployment / Service 各自的角色

```
Namespace   一個命名空間，名字在裡面唯一、配額在這一層算
  └─ Deployment   「我要 N 個長這樣的 pod」——負責數量與版本
       └─ Pod     實際跑的東西
  └─ Service      「叫這個名字的流量，送到帶這些 label 的 pod」
```

**Service 不「擁有」pod。** 它只是一條規則：

```yaml
kind: Service
metadata: { name: gateway }     # ← DNS 名稱，別人用這個找它
spec:
  selector: { app: gateway }    # ← 憑 label 找 pod，不管誰建的
  ports: [{ port: 8000 }]
```

pod 的 label 對上 selector 就會被納入，跟它屬於哪個 Deployment 無關。
這種「靠標籤而不是靠所有權」的鬆耦合，是 K8s 很多設計的共通模式。

### Service 名字 vs Deployment 名字

兩者都在 `metadata.name`，但用途完全不同：

| | 用途 |
|---|---|
| Deployment 的 name | 管理用的識別碼（`kubectl rollout restart deploy/gateway`） |
| **Service 的 name** | **DNS 名稱**——別的 pod 用 `gateway:8000` 連得到 |

所以 `http://kafka-bootstrap:9092` 裡的 `kafka-bootstrap` 是 Service 名，
不是任何 Deployment 或 pod 的名字。

### 完整的 DNS 形式

```
kafka-bootstrap                                  同 namespace 內
kafka-bootstrap.streaming                        跨 namespace
kafka-bootstrap.streaming.svc.cluster.local      完整
```

同一個 namespace 內用短名就夠，因為 pod 的 `/etc/resolv.conf` 有
`search streaming.svc.cluster.local ...`。

---

## Deployment vs StatefulSet

| | Deployment | StatefulSet |
|---|---|---|
| pod 名字 | `gateway-d4464ffd6-52sc8`（隨機） | `broker-0`、`broker-1`（穩定） |
| 重建後身分 | 換一個 | **同一個** |
| 儲存 | 共用或無 | `volumeClaimTemplates` 每個 pod 一顆 |
| 啟動順序 | 任意 | 預設依序（可改 `Parallel`） |
| 適用 | 可互換的副本 | 有身分、有自己資料的成員 |

**判準不是「有沒有資料」，是「別人需不需要指名找它」。**

Kafka 的 broker 必須用 StatefulSet，因為 `advertised.listeners` 要
告訴 client「我是 `broker-0.kafka.streaming.svc.cluster.local`」——
那個名字必須在重建後不變，否則 client 手上的 metadata 全部失效。

而 `iceberg-rest` 現在是 Deployment，因為它的狀態全在外部的 Postgres，
沒有人需要指名找某一個副本。

### StatefulSet 需要 headless Service

```yaml
spec:
  serviceName: kafka       # ← 指向一個 clusterIP: None 的 Service
```

headless（`clusterIP: None`）的 Service 不做負載平衡，
而是讓每個 pod 有自己的 DNS A 記錄：

```
kafka                → 回傳所有 pod 的 IP（供發現）
broker-0.kafka       → 只回傳 broker-0 的 IP（供指名）
```

本專案兩種都有：`kafka` 是 headless（給 broker 之間互相找），
`kafka-bootstrap` 是一般 ClusterIP（給 client 進來的第一個入口）。

---

## 為什麼 pod 之間可以直接用 IP 通訊

GKE 的 VPC-native 叢集（`--enable-ip-alias`）讓每個 pod IP 在 VPC 裡可路由。
所以：

- Prometheus 可以直接抓 `10.8.0.8:8000/metrics`，不必經過 Service
- Flink 的 TaskManager 用 pod IP 向 JobManager 註冊

這是「Service 只是可選的抽象」的原因——底層本來就通。
Service 提供的是**穩定的名字**和**負載平衡**，不是連通性。

---

## 延伸

- 排程、資源、健康檢查 → [k8s-scheduling-health.md](k8s-scheduling-health.md)
- 誰能對 API 做什麼 → [k8s-rbac.md](k8s-rbac.md)
- etcd 的 CAS 怎麼變成 leader election → [distributed-coordination.md](distributed-coordination.md)
