# k8s/ — GKE manifests

手寫 raw YAML，不用 Helm。不是因為 Helm 不好，是因為這個專案的目的是
學會 K8s 的原生物件怎麼串起來——先用 chart 就跳過了那一段。

---

## 目錄：依部署層次分組

```
k8s/
├── base/            Namespace + ResourceQuota + LimitRange
├── 10-storage/      MinIO（物件儲存）、Postgres（catalog 後端）
├── 20-platform/     Kafka ×3、Schema Registry、iceberg-rest
├── 30-compute/      Flink（JM/TM/RBAC/submit）、Trino
├── 40-app/          Producer、Gateway、Web、Discord Bot
├── 50-monitoring/   kafka-exporter、Prometheus、Alertmanager、Grafana
├── deploy/          部署腳本
└── _archive/        Week 6 暖身用的 manifests，與各週的舊 README
```

**編號代表依賴方向，不是「必須逐層等待」。** 同一層內的服務靠
initContainer 互相等待，層與層之間才是真正的 barrier——
因為跨層有 K8s 表達不了的東西（見下）。

> 這個結構是 2026-08-26 從 `week7/` `week8/` `week9/` 改過來的。
> 舊路徑的對照見本檔最下方；封存的週 README 在 `_archive/`。

---

## 部署

```bash
bash k8s/deploy/deploy.sh              # 全部
bash k8s/deploy/deploy.sh --from 30    # 從 30-compute 開始
bash k8s/deploy/deploy.sh --only 50    # 只部署監控
bash k8s/deploy/deploy.sh --dry-run    # 只印會做什麼
```

### 為什麼不是 `kubectl apply -f k8s/ -R`

K8s 是宣告式的，理論上把所有 manifest 丟進去，controller 會自己收斂到目標狀態。
實務上不行，因為**有些依賴不是 K8s 能表達的**：

| 依賴 | 為什麼 K8s 管不了 |
|---|---|
| Kafka 要先有 topic，Flink 的 job 才提交得起來 | 建 topic 是一次性動作，用 Job 表達，而 **Job 之間沒有依賴機制** |
| Schema 要先註冊，producer 送的 Avro 才解得開 | 同上 |
| Iceberg 的表要先建立，Trino 才查得到 | 建表發生在 Flink job 提交時，不是部署時 |
| ServiceAccount 要早於使用它的 pod | pod 會**直接建立失敗**，不是稍後重試 |

所以分層的本質是：**把 K8s 表達不了的順序，寫在腳本裡。**

### Job 為什麼要「先刪再建」

`Job.spec.template` 是不可變的。對已存在的 Job 跑 `kubectl apply`，
只要 template 有任何差異就會回 `field is immutable`——
而且訊息不會告訴你「應該先刪掉它」。這是重跑部署最常見的失敗。

`deploy.sh` 的 `run_job()` 固定先 `kubectl delete --ignore-not-found` 再 apply。

---

## 每日流程

```bash
bash k8s/deploy/gcp/05-up.sh              # 開工：node pool 擴回 3
bash k8s/deploy/gcp/16-verify-week8.sh    # 確認隔夜沒退化
#   ...做事...
bash k8s/deploy/gcp/06-down.sh            # 收工：縮回 0
```

⚠️ `06-down.sh` **只停運算**。PVC 的磁碟（約 196GB / $13.6 每月）照樣計費。

叢集：`streaming-poc` @ `asia-east1-b`，3 × e2-standard-4 **Spot**。
Node 數是為 Kafka quorum（RF=3、`minDomains: 2`）決定的，不是為算力。

---

## 從零重建

| # | 指令 | 說明 |
|---|---|---|
| 1 | `k8s/deploy/gcp/01-preflight.sh` | 檢查 gcloud / API / 權限 |
| 2 | `k8s/deploy/gcp/02-budget-alert.sh` | ⚠️ 幣別要對（本專案是 TWD） |
| 3 | `k8s/deploy/gcp/03-check-quotas.sh` | 真正的瓶頸是 `CPUS_ALL_REGIONS=12` |
| 4 | `k8s/deploy/gcp/04-create-cluster.sh` | **開始計費**；Spot 可能 `ZONE_RESOURCE_POOL_EXHAUSTED` |
| 5 | `k8s/deploy/gcp/04b-verify-cluster.sh` | node 真的起來了沒（別只看 `list-errors`） |
| 6 | `k8s/deploy/gcp/07-create-secrets.sh` | 從 `.env` 建 Secret |
| 7 | `k8s/deploy/gcp/12-setup-registry.sh` | 建 Artifact Registry **並授權 node SA** |
| 8 | `k8s/deploy/gcp/13-build-push.sh` | `buildx --platform linux/amd64`（arm64 Mac 必要） |
| 9 | **`bash k8s/deploy/deploy.sh`** | 全部 manifests + 三個 Job + 提交 Flink job |
| 10 | `k8s/deploy/gcp/16-verify-week8.sh` | 全鏈路 27 項 |

換 region：`k8s/deploy/gcp/00-teardown.sh <舊REGION> <舊ZONE>`
（會刪資料，且**必須**先做才有 vCPU 配額）。

---

## 驗收

```bash
bash k8s/deploy/gcp/16-verify-week8.sh     # 全鏈路 27 項
bash k8s/deploy/gcp/18-security-check.sh   # 對外暴露 / IAM / RBAC / 憑證 / 容器身分
bash k8s/deploy/gcp/19-alert-drill.sh      # 告警演練，約 10 分鐘
```

⚠️ **這些只驗「現在能動」。** 真正的驗收是隔天：`06-down.sh` 收工，
隔天 `05-up.sh` 之後**不重送 job** 而能 27/27。
Week 8 曾經 25 項全過，隔夜卻整條斷掉——因為一項持久性都沒驗。

---

## Compose → K8s 對照

| docker-compose.yml | 這裡的對應 | 注意 |
|---|---|---|
| 一個 service | Deployment（無狀態）/ StatefulSet（Kafka、MinIO、Postgres）| StatefulSet 才有穩定 pod 名 `broker-0` |
| `depends_on: service_healthy` | initContainer + readinessProbe | K8s 沒有原生的啟動順序 |
| `environment:` | ConfigMap | |
| `.env` | Secret（`07-create-secrets.sh` 生成）| 值不進 git |
| `volumes: ./x:/y`（bind mount） | ConfigMap | ⚠️ 掛載會**蓋掉整個目錄**，單檔要用 `subPath` |
| `volumes: named:/y` | PersistentVolumeClaim | **PVC 存在就一直算錢** |
| `ports:` | Service | 全部 ClusterIP + port-forward，無 LB |
| 一次性容器 | Job | 要冪等，且 `spec.template` 不可變 |
| `broker-1:29092` | `broker-0.kafka.streaming.svc.cluster.local:9092` | advertised listener 是最容易卡的地方 |

---

## 常用檢查

```bash
kubectl get pods -n streaming -o wide          # 跑在哪台 node
kubectl describe pod -n streaming <pod>        # 排不進去的原因在 Events
kubectl top pods -n streaming                  # 實際用量 vs requests
kubectl get events -n streaming --sort-by=.lastTimestamp | tail -20

# 部署前的兩個 3 秒檢查，各省下一輪「部署→失敗→診斷」
kubectl run tmp -n streaming --restart=Never --image=<img> --command -- sleep 30
kubectl exec -n streaming tmp -- cat /proc/self/status | grep -i uid
kubectl auth can-i list pods --as=system:serviceaccount:streaming:prometheus -n streaming
```

## 收工前必看

```bash
kubectl get pvc -A                                         # 每個 = 一顆持續計費的磁碟
kubectl get svc -A --field-selector spec.type=LoadBalancer # 每條 = 約 $18/月
```

⚠️ **移除 manifest 裡的定義 ≠ 刪除叢集裡的物件。**
「刪了資源卻繼續收費」的頭號原因就是孤兒 PVC。

---

## 舊路徑對照（2026-08-26 重組）

| 舊 | 新 |
|---|---|
| `k8s/00-namespace.yaml` | `k8s/base/namespace.yaml` |
| `k8s/week7/minio-statefulset.yaml` | `k8s/10-storage/minio.yaml` |
| `k8s/week7/minio-init-job.yaml` | `k8s/10-storage/minio-init-job.yaml` |
| `k8s/week7/catalog-postgres.yaml` | `k8s/10-storage/catalog-postgres.yaml` |
| `k8s/week7/kafka-statefulset.yaml` | `k8s/20-platform/kafka.yaml` |
| `k8s/week7/kafka-configmap.yaml` | `k8s/20-platform/kafka-configmap.yaml` |
| `k8s/week7/kafka-init-job.yaml` | `k8s/20-platform/kafka-init-job.yaml` |
| `k8s/week7/schema-registry.yaml` | `k8s/20-platform/schema-registry.yaml` |
| `k8s/week7/schema-register-job.yaml` | `k8s/20-platform/schema-register-job.yaml` |
| `k8s/week7/iceberg-rest.yaml` | `k8s/20-platform/iceberg-rest.yaml` |
| `k8s/week8/flink-*.yaml` | `k8s/30-compute/flink-*.yaml` |
| `k8s/week8/trino.yaml` | `k8s/30-compute/trino.yaml` |
| `k8s/week8/producer.yaml` | `k8s/40-app/producer.yaml` |
| `k8s/week8/app.yaml` | `k8s/40-app/app.yaml` |
| `k8s/week9/*.yaml` | `k8s/50-monitoring/*.yaml` |
| `k8s/warmup/` | `k8s/_archive/warmup/` |
