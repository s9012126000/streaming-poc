# k8s/deploy/

部署與運維的所有入口。

```
deploy/
├── deploy.sh    依層次部署整套系統
├── lib.sh       共用函式（由 deploy.sh source，不單獨執行）
└── gcp/         GCP / 叢集層級的操作，編號＝執行順序
```

---

## 兩者的分工

| | 管什麼 |
|---|---|
| `deploy.sh` | **叢集內**：apply manifests、跑 Job、等就緒 |
| `gcp/*.sh` | **叢集外**：建叢集、配額、Secret、image、成本，以及驗收與演練 |

分界是「這件事需不需要 `gcloud`」。

---

## 常用

```bash
# 每天
bash k8s/deploy/gcp/05-up.sh              # 開工：node pool 擴回 3
bash k8s/deploy/gcp/06-down.sh            # 收工：縮回 0

# 部署
bash k8s/deploy/deploy.sh                 # 全部
bash k8s/deploy/deploy.sh --from 30       # 從 30-compute 開始
bash k8s/deploy/deploy.sh --only 50       # 只部署監控
bash k8s/deploy/deploy.sh --dry-run       # 只印會做什麼

# 驗收
bash k8s/deploy/gcp/16-verify-week8.sh    # 全鏈路 27 項
bash k8s/deploy/gcp/18-security-check.sh  # 安全檢查
bash k8s/deploy/gcp/19-alert-drill.sh     # 告警演練（約 10 分鐘）
```

---

## `gcp/` 的編號

編號代表**從零開始的執行順序**，不是每次都要全部跑。

| 範圍 | 什麼時候用 |
|---|---|
| `00` | 換 region 或砍掉重來（**會刪資料**） |
| `01`–`04` | 建叢集。只有第一次 |
| `05` / `06` | **每天開工／收工** |
| `07` | `.env` 有變動時 |
| `08`–`13` | 一次性設定（node SA、topic、schema、registry、image） |
| `14`–`15` | Flink job 提交、Trino 查詢 |
| `16`–`19` | 驗收、清理、安全檢查、演練 |
| `99` | 成本盤點，每週一次 |

`env.sh` 被所有腳本 source，改設定（region、機型、節點數）動它一個就好。

---

## deploy.sh 的三個設計決定

### ① 為什麼不是 `kubectl apply -f k8s/ -R`

K8s 是宣告式的，理論上把所有 manifest 丟進去 controller 會自己收斂。
實務上不行——**有些依賴不是 K8s 能表達的**：

- Kafka 要先有 topic，Flink 的 job 才提交得起來
- Schema 要先註冊，producer 送的 Avro 才解得開
- ServiceAccount 要早於使用它的 pod（否則 pod **直接建立失敗**，不是稍後重試）

這些是一次性動作，K8s 用 Job 表達，而 **Job 之間沒有依賴機制**。

> 分層的本質是：**把 K8s 表達不了的順序，寫在腳本裡。**

### ② Job 固定「先刪再建」

`Job.spec.template` 是不可變的。對已存在的 Job 跑 `kubectl apply`，
只要 template 有任何差異就回 `field is immutable`——
**而且訊息不會說「你應該先刪掉它」**。這是重跑部署最常見的失敗。

### ③ 前置檢查放在最前面

```bash
require_secret_keys streaming-secrets MINIO_ROOT_USER ... CATALOG_DB_PASSWORD
kubectl auth can-i create configmaps --as=system:serviceaccount:streaming:flink
```

兩個都是幾秒的事，各自省下一輪「部署 → 失敗 → 診斷」。

而且這兩類失敗的症狀**都不像它們的原因**：

- Secret 少一個 key → pod 卡在 `CreateContainerConfigError`，
  而那個狀態**不會出現在 `rollout status` 的輸出裡**，只會一直等到逾時
- RBAC 不足 → Flink JM 起得來但選不出 leader，REST 一直 503

---

## 失敗時

`deploy.sh` 在每個失敗點都會印出足夠的診斷資訊（Job 日誌、pod 狀態、Events），
不會只說「失敗」。若要自己往下查，順序是：

```bash
kubectl get pods -n streaming            # 它在跑嗎
kubectl describe pod -n streaming <pod>  # Events 說了什麼
kubectl logs -n streaming <pod> --previous   # 死掉那次的日誌 ← 不是 kubectl logs
```

症狀對照表見 [`doc/knowledge/debugging-playbook.md`](../../doc/knowledge/debugging-playbook.md)。
