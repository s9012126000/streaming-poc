# 可觀測性：Prometheus 的服務發現與告警

> 起因：`static_configs` → `kubernetes_sd_configs` 的轉換

---

## 核心轉換：誰決定「要監控什麼」

compose 的做法是把位址寫死在 Prometheus 這一側：

```yaml
static_configs:
  - targets: ["flink-jobmanager:9249"]
dns_sd_configs:                      # TaskManager 有兩個副本
  - names: ["flink-taskmanager"]
```

在 K8s 上這會壞兩次：TaskManager 從 2 個變 4 個要回來改設定；
Deployment 的 pod IP 每次重建都不同。

**K8s 的做法是反過來的：Prometheus 去問 API「誰想被抓」**，
而「想被抓」由各服務自己用 pod annotation 宣告：

```yaml
# k8s/30-compute/flink-jobmanager.yaml
template:
  metadata:
    annotations:
      prometheus.io/scrape: "true"
      prometheus.io/port: "9249"
```

結果是整份 `prometheus.yml` 裡**沒有任何位址**。TaskManager 擴到 4 個會自動納入。

> 決定權從「監控系統」移到「被監控的服務」——這跟 Service 靠 label
> 而不是靠所有權找 pod 是同一種鬆耦合。

---

## 兩個完全獨立的階段

```
① 發現（問 K8s API，要 RBAC）    ② 抓取（直連 pod IP，不經過 API Server）

Prometheus ──▶ API Server         Prometheus ──▶ 10.8.0.8:8000/metrics
```

**API Server 是公佈欄，不是總機。** 這跟 etcd 對 Flink JM/TM 的角色完全一樣。

實際的好處：control plane 短暫不可用時（例如 GKE 升級 master），
**已建立的抓取不受影響**，只有「發現新 pod」會停。

### 發現階段拿到什麼

K8s API 回的是完整的 Pod 物件（就是 `kubectl get pod -o yaml` 那份）。
Prometheus 把它攤平成標籤：

| Pod 物件的欄位 | Prometheus 的標籤 |
|---|---|
| `.metadata.annotations["prometheus.io/scrape"]` | `__meta_kubernetes_pod_annotation_prometheus_io_scrape` |
| `.metadata.labels["app"]` | `__meta_kubernetes_pod_label_app` |
| `.status.podIP` + 第一個 `containerPort` | `__address__` |
| `.status.phase` | `__meta_kubernetes_pod_phase` |

注意 annotation 名字的轉換：`prometheus.io/scrape` → `prometheus_io_scrape`
（點和斜線都變底線）。這是 Prometheus 的規則，不是 K8s 的。

⚠️ RBAC 的 `verbs` 要包含 **`watch`**，不只 `list`。
`list` 是啟動時抓一次，`watch` 是保持長連線接收變化。
只給 `list` 的症狀是**一開始正常，之後新 pod 都不會被監控到**。

---

## relabel：過濾與改寫

此時「所有 pod」都是候選，包括 Kafka、MinIO、臨時的 curl pod。
五段規則做四件事：

```yaml
# ① 只保留有宣告的 —— 沒這條會去抓每一個 pod，全部失敗、噪音滿天
- source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
  action: keep
  regex: "true"

# ② 跳過非 Running 的 —— Completed 的 Job pod 抓不到東西，只會讓 /targets 一直紅
- source_labels: [__meta_kubernetes_pod_phase]
  action: drop
  regex: (Pending|Succeeded|Failed|Unknown)

# ③ 自訂路徑（沒宣告就用預設的 /metrics）
- source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
  action: replace
  target_label: __metrics_path__
  regex: (.+)

# ④ ★ 把 pod IP 和 annotation 指定的 port 組起來
- source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
  action: replace
  regex: ([^:]+)(?::\d+)?;(\d+)
  replacement: $1:$2
  target_label: __address__

# ⑤ 補上人看得懂的標籤（否則 Grafana 上只看得到 IP）
- source_labels: [__meta_kubernetes_pod_label_app]
  action: replace
  target_label: app
```

**第 ④ 條最容易被忽略，但不能省。** `__address__` 預設是
`<podIP>:<第一個 containerPort>`——而那不一定是 metrics 的埠。

Flink JobManager 就是最好的例子，它宣告了四個埠：

```
rpc: 6123 ← 預設會抓這個
blob: 6124
ui: 8081
metrics: 9249 ← 實際要抓的
```

往 6123 發 HTTP 請求當然什麼都拿不到。

---

## 排查要照三頁的順序

```bash
kubectl port-forward -n streaming svc/prometheus 9090:9090
```

| 頁面 | 看什麼 |
|---|---|
| `/service-discovery` | **relabel 之前**，發現到的所有 pod + 原始標籤 |
| `/targets` | **relabel 之後**，實際在抓的目標與健康狀態 |
| `/alerts` | 規則與狀態 |

判斷是哪一段斷掉：

- `/service-discovery` **空的** → 發現階段失敗 = **RBAC 問題**
- 有發現但 `/targets` 空的 → **relabel 規則寫錯**（多半是 annotation 名字或 regex）
- `/targets` 有但紅色 → **抓取失敗**：埠錯、網路不通、或端點不是 Prometheus 格式

**這三段對應三個完全不同的原因**，先分辨再查，比直接翻日誌快得多。

⚠️ RBAC 不足的症狀特別難認：Prometheus **正常啟動、UI 打得開**，
只有 `/targets` 是空的——看起來像「還沒發現目標」而不像「權限被拒」。

---

## 告警的四個狀態

```
INACTIVE  → 條件不成立
PENDING   → 條件成立，但還沒滿足 for
FIRING    → 持續超過 for，推給 Alertmanager
（解除）   → 條件不再成立，立即回 INACTIVE
```

`for` 的存在是為了濾掉瞬間抖動——沒有它，producer 重啟 20 秒就會叫一次。

### 分工要分清楚

| | 負責 |
|---|---|
| **Prometheus** | 決定「什麼時候該告警」（評估規則、PENDING → FIRING） |
| **Alertmanager** | 決定「怎麼通知」（分組、抑制、靜音、去重、送到哪） |

所以規則寫在 Prometheus 的 ConfigMap，收件人寫在 Alertmanager 的。

### 實測的偵測延遲：7 分鐘

| 階段 | 耗時 | 由什麼決定 |
|---|---|---|
| 故障 → PENDING | **135 秒** | `rate(...[1m])` 視窗 + 15 秒 scrape interval |
| PENDING → FIRING | **285 秒** | 規則的 `for: 5m` |
| FIRING → Discord | ~10 秒 | `group_wait: 10s` |
| **合計** | **約 7 分鐘** | |

第一段原本估 60 秒（只算 rate 的視窗），**實際是兩倍多**——
每一層都有自己的延遲，而它們是**相加**的，不是取最大值。

**這是個必須寫進 SLO 的數字，不能默認為「即時」。**
縮短要動 `for`，而那會提高誤報率——這是取捨，不是可以兩全的參數。

恢復只花 30 秒，因為 `for` 只作用在觸發方向。
**這個不對稱是刻意的**：對「開始出問題」保守，對「恢復正常」積極。

---

## ⚠️ 靜默失效的告警

這是本專案最重要的一個發現。

`kafka-exporter` 一度沒有部署上 K8s（compose 有、K8s 沒有）。
沒有它，兩條規則引用的指標**根本不存在**：

```
ConsumerLagGrowing → kafka_consumergroup_lag
DLQNotEmpty        → kafka_topic_partition_{current,oldest}_offset
```

而 **Prometheus 對不存在的指標不會報錯，只回空集合**。
表達式永遠是 false，告警永遠不觸發，`/alerts` 顯示綠色的 INACTIVE——
**跟「一切正常」長得一模一樣**。

> **靜默失效的告警比沒有告警更危險，因為你以為它在看著。**

### 同一個機制的另一個入口：告警疲勞

gateway 曾經有 `prometheus.io/*` 註記，但 `/api/health` 回的是 JSON，
Prometheus parse 不了，target 永遠是紅的：

```
expected a valid start token, got "{"
```

不會讓任何東西壞掉，但驗收標準是「所有 target 都 UP」——
留著等於讓標準永遠達不到，只能每次人工忽略一項。
**「已知可忽略的紅燈」是告警疲勞的起點**，所以直接把註記移除。

同樣的道理也適用於驗證腳本裡寫得太死的斷言（見 [debugging-playbook.md](debugging-playbook.md)）。

### 誰來監控監控系統

`alertmanager.yaml` 也加了 `prometheus.io/scrape` 註記。

「告警系統壞掉時沒有人會告訴你」是監控的經典盲點——
Alertmanager 掛了，所有告警靜默消失，而畫面一片綠，**看起來比平常更健康**。

---

## 演練是唯一的證明

壞掉的告警系統長得跟正常的一模一樣：target 全 UP、規則全 INACTIVE、畫面一片綠。
**你沒辦法從「沒有告警」推論出「系統正常」**，因為「告警壞了」也長這樣。

```bash
bash k8s/deploy/gcp/19-alert-drill.sh
```

腳本會停掉 producer、輪詢狀態轉換、確認 Alertmanager 收到、
問你 Discord 有沒有響、再恢復並確認 RESOLVED。

三欄輸出各自回答不同的問題：

```
[135s] pending    records/s=0.0    Alertmanager 持有=0
        ↑ 規則狀態   ↑ 條件實際值      ↑ 有沒有推出去
```

- 中間那欄分辨「規則沒在算」和「條件真的成立」
- 右邊那欄分辨「Prometheus 判定 FIRING 但沒推出去」和「推了但 Discord 沒收到」

### 兩個設計上的細節

**起始狀態要先擋。** 4 個 job 沒到齊、或告警本來就不是 INACTIVE 就直接退出——
否則分不出「告警是因為你停了 producer」還是「別的東西壞了」。

**中斷時要自動恢復。** 腳本用 `trap` 在 Ctrl-C 時把 producer 開回來。
沒有這個的話，中途放棄就會留下一個「以為只是演練」的真實故障。

### 演練本身抓到的東西

第一次跑沒有通過——producer 恢復了但告警不解除。追下去發現是
Iceberg catalog 的 `SQLITE_BUSY`，**一個兩天前自己引進、而且完全不知道的故障**。

> 我以為在測告警，結果告警在測我。

這比收到 RESOLVED 更能證明監控有用。

---

## Alertmanager 的兩個坑

**1. 它不展開環境變數。**
寫 `${DISCORD_WEBHOOK}` 只會被當字面字串送出去，Discord 回 404，
而日誌只說「通知失敗」，不會說是變數沒展開。
這是它跟 Prometheus / Grafana 不一樣的地方。

解法是 `webhook_url_file` 指向掛載的 Secret 檔案：

```
### <discord_config>
# webhook_url and webhook_url_file are mutually exclusive.
webhook_url: <secret>
webhook_url_file: <filepath>
```

**互斥**代表同時寫會啟動失敗，不是其中一個被忽略。

**2. Secret 掛載要用 `items` 只暴露需要的那一把。**
不寫 `items` 的話，整個 Secret（含 Discord token、Alpaca 金鑰、MinIO 密碼）
都會變成檔案。Alertmanager 用不到那些，沒有理由讓它看得到。
