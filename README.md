# streaming-poc

即時市場行情 pipeline。加密貨幣與美股的成交資料持續流入，落進可查詢的湖倉，
經聚合與異常篩選後，同時輸出到即時圖表與 Discord 推播——而 pipeline 自身全程被監控。

從 Docker Compose 起步，第 6–9 週搬上 GKE。**這個 README 的重點在後半段**：
遷移過程中做錯的判斷，比做對的部分更值得看。

---

## 架構

```
Binance / Alpaca ──WebSocket──▶ Producer ──▶ Kafka (3 broker, RF=3, KRaft)
                                   ▲              │
                          Schema Registry         │
                             (Avro)               │
                                                  ▼
                                        ┌──────── Flink ────────┐
                                        │  4 個 streaming job    │
                                        │  VWAP / 量能異常 /      │
                                        │  價格跳動 / bronze 寫入 │
                                        └───┬───────────────┬───┘
                                            │               │
                          Iceberg ◀─────────┘               └──▶ Kafka
                     ┌──────┴───────┐                          (alerts)
                     ▼              ▼                              │
              iceberg-rest       MinIO                             ▼
              (catalog:        (Parquet)                     Discord Bot
               Postgres)           │
                     └──────┬──────┘
                            ▼
                          Trino ──▶ Gateway (SSE) ──▶ Web (K 線圖)

           Prometheus ──▶ Alertmanager ──▶ Discord
                ▲              (SLO 告警)
                └── kafka-exporter / Flink / Producer / Gateway
```

| 層 | 技術 |
|---|---|
| 攝取 | Python + `confluent-kafka`、Avro + Schema Registry |
| 匯流排 | Kafka 7.9（KRaft，無 ZooKeeper），3 broker / RF=3 / 16 partition |
| 運算 | Flink 2.0（PyFlink + SQL），RocksDB state、`EXACTLY_ONCE`、Kubernetes HA |
| 湖倉 | Apache Iceberg 1.10 + MinIO（S3 相容），catalog 走 Postgres |
| 查詢 | Trino 476 |
| 應用 | FastAPI（SSE）+ React 前端、discord.py |
| 監控 | Prometheus + Alertmanager + Grafana + kafka-exporter |
| 平台 | GKE Standard、Spot node pool、手寫 raw YAML（不用 Helm） |

---

## 目前狀態

```
GKE Standard，asia-east1-b，3 × e2-standard-4 Spot

Week 8 全鏈路驗收    27 / 27
安全檢查             11 過 0 失
告警演練             完整循環通過（偵測延遲 7 分鐘）
```

沒有 LoadBalancer、沒有 Ingress、沒有對外埠——全部走 `port-forward`。
這既是安全考量也是成本考量（每條 LB 每月約 $18）。

---

## 這個專案真正學到的東西

> **系統本身壞了 3 次；「關於系統的描述」壞了 12 次。**

前者是工程問題，後者是認知問題——而後者的修復成本更高，
因為它們不會在 `kubectl apply` 時報錯，要等到隔天才顯現。

三個代表性的例子：

### 1. 「資料還在」和「系統知道資料在哪」是兩件事

某天早上全鏈路斷掉。Trino 回 `Schema 'bronze' does not exist`，
但 MinIO 裡 59,078 筆資料的 Parquet 檔好端端躺著。

根因是 Iceberg catalog 用了 image 預設的 **in-memory SQLite**——
pod 一重啟，「哪張表對應哪個 metadata.json」的指標全部歸零。
**書還在書架上，被燒掉的是索引卡。**

同樣的結構在 Flink 又出現一次：job graph 本體在 S3，指標在 K8s ConfigMap，
沒有 HA 就等於指標不存在，checkpoint 全變孤兒。

而我在那份 manifest 裡原本寫著「catalog 中繼資料存在 MinIO，所以無狀態」——
**錯誤的註解成了後續三天決策的依據。**

### 2. 驗證工具比系統本身更常壞

8 次，其中 **6 次是誤報成功**。誤報失敗會逼你去查；誤報成功讓你直接走人。

最典型的一次：驗收腳本用 `kubectl logs --tail=50 | grep "connected"` 判斷 Discord Bot 健康，
於是 bot 明明在 `CrashLoopBackOff`（被我自己寫錯的 liveness probe 殺了 26 次），
驗收照樣印 ✅——**它曾經連上過，那行日誌還在範圍內。**

後來寫進腳本的三條規則：

- **別吞 stderr。** `2>/dev/null` 是這 8 次裡出現最多的元凶，
  它把「查詢炸了」和「查詢回 0 筆」變成同一個畫面
- **先看 K8s 眼中的狀態，再看應用程式日誌。** pod 沒 `Ready`，日誌講什麼都不算數
- **斷言要對著「性質」不要對著「實作」。** 寫死「必須是 `/data` 的 SQLite」的守衛，
  在正常演進時自己變成紅燈，然後被歸類成「已知可忽略」——那是告警疲勞的入口

### 3. 演練是唯一能證明監控有用的方法

壞掉的告警系統長得跟正常的一模一樣：target 全 UP、規則全 INACTIVE、畫面一片綠。
**你沒辦法從「沒有告警」推論出「系統正常」**，因為「告警壞了」也長這樣。

第一次演練沒通過——停掉 producer 再開回來，告警不解除。
追下去發現是 catalog 的 `SQLITE_BUSY`（單寫者模型撐不住兩個 Flink job 同時 commit），
**一個我兩天前自己引進、而且完全不知道的故障。**

> 我以為在測告警，結果告警在測我。

順帶量到一個必須寫進 SLO 的數字：**偵測延遲 7 分鐘**。
`rate()` 視窗、scrape interval、規則的 `for` 是**相加**的，不是取最大值——
我原本估的是 60 秒。

---

## 快速開始

### 本機（Docker Compose）

```bash
cp .env.example .env        # 填入 Binance / Alpaca / Discord 憑證
docker compose up -d
docker compose exec flink-jobmanager \
  /opt/flink/bin/sql-client.sh -i /opt/flink/sql/flink-init.sql \
                               -f /opt/flink/sql/submit-jobs.sql
# → http://localhost:5173  K 線圖
# → http://localhost:3000  Grafana
```

### GKE

`k8s/deploy/gcp/` 底下編號即執行順序，完整說明見 [`k8s/README.md`](k8s/README.md)。

```bash
bash k8s/deploy/gcp/05-up.sh              # 開工：node pool 擴回 3
bash k8s/deploy/gcp/16-verify-week8.sh    # 全鏈路驗收 27 項
bash k8s/deploy/gcp/19-alert-drill.sh     # 告警演練
bash k8s/deploy/gcp/06-down.sh            # 收工：縮回 0
```

⚠️ `06-down.sh` 只停運算。**磁碟（約 196GB / $13.6 每月）照樣計費**，
而「刪了叢集卻繼續收費」的頭號原因是孤兒 PVC。

---

## 文件

| 文件 | 內容 |
|---|---|
| [**`doc/knowledge/`**](doc/knowledge/) | ★ 知識庫：K8s 物件模型、RBAC、分散式協調、可觀測性、Iceberg catalog、除錯手冊 |
| [`doc/knowledge/project_schema.md`](doc/knowledge/project_schema.md) | 服務之間怎麼串（含圖） |
| [`doc/knowledge/kb-kafka.md`](doc/knowledge/kb-kafka.md) | Kafka：quorum、ISR、advertised listener、Avro wire format |
| [`doc/knowledge/debugging-playbook.md`](doc/knowledge/debugging-playbook.md) | 這四週歸納出的除錯方法與症狀索引 |
| [`k8s/README.md`](k8s/README.md) | manifests 結構、每日流程、從零重建的完整順序 |

知識庫的每個結論都盡量附上這個專案的實際證據——錯誤訊息、指令輸出、
manifest 片段。**沒有實測過的地方會明說。**

---

## 已知的缺口

刻意留著並記錄，而不是假裝不存在：

- Discord Bot 沒有健康端點（`pgrep` probe 已移除——它殺過 26 次健康的 bot）
- Gateway 沒有 Prometheus 格式的 metrics（`/api/health` 回 JSON）
- Producer 沒開 `enable.idempotence`，Spot 環境的網路抖動會產生重複訊息
- 憑證仍在 K8s Secret（etcd 裡只是 base64），未接 Secret Manager
- 沒有 NetworkPolicy，namespace 內誰都連得到誰
- 一個無法解釋的現象：某個 pod 的 `RESTARTS` 前 21 小時只有 3 次、
  之後 2 小時衝到 26，而根因是確定性的。K8s Events 只保留一小時，查不到了。
  **能解釋的和不能解釋的分開放**——把猜測寫成結論，正是前面那些錯誤的起點。
