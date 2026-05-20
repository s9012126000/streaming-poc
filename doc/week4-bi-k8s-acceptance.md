# 📅 Week 4 — Superset BI + Data Lineage + K8s 遷移 + 整合驗收
> **主題**：完成 BI 儀表板、Marquez 血緣圖，並將 Flink Job 遷移至 Kubernetes  
> **目標**：全鏈路通暢，10 項整合驗收測試全部通過，POC 成果可展示

---

## 🎯 本週交付目標

- [ ] Superset NASDAQ Dashboard 上線（VWAP 走勢圖 + Top Volume 排名）
- [ ] Marquez Data Lineage：從 Kafka → Flink → Iceberg → Trino 全鏈路血緣圖
- [ ] Schema Evolution 驗證：動態新增欄位不重啟 Flink Job
- [ ] Iceberg Time Travel 驗證：可查詢過去任意時間點的資料快照
- [ ] Flink K8s Operator 部署（遷移演練）
- [ ] 10 項整合驗收測試全部通過

---

## Day 22～23：Superset NASDAQ Dashboard

### 新增 Superset 服務

```yaml
# docker-compose.yml 新增
  superset:
    image: apache/superset:4.0.0
    ports:
      - "8088:8088"
    depends_on: [trino]
    environment:
      SUPERSET_SECRET_KEY: "nasdaq_poc_superset_secret"
    command: >
      bash -c "superset db upgrade &&
               superset fab create-admin
                 --username admin --firstname Admin
                 --lastname User --email admin@example.com
                 --password admin &&
               superset init &&
               superset run -p 8088 --with-threads --reload --debugger"

  trino:
    image: trinodb/trino:450
    ports:
      - "8080:8080"
    depends_on: [iceberg-rest]
    volumes:
      - ./trino/catalog:/etc/trino/catalog
```

### trino/catalog/iceberg.properties

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
```

### 建立 Superset 資料集（Trino SQL）

```sql
-- Dataset 1：即時 VWAP（過去 1 小時，供折線圖使用）
-- 在 Superset → Datasets → New Dataset → 選擇 Trino 連線，輸入以下 SQL

SELECT
  window_start,
  symbol,
  ROUND(vwap, 2)                              AS vwap_price,
  total_volume,
  trade_count,
  ROUND(price_high, 2)                        AS price_high,
  ROUND(price_low, 2)                         AS price_low,
  ROUND(price_high - price_low, 2)            AS price_range,
  ROUND((price_high - price_low) / price_low * 100, 3) AS intraday_range_pct
FROM iceberg.silver.vwap_1min
WHERE window_start >= NOW() - INTERVAL '2' HOUR
  AND symbol IN ('AAPL','MSFT','NVDA','TSLA','META','AMZN','GOOGL')
ORDER BY window_start DESC;

-- Dataset 2：今日 Top 10 Volume（供 Bar Chart 使用）
SELECT
  symbol,
  SUM(total_volume)  AS total_daily_volume,
  ROUND(AVG(vwap), 2) AS avg_vwap,
  MAX(price_high)     AS day_high,
  MIN(price_low)      AS day_low,
  COUNT(*)            AS bar_count
FROM iceberg.silver.vwap_1min
WHERE DATE(window_start) = CURRENT_DATE
GROUP BY symbol
ORDER BY total_daily_volume DESC
LIMIT 10;

-- Dataset 3：異常事件時間軸（供 Timeline Chart 使用）
SELECT
  CAST(from_unixtime(CAST(alert_ts AS DOUBLE) / 1000) AS TIMESTAMP) AS alert_time,
  symbol,
  volume,
  mean,
  stddev,
  ROUND(CAST(volume AS DOUBLE) / NULLIF(stddev, 0), 1) AS sigma_level
FROM iceberg.gold.volume_alerts
ORDER BY alert_time DESC
LIMIT 100;
```

### 建議 Dashboard 頁面配置

| 頁面 | 圖表類型 | 資料集 | 說明 |
|---|---|---|---|
| **Overview** | Bar Chart（水平） | Dataset 2 | Top 10 今日成交量排名 |
| **Overview** | Big Number | Dataset 1 | AAPL/NVDA 即時 VWAP |
| **VWAP 走勢** | Line Chart（多線） | Dataset 1 | 7 支股票過去 2 小時 VWAP 折線 |
| **價格波動** | Candlestick | Dataset 1 | 選定股票的 1min OHLC |
| **異常告警** | Event Chart | Dataset 3 | 成交量異常事件標記 |

---

## Day 24：Data Lineage（OpenLineage + Marquez）

### 新增 Marquez 服務

```yaml
  marquez:
    image: marquezproject/marquez:0.47.0
    ports:
      - "5000:5000"
      - "5001:5001"
    environment:
      MARQUEZ_PORT: 5000
      MARQUEZ_ADMIN_PORT: 5001
    depends_on: [postgres-marquez]

  postgres-marquez:
    image: postgres:15
    environment:
      POSTGRES_USER: marquez
      POSTGRES_PASSWORD: marquez
      POSTGRES_DB: marquez
```

### 在 Flink Job 加入 OpenLineage 追蹤

在 `flink-jobmanager` 的 `FLINK_PROPERTIES` 中加入：

```yaml
FLINK_PROPERTIES: |
  # ... 原有設定 ...
  metrics.reporter.openlineage.factory.class: io.openlineage.flink.FlinkMetricsReporter
  openlineage.transport.type: http
  openlineage.transport.url: http://marquez:5000
  openlineage.transport.endpoint: /api/v1/lineage
  openlineage.namespace: nasdaq-poc
```

### 查看血緣圖

開啟 `http://localhost:5001`（Marquez UI），預期看到以下血緣圖：

```
[Kafka: nasdaq-trades]
        ↓
[Flink Job: nasdaq_bronze_writer]
        ↓
[Iceberg: bronze.nasdaq_trades]
        ↓
[Flink Job: nasdaq_vwap_calculator]
        ↓
[Iceberg: silver.vwap_1min]
        ↓
[Trino: nasdaq_vwap_query]
        ↓
[Superset Dashboard: NASDAQ Overview]
```

---

## Day 25：Schema Evolution 驗證（不停機升級）

```python
# 場景：Alpaca 新增了 `sector` 欄位，驗證 Flink 可動態適應

# Step 1：在 Schema Registry 新增版本（Backward Compatible）
curl -X POST http://localhost:8081/subjects/nasdaq-trades-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{
    "schema": "{\"type\":\"record\",\"name\":\"StockTrade\",\"namespace\":\"com.nasdaq.poc\",\"fields\":[{\"name\":\"symbol\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"size\",\"type\":\"int\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"exchange\",\"type\":\"string\"},{\"name\":\"tape\",\"type\":\"string\"},{\"name\":\"sector\",\"type\":[\"null\",\"string\"],\"default\":null}]}"
  }'

# Step 2：確認 Flink Job 仍在 RUNNING 狀態（不需重啟）
docker exec flink-jobmanager flink list --running

# Step 3：查詢 Iceberg Bronze Table，確認新欄位自動出現
docker exec trino trino --execute \
  "DESCRIBE iceberg.bronze.nasdaq_trades"
# 預期：sector 欄位出現在欄位列表中

# Step 4：Time Travel — 查詢 schema evolution 前的資料快照
docker exec trino trino --execute "
  SELECT symbol, price, size
  FROM iceberg.bronze.nasdaq_trades
  FOR TIMESTAMP AS OF TIMESTAMP '$(date -u -d '1 hour ago' '+%Y-%m-%d %H:%M:%S')'
  LIMIT 5
"
```

---

## Day 26～27：Flink on Kubernetes 遷移演練

### 前置準備

```bash
# 安裝 cert-manager（Flink Operator 依賴）
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.0/cert-manager.yaml
kubectl wait --for=condition=Available deployment/cert-manager -n cert-manager --timeout=120s

# 安裝 Flink Kubernetes Operator 1.11
helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.11.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace

# 確認 Operator 正常運行
kubectl get pods -n flink-operator
```

### k8s/nasdaq-flink-deployment.yaml

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: nasdaq-vwap-job
  namespace: streaming
spec:
  image: your-registry/nasdaq-flink-job:v1.0.0
  flinkVersion: v2_0
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "4"
    state.backend: rocksdb
    state.backend.rocksdb.memory.managed: "true"
    state.checkpoints.dir: s3://warehouse/checkpoints
    execution.checkpointing.interval: "30s"
    execution.checkpointing.mode: EXACTLY_ONCE
    metrics.reporter.prometheus.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
    metrics.reporter.prometheus.port: "9249"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 2
    replicas: 1
  taskManager:
    resource:
      memory: "4096m"
      cpu: 2
    replicas: 2
  job:
    jarURI: local:///opt/flink/usrlib/nasdaq-streaming.jar
    parallelism: 8
    upgradeMode: savepoint      # 升版時自動建立 Savepoint，零停機
    savepointTriggerNonce: 0
```

```bash
# 部署 NASDAQ Flink Job
kubectl create namespace streaming
kubectl apply -f k8s/nasdaq-flink-deployment.yaml

# 觀察 Job 狀態
kubectl get flinkdeployment -n streaming
kubectl describe flinkdeployment nasdaq-vwap-job -n streaming

# 預期輸出：
# NAME              JOB STATUS   LIFECYCLE STATE
# nasdaq-vwap-job   RUNNING      STABLE
```

---

## Day 28：整合驗收測試（Final Acceptance）

### 10 項驗收測試矩陣

| # | 測試項目 | 驗證步驟 | 通過標準 |
|---|---|---|---|
| 1 | **端到端延遲** | 記錄 Trade 產生時間 vs. Silver Iceberg 寫入時間差 | 平均延遲 < 5 秒 |
| 2 | **資料完整性** | `SELECT count(*)` 對比 Alpaca 推送量與 Bronze 筆數 | 差異 < 0.5% |
| 3 | **Exactly-Once** | 殺掉 TaskManager 後查重複 `(symbol, trade_ts)` | 重複筆數 = 0 |
| 4 | **Schema Evolution** | 動態新增 `sector` 欄位 | Flink Job 持續 RUNNING，新欄位自動出現 |
| 5 | **Time Travel** | `SELECT ... FOR TIMESTAMP AS OF` 查詢 1 小時前 | 返回正確歷史快照 |
| 6 | **Compaction 效果** | 手動觸發 Compaction 前後比較 MinIO 檔案數 | 檔案數減少 > 70% |
| 7 | **Airflow 排程** | 手動 Trigger `nasdaq_iceberg_maintenance` | 4 個 Task 全部 success |
| 8 | **Grafana 告警** | 停止 Producer，模擬資料斷流 | Lag 告警在 1 分鐘內觸發 |
| 9 | **Data Lineage** | 開啟 Marquez UI | 顯示完整 Kafka→Flink→Iceberg→Trino 鏈路 |
| 10 | **K8s 部署** | `kubectl get flinkdeployment` | STATUS = STABLE / RUNNING |

### 驗收報告 SQL

```sql
-- 最終驗收用的 Trino 查詢集，執行後截圖作為 POC 成果

-- 1. 資料總量
SELECT
  'bronze.nasdaq_trades' AS table_name,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT symbol) AS unique_symbols,
  MIN(trade_ts) AS earliest_trade,
  MAX(trade_ts) AS latest_trade
FROM iceberg.bronze.nasdaq_trades;

-- 2. 各股票今日 VWAP 最終結果
SELECT
  symbol,
  ROUND(AVG(vwap), 2)       AS daily_avg_vwap,
  SUM(total_volume)          AS total_daily_volume,
  MAX(price_high)            AS day_high,
  MIN(price_low)             AS day_low,
  ROUND((MAX(price_high) - MIN(price_low))
        / MIN(price_low) * 100, 2) AS intraday_move_pct
FROM iceberg.silver.vwap_1min
WHERE DATE(window_start) = CURRENT_DATE
GROUP BY symbol
ORDER BY total_daily_volume DESC;

-- 3. Iceberg 快照資訊（Time Travel 基礎）
SELECT
  snapshot_id,
  CAST(from_unixtime(committed_at / 1000) AS TIMESTAMP) AS snapshot_time,
  operation,
  summary
FROM iceberg.bronze."nasdaq_trades$snapshots"
ORDER BY committed_at DESC
LIMIT 5;
```

---

## ✅ Week 4 驗收清單

| 項目 | 驗證方式 | 預期結果 |
|---|---|---|
| Superset Dashboard | `http://localhost:8088` | VWAP 折線圖 + Volume Bar 正常顯示 |
| Data Lineage | `http://localhost:5001` | 完整血緣圖展示 |
| Schema Evolution | Trino DESCRIBE table | `sector` 欄位存在 |
| Time Travel | Trino `FOR TIMESTAMP AS OF` | 返回歷史資料 |
| K8s Deployment | `kubectl get flinkdeployment` | STATUS = STABLE |
| 全部 10 項驗收 | 測試矩陣 | 全數通過 ✅ |

---

## 🌐 完整服務 URL 總覽（Week 4 全量）

| 服務 | URL | 帳號密碼 |
|---|---|---|
| Kafka Control Center | http://localhost:9021 | 無需登入 |
| Schema Registry | http://localhost:8081 | API |
| Flink Dashboard | http://localhost:8082 | 無需登入 |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Iceberg REST API | http://localhost:8181 | API |
| Airflow | http://localhost:8083 | admin / admin |
| Trino UI | http://localhost:8080 | 無需登入 |
| Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9091 | 無需登入 |
| Marquez UI | http://localhost:5001 | 無需登入 |
| Spark Master | http://localhost:9090 | 無需登入 |
