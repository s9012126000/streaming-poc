# 📅 Week 3 — Airflow 協調 + 可觀測性建置
> **主題**：加入 Batch 維護排程、Grafana 監控 Dashboard、Exactly-Once 深度驗證  
> **目標**：MinIO 碎片化問題解決（Compaction）、監控 Dashboard 上線、所有警報設定完成

---

## 🎯 本週交付目標

- [ ] Airflow DAG `nasdaq_iceberg_maintenance` 每小時排程正常執行
- [ ] Iceberg Compaction 執行後，Bronze Parquet 檔案數量大幅減少
- [ ] Grafana Dashboard 有 Kafka Lag、Flink Throughput 即時圖表
- [ ] Prometheus 成功抓取 Kafka + Flink 指標
- [ ] Flink Checkpoint Duration < 5 秒（Dashboard 可見）
- [ ] 資料品質告警測試通過（人工注入 Bad Data → DLQ 收到）

---

## Day 15～16：新增 Airflow + Spark + 監控服務

### 更新 docker-compose.yml（新增服務）

```yaml
# 在原本 Week 1 的 docker-compose.yml 末尾新增以下服務：

  # === AIRFLOW ===
  airflow-webserver:
    image: apache/airflow:2.9.0
    ports:
      - "8083:8080"
    depends_on: [broker]
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
      AIRFLOW__CORE__FERNET_KEY: "46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLt0nfdstY="
      AIRFLOW__WEBSERVER__SECRET_KEY: "nasdaq_poc_secret"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./plugins:/opt/airflow/plugins
    command: >
      bash -c "airflow db init && airflow users create
        --username admin --password admin
        --firstname Admin --lastname User
        --role Admin --email admin@nasdaq-poc.local
        && airflow webserver"

  airflow-scheduler:
    image: apache/airflow:2.9.0
    depends_on: [airflow-webserver]
    volumes:
      - ./dags:/opt/airflow/dags
    command: airflow scheduler

  # === SPARK（用於 Batch Iceberg 維護）===
  spark-master:
    image: bitnami/spark:3.5
    ports:
      - "7077:7077"
      - "9090:8080"
    environment:
      SPARK_MODE: master
    volumes:
      - ./jobs:/jobs

  spark-worker:
    image: bitnami/spark:3.5
    depends_on: [spark-master]
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077

  # === 監控 ===
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9091:9090"
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    depends_on: [prometheus]
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning
```

---

## Day 15：Airflow DAG 實作（Iceberg 維護排程）

### dags/nasdaq_iceberg_maintenance.py

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.sensors.kafka import KafkaConsumerSensor
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

SPARK_SUBMIT = (
    "docker exec spark-master spark-submit "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.0,"
    "software.amazon.awssdk:bundle:2.20.18 "
    "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.iceberg.type=rest "
    "--conf spark.sql.catalog.iceberg.uri=http://iceberg-rest:8181 "
    "--conf spark.sql.catalog.iceberg.warehouse=s3://warehouse/ "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
    "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
)

default_args = {
    "owner":           "nasdaq-poc",
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    "nasdaq_iceberg_maintenance",
    schedule_interval="0 * * * *",   # 每小時整點執行
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["iceberg", "maintenance", "nasdaq"]
) as dag:

    # -------------------------------------------------------
    # Task 1: Bronze Small File Compaction
    # 問題：Flink 每分鐘寫入，產生大量 <64MB 的小 Parquet 檔
    # 解法：rewrite_data_files 將碎片合併為 128MB 的標準大小
    # -------------------------------------------------------
    compact_bronze = BashOperator(
        task_id="compact_bronze_trades",
        bash_command=f"""
            {SPARK_SUBMIT} /jobs/iceberg_compaction.py \\
              --table iceberg.bronze.nasdaq_trades \\
              --target-file-size-bytes 134217728
        """,
        execution_timeout=timedelta(minutes=30)
    )

    # -------------------------------------------------------
    # Task 2: Bronze → Silver MERGE（每日 OHLCV 彙總）
    # -------------------------------------------------------
    merge_silver_ohlcv = BashOperator(
        task_id="merge_silver_daily_ohlcv",
        bash_command=f"""
            {SPARK_SUBMIT} /jobs/silver_daily_ohlcv.py \\
              --date "{{{{ ds }}}}"
        """,
        execution_timeout=timedelta(minutes=20)
    )

    # -------------------------------------------------------
    # Task 3: Silver → Gold Top Movers
    # -------------------------------------------------------
    build_gold = BashOperator(
        task_id="build_gold_top_movers",
        bash_command=f"""
            {SPARK_SUBMIT} /jobs/gold_top_movers.py \\
              --date "{{{{ ds }}}}"
        """,
        execution_timeout=timedelta(minutes=15)
    )

    # -------------------------------------------------------
    # Task 4: 清除過期 Snapshots（保留 7 天）
    # -------------------------------------------------------
    expire_snapshots = BashOperator(
        task_id="expire_old_snapshots",
        bash_command=f"""
            {SPARK_SUBMIT} /jobs/iceberg_expire_snapshots.py \\
              --table iceberg.bronze.nasdaq_trades \\
              --older-than-days 7
        """,
        execution_timeout=timedelta(minutes=10)
    )

    # DAG 依賴關係
    compact_bronze >> merge_silver_ohlcv >> build_gold >> expire_snapshots
```

### jobs/iceberg_compaction.py

```python
import argparse
from pyspark.sql import SparkSession

def run_compaction(table: str, target_file_size: int):
    spark = SparkSession.builder.appName("IcebergCompaction").getOrCreate()

    result = spark.sql(f"""
        CALL iceberg.system.rewrite_data_files(
          table => '{table}',
          strategy => 'binpack',
          options => map(
            'target-file-size-bytes', '{target_file_size}',
            'min-file-size-bytes', '{target_file_size // 2}',
            'max-file-size-bytes', '{target_file_size * 2}',
            'max-concurrent-file-group-rewrites', '5'
          )
        )
    """)

    row = result.first()
    print(f"✅ Compaction done: {row.rewritten_files_count} files → {row.added_files_count} files")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--target-file-size-bytes", type=int, default=134217728)
    args = parser.parse_args()
    run_compaction(args.table, args.target_file_size_bytes)
```

---

## Day 17～18：Prometheus + Grafana 監控建置

### monitoring/prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: "kafka-jmx"
    static_configs:
      - targets: ["broker:7071"]   # 需在 broker 啟動時加 JMX Exporter
    metrics_path: /metrics

  - job_name: "flink"
    static_configs:
      - targets: ["flink-jobmanager:9249"]  # Flink Prometheus Reporter
    metrics_path: /

  - job_name: "minio"
    static_configs:
      - targets: ["minio:9000"]
    metrics_path: /minio/v2/metrics/cluster
```

### Grafana Dashboard 建議面板

建立 `monitoring/grafana/dashboards/nasdaq_overview.json`，包含以下面板：

| 面板名稱 | PromQL 指標 | 告警條件 |
|---|---|---|
| **Kafka Consumer Lag（每 symbol）** | `kafka_consumer_group_lag{topic="nasdaq-trades"}` | Lag > 5,000 |
| **Flink Records/sec** | `flink_taskmanager_Records_IN_per_second` | < 50/s |
| **Flink Checkpoint Duration** | `flink_jobmanager_job_lastCheckpointDuration` | > 10,000ms |
| **Flink Job Restart 次數** | `flink_jobmanager_job_numberOfRestarts` | > 0 |
| **MinIO Storage 使用量** | `minio_cluster_capacity_usable_free_bytes` | < 20% free |
| **Bronze Table 每小時增量** | Custom（Trino API） | 增量 = 0 |

---

## Day 19：資料品質 — Dead Letter Queue 實作

```python
# 在 Flink Bronze Job 中加入 DLQ 邏輯
# 當資料不符合品質規則時，寫入 Dead Letter Topic

CREATE TABLE kafka_dlq (
  original_symbol  STRING,
  original_price   DOUBLE,
  original_size    INT,
  error_reason     STRING,
  failed_at        TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'nasdaq-dlq',
  'properties.bootstrap.servers' = 'broker:29092',
  'format'                       = 'json'
);

-- 品質規則：價格異常（漲跌超過 50%）或 size 為 0
INSERT INTO kafka_dlq
SELECT
  symbol  AS original_symbol,
  price   AS original_price,
  size    AS original_size,
  CASE
    WHEN price <= 0          THEN 'INVALID_PRICE_ZERO_OR_NEGATIVE'
    WHEN size  <= 0          THEN 'INVALID_SIZE_ZERO_OR_NEGATIVE'
    WHEN price > 100000      THEN 'PRICE_EXCEEDS_THRESHOLD'
    ELSE                          'UNKNOWN_ERROR'
  END                        AS error_reason,
  CURRENT_TIMESTAMP          AS failed_at
FROM kafka_nasdaq_trades
WHERE price <= 0 OR size <= 0 OR price > 100000;
```

---

## Day 20～21：Flink Tuning — 效能調優

### RocksDB State Backend 調優

在 `docker-compose.yml` 的 Flink 環境變數中加入：

```yaml
FLINK_PROPERTIES: |
  # State Backend
  state.backend: rocksdb
  state.backend.rocksdb.memory.managed: true
  state.backend.rocksdb.memory.write-buffer-ratio: 0.5
  state.backend.rocksdb.memory.high-prio-pool-ratio: 0.1

  # Checkpoint 設定
  execution.checkpointing.interval: 30s
  execution.checkpointing.mode: EXACTLY_ONCE
  execution.checkpointing.max-concurrent-checkpoints: 1
  execution.checkpointing.timeout: 60s

  # Iceberg Commit 優化
  sink.iceberg.write.target-file-size-bytes: 67108864
  sink.iceberg.write.fanout.enabled: true
```

---

## ✅ Week 3 驗收清單

| 項目 | 驗證方式 | 預期結果 |
|---|---|---|
| Airflow DAG 執行 | `http://localhost:8083` | `nasdaq_iceberg_maintenance` 4 個 Task 全部 ✅ |
| Compaction 效果 | MinIO → bronze/ parquet 數量 | 執行前後檔案數減少 > 80% |
| Grafana 監控 | `http://localhost:3000` | Consumer Lag、Checkpoint Duration 圖表正常顯示 |
| DLQ 機制 | `kafka-console-consumer nasdaq-dlq` | 注入壞資料後，DLQ 有收到訊息 |
| Checkpoint 時間 | Flink Dashboard → Checkpoints | Duration < 5,000ms |
| 告警測試 | 手動模擬 Lag 暴增 | Grafana 告警觸發，顏色變紅 |
