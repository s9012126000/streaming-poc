-- ================== Session Settings ==================
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.timeout' = '60s';
SET 'table.exec.source.idle-timeout' = '30s';

-- ================== Iceberg Catalog ==================
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type'                  = 'iceberg',
  'catalog-type'          = 'rest',
  'uri'                   = 'http://iceberg-rest:8181',
  'warehouse'             = 's3://warehouse/',
  'io-impl'               = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'           = 'http://minio:9000',
  's3.path-style-access'  = 'true',
  'client.region'         = 'us-east-1'
);

-- ================== Database ==================
CREATE DATABASE IF NOT EXISTS iceberg_catalog.bronze;
CREATE DATABASE IF NOT EXISTS iceberg_catalog.silver;
CREATE DATABASE IF NOT EXISTS iceberg_catalog.gold;

-- ================== Kafka Source Table ==================
USE CATALOG default_catalog;
USE default_database;

DROP TABLE IF EXISTS kafka_trades_source;

CREATE TABLE kafka_trades_source (
  source_type     STRING NOT NULL,
  asset_class     STRING NOT NULL,
  symbol          STRING NOT NULL,
  price           DOUBLE NOT NULL,
  size            DOUBLE NOT NULL,
  trade_time      BIGINT NOT NULL,
  exchange        STRING,
  tape            STRING,
  is_buyer_maker  BOOLEAN,
  agg_trade_id    BIGINT,
  event_ts        AS TO_TIMESTAMP_LTZ(trade_time, 3),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                     = 'kafka',
  -- 讀「已驗證」的 clean topic（由 PyFlink Bronze job 產出），不直接讀 raw market-trades。
  -- 毒訊息只會打到 Bronze decoder（→ DLQ），不會 crash-loop 這裡的 VWAP / 3-sigma。
  'topic'                         = 'market-trades-clean',
  'properties.bootstrap.servers'  = 'broker-1:29092,broker-2:29092,broker-3:29092',
  'properties.group.id'           = 'flink-analytics-consumer',
  'properties.enable.auto.commit' = 'false',
  'scan.startup.mode'             = 'latest-offset',
  'format'                        = 'json',
  'json.ignore-parse-errors'      = 'true'
);

DROP TABLE IF EXISTS kafka_alerts_sink;

CREATE TABLE kafka_alerts_sink (
  symbol       STRING,
  source_type  STRING,
  volume       DOUBLE,
  mean         DOUBLE,
  stddev       DOUBLE,
  sigma        DOUBLE,
  price_chg    DOUBLE,
  alert_ts     TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'market-alerts',
  'properties.bootstrap.servers' = 'broker-1:29092,broker-2:29092,broker-3:29092',
  'format'                       = 'json'
);

-- ================== Iceberg Bronze Sink Table ==================
USE CATALOG iceberg_catalog;
USE bronze;

DROP TABLE IF EXISTS iceberg_catalog.bronze.trades;

CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.trades (
  source_type     STRING,
  asset_class     STRING,
  symbol          STRING,
  price           DOUBLE,
  size            DOUBLE,
  trade_time      BIGINT,
  exchange        STRING,
  tape            STRING,
  is_buyer_maker  BOOLEAN,
  agg_trade_id    BIGINT,
  event_ts        TIMESTAMP(3),
  ingest_ts       TIMESTAMP(3),
  trade_date      STRING
) PARTITIONED BY (source_type, trade_date)
WITH (
  'write.format.default'          = 'parquet',
  'write.target-file-size-bytes'  = '67108864',
  'write.parquet.compression-codec' = 'snappy'
);

USE silver;

DROP TABLE IF EXISTS iceberg_catalog.silver.vwap_1min;

CREATE TABLE IF NOT EXISTS iceberg_catalog.silver.vwap_1min (
  source_type   STRING,
  symbol        STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  vwap          DOUBLE,
  volume        DOUBLE,
  trade_count   BIGINT,
  window_date   STRING
) PARTITIONED BY (source_type, window_date)
WITH (
  'write.format.default'            = 'parquet',
  'write.target-file-size-bytes'    = '67108864',
  'write.parquet.compression-codec' = 'snappy'
);