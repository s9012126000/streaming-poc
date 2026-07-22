-- ================== Session tables only (non-destructive) ==================
-- 給「重新提交分析 job」用的 init file，不碰 Iceberg 資料：
--
--   flink cancel <jid> …
--   sql-client.sh -D rest.address=flink-jobmanager \
--     -i /opt/flink/sql/session-tables.sql -f /opt/flink/sql/submit-jobs.sql
--
-- 為什麼需要這個檔：flink-init.sql 會 DROP 掉 iceberg_catalog.bronze.trades /
-- silver.vwap_1min，拿它來重啟 job 等於把倉庫清空。這裡只建 sql-client session 內的
-- 記憶體表（default_catalog）與註冊既有的 Iceberg catalog，DDL 必須與 flink-init.sql
-- 的對應段落保持一致。

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.timeout' = '60s';
SET 'table.exec.source.idle-timeout' = '30s';

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
  'topic'                         = 'market-trades-clean',
  'properties.bootstrap.servers'  = 'broker-1:29092,broker-2:29092,broker-3:29092',
  'properties.group.id'           = 'flink-analytics-consumer',
  'properties.enable.auto.commit' = 'false',
  -- latest-offset：重新提交時跳過 topic 裡既有的訊息。
  -- 一筆時間戳在未來的訊息會把 watermark 拉到未來，之後所有真實成交都變成遲到資料
  -- 而被 OVER window 丟掉——這正是需要重啟 job 的典型情境，所以絕不能從頭重讀。
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
  alert_ts     TIMESTAMP(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'market-alerts',
  'properties.bootstrap.servers' = 'broker-1:29092,broker-2:29092,broker-3:29092',
  'format'                       = 'json'
);
