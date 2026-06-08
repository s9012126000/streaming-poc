-- ================== Catalog ==================
CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
  'type'                  = 'iceberg',
  'catalog-type'          = 'rest',
  'uri'                   = 'http://iceberg-rest:8181',
  'warehouse'             = 's3://warehouse/',
  'io-impl'               = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'           = 'http://minio:9000',
  's3.access-key-id'      = 'minioadmin',
  's3.secret-access-key'  = 'minioadmin',
  's3.path-style-access'  = 'true',
  'client.region'         = 'us-east-1'
);

-- ================== Global Settings ==================
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode'     = 'EXACTLY_ONCE';

-- ================== Database ==================
CREATE DATABASE IF NOT EXISTS iceberg_catalog.bronze;

-- ================== Kafka Source Table (default catalog) ==================
USE CATALOG default_catalog;
USE default_database;

DROP TABLE IF EXISTS kafka_trades_source;

CREATE TABLE kafka_trades_source (
  source_type    STRING NOT NULL,
  asset_class    STRING NOT NULL,
  symbol         STRING NOT NULL,
  price          DOUBLE NOT NULL,
  size           DOUBLE NOT NULL,
  trade_time     BIGINT NOT NULL,
  exchange       STRING,
  tape           STRING,
  is_buyer_maker BOOLEAN,
  agg_trade_id   BIGINT
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = 'market-trades',
  'properties.bootstrap.servers' = 'broker-1:29092,broker-2:29092,broker-3:29092',
  'properties.group.id'          = 'flink-bronze-consumer',
  'scan.startup.mode'            = 'latest-offset',
  'value.format'                 = 'avro-confluent',
  'value.avro-confluent.url'      = 'http://schema-registry:8081',
  'value.avro-confluent.schema-registry.subject'  = 'market-trades-value'
);


-- 在 Flink SQL Client 裡執行
USE CATALOG iceberg_catalog;
USE bronze;

-- 先 DROP（清除 catalog 裡的 metadata pointer）
DROP TABLE IF EXISTS bronze.trades;

-- ================== Iceberg Bronze Table ==================
CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.trades (
  source_type    STRING,
  asset_class    STRING,
  symbol         STRING,
  price          DOUBLE,
  size           DOUBLE,
  trade_time     BIGINT,
  exchange       STRING,
  tape           STRING,
  is_buyer_maker BOOLEAN,
  agg_trade_id   BIGINT,
  event_ts       TIMESTAMP(3),
  ingest_ts      TIMESTAMP(3),
  trade_date     STRING
) PARTITIONED BY (
  source_type, trade_date
) WITH (
  'write.format.default'        = 'parquet',
  'write.target-file-size-bytes' = '67108864'
);

-- ================== Streaming Job ==================
INSERT INTO iceberg_catalog.bronze.trades
SELECT
  source_type,
  asset_class,
  symbol,
  price,
  size,
  trade_time,
  exchange,
  tape,
  is_buyer_maker,
  agg_trade_id,
  TO_TIMESTAMP_LTZ(trade_time, 3)                            AS event_ts,
  CURRENT_TIMESTAMP                                          AS ingest_ts,
  DATE_FORMAT(TO_TIMESTAMP_LTZ(trade_time, 3), 'yyyy-MM-dd') AS trade_date
FROM default_catalog.default_database.kafka_trades_source;
