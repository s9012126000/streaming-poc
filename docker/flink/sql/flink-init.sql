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
  agg_trade_id    BIGINT
) WITH (
  'connector'                                   = 'kafka',
  'topic'                                       = 'market-trades',
  'properties.bootstrap.servers'                = 'broker-1:29092,broker-2:29092,broker-3:29092',
  'properties.group.id'                         = 'flink-bronze-consumer',
  'properties.enable.auto.commit'               = 'false',
  'scan.startup.mode'                           = 'latest-offset',
  'value.format'                                = 'avro-confluent',
  'value.avro-confluent.url'                    = 'http://schema-registry:8081',
  'value.avro-confluent.schema-registry.subject'= 'market-trades-value'
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