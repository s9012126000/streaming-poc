-- ================== Job: Kafka → Iceberg Bronze Trades ==================
SET 'pipeline.name' = 'kafka-to-iceberg-bronze-trades';

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
  TO_TIMESTAMP_LTZ(trade_time, 3)                             AS event_ts,
  CURRENT_TIMESTAMP                                           AS ingest_ts,
  DATE_FORMAT(TO_TIMESTAMP_LTZ(trade_time, 3), 'yyyy-MM-dd') AS trade_date
FROM default_catalog.default_database.kafka_trades_source;
