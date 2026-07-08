-- ================== Job: Kafka → Iceberg Bronze Trades ==================
SET 'pipeline.name' = 'kafka-to-iceberg-bronze-trades';
SET 'parallelism.default' = '2';
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

-- ================== Job: VWAP 1-min Silver ==================
SET 'pipeline.name' = 'vwap-1min-silver';
SET 'parallelism.default' = '1';

INSERT INTO iceberg_catalog.silver.vwap_1min
SELECT
  source_type,
  symbol,
  window_start,
  window_end,
  SUM(price * size) / NULLIF(SUM(size), 0)              AS vwap,
  SUM(size)                                             AS volume,
  COUNT(*)                                              AS trade_count,
  DATE_FORMAT(window_start, 'yyyy-MM-dd')              AS window_date
FROM TABLE(
  TUMBLE(TABLE default_catalog.default_database.kafka_trades_source,
         DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
GROUP BY source_type, symbol, window_start, window_end;


-- ================== Job: Volume Anomaly (3-sigma) ==================
SET 'pipeline.name' = 'volume-anomaly-3sigma';
SET 'parallelism.default' = '1';

INSERT INTO default_catalog.default_database.kafka_alerts_sink
SELECT symbol, source_type, size AS volume, mean, stddev,
       (size - mean) / NULLIF(stddev, 0) AS sigma,
       CURRENT_TIMESTAMP AS alert_ts
FROM (
  SELECT
    symbol, source_type, size,
    AVG(size)    OVER w AS mean,
    STDDEV_POP(size) OVER w AS stddev,
    COUNT(*)     OVER w AS n
  FROM default_catalog.default_database.kafka_trades_source
  WINDOW w AS (
    PARTITION BY source_type, symbol
    ORDER BY event_ts
    ROWS BETWEEN 20 PRECEDING AND CURRENT ROW)
)
WHERE n >= 20 AND size > mean + 3 * stddev;