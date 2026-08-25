-- ================== Job: Kafka → Iceberg Bronze Trades ==================
-- Bronze ingest moved to the PyFlink DataStream job jobs/bronze_ingest.py (Week 4):
-- SQL avro-confluent can't side-output poison messages to the DLQ. Submitted by
-- flink-init after these SQL jobs via `flink run -py`.

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


-- ================== Job: Volume Anomaly ==================
-- Moved to the PyFlink job jobs/volume_anomaly.py: the 3-sigma threshold was replaced by
-- median + MAD on log1p(size), and MAD is a two-pass statistic that no SQL over-window can
-- express. Submitted by flink-init after these SQL jobs via `flink run -py`.