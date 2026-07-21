-- Iceberg 表維護 (Week 4) — 在 Trino 上手動跑一次，記錄前後數字到 week4 doc。
-- 連線： docker exec -it trino trino   （catalog=iceberg）

-- 1. Compaction 前：檔案數 + 平均檔案大小
SELECT count(*) AS file_count, avg(file_size_in_bytes) AS avg_bytes
FROM iceberg.bronze."trades$files";

-- （另跑一條代表性查詢並記錄耗時，例如：）
-- SELECT source_type, count(*) FROM iceberg.bronze.trades GROUP BY source_type;

-- 2. 小檔案合併
ALTER TABLE iceberg.bronze.trades   EXECUTE optimize(file_size_threshold => '64MB');
ALTER TABLE iceberg.silver.vwap_1min EXECUTE optimize(file_size_threshold => '64MB');

-- 3. Snapshot expiration（保留 7 天，對齊 Kafka retention 168h）
ALTER TABLE iceberg.bronze.trades   EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.silver.vwap_1min EXECUTE expire_snapshots(retention_threshold => '7d');

-- 4. Compaction 後：重跑步驟 1，比對 file_count / avg_bytes / 查詢耗時
SELECT count(*) AS file_count, avg(file_size_in_bytes) AS avg_bytes
FROM iceberg.bronze."trades$files";
