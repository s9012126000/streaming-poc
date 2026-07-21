    -- 對帳 (Week 4) — Kafka 消費量 vs Bronze 落地筆數，差異應 = DLQ 筆數（正常 0）。
-- 兩步驟：
--
-- (1) 抓 Kafka 端 market-trades 各 partition 已消費 offset 總和（flink-bronze-consumer group）：
--     docker exec broker-1 kafka-consumer-groups --bootstrap-server broker-1:29092 \
--       --describe --group flink-bronze-consumer
--     把 CURRENT-OFFSET 欄位加總 = KAFKA_CONSUMED（扣掉每個 partition 的起始 offset 若非 0）。
--
-- (2) Bronze 落地筆數（Trino, catalog=iceberg）：
SELECT trade_date, count(*) AS bronze_rows
FROM iceberg.bronze.trades
GROUP BY trade_date
ORDER BY trade_date;

-- (3) 對帳：SUM(bronze_rows) + market-dlq 筆數  應等於  KAFKA_CONSUMED。
--     market-dlq 筆數：
--     docker exec broker-1 kafka-run-class kafka.tools.GetOffsetShell \
--       --broker-list broker-1:29092 --topic market-dlq | awk -F: '{s+=$3} END{print s}'
