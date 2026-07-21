"""Bronze ingest with a real DLQ.

Replaces the SQL `kafka-to-iceberg-bronze-trades` job. Flink SQL's avro-confluent
source crash-loops on a poison message and cannot side-output; this DataStream job
reads raw Kafka bytes, decodes Confluent-Avro in Python, routes good records to
Iceberg bronze.trades and undecodable ones to the market-dlq topic.

Run:  flink run -py /opt/flink/jobs/bronze_ingest.py
"""
import base64
import json
import time

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction, OutputTag
from pyflink.table import StreamTableEnvironment

BOOTSTRAP = "broker-1:29092,broker-2:29092,broker-3:29092"
SCHEMA_REGISTRY = "http://schema-registry:8081"

DLQ_TAG = OutputTag("dlq", Types.STRING())

# good-record shape = kafka_trades_source minus the SQL-computed columns
MAIN_TYPE = Types.ROW_NAMED(
    ["source_type", "asset_class", "symbol", "price", "size", "trade_time",
     "exchange", "tape", "is_buyer_maker", "agg_trade_id"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE(),
     Types.LONG(), Types.STRING(), Types.STRING(), Types.BOOLEAN(), Types.LONG()],
)


class Decode(ProcessFunction):
    """Decode Confluent-Avro; good -> main output, bad -> DLQ side output."""

    def open(self, ctx):
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        self._deser = AvroDeserializer(SchemaRegistryClient({"url": SCHEMA_REGISTRY}))

    def process_element(self, row, ctx):
        from confluent_kafka.serialization import SerializationContext, MessageField
        raw, partition, offset, topic = row[0], row[1], row[2], row[3]
        try:
            r = self._deser(bytes(raw), SerializationContext(topic, MessageField.VALUE))
            yield Row(
                r["source_type"], r["asset_class"], r["symbol"],
                float(r["price"]), float(r["size"]), int(r["trade_time"]),
                r.get("exchange"), r.get("tape"), r.get("is_buyer_maker"),
                r.get("agg_trade_id"),
            )
        except Exception as e:
            yield DLQ_TAG, json.dumps({
                "raw_b64": base64.b64encode(bytes(raw)).decode(),
                "error": f"{type(e).__name__}: {e}",
                "topic": topic, "partition": partition, "offset": offset,
                "ts": int(time.time() * 1000),
            })


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    # flink-init has no FLINK_PROPERTIES, so `flink run` resolves this job's config
    # from the image defaults (checkpointing off) — and the Iceberg sink only commits
    # on checkpoint. Set it here like flink-init.sql does for the SQL jobs.
    env.enable_checkpointing(30000)
    t_env = StreamTableEnvironment.create(env)
    # 否則 job name 會是三個 sink 串接起來的長字串（StatementSet 的預設行為）
    t_env.get_config().set("pipeline.name", "insert_bronze_trades")

    # Iceberg catalog + bronze table already exist (created by flink-init); just re-register.
    t_env.execute_sql("""
        CREATE CATALOG IF NOT EXISTS iceberg_catalog WITH (
          'type'='iceberg', 'catalog-type'='rest',
          'uri'='http://iceberg-rest:8181', 'warehouse'='s3://warehouse/',
          'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
          's3.endpoint'='http://minio:9000', 's3.path-style-access'='true',
          'client.region'='us-east-1')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE kafka_trades_raw (
          raw_value BYTES,
          `partition` INT METADATA VIRTUAL,
          `offset` BIGINT METADATA VIRTUAL,
          `topic` STRING METADATA VIRTUAL
        ) WITH (
          'connector'='kafka', 'topic'='market-trades',
          'properties.bootstrap.servers'='{BOOTSTRAP}',
          'properties.group.id'='flink-bronze-consumer',
          'properties.enable.auto.commit'='false',
          -- 從 group 的 committed offset 接續（Flink 在每次 checkpoint 時 commit），
          -- 重啟不漏資料。latest-offset 只在 group 沒有 offset 時才由
          -- auto.offset.reset 決定，等同冷啟行為。
          'scan.startup.mode'='group-offsets',
          'properties.auto.offset.reset'='latest',
          'format'='raw')
    """)

    # record in the DLQ is harmless. EXACTLY_ONCE here would need txn sink config.
    t_env.execute_sql(f"""
        CREATE TABLE kafka_dlq_sink (payload STRING) WITH (
          'connector'='kafka', 'topic'='market-dlq',
          'properties.bootstrap.servers'='{BOOTSTRAP}',
          'format'='raw')
    """)

    # Validated-records gate: downstream SQL analytics (VWAP / 3-sigma) read this
    # clean JSON topic instead of raw market-trades, so a poison message can only
    # reach this decoder (-> DLQ) and never crash-loops the SQL jobs.
    t_env.execute_sql(f"""
        CREATE TABLE kafka_trades_clean (
          source_type STRING, asset_class STRING, symbol STRING,
          price DOUBLE, size DOUBLE, trade_time BIGINT,
          exchange STRING, tape STRING, is_buyer_maker BOOLEAN, agg_trade_id BIGINT
        ) WITH (
          'connector'='kafka', 'topic'='market-trades-clean',
          'properties.bootstrap.servers'='{BOOTSTRAP}',
          'format'='json')
    """)

    raw_ds = t_env.to_data_stream(t_env.from_path("kafka_trades_raw"))
    main_ds = raw_ds.process(Decode(), output_type=MAIN_TYPE)
    dlq_ds = main_ds.get_side_output(DLQ_TAG)

    t_env.create_temporary_view("good_trades", t_env.from_data_stream(main_ds))
    t_env.create_temporary_view("dlq_rows", t_env.from_data_stream(dlq_ds))

    stmt = t_env.create_statement_set()
    # same computed columns as the original SQL Bronze job
    stmt.add_insert_sql("""
        INSERT INTO iceberg_catalog.bronze.trades
        SELECT source_type, asset_class, symbol, price, size, trade_time,
               exchange, tape, is_buyer_maker, agg_trade_id,
               TO_TIMESTAMP_LTZ(trade_time, 3),
               CURRENT_TIMESTAMP,
               DATE_FORMAT(TO_TIMESTAMP_LTZ(trade_time, 3), 'yyyy-MM-dd')
        FROM good_trades
    """)
    stmt.add_insert_sql("INSERT INTO kafka_dlq_sink SELECT f0 FROM dlq_rows")
    stmt.add_insert_sql("""
        INSERT INTO kafka_trades_clean
        SELECT source_type, asset_class, symbol, price, size, trade_time,
               exchange, tape, is_buyer_maker, agg_trade_id
        FROM good_trades
    """)
    stmt.execute()


if __name__ == "__main__":
    main()
