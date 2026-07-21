"""Send one poison message to market-trades to exercise the DLQ (Week 4).

Garbage bytes with a fake Confluent framing (magic 0x00 + bogus schema id) so the
Bronze job's Avro decode fails and routes it to market-dlq — job must stay RUNNING.

Run:  python scripts/send_poison.py
"""
import os
from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

poison = b"\x00\x00\x00\x00\x63" + b"this-is-not-valid-avro"  # magic + schema id 99 + junk

p = Producer({"bootstrap.servers": BOOTSTRAP})
p.produce("market-trades", key="POISON", value=poison)
p.flush(10)
print("sent poison message to market-trades -> expect it in market-dlq")
