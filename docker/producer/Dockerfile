# ---------------------------------------------------------------------------
# Producer —— compose 時代是在 host 上直接跑 main_producer.py，
# 沒有對應的 image。上 K8s 必須容器化，所以這支是新寫的。
#
# 依賴釘死版本、直接寫在 Dockerfile —— 照 repo 既有慣例
# （gateway / bot 的 Dockerfile 也是這樣，不另開 requirements.txt）。
# ---------------------------------------------------------------------------
FROM python:3.12-slim

# 版本經沙箱實測安裝並驗證 import 成功，不是憑印象寫的。
#
# 兩個容易寫錯的地方：
#   1. websockets ≠ websocket-client —— 是兩個不同的套件。
#      binance_producer.py 用的是 `import websockets`（async 版）。
#   2. confluent-kafka 要帶 [avro,schemaregistry] extras——
#      沒有的話 `from confluent_kafka.schema_registry.avro import AvroSerializer`
#      會在**執行時**才 ImportError（build 是成功的），
#      症狀是 pod CrashLoop 而 image 看起來沒問題。
RUN pip install --no-cache-dir \
      "confluent-kafka[avro,schemaregistry]==2.15.0" \
      websockets==16.1.1 \
      alpaca-py==0.44.0 \
      python-dotenv==1.2.3 \
      pytz==2026.3.post1

WORKDIR /app

# schemas/ 是必要的：main_producer.py 會 open("schemas/market_trade.avsc")
COPY schemas/  ./schemas/
COPY utils/    ./utils/
COPY producers/ ./producers/
COPY main_producer.py ./

# 極簡的 health / metrics 端點。
#
# 為什麼需要：Producer 是常駐的 Kafka client，沒有對外服務，
# 但 Week 8 驗收要求「全部 pod 有 probe」。
# 用 exec probe 檢查 process 訊號太弱（活著不代表在工作），
# 加一個 HTTP 端點順便讓 Week 9 的 Prometheus 抓得到指標。
COPY docker/producer/healthz.py ./healthz.py

EXPOSE 8000

# 先在背景起 health server，再跑 producer 本體。
# exec 讓 producer 成為 PID 1，才收得到 SIGTERM 做優雅關閉。
CMD ["sh", "-c", "python healthz.py & exec python -u main_producer.py"]
