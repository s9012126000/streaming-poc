"""
極簡 health / metrics 端點，給 K8s probe 和 Prometheus 用。

為什麼需要：
    Producer 和 Bot 都是常駐的 Kafka client，沒有對外的 HTTP 服務。
    但 K8s 的 probe 需要一個可檢查的端點——用 exec 檢查 process 存在
    訊號太弱（process 活著不代表它還在送資料）。

    加一個 HTTP 端點還有第二個好處：Week 9 的 Prometheus 可以直接抓，
    不用另外裝 exporter。

刻意不用 FastAPI / prometheus_client：
    這支只需要回兩個固定端點，標準函式庫的 http.server 就夠了，
    省下一個相依套件。producer 的 image 已經夠大了。

端點：
    GET /healthz  → 200 "ok"        （liveness：這個 process 還活著嗎）
    GET /readyz   → 200 / 503       （readiness：最近有成功送出資料嗎）
    GET /metrics  → Prometheus 格式
"""
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(os.environ.get("HEALTH_PORT", "8000"))

# 「最近一次成功送出」的時間戳。producer 本體可以透過
# touch 這個檔案來更新——用檔案而不是共享記憶體，
# 是因為 health server 和 producer 是兩個獨立的 Python 行程。
HEARTBEAT_FILE = os.environ.get("HEARTBEAT_FILE", "/tmp/producer-heartbeat")

# 超過這個秒數沒有心跳就視為 not ready。
# 設 180 秒是因為 main_producer.py 的檢查週期是 60 秒，
# 留三個週期的緩衝，避免正常的短暫停頓造成誤判。
STALE_AFTER = int(os.environ.get("HEARTBEAT_STALE_SECS", "180"))

_started_at = time.time()


def _heartbeat_age():
    """距離上次心跳幾秒。檔案不存在時回 None（還沒開始送資料）。"""
    try:
        return time.time() - os.path.getmtime(HEARTBEAT_FILE)
    except OSError:
        return None


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="text/plain; charset=utf-8"):
        payload = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/healthz":
            # liveness 只問「這個 process 還在嗎」——
            # 絕不檢查 Kafka 之類的下游。下游一慢就整批重啟是經典災難。
            self._send(200, "ok\n")

        elif path == "/readyz":
            age = _heartbeat_age()
            if age is None:
                # 心跳檔案從未出現 = producer 還沒接上這個機制。
                # 回 200 而不是 503——否則 readiness 會永遠失敗，
                # 把「功能未實作」誤報成「服務故障」。
                #
                # 要讓這個端點真正有意義，producer 送出訊息成功時要
                # touch HEARTBEAT_FILE。那是一行 os.utime()，
                # 但屬於應用程式碼的變更，留給你決定。
                self._send(200, "ready (heartbeat not instrumented)\n")
            elif age < STALE_AFTER:
                self._send(200, "ready\n")
            else:
                self._send(503, f"stale (last heartbeat {age:.0f}s ago)\n")

        elif path == "/metrics":
            age = _heartbeat_age()
            lines = [
                "# HELP producer_up 1 表示 producer 行程存活",
                "# TYPE producer_up gauge",
                "producer_up 1",
                "# HELP producer_uptime_seconds 啟動至今秒數",
                "# TYPE producer_uptime_seconds gauge",
                f"producer_uptime_seconds {time.time() - _started_at:.0f}",
                "# HELP producer_heartbeat_age_seconds 距離上次成功送出資料的秒數",
                "# TYPE producer_heartbeat_age_seconds gauge",
                f"producer_heartbeat_age_seconds {age if age is not None else -1:.0f}",
            ]
            self._send(200, "\n".join(lines) + "\n")

        else:
            self._send(404, "not found\n")

    def log_message(self, *args):
        """關掉每個請求一行的存取日誌——probe 每 10 秒打一次會洗版。"""
        pass


def serve():
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    print(f"[healthz] listening on :{PORT}", flush=True)
    serve()
else:
    # 被 import 時以背景執行緒啟動，方便之後直接嵌進 producer 本體
    threading.Thread(target=serve, daemon=True).start()
