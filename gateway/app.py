# gateway/app.py
"""SSE gateway: fans out market-alerts + market-trades-clean to browsers, and
serves the Trino kline backfill.

Display layer only — the median+MAD 判定 lives in the Flink job. This service
parses, filters by symbol and forwards; it never re-derives a signal.

Fan-out shape (see doc/week5/PLAN.md §5):
  alert   — 每一筆，全部訂閱者都收
  trade   — 全速，只送給正在看該 symbol 的訂閱者
  quotes  — 每秒一次的最新價快照，涵蓋所有 symbol（watchlist 用）

Run:  uvicorn gateway.app:app --host 0.0.0.0 --port 8000
"""
import asyncio
import json
import logging
import os
import threading
from collections import deque
from contextlib import asynccontextmanager

from confluent_kafka import Consumer
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from gateway.kline_sql import fetch_klines, fetch_symbols
from utils.alerts import PRICE_TOPIC, TOPIC, parse_alert, parse_price_alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONSUMER_GROUP  = "web-gateway-consumer"   # Grafana lag 面板靠這個名字認人
CHART_SYMBOL    = os.environ.get("CHART_SYMBOL", "BTCUSDT")   # 未指定時的預設圖表 symbol

ALERT_HISTORY   = 50    # 新開的分頁一進來就有東西可看，不用等下一筆告警
QUEUE_MAXSIZE   = 500   # 慢的瀏覽器不能拖垮 gateway：塞滿就丟該連線的訊息
PING_SECS       = 15    # 閒置時的 SSE comment，避免中間的 proxy 砍掉連線
QUOTE_SECS      = 1.0   # watchlist 用不到逐筆：每秒推一次「有變動的 symbol」就夠

_subscribers: dict[asyncio.Queue, str] = {}   # queue -> 該連線正在看的 symbol
_alert_history: deque = deque(maxlen=ALERT_HISTORY)
# 價格跳躍另外存一條：跳躍比量能異常稀有得多（約 0.1% 的 bar），
# 混在同一個 buffer 裡會被成交量告警洗掉。
_price_history: deque = deque(maxlen=ALERT_HISTORY)
_quotes: dict[str, dict] = {}                 # symbol -> 最新價（watchlist 快照）
_dirty: set[str] = set()                      # 上次推播後有變動的 symbol
_stop = threading.Event()


def _send(q: asyncio.Queue, event: str, payload):
    try:
        q.put_nowait(f"event: {event}\ndata: {json.dumps(payload)}\n\n")
    except asyncio.QueueFull:
        logger.warning(f"[GATEWAY] subscriber queue full, dropping {event}")


def _on_alert(alert: dict):
    """Runs on the event loop thread."""
    _alert_history.append(alert)
    for q in _subscribers:
        _send(q, "alert", alert)


def _on_price_alert(alert: dict):
    """Runs on the event loop thread."""
    _price_history.append(alert)
    for q in _subscribers:
        _send(q, "price_alert", alert)


def _on_trade(tick: dict):
    """Runs on the event loop thread."""
    _quotes[tick["symbol"]] = tick
    _dirty.add(tick["symbol"])
    # 逐筆只送給正在看這個 symbol 的連線；其他 symbol 走每秒的 quotes 快照
    for q, symbol in _subscribers.items():
        if symbol == tick["symbol"]:
            _send(q, "trade", tick)


def _consume(loop: asyncio.AbstractEventLoop):
    """Kafka poll loop (background thread). One group, both topics."""
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        # 展示用即時資料，重啟後從最新開始；歷史 K 棒本來就走 Trino 回補
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC, PRICE_TOPIC, "market-trades-clean"])
    logger.info(f"[GATEWAY] 🟢 consuming as {CONSUMER_GROUP}")

    while not _stop.is_set():
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logger.warning(f"[GATEWAY] kafka error: {msg.error()}")
            continue

        if msg.topic() == TOPIC:
            alert = parse_alert(msg.value())
            if alert:
                loop.call_soon_threadsafe(_on_alert, alert)
            continue

        if msg.topic() == PRICE_TOPIC:
            jump = parse_price_alert(msg.value())
            if jump:
                loop.call_soon_threadsafe(_on_price_alert, jump)
            continue

        try:
            t = json.loads(msg.value())
            tick = {"symbol": t["symbol"], "source_type": t["source_type"],
                    "price": float(t["price"]), "size": float(t["size"]),
                    "trade_time": int(t["trade_time"])}
        except Exception as e:
            logger.warning(f"[GATEWAY] 丟棄壞 trade {type(e).__name__}: {e}")
            continue
        loop.call_soon_threadsafe(_on_trade, tick)

    consumer.close()
    logger.info("[GATEWAY] ⏹ consumer stopped")


async def _quote_ticker():
    """Push a batched last-price snapshot once a second.

    30 檔美股 + 20 檔幣的逐筆全推給每個瀏覽器是白費頻寬——watchlist 只需要最新價。
    """
    while True:
        await asyncio.sleep(QUOTE_SECS)
        if not _dirty:
            continue
        batch = [_quotes[s] for s in _dirty if s in _quotes]
        _dirty.clear()
        for q in _subscribers:
            _send(q, "quotes", batch)


@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=_consume, args=(asyncio.get_running_loop(),), daemon=True)
    thread.start()
    ticker = asyncio.create_task(_quote_ticker())
    yield
    ticker.cancel()
    _stop.set()
    thread.join(timeout=5)


app = FastAPI(title="Week5 Market Gateway", lifespan=lifespan)


@app.get("/api/symbols")
def symbols():
    """Watchlist universe + today's open, straight from the warehouse.

    同步函式 → FastAPI 自動丟到 threadpool，不卡事件迴圈。
    """
    return {"symbols": fetch_symbols()}


@app.get("/api/klines")
def klines(symbol: str = CHART_SYMBOL, limit: int = 240):
    """Historical candles from Trino."""
    return {"symbol": symbol, "interval": "1m", "candles": fetch_klines(symbol, limit)}


@app.get("/api/stream")
async def stream(symbol: str = CHART_SYMBOL):
    q: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    _subscribers[q] = symbol

    async def events():
        try:
            # 一連上先補歷史，否則剛開的頁面異常榜與 watchlist 都是空的
            _send(q, "snapshot", list(_alert_history))
            # 跳躍另發一個事件，不動原本 snapshot 的形狀
            _send(q, "price_snapshot", list(_price_history))
            _send(q, "quotes", list(_quotes.values()))
            while True:
                try:
                    yield await asyncio.wait_for(q.get(), PING_SECS)
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            _subscribers.pop(q, None)

    return StreamingResponse(events(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/health")
def health():
    return {"subscribers": len(_subscribers), "alerts_buffered": len(_alert_history),
            "price_alerts_buffered": len(_price_history),
            "symbols_seen": len(_quotes), "consumer_group": CONSUMER_GROUP}
