# gateway/kline_sql.py
"""1-minute OHLCV backfill for the chart, computed in Trino from bronze.trades.

There is no OHLC table in the warehouse (silver.vwap_1min carries vwap/volume only),
so candles are derived at query time — no new Iceberg table, no new Flink job
(doc/week5/SPEC.md §1.2.2).

Self-check:  python -m gateway.kline_sql   (needs a running Trino)
"""
import logging
import os
from datetime import datetime, timedelta, timezone

import trino

logger = logging.getLogger(__name__)

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

# min_by/max_by(price, trade_time) = 該分鐘內最早/最晚成交價 = 開盤/收盤價。
# trade_date 是 partition 欄位，帶進 WHERE 才會 partition pruning（不然全表掃）。
SQL = """
SELECT to_unixtime(date_trunc('minute', event_ts)) AS bucket_ts,
       min_by(price, trade_time)                   AS open,
       max(price)                                  AS high,
       min(price)                                  AS low,
       max_by(price, trade_time)                   AS close,
       sum(size)                                   AS volume,
       -- 與 Flink 的 vwap_1min job 同一條式子，兩邊算出來的分鐘 VWAP 才會一致
       sum(price * size) / nullif(sum(size), 0)    AS vwap
FROM iceberg.bronze.trades
WHERE symbol = ? AND trade_date >= ?
GROUP BY 1
ORDER BY 1 DESC
LIMIT ?
"""


# watchlist：symbol 清單直接問倉庫，不寫死 producer 的名單——UI 不該列出從來沒進過資料的標的。
# 美股週末沒有資料，往回看 7 天才不會在星期一開出空清單。
SYMBOLS_SQL = """
SELECT symbol,
       source_type,
       trade_date,
       min_by(price, trade_time) AS day_open,
       max_by(price, trade_time) AS last_price,
       max(trade_time)           AS last_trade_time
FROM iceberg.bronze.trades
WHERE trade_date >= ?
GROUP BY symbol, source_type, trade_date
"""

SYMBOLS_LOOKBACK_DAYS = 7


def _connect():
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="week5-gateway",
                               catalog="iceberg", schema="bronze")


def fetch_symbols() -> list[dict]:
    """Every symbol the warehouse has seen recently, with its most recent session open.

    day_open 是「該 symbol 最後一個有資料的交易日的第一筆成交價」，日期一起回傳，
    前端才能把漲跌幅的基準日寫在畫面上——不會拿週五的開盤價假裝是今天的。
    """
    since = (datetime.now(timezone.utc) - timedelta(days=SYMBOLS_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(SYMBOLS_SQL, (since,))
        rows = cur.fetchall()
    finally:
        conn.close()

    latest: dict[str, dict] = {}
    for symbol, source_type, trade_date, day_open, last_price, last_trade_time in rows:
        prev = latest.get(symbol)
        if prev is None or trade_date > prev["session_date"]:
            latest[symbol] = {"symbol": symbol, "source_type": source_type,
                              "session_date": trade_date, "day_open": float(day_open),
                              # 收盤/非交易時段沒有即時 tick 時，畫面至少有倉庫裡的最後成交價
                              "last_price": float(last_price),
                              "last_trade_time": int(last_trade_time)}
    return sorted(latest.values(), key=lambda s: (s["source_type"], s["symbol"]))


def fetch_klines(symbol: str, limit: int = 240) -> list[dict]:
    """Most recent `limit` 1-minute candles, oldest first.

    沒有成交的分鐘不會有 row——缺口就是缺口，這裡不補值也不內插。
    """
    since = (datetime.now(timezone.utc) - timedelta(days=limit // 1440 + 1)).strftime("%Y-%m-%d")
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(SQL, (symbol, since, limit))
        rows = cur.fetchall()
    finally:
        conn.close()

    # Lightweight Charts 的 time 是 UTC epoch 秒
    # vwap 只在整分鐘成交量為 0 時才會是 NULL（nullif 擋除以零）——那種分鐘
    # 沒有價格資訊可言，退回收盤價，前端才不會因為一個 null 斷掉整條線。
    return [{"time": int(r[0]), "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4]), "volume": float(r[5]),
             "vwap": float(r[6]) if r[6] is not None else float(r[4])}
            for r in reversed(rows)]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    syms = fetch_symbols()
    assert all(s["day_open"] > 0 for s in syms)
    assert len({s["symbol"] for s in syms}) == len(syms)   # 一個 symbol 只留最後一個交易日
    logger.info(f"✅ {len(syms)} symbols: {[s['symbol'] for s in syms[:5]]} …")

    ks = fetch_klines(os.environ.get("CHART_SYMBOL", "BTCUSDT"), limit=10)
    assert all(k["low"] <= k["open"] <= k["high"] and k["low"] <= k["close"] <= k["high"] for k in ks)
    assert [k["time"] for k in ks] == sorted(k["time"] for k in ks)
    # 成交量加權平均價一定落在當分鐘的高低點之間，落在外面就是 SQL 寫錯了
    assert all(k["low"] <= k["vwap"] <= k["high"] for k in ks)
    logger.info(f"✅ fetched {len(ks)} candles: {ks[-1] if ks else 'empty'}")
