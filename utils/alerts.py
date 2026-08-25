# utils/alerts.py
"""Shared `market-alerts` schema — the single parser for Gateway and Discord bot.

Contract (doc/week5/SPEC.md §4): symbol / source_type / volume / mean / stddev /
sigma / alert_ts. Display only: the 判定 lives in the Flink job
(docker/flink/jobs/volume_anomaly.py) and must never be re-derived here.

欄位名沿用舊 schema，但統計已換成 log1p 空間的 median + MAD：mean 是中位數（已轉回
成交量單位），stddev 是 log 空間的 robust sigma，sigma 是穩健 z 值。

Self-check:  python -m utils.alerts
"""
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

TOPIC = "market-alerts"
PRICE_TOPIC = "market-price-alerts"

REQUIRED = ("symbol", "source_type", "volume", "mean", "stddev", "alert_ts")
# 價格跳躍是另一種事件，欄位完全不同（報酬/L 統計量/佔日均量），所以走自己的 topic
# 與自己的 schema，不塞進上面那組以量為主的欄位裡。
PRICE_REQUIRED = ("symbol", "source_type", "ret", "l_stat", "alert_ts")

# Flink 的 JSON format 預設 timestamp-format.standard = 'SQL'，TIMESTAMP(3) 會寫成
# "2026-07-22 03:14:15.926"：沒有 T、沒有時區。瀏覽器 new Date() 解析這種字串是
# undefined behavior（Safari 直接 NaN），所以時間在這裡就轉成 epoch ms（UTC）。
_TS_FORMATS = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")


def _to_epoch_ms(ts: str) -> int:
    for fmt in _TS_FORMATS:
        try:
            return int(datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc).timestamp() * 1000)
        except ValueError:
            continue
    # ISO-8601 fallback：萬一之後有人在 sink 上打開 'ISO-8601' 也不會壞掉
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


def parse_alert(raw: bytes | str) -> dict | None:
    """Parse one market-alerts message. Returns None for anything unusable.

    壞訊息只丟這一筆並 log，不能讓 consumer 整個停下來（SPEC §4 契約規則）。
    """
    try:
        a = json.loads(raw)
        missing = [f for f in REQUIRED if a.get(f) is None]
        if missing:
            raise ValueError(f"missing fields: {missing}")
        return {
            "symbol":      a["symbol"],
            "source_type": a["source_type"],
            "volume":      float(a["volume"]),
            "mean":        float(a["mean"]),
            "stddev":      float(a["stddev"]),
            # Flink 端 NULLIF 擋掉除以零，但 sigma 仍可能是 null → 保留 None，不自己算
            "sigma":       None if a.get("sigma") is None else float(a["sigma"]),
            # price_chg 是後加的欄位，刻意不放進 REQUIRED：topic 裡還有一堆舊訊息，
            # 拿它當必填會把歷史告警全部丟掉。沒有就是 None，顯示端自己處理。
            "price_chg":   None if a.get("price_chg") is None else float(a["price_chg"]),
            "alert_ts_ms": _to_epoch_ms(a["alert_ts"]),
        }
    except Exception as e:
        logger.warning(f"[ALERT] 丟棄壞訊息 {type(e).__name__}: {e} | raw={raw!r:.200}")
        return None


def parse_price_alert(raw: bytes | str) -> dict | None:
    """Parse one market-price-alerts message. Returns None for anything unusable."""
    try:
        a = json.loads(raw)
        missing = [f for f in PRICE_REQUIRED if a.get(f) is None]
        if missing:
            raise ValueError(f"missing fields: {missing}")
        return {
            "symbol":      a["symbol"],
            "source_type": a["source_type"],
            "ret":         float(a["ret"]),
            "l_stat":      float(a["l_stat"]),
            "volume":      None if a.get("volume") is None else float(a["volume"]),
            # 日均量要等第一個「完整交易日」才算得出來，暖機期間 Flink 端就是 null
            "pct_adv":     None if a.get("pct_adv") is None else float(a["pct_adv"]),
            "alert_ts_ms": _to_epoch_ms(a["alert_ts"]),
        }
    except Exception as e:
        logger.warning(f"[PRICE] 丟棄壞訊息 {type(e).__name__}: {e} | raw={raw!r:.200}")
        return None


def format_alert(a: dict) -> str:
    """One-line human summary, shared by the bot and any CLI debugging."""
    ts = datetime.fromtimestamp(a["alert_ts_ms"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sigma = "n/a" if a["sigma"] is None else f"{a['sigma']:.2f}σ"
    chg = "" if a.get("price_chg") is None else \
        f" {'▲' if a['price_chg'] >= 0 else '▼'}{abs(a['price_chg']) * 100:.2f}%"
    return (f"**{a['symbol']}** 成交量異常 {sigma}{chg} — "
            f"volume {a['volume']:.4f}（median {a['mean']:.4f} / robust σ {a['stddev']:.4f}）"
            f" @ {ts} UTC")


def format_price_alert(a: dict) -> str:
    """One-line human summary for a price jump."""
    ts = datetime.fromtimestamp(a["alert_ts_ms"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    arrow = "▲" if a["ret"] >= 0 else "▼"
    adv = "" if a["pct_adv"] is None else f"，佔日均量 {a['pct_adv'] * 100:.2f}%"
    return (f"**{a['symbol']}** 價格跳躍 {arrow}{abs(a['ret']) * 100:.3f}% "
            f"（L={a['l_stat']:.2f}）{adv} @ {ts} UTC")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    ok = parse_alert(json.dumps({
        "symbol": "BTCUSDT", "source_type": "BINANCE_CRYPTO", "volume": 12.345,
        "mean": 0.821, "stddev": 0.402, "sigma": 28.666, "price_chg": -0.0123,
        "alert_ts": "2026-07-22 03:14:15.926",
    }))
    assert ok["alert_ts_ms"] == 1784690055926, ok["alert_ts_ms"]   # UTC, 不是本地時間
    assert ok["sigma"] == 28.666
    assert "BTCUSDT" in format_alert(ok) and "28.67σ" in format_alert(ok)
    assert "▼1.23%" in format_alert(ok), format_alert(ok)

    # 舊訊息沒有 price_chg 也必須照常解析（欄位是後加的，不能回頭把歷史丟掉）
    old = parse_alert('{"symbol":"E","source_type":"S","volume":1,"mean":1,"stddev":1,'
                      '"sigma":1,"alert_ts":"2026-07-22 03:14:15"}')
    assert old is not None and old["price_chg"] is None
    assert "▲" not in format_alert(old) and "▼" not in format_alert(old)

    # 秒級（.SSS 剛好是 000 時 Flink 會省略毫秒）
    assert parse_alert('{"symbol":"E","source_type":"S","volume":1,"mean":1,"stddev":1,'
                       '"sigma":1,"alert_ts":"2026-07-22 03:14:15"}')["alert_ts_ms"] == 1784690055000
    # ISO-8601 fallback
    assert parse_alert('{"symbol":"E","source_type":"S","volume":1,"mean":1,"stddev":1,'
                       '"sigma":1,"alert_ts":"2026-07-22T03:14:15Z"}')["alert_ts_ms"] == 1784690055000

    # sigma 為 null 仍可用，其餘欄位缺一不可
    assert parse_alert('{"symbol":"E","source_type":"S","volume":1,"mean":1,"stddev":1,'
                       '"sigma":null,"alert_ts":"2026-07-22 03:14:15"}')["sigma"] is None
    assert parse_alert('{"symbol":"E","volume":1}') is None
    assert parse_alert(b"not json") is None
    assert parse_alert('{"symbol":"E","source_type":"S","volume":1,"mean":1,"stddev":1,'
                       '"sigma":1,"alert_ts":"garbage"}') is None

    # ---- 價格跳躍 ----
    pj = parse_price_alert(json.dumps({
        "symbol": "AMAT", "source_type": "ALPACA_STOCK", "ret": 0.00249,
        "l_stat": 8.33, "volume": 275.0, "pct_adv": 0.0128,
        "alert_ts": "2026-07-22 18:00:01.5",
    }))
    assert pj["ret"] == 0.00249 and pj["l_stat"] == 8.33
    assert "AMAT" in format_price_alert(pj) and "▲0.249%" in format_price_alert(pj)
    assert "1.28%" in format_price_alert(pj)

    # 下跌方向
    down = parse_price_alert(json.dumps({
        "symbol": "SBUX", "source_type": "ALPACA_STOCK", "ret": -0.00188,
        "l_stat": -9.32, "alert_ts": "2026-07-22 18:44:00",
    }))
    assert "▼0.188%" in format_price_alert(down)
    # ADV 暖機期間沒有日均量：不能因此丟掉整筆告警，也不要假裝是 0%
    assert down["pct_adv"] is None and "日均量" not in format_price_alert(down)

    assert parse_price_alert('{"symbol":"X","ret":0.01}') is None
    assert parse_price_alert(b"not json") is None

    print("✅ utils/alerts.py self-check passed")
