# utils/alerts.py
"""Shared `market-alerts` schema — the single parser for Gateway and Discord bot.

Contract (doc/week5/SPEC.md §4): symbol / source_type / volume / mean / stddev /
sigma / alert_ts. Display only: the 3-sigma decision lives in the Flink job
(docker/flink/sql/submit-jobs.sql) and must never be re-derived here.

Self-check:  python -m utils.alerts
"""
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

TOPIC = "market-alerts"

REQUIRED = ("symbol", "source_type", "volume", "mean", "stddev", "alert_ts")

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
            "alert_ts_ms": _to_epoch_ms(a["alert_ts"]),
        }
    except Exception as e:
        logger.warning(f"[ALERT] 丟棄壞訊息 {type(e).__name__}: {e} | raw={raw!r:.200}")
        return None


def format_alert(a: dict) -> str:
    """One-line human summary, shared by the bot and any CLI debugging."""
    ts = datetime.fromtimestamp(a["alert_ts_ms"] / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sigma = "n/a" if a["sigma"] is None else f"{a['sigma']:.2f}σ"
    return (f"**{a['symbol']}** 成交量異常 {sigma} — "
            f"volume {a['volume']:.4f}（mean {a['mean']:.4f} / stddev {a['stddev']:.4f}）"
            f" @ {ts} UTC")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    ok = parse_alert(json.dumps({
        "symbol": "BTCUSDT", "source_type": "BINANCE_CRYPTO", "volume": 12.345,
        "mean": 0.821, "stddev": 0.402, "sigma": 28.666,
        "alert_ts": "2026-07-22 03:14:15.926",
    }))
    assert ok["alert_ts_ms"] == 1784690055926, ok["alert_ts_ms"]   # UTC, 不是本地時間
    assert ok["sigma"] == 28.666
    assert "BTCUSDT" in format_alert(ok) and "28.67σ" in format_alert(ok)

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

    print("✅ utils/alerts.py self-check passed")
