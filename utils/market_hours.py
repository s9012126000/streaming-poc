# utils/market_hours.py
from datetime import datetime, time
import pytz

US_EASTERN = pytz.timezone("America/New_York")

# 2026 美股假日（NYSE Holiday Calendar）
NYSE_HOLIDAYS_2026 = {
    datetime(2026, 1, 1).date(),   # New Year's Day
    datetime(2026, 1, 19).date(),  # MLK Day
    datetime(2026, 2, 16).date(),  # Presidents' Day
    datetime(2026, 4, 3).date(),   # Good Friday
    datetime(2026, 5, 25).date(),  # Memorial Day
    datetime(2026, 7, 3).date(),   # Independence Day (observed)
    datetime(2026, 9, 7).date(),   # Labor Day
    datetime(2026, 11, 26).date(), # Thanksgiving Day
    datetime(2026, 12, 25).date(), # Christmas Day
}

MARKET_OPEN = time(9, 30, 0)
MARKET_CLOSE = time(16, 0, 0)


def is_market_open(now: datetime | None = None) -> bool:
    """判斷美股目前是否在交易時段。

    以美東時間判斷，夏令時等同 UTC+8 週一至週五 21:30 到隔天 04:00；
    冬令時整段自動往後移一小時（22:30–05:00），假日也一併排除。
    不要改成寫死 UTC+8 的時段：UTC+8 的週六 00:00–04:00 其實是美東週五盤中，
    用 UTC+8 的星期幾去判斷會把週五尾盤整段砍掉。
    """
    now_et = now.astimezone(US_EASTERN) if now else datetime.now(US_EASTERN)

    # 週末
    if now_et.weekday() >= 5:
        return False

    # 假日
    if now_et.date() in NYSE_HOLIDAYS_2026:
        return False

    # 交易時間
    current_time = now_et.time()
    return MARKET_OPEN <= current_time < MARKET_CLOSE


if __name__ == "__main__":
    TAIPEI = pytz.timezone("Asia/Taipei")

    def tpe(y, m, d, hh, mm):
        return TAIPEI.localize(datetime(y, m, d, hh, mm))

    # 夏令時：台北時間 週一 21:30 開盤、週六 04:00 收盤（= 美東週五 16:00）
    assert not is_market_open(tpe(2026, 8, 3, 21, 29))   # 週一 開盤前一分鐘
    assert is_market_open(tpe(2026, 8, 3, 21, 30))       # 週一 開盤
    assert is_market_open(tpe(2026, 8, 8, 3, 59))        # 週六凌晨 = 美東週五盤中
    assert not is_market_open(tpe(2026, 8, 8, 4, 0))     # 週六凌晨 = 美東週五收盤
    assert not is_market_open(tpe(2026, 8, 9, 2, 0))     # 週日凌晨，美股週末
    # 冬令時整段往後一小時
    assert not is_market_open(tpe(2026, 12, 1, 21, 30))
    assert is_market_open(tpe(2026, 12, 1, 22, 30))
    # 假日（Christmas，美東週五）
    assert not is_market_open(tpe(2026, 12, 25, 23, 0))
    print("market_hours OK")