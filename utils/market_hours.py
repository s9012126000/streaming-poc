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


def is_market_open() -> bool:
    """判斷美股目前是否在交易時段"""
    now_et = datetime.now(US_EASTERN)

    # 週末
    if now_et.weekday() >= 5:
        return False

    # 假日
    if now_et.date() in NYSE_HOLIDAYS_2026:
        return False

    # 交易時間
    current_time = now_et.time()
    return MARKET_OPEN <= current_time < MARKET_CLOSE


def seconds_until_market_open() -> int:
    """計算距離下一個開盤還有幾秒"""
    now_et = datetime.now(US_EASTERN)
    # 簡化計算，實際可加入假日邏輯
    next_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    if now_et >= next_open:
        from datetime import timedelta
        next_open += timedelta(days=1)
    delta = next_open - now_et
    return int(delta.total_seconds())