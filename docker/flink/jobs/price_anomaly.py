"""價格跳躍偵測：1 分鐘 bar 的 Lee–Mykland jump test。

和 volume_anomaly.py 是兩種不同的事件，所以走自己的 topic：
  volume_anomaly = 「這筆成交量異常大」——講的是量，單邊，沒有方向。
  price_anomaly  = 「價格出現不連續跳躍」——講的是價，雙邊，有方向。

作法：用前 K 根報酬的 bipower variation（相鄰絕對報酬的乘積）估計當下的波動度，
再看當根報酬是它的幾倍。bipower 對跳躍不敏感，所以跳躍不會抬高自己的門檻——
和成交量那邊用 median+MAD 擋 masking 是同一個道理。

門檻不是隨手挑的：|L| 的極值在虛無假設下服從 Gumbel 分布，5.9 對應 alpha=1%
（用本專案歷史資料 n≈2462 算出來的臨界值）。

Run:         flink run -py /opt/flink/jobs/price_anomaly.py
Self-check:  python docker/flink/jobs/price_anomaly.py --selfcheck   (不用裝任何依賴)
"""
import math
import os
import statistics
import sys

BOOTSTRAP = "broker-1:29092,broker-2:29092,broker-3:29092"

# ponytail: Lee–Mykland 原文建議 1 分鐘資料用 K≈270。本專案的 Alpaca feed 只有
# 單一交易所（exchange 全是 'V'）、每檔每天約 355 筆成交、換算成 1 分鐘 bar 只有
# ~107 根，K=270 根本湊不出來。K=20 是資料密度逼出來的折衷：bipower 只有 18 項，
# 波動度估計偏噪，所以輸出定位是「跳躍候選」而不是統計上確認的跳躍。
# 之後 feed 變密（全市場行情，或 Binance 那條真的跑起來）就把 K 拉回去。
K = int(os.environ.get("JUMP_K", "20"))
THRESHOLD = float(os.environ.get("JUMP_THRESHOLD", "5.9"))
ADV_DAYS = int(os.environ.get("JUMP_ADV_DAYS", "5"))

BAR_SECS = 60
DAY_SECS = 86_400


def bar_return(prev_ts, prev_vwap, ts, vwap):
    """相鄰兩根 1 分鐘 bar 的對數報酬；不相鄰就回傳 None。

    隔夜與冷清時段的跳空不是「1 分鐘報酬」，混進來會在每個交易時段開頭
    製造假跳躍（本專案歷史資料裡有 8.6% 的 bar pair 是這種跳空）。
    """
    if prev_ts is None or ts - prev_ts != BAR_SECS:
        return None
    if prev_vwap <= 0 or vwap <= 0:
        return None
    return math.log(vwap / prev_vwap)


def bipower_sigma(rets):
    """用 bipower variation 估計局部波動度。

    相鄰絕對報酬相乘：單一根的跳躍只會影響到兩項，不會像平方和那樣被自己撐大。
    """
    if len(rets) < 3:
        return 0.0
    bp = sum(abs(rets[j]) * abs(rets[j - 1]) for j in range(1, len(rets)))
    var = bp / (len(rets) - 2)
    return math.sqrt(var) if var > 0 else 0.0


def jump_stat(prev_rets, ret):
    """Lee–Mykland L 統計量；波動度只用「當根之前」的報酬，估計量才不被污染。

    回傳 None 代表無法判定（樣本不足或該段完全沒有波動），不是「沒跳」。
    """
    sigma = bipower_sigma(prev_rets)
    if sigma == 0.0:
        return None
    return ret / sigma


def is_jump(l_stat, threshold=THRESHOLD):
    """雙邊：暴跌和暴漲一樣重要，和成交量的單邊上尾不同。"""
    return l_stat is not None and abs(l_stat) > threshold


def _make_detect():
    """KeyedProcessFunction：每個 (source_type, symbol) 各留 K+1 根報酬與日成交量。"""
    from pyflink.common import Row
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.state import ListStateDescriptor, MapStateDescriptor, ValueStateDescriptor
    from pyflink.common.typeinfo import Types

    class Detect(KeyedProcessFunction):
        def open(self, ctx):
            self._rets = ctx.get_list_state(ListStateDescriptor("rets", Types.DOUBLE()))
            self._prev_ts = ctx.get_state(ValueStateDescriptor("prev_ts", Types.LONG()))
            self._prev_vwap = ctx.get_state(ValueStateDescriptor("prev_vwap", Types.DOUBLE()))
            # 日均量自己在 state 裡累，不去查 Trino：少一個外部相依，Trino 掛掉
            # （本專案已經被 OOM 砍過兩次）也不會把這個 job 一起拖下水。
            self._daily = ctx.get_map_state(
                MapStateDescriptor("daily_vol", Types.LONG(), Types.DOUBLE()))

        def _update_adv(self, ts, volume):
            """把這根的量累進當日，並回傳「已完成日」的平均量（不含今天）。"""
            day = ts // DAY_SECS
            self._daily.put(day, (self._daily.get(day) or 0.0) + volume)
            done = {d: v for d, v in self._daily.items() if d < day}
            for d in list(done):                      # 只留最近 ADV_DAYS 天
                if d < day - ADV_DAYS:
                    self._daily.remove(d)
                    done.pop(d)
            return sum(done.values()) / len(done) if done else None

        def process_element(self, row, ctx):
            ts = int(row.bar_ts.timestamp()) if hasattr(row.bar_ts, "timestamp") else int(row.bar_ts)
            vwap, volume = float(row.vwap), float(row.volume)

            adv = self._update_adv(ts, volume)
            ret = bar_return(self._prev_ts.value(), self._prev_vwap.value() or 0.0, ts, vwap)
            self._prev_ts.update(ts)
            self._prev_vwap.update(vwap)
            if ret is None:
                return

            prev = list(self._rets.get() or [])
            l_stat = jump_stat(prev[-K:], ret) if len(prev) >= K else None
            prev.append(ret)
            del prev[:-(K + 1)]
            self._rets.update(prev)

            if is_jump(l_stat):
                yield Row(row.symbol, row.source_type, ret, l_stat, volume,
                          None if not adv else volume / adv)

    return Detect()


def main():
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(30000)
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("pipeline.name", "price-anomaly-jump")

    t_env.execute_sql(f"""
        CREATE TABLE kafka_trades_clean (
          source_type STRING, symbol STRING, price DOUBLE, size DOUBLE, trade_time BIGINT,
          event_ts AS TO_TIMESTAMP_LTZ(trade_time, 3),
          WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
        ) WITH (
          'connector'='kafka', 'topic'='market-trades-clean',
          'properties.bootstrap.servers'='{BOOTSTRAP}',
          'properties.group.id'='flink-jump-consumer',
          'properties.enable.auto.commit'='false',
          'scan.startup.mode'='latest-offset',
          'format'='json', 'json.ignore-parse-errors'='true')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE kafka_price_alerts_sink (
          symbol STRING, source_type STRING, ret DOUBLE, l_stat DOUBLE,
          volume DOUBLE, pct_adv DOUBLE, alert_ts TIMESTAMP(3)
        ) WITH (
          'connector'='kafka', 'topic'='market-price-alerts',
          'properties.bootstrap.servers'='{BOOTSTRAP}',
          'format'='json')
    """)

    # bar 價用 VWAP 而不是收盤價：每分鐘中位數只有 13 筆成交，單一最後成交價太吵；
    # 而且 VWAP 這條式子與順序無關，Flink 的 window 聚合算出來是確定的。
    bars = t_env.sql_query("""
        SELECT source_type, symbol,
               window_start AS bar_ts,
               SUM(price * size) / NULLIF(SUM(size), 0) AS vwap,
               SUM(size) AS volume
        FROM TABLE(TUMBLE(TABLE kafka_trades_clean, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE))
        GROUP BY source_type, symbol, window_start, window_end
    """)

    alert_type = Types.ROW_NAMED(
        ["symbol", "source_type", "ret", "l_stat", "volume", "pct_adv"],
        [Types.STRING(), Types.STRING(), Types.DOUBLE(),
         Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()],
    )

    alerts = (t_env.to_data_stream(bars)
              .key_by(lambda r: (r.source_type, r.symbol),
                      key_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
              .process(_make_detect(), output_type=alert_type))

    t_env.create_temporary_view("price_alerts", t_env.from_data_stream(alerts))
    t_env.execute_sql("""
        INSERT INTO kafka_price_alerts_sink
        SELECT symbol, source_type, ret, l_stat, volume, pct_adv, CURRENT_TIMESTAMP
        FROM price_alerts
    """)


def _selfcheck():
    quiet = [0.0004, -0.0003, 0.0005, -0.0004, 0.0003, -0.0005, 0.0004, -0.0003,
             0.0005, -0.0004, 0.0003, -0.0005, 0.0004, -0.0003, 0.0005, -0.0004,
             0.0003, -0.0005, 0.0004, -0.0003]

    # 1. 平靜期之後來一根大報酬 → L 應該很大，而且是雙邊的
    up = jump_stat(quiet, 0.008)
    down = jump_stat(quiet, -0.008)
    assert up > THRESHOLD and is_jump(up), up
    assert down < -THRESHOLD and is_jump(down), down
    assert abs(up + down) < 1e-9, "對稱的報酬要給出對稱的統計量"

    # 2. 正常大小的報酬不該告警
    assert not is_jump(jump_stat(quiet, 0.0005))

    # 3. bipower 對跳躍不敏感：把跳躍塞進估計窗，門檻不該像平方和那樣被撐大
    contaminated = quiet[:-1] + [0.008]
    infl = bipower_sigma(contaminated) / bipower_sigma(quiet)
    rms = (statistics.pstdev(contaminated) / statistics.pstdev(quiet))
    assert infl < rms, f"bipower 被撐大 {infl:.2f}x，應該小於 stdev 的 {rms:.2f}x"

    # 4. 完全沒有波動 → 回傳 None，永不除以零
    assert bipower_sigma([0.0] * 20) == 0.0
    assert jump_stat([0.0] * 20, 0.01) is None
    assert not is_jump(None)
    assert jump_stat([0.001], 0.01) is None          # 樣本不足

    # 5. 只有真正相鄰的分鐘才算報酬
    assert bar_return(1000, 100.0, 1060, 101.0) is not None      # 相鄰
    assert bar_return(1000, 100.0, 1120, 101.0) is None          # 中間缺一根
    assert bar_return(1000, 100.0, 1000 + DAY_SECS, 101.0) is None   # 隔夜跳空
    assert bar_return(None, 0.0, 1060, 101.0) is None            # 第一根
    assert bar_return(1000, 0.0, 1060, 101.0) is None            # 價格為 0

    # 6. 門檻可設定
    mid = jump_stat(quiet, 0.004)
    assert is_jump(mid, threshold=3.0) and not is_jump(mid, threshold=50.0)

    print("✅ price_anomaly Lee–Mykland self-check passed")


if __name__ == "__main__":
    if "--selfcheck" in sys.argv:
        _selfcheck()
    else:
        main()
