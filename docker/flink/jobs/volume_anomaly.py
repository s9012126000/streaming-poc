"""Volume anomaly detection: median + MAD on log1p(size).

Replaces the SQL `volume-anomaly-3sigma` job. 成交量是重尾右偏的，mean/stddev 會被
「我們要抓的那幾筆大單」自己抬高（masking）——門檻跟著離群值一起漂走，該告的不告、
不該告的亂告。median + MAD 有 50% 崩潰點，離群值本身影響不到它。

MAD 是兩趟統計（median(|xi - median_w|) 要用到同一個 window 的 median），SQL 的
over-window 表達不出來，所以窗留在 state 裡由這個 job 自己算。

Run:         flink run -py /opt/flink/jobs/volume_anomaly.py
Self-check:  python docker/flink/jobs/volume_anomaly.py --selfcheck   (不用裝任何依賴)
"""
import math
import os
import statistics
import sys

BOOTSTRAP = "broker-1:29092,broker-2:29092,broker-3:29092"

# 原 SQL 的字面 3 搬到這裡；robust sigma 的尺度和 stddev 不同，預設放寬到 3.5
K = float(os.environ.get("ANOMALY_K", "3.5"))
# 美股成交量集中在整股單位（100/200/400），冷清的窗裡 MAD 會塌到接近 0，
# 這時 z 會爆到幾十上百，任何 k 都擋不住——實測把 k 拉到 12 反而先殺掉真正的
# 大單（p90 從 20 倍掉到 6 倍），留下的還是 1.2 倍的雜訊。所以改用「至少要是
# 中位量的幾倍」當第二道門檻，它不受 MAD 崩塌影響。
MIN_RATIO = float(os.environ.get("ANOMALY_MIN_RATIO", "3.0"))
WINDOW   = 21   # = ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
MIN_ROWS = 20   # = 原 SQL 的 n >= 20
MAD_SCALE = 1.4826   # MAD → 常態分布下的 sigma 等效尺度


def robust_stats(values):
    """log1p 空間的 median 與 robust sigma。回傳 (median, scale)。"""
    logs = [math.log1p(v) for v in values]
    med  = statistics.median(logs)
    dev  = [abs(x - med) for x in logs]
    scale = MAD_SCALE * statistics.median(dev)
    if scale == 0.0:
        # MAD==0：窗內過半是同一個值（冷門標的很常見）。退回平均絕對離差；
        # 仍是 0 表示窗內毫無離散度 → 由 flag() 判定不告警，永不除以零。
        scale = MAD_SCALE * (sum(dev) / len(dev))
    return med, scale


def flag(values, k=K, min_ratio=MIN_RATIO):
    """values[-1] 是當前這筆。回傳 (is_anomaly, median, scale, z)。

    兩道門檻都要過：離中位數 k 個 robust sigma（單邊上尾，與原本
    size > mean + 3*stddev 同向），且至少是中位量的 min_ratio 倍。
    倍數這道刻意在線性空間比，才真的是「中位量的幾倍」——在 log1p 空間比的是
    (1+v)/(1+m)，成交量小的幣種會被稀釋掉（0.5 對中位 0.1 只算 1.36 倍）。
    """
    med, scale = robust_stats(values)
    if scale == 0.0:
        return False, med, 0.0, None
    z = (math.log1p(values[-1]) - med) / scale
    is_anomaly = z > k and values[-1] > min_ratio * math.expm1(med)
    return is_anomaly, med, scale, z


def price_change(prices):
    """成交價相對窗內中位價的變動比例。中位價是 0 就回傳 None，不除以零。

    用中位價當基準（而不是前一筆）跟成交量那邊同一套邏輯：單一筆跳價不會
    把基準帶走，看到的是「這筆大單相對這段時間的價位是高還是低」。
    """
    ref = statistics.median(prices)
    if ref == 0.0:
        return None
    return (prices[-1] - ref) / ref


def _make_detect():
    """KeyedProcessFunction：每個 (source_type, symbol) 各留一個 21 筆的窗。"""
    from pyflink.common import Row
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.state import ListStateDescriptor
    from pyflink.common.typeinfo import Types

    class Detect(KeyedProcessFunction):
        def open(self, ctx):
            self._win = ctx.get_list_state(
                ListStateDescriptor("window", Types.DOUBLE()))
            # 價格另存一條，兩條同進同出，索引一定對得起來
            self._px = ctx.get_list_state(
                ListStateDescriptor("window_px", Types.DOUBLE()))

        def process_element(self, row, ctx):
            # ponytail: 原 SQL 是 ORDER BY event_ts，這裡是 per-key 到達順序。
            # 本專案每個 symbol 只有一個 producer、Kafka 分區內有序，兩者等價；
            # 真的出現亂序再改成 onTimer 緩衝到 watermark 才計算。
            vals = list(self._win.get() or [])
            vals.append(float(row.size))
            del vals[:-WINDOW]
            self._win.update(vals)

            pxs = list(self._px.get() or [])
            pxs.append(float(row.price))
            del pxs[:-WINDOW]
            self._px.update(pxs)

            if len(vals) < MIN_ROWS:
                return

            is_anomaly, med, scale, z = flag(vals)
            if is_anomaly:
                # 欄位沿用原本的 sink schema：mean 轉回成交量單位好和 volume 對照，
                # stddev 放 log 空間的 robust sigma，sigma 仍是「離中心幾個 sigma」。
                # price_chg 是新增欄位：這筆大單相對窗內中位價的漲跌。
                yield Row(row.symbol, row.source_type, vals[-1],
                          math.expm1(med), scale, z, price_change(pxs))

    return Detect()


def main():
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment

    env = StreamExecutionEnvironment.get_execution_environment()
    # 同 bronze_ingest.py：flink-init 沒有 FLINK_PROPERTIES，checkpointing 要自己開
    env.enable_checkpointing(30000)
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("pipeline.name", "volume-anomaly-mad")

    # flink run -py 不共用 sql-client 的 session，來源/輸出表在這裡自己宣告一次
    t_env.execute_sql(f"""
        CREATE TABLE kafka_trades_clean (
          source_type STRING, symbol STRING, size DOUBLE, price DOUBLE
        ) WITH (
          'connector'='kafka', 'topic'='market-trades-clean',
          'properties.bootstrap.servers'='{BOOTSTRAP}',
          'properties.group.id'='flink-anomaly-consumer',
          'properties.enable.auto.commit'='false',
          'scan.startup.mode'='latest-offset',
          'format'='json', 'json.ignore-parse-errors'='true')
    """)

    t_env.execute_sql(f"""
        CREATE TABLE kafka_alerts_sink (
          symbol STRING, source_type STRING, volume DOUBLE,
          mean DOUBLE, stddev DOUBLE, sigma DOUBLE, price_chg DOUBLE,
          alert_ts TIMESTAMP(3)
        ) WITH (
          'connector'='kafka', 'topic'='market-alerts',
          'properties.bootstrap.servers'='{BOOTSTRAP}',
          'format'='json')
    """)

    alert_type = Types.ROW_NAMED(
        ["symbol", "source_type", "volume", "mean", "stddev", "sigma", "price_chg"],
        [Types.STRING(), Types.STRING(), Types.DOUBLE(),
         Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE()],
    )

    trades = t_env.to_data_stream(t_env.from_path("kafka_trades_clean"))
    alerts = (trades
              .key_by(lambda r: (r.source_type, r.symbol),
                      key_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
              .process(_make_detect(), output_type=alert_type))

    t_env.create_temporary_view("alerts", t_env.from_data_stream(alerts))
    # alert_ts 用 CURRENT_TIMESTAMP，維持 utils/alerts.py 解析的那個字串格式
    t_env.execute_sql("""
        INSERT INTO kafka_alerts_sink
        SELECT symbol, source_type, volume, mean, stddev, sigma, price_chg, CURRENT_TIMESTAMP
        FROM alerts
    """)


def _selfcheck():
    # 1. masking：一連串大單之後，3-sigma 的門檻被自己抬高到抓不到下一筆大單
    win = [0.9, 1.0, 1.1, 1.05] * 4 + [100.0] * 5
    mean = sum(win) / len(win)
    stddev = statistics.pstdev(win)
    assert not (win[-1] > mean + 3 * stddev), "3σ 門檻應該已經被大單自己抬高"
    assert flag(win)[0], "median+MAD 應該仍然抓得到這筆大單"

    # 2. MAD==0 且完全沒有離散度：不判定、不除以零
    same = [2.5] * WINDOW
    is_anomaly, _, scale, z = flag(same)
    assert scale == 0.0 and z is None and not is_anomaly

    # 3. MAD==0 但平均絕對離差 > 0：退回 fallback，離群值照樣抓得到
    mostly_same = [1.0] * 20 + [5.0]
    is_anomaly, _, scale, _ = flag(mostly_same)
    assert scale > 0.0 and is_anomaly, "MAD==0 的 fallback 應該讓 5.0 被判為離群"

    # 4. k 是可設定的：同一個窗（z≈14.2），門檻拉到 20 就不該告警
    assert flag(mostly_same, k=3.5)[0]
    assert not flag(mostly_same, k=20.0)[0]

    # 5. 整股單位：全窗都是 100 股，來一筆 200 股。MAD 塌掉讓 z≈14，
    #    單看 k 一定會告警（拉高 k 也沒用），要靠倍數門檻擋下來。
    round_lots = [100.0] * 20 + [200.0]
    assert flag(round_lots, min_ratio=1.0)[3] > 12.0, "MAD 塌掉時 z 本來就會爆掉"
    assert flag(round_lots, min_ratio=1.0)[0], "沒有倍數門檻時這筆會誤報"
    assert not flag(round_lots)[0], "200 只有中位量的 2 倍，預設 3 倍門檻要擋下來"
    assert flag([100.0] * 20 + [400.0])[0], "4 倍的大單仍然要告警"

    # 6. 倍數門檻同樣可設定
    assert flag(round_lots, min_ratio=1.5)[0]
    assert not flag(round_lots, min_ratio=5.0)[0]

    # 7. 漲跌方向：以窗內中位價為基準，且中位價為 0 時不除以零
    assert price_change([10.0] * 20 + [11.0]) == 0.1      # 高於中位價 10%
    assert price_change([10.0] * 20 + [9.0]) == -0.1      # 低於中位價 10%
    assert price_change([10.0] * 21) == 0.0               # 沒動就是沒動
    assert price_change([0.0] * 21) is None               # 中位價 0 → 不判方向

    print("✅ volume_anomaly median+MAD self-check passed")


if __name__ == "__main__":
    if "--selfcheck" in sys.argv:
        _selfcheck()
    else:
        main()
