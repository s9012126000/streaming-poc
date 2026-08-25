import { fmtQty, fmtSigma, fmtTimeUTC, fmtPct } from './format'

/**
 * 成交量異常榜。純顯示：門檻由 Flink job 的 median+MAD 算好，這裡不重算、不重新判斷。
 *
 * 欄位語意（sink schema 沒動，但統計換了）：mean 帶的是中位數（在 log1p 空間算完再轉回
 * 成交量單位，所以仍可和成交量直接對照），sigma 是穩健 z 值 = 離中位數幾個 robust sigma。
 * stddev 是 log 空間的尺度，和成交量不同單位，所以不列在表上。
 */
export default function AnomalyPanel({ alerts, onSelect }) {
  return (
    <section className="panel">
      <h2>成交量異常榜 <span className="hint">market-alerts · median+MAD · 最新 {alerts.length} 筆 · 點選切換主圖</span></h2>
      {alerts.length === 0 ? (
        <p className="empty">尚無告警。Flink job 偵測到成交量異常時會即時出現在這裡。</p>
      ) : (
        <table>
          <thead>
            <tr><th>時間 (UTC)</th><th>Symbol</th><th>來源</th>
              <th className="num">σ</th><th className="num">vs 中位價</th>
              <th className="num">成交量</th><th className="num">中位量</th></tr>
          </thead>
          <tbody>
            {alerts.map((a, i) => (
              <tr key={`${a.alert_ts_ms}-${a.symbol}-${i}`} onClick={() => onSelect(a.symbol)}>
                <td className="mono">{fmtTimeUTC(a.alert_ts_ms)}</td>
                <td className="sym">{a.symbol}</td>
                <td className="hint">{a.source_type}</td>
                <td className="num sigma">{fmtSigma(a.sigma)}</td>
                {/* 舊訊息沒有 price_chg，欄位就留白，不要拿 0% 假裝「沒有漲跌」 */}
                <td className={`num mono ${a.price_chg == null ? 'hint' : a.price_chg >= 0 ? 'up' : 'down'}`}>
                  {a.price_chg == null ? '—' : `${a.price_chg >= 0 ? '▲' : '▼'} ${fmtPct(a.price_chg)}`}
                </td>
                <td className="num mono">{fmtQty(a.volume)}</td>
                <td className="num mono hint">{fmtQty(a.mean)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
