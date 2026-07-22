import { fmtQty, fmtSigma, fmtTimeUTC } from './format'

/**
 * 成交量異常榜。純顯示：sigma 由 Flink job 算好，這裡不重算、不重新判斷門檻。
 */
export default function AnomalyPanel({ alerts, onSelect }) {
  return (
    <section className="panel">
      <h2>成交量異常榜 <span className="hint">market-alerts · 3σ · 最新 {alerts.length} 筆 · 點選切換主圖</span></h2>
      {alerts.length === 0 ? (
        <p className="empty">尚無告警。Flink job 偵測到 3σ 成交量異常時會即時出現在這裡。</p>
      ) : (
        <table>
          <thead>
            <tr><th>時間 (UTC)</th><th>Symbol</th><th>來源</th>
              <th className="num">σ</th><th className="num">成交量</th><th className="num">均量</th></tr>
          </thead>
          <tbody>
            {alerts.map((a, i) => (
              <tr key={`${a.alert_ts_ms}-${a.symbol}-${i}`} onClick={() => onSelect(a.symbol)}>
                <td className="mono">{fmtTimeUTC(a.alert_ts_ms)}</td>
                <td className="sym">{a.symbol}</td>
                <td className="hint">{a.source_type}</td>
                <td className="num sigma">{fmtSigma(a.sigma)}</td>
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
