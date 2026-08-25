import { fmtPct, fmtPctPlain, fmtQty, fmtTimeUTC } from './format'

/**
 * 價格跳躍榜。純顯示：Lee–Mykland 的判定在 Flink job 裡做完了，這裡不重算。
 *
 * 和成交量異常榜刻意分開：那邊講「量」（單邊、沒有方向），這邊講「價」
 * （雙邊、有方向），欄位完全不重疊，硬併成一張表會有一半的格子是空的。
 *
 * L 是報酬除以 bipower 波動度——跳躍自己不會抬高門檻，所以量到的是
 * 「相對這檔股票自己近期的波動，這根有多不連續」，不是絕對漲跌幅。
 */
export default function PriceJumpPanel({ alerts, onSelect }) {
  return (
    <section className="panel">
      <h2>價格跳躍榜 <span className="hint">market-price-alerts · Lee–Mykland · 最新 {alerts.length} 筆 · 點選切換主圖</span></h2>
      {alerts.length === 0 ? (
        <p className="empty">尚無跳躍。需要每檔累積約 21 根 1 分鐘 K 棒才會開始判定，且跳躍本來就稀有（約 0.1% 的 K 棒）。</p>
      ) : (
        <table>
          <thead>
            <tr><th>時間 (UTC)</th><th>Symbol</th><th>來源</th>
              <th className="num">1 分鐘報酬</th><th className="num">L</th>
              <th className="num">成交量</th><th className="num">佔日均量</th></tr>
          </thead>
          <tbody>
            {alerts.map((a, i) => (
              <tr key={`${a.alert_ts_ms}-${a.symbol}-${i}`} onClick={() => onSelect(a.symbol)}>
                <td className="mono">{fmtTimeUTC(a.alert_ts_ms)}</td>
                <td className="sym">{a.symbol}</td>
                <td className="hint">{a.source_type}</td>
                <td className={`num mono ${a.ret >= 0 ? 'up' : 'down'}`}>
                  {a.ret >= 0 ? '▲' : '▼'} {fmtPct(a.ret)}
                </td>
                <td className="num sigma">{a.l_stat.toFixed(2)}</td>
                <td className="num mono">{fmtQty(a.volume)}</td>
                {/* 日均量要等第一個完整交易日；暖機期間留白，不要顯示 0% */}
                <td className="num mono hint">{fmtPctPlain(a.pct_adv)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
