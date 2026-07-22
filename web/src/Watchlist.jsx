import { fmtPrice, fmtPct, fmtTimeUTC } from './format'

const LABEL = { ALPACA_STOCK: '美股', BINANCE_CRYPTO: '幣' }

/**
 * 全 symbol 觀察清單。點一列就把主圖切過去。
 *
 * 有即時 tick 就用即時價；沒有（收盤、或該來源目前沒在產資料）就退回倉庫裡的
 * 最後成交價，並且標示出來——不拿舊價假裝是即時價。
 */
export default function Watchlist({ universe, quotes, selected, onSelect }) {
  return (
    <section className="panel watchlist">
      <h2>觀察清單 <span className="hint">{universe.length} 檔 · 點選切換主圖</span></h2>
      {universe.length === 0 ? (
        <p className="empty">倉庫裡還沒有任何 symbol（bronze.trades 是空的）。</p>
      ) : (
        <div className="scroll">
          <table>
            <thead>
              <tr><th>Symbol</th><th className="num">最新價</th>
                <th className="num">vs 開盤</th><th className="num">更新</th></tr>
            </thead>
            <tbody>
              {universe.map((s) => {
                const live = quotes[s.symbol]
                const price = live ? live.price : s.last_price
                const at = live ? live.trade_time : s.last_trade_time
                const pct = (price - s.day_open) / s.day_open
                return (
                  <tr key={s.symbol}
                      className={`${s.symbol === selected ? 'selected' : ''} ${live ? '' : 'stalerow'}`}
                      onClick={() => onSelect(s.symbol)}>
                    <td>
                      <span className="sym">{s.symbol}</span>
                      <span className="tag">{LABEL[s.source_type] ?? s.source_type}</span>
                    </td>
                    <td className="num mono">{fmtPrice(price)}</td>
                    <td className={`num mono ${pct >= 0 ? 'up' : 'down'}`}>{fmtPct(pct)}</td>
                    <td className="num hint mono" title={live ? '即時' : `倉庫資料 · ${s.session_date}`}>
                      {live ? fmtTimeUTC(at) : `${fmtTimeUTC(at)}*`}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
      <p className="hint foot">* 無即時成交，顯示倉庫最後成交價；漲跌幅基準為該 symbol 最後交易日開盤價。</p>
    </section>
  )
}
