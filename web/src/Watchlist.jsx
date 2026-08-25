import { fmtPrice, fmtPct, fmtTimeUTC } from './format'

const LABEL = { ALPACA_STOCK: '美股', BINANCE_CRYPTO: '幣' }

/**
 * 釘選的 symbol 觀察清單。點一列就把主圖切過去。
 *
 * 倉庫裡的全部標的放在下拉選單裡（依來源分組），清單本身只列釘選的——
 * 50 檔全列出來的表沒人看得完，要看哪幾檔是使用者的事。
 *
 * 有即時 tick 就用即時價；沒有（收盤、或該來源目前沒在產資料）就退回倉庫裡的
 * 最後成交價，並且標示出來——不拿舊價假裝是即時價。
 */
export default function Watchlist({ universe, quotes, selected, pinned, onPin, onUnpin, onSelect }) {
  const rows = universe.filter((s) => pinned.includes(s.symbol))
  const types = [...new Set(universe.map((s) => s.source_type))]

  return (
    <section className="panel watchlist">
      <h2>觀察清單 <span className="hint">{rows.length} 檔 · 點選切換主圖</span></h2>

      {/* value 固定空字串：選完就重設回提示項，同一檔取消釘選後才選得回來 */}
      <select className="picker" value="" onChange={(e) => { onPin(e.target.value); onSelect(e.target.value) }}>
        <option value="">＋ 加入標的（倉庫共 {universe.length} 檔）</option>
        {types.map((t) => {
          const opts = universe.filter((s) => s.source_type === t && !pinned.includes(s.symbol))
          return opts.length === 0 ? null : (
            <optgroup key={t} label={LABEL[t] ?? t}>
              {opts.map((s) => <option key={s.symbol} value={s.symbol}>{s.symbol}</option>)}
            </optgroup>
          )
        })}
      </select>

      {rows.length === 0 ? (
        <p className="empty">
          {universe.length === 0
            ? '倉庫裡還沒有任何 symbol（bronze.trades 是空的）。'
            : '還沒釘選任何標的——用上面的下拉選單加入。'}
        </p>
      ) : (
        <div className="scroll">
          <table>
            <thead>
              <tr><th>Symbol</th><th className="num">最新價</th>
                <th className="num">vs 開盤</th><th className="num">更新</th><th /></tr>
            </thead>
            <tbody>
              {rows.map((s) => {
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
                    {/* stopPropagation：移除不該順手把主圖切到這一檔 */}
                    <td className="num">
                      <button className="unpin" title="從清單移除"
                              onClick={(e) => { e.stopPropagation(); onUnpin(s.symbol) }}>×</button>
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
