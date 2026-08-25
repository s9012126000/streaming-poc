import { useEffect, useRef, useState } from 'react'
import Chart from './Chart'
import AnomalyPanel from './AnomalyPanel'
import PriceJumpPanel from './PriceJumpPanel'
import Watchlist from './Watchlist'
import { useStream, STALE_MS } from './useStream'
import { fmtPrice, fmtTimeUTC } from './format'

const DEFAULT_SYMBOL = 'BTCUSDT'

export default function App() {
  const [symbol, setSymbol] = useState(DEFAULT_SYMBOL)
  const [universe, setUniverse] = useState([])
  // 逗號字串而不是 JSON：symbol 不含逗號，壞掉的 localStorage 也 parse 不出 exception
  const [pinned, setPinned] = useState(() =>
    (localStorage.getItem('pinned') ?? DEFAULT_SYMBOL).split(',').filter(Boolean))
  const { status, alerts, priceAlerts, quotes, trade, tradeSeq, lastTickAt } = useStream(symbol)
  const [candles, setCandles] = useState([])
  const [backfill, setBackfill] = useState('loading')
  const [now, setNow] = useState(Date.now())
  const resetChart = useRef(null)   // Chart 把重設視圖的函式掛在這上面

  // 觀察清單的 symbol 直接問倉庫，不寫死 producer 的名單
  useEffect(() => {
    fetch('/api/symbols')
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.status))))
      .then((d) => setUniverse(d.symbols))
      .catch(() => setUniverse([]))
  }, [])

  // 歷史 K 棒回補（Trino 現算），失敗就明講，不要拿空圖假裝正常
  useEffect(() => {
    let cancelled = false
    setBackfill('loading')
    setCandles([])
    fetch(`/api/klines?symbol=${encodeURIComponent(symbol)}&limit=240`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.status))))
      .then((d) => { if (!cancelled) { setCandles(d.candles); setBackfill('ok') } })
      .catch(() => { if (!cancelled) setBackfill('failed') })
    return () => { cancelled = true }   // 快速連點時，舊查詢的結果不能蓋掉新 symbol 的圖
  }, [symbol])

  useEffect(() => { localStorage.setItem('pinned', pinned.join(',')) }, [pinned])

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(id)
  }, [])

  const stale = lastTickAt !== null && now - lastTickAt > STALE_MS
  const last = trade ? trade.price : quotes[symbol]?.price

  return (
    <div className="app">
      <header>
        <h1>{symbol} <span className="hint">1m · UTC</span></h1>
        <div className="last">{fmtPrice(last)}</div>
        <div className="badges">
          <span className={`badge ${status}`}>{status === 'live' ? '● 連線中' : '● 重連中'}</span>
          {stale && <span className="badge stale">⚠ 無新成交 {Math.floor((now - lastTickAt) / 1000)}s</span>}
          <span className="hint">最後成交 {fmtTimeUTC(lastTickAt)}</span>
        </div>
      </header>

      {backfill === 'failed' && (
        <p className="error">歷史 K 棒回補失敗（Trino 查詢錯誤）——圖上只有連線後的即時 K 棒。</p>
      )}

      <div className="main">
        <section className="panel">
          <h2>K 線 <span className="hint">1m · UTC</span>
            {/* 拖動/縮放之後把時間軸與價格軸一次拉回預設視圖 */}
            <button className="ghost" onClick={() => resetChart.current?.()}>重設視圖</button>
          </h2>
          <Chart candles={candles} trade={trade} tradeSeq={tradeSeq}
               alerts={alerts.filter((a) => a.symbol === symbol)}
               priceAlerts={priceAlerts.filter((a) => a.symbol === symbol)}
               resetRef={resetChart} />
        </section>
        <Watchlist universe={universe} quotes={quotes} selected={symbol} pinned={pinned}
                   onPin={(s) => setPinned((p) => (p.includes(s) ? p : [...p, s]))}
                   onUnpin={(s) => setPinned((p) => p.filter((x) => x !== s))}
                   onSelect={setSymbol} />
      </div>

      <PriceJumpPanel alerts={priceAlerts} onSelect={setSymbol} />
      <AnomalyPanel alerts={alerts} onSelect={setSymbol} />
    </div>
  )
}
