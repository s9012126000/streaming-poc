import { useEffect, useRef, useState } from 'react'

const MAX_ALERTS = 50
export const STALE_MS = 15_000

/**
 * Subscribe to the gateway SSE stream for one charted symbol.
 * EventSource 自己會重連（含退避），所以這裡不寫任何重連邏輯——只把狀態顯示出來。
 *
 * trade  = 圖表 symbol 的逐筆（換 symbol 就重開連線，由 gateway 換過濾條件）
 * quotes = 每秒一次的全 symbol 最新價快照（watchlist 用）
 */
export function useStream(symbol) {
  const [status, setStatus] = useState('connecting')
  const [alerts, setAlerts] = useState([])
  const [priceAlerts, setPriceAlerts] = useState([])
  const [quotes, setQuotes] = useState({})
  const [lastTickAt, setLastTickAt] = useState(null)
  const tradeRef = useRef(null)          // 最新一筆成交，交給 Chart 以 imperative 方式更新
  const [tradeSeq, setTradeSeq] = useState(0)

  useEffect(() => {
    // 換 symbol 等於換一張圖：舊 symbol 的 tick 絕不能併進新圖的 K 棒
    tradeRef.current = null
    setLastTickAt(null)

    const es = new EventSource(`/api/stream?symbol=${encodeURIComponent(symbol)}`)
    es.onopen = () => setStatus('live')
    es.onerror = () => setStatus('reconnecting')   // EventSource 會自動再試

    es.addEventListener('snapshot', (e) => setAlerts(JSON.parse(e.data).reverse()))
    es.addEventListener('alert', (e) =>
      setAlerts((prev) => [JSON.parse(e.data), ...prev].slice(0, MAX_ALERTS)))
    es.addEventListener('price_snapshot', (e) => setPriceAlerts(JSON.parse(e.data).reverse()))
    es.addEventListener('price_alert', (e) =>
      setPriceAlerts((prev) => [JSON.parse(e.data), ...prev].slice(0, MAX_ALERTS)))
    es.addEventListener('quotes', (e) => {
      const batch = JSON.parse(e.data)
      setQuotes((prev) => {
        const next = { ...prev }
        for (const tick of batch) next[tick.symbol] = tick
        return next
      })
    })
    es.addEventListener('trade', (e) => {
      tradeRef.current = JSON.parse(e.data)
      setTradeSeq((n) => n + 1)
      setLastTickAt(Date.now())
    })

    return () => es.close()
  }, [symbol])

  return { status, alerts, priceAlerts, quotes, trade: tradeRef.current, tradeSeq, lastTickAt }
}
