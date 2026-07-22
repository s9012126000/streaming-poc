import { useEffect, useRef } from 'react'
import { createChart, CandlestickSeries, HistogramSeries } from 'lightweight-charts'
import { mergeTrade } from './candles'

// 西式配色：綠漲紅跌（對齊 Binance / TradingView，也是圖表庫預設）
const UP = '#26a69a'
const DOWN = '#ef5350'

export default function Chart({ candles, trade, tradeSeq }) {
  const boxRef = useRef(null)
  const priceRef = useRef(null)
  const volumeRef = useRef(null)
  const dataRef = useRef([])

  useEffect(() => {
    const chart = createChart(boxRef.current, {
      layout: { background: { color: '#0f1115' }, textColor: '#c9d1d9', attributionLogo: false },
      grid: { vertLines: { color: '#1c1f26' }, horzLines: { color: '#1c1f26' } },
      // 交易所時間統一用 UTC 顯示，時區標示在標題列，不讓瀏覽器本地時區偷偷換算
      timeScale: { timeVisible: true, secondsVisible: false },
      localization: { timeFormatter: (t) => new Date(t * 1000).toISOString().slice(11, 16) },
      rightPriceScale: { borderColor: '#1c1f26' },
      crosshair: { mode: 0 },
    })
    priceRef.current = chart.addSeries(CandlestickSeries, {
      upColor: UP, downColor: DOWN, borderUpColor: UP, borderDownColor: DOWN,
      wickUpColor: UP, wickDownColor: DOWN,
    })
    volumeRef.current = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' }, priceScaleId: '',
    })
    volumeRef.current.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })

    const ro = new ResizeObserver(([e]) =>
      chart.resize(e.contentRect.width, e.contentRect.height))
    ro.observe(boxRef.current)
    return () => { ro.disconnect(); chart.remove() }
  }, [])

  // Trino 回補的歷史 K 棒
  useEffect(() => {
    dataRef.current = candles.map((c) => ({ ...c }))
    priceRef.current.setData(dataRef.current.map(({ volume, ...ohlc }) => ohlc))
    volumeRef.current.setData(dataRef.current.map((c) => ({
      time: c.time, value: c.volume, color: c.close >= c.open ? `${UP}80` : `${DOWN}80`,
    })))
  }, [candles])

  // 即時成交：只更新受影響的那一根，不重畫整張圖
  useEffect(() => {
    if (!trade) return
    const c = mergeTrade(dataRef.current, trade)
    if (!c) return
    priceRef.current.update({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })
    volumeRef.current.update({
      time: c.time, value: c.volume, color: c.close >= c.open ? `${UP}80` : `${DOWN}80`,
    })
  }, [tradeSeq])

  return <div className="chart" ref={boxRef} />
}
