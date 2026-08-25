import { useCallback, useEffect, useRef } from 'react'
import { createChart, CandlestickSeries, HistogramSeries, LineSeries, createSeriesMarkers } from 'lightweight-charts'
import { mergeTrade, sessionVwap, BUCKET_MS } from './candles'

// 西式配色：綠漲紅跌（對齊 Binance / TradingView，也是圖表庫預設）
const UP = '#26a69a'
const DOWN = '#ef5350'

// 背景/文字/格線讀 styles.css 的 :root 變數，不在這裡抄第二份 hex——
// 否則改主題時整個頁面換色、圖表卻留在舊配色。
const cssVar = (name) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim()

// 最後一根 K 棒右邊留幾根的空白，價格軸標籤才不會壓在最新的棒子上
const RIGHT_BARS = 8

export default function Chart({ candles, trade, tradeSeq, alerts = [], priceAlerts = [], resetRef }) {
  const boxRef = useRef(null)
  const chartRef = useRef(null)
  const priceRef = useRef(null)
  const volumeRef = useRef(null)
  const vwapRef = useRef(null)
  const markersRef = useRef(null)
  const dataRef = useRef([])

  useEffect(() => {
    const chart = chartRef.current = createChart(boxRef.current, {
      // fontSize 是圖表唯一的字級開關（預設 12）：價格軸標籤、VWAP tag、σ marker
      // 的文字全都吃它，沒有辦法只縮其中一個。
      layout: { background: { color: cssVar('--panel') }, textColor: cssVar('--text'),
                fontSize: 10, attributionLogo: false },
      grid: { vertLines: { color: cssVar('--line') }, horzLines: { color: cssVar('--line') } },
      // 交易所時間統一用 UTC 顯示，時區標示在標題列，不讓瀏覽器本地時區偷偷換算
      timeScale: { timeVisible: true, secondsVisible: false,
                   rightOffset: RIGHT_BARS, fixLeftEdge: true },
      localization: { timeFormatter: (t) => new Date(t * 1000).toISOString().slice(11, 16) },
      rightPriceScale: { borderColor: cssVar('--line') },
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
    // K 棒壓在上面 80%，不畫進下面的量能帶
    priceRef.current.priceScale().applyOptions({ scaleMargins: { top: 0.05, bottom: 0.22 } })

    // 當日累積 VWAP：參考線，用中性色，不跟漲跌的紅綠搶顏色。
    // title 是掛在價格軸的 last-value 標籤上的，所以 lastValueVisible 必須開著，
    // 順便把當下的 VWAP 數字也標出來——標籤就在線的高度上，不用靠顏色去猜是哪條。
    vwapRef.current = chart.addSeries(LineSeries, {
      color: cssVar('--accent'), lineWidth: 2, title: 'VWAP',
      priceLineVisible: false, lastValueVisible: true, crosshairMarkerVisible: false,
    })
    // v5 的 marker 是 plugin primitive，series.setMarkers() 已經拿掉了
    markersRef.current = createSeriesMarkers(priceRef.current, [])

    const ro = new ResizeObserver(([e]) =>
      chart.resize(e.contentRect.width, e.contentRect.height))
    ro.observe(boxRef.current)
    return () => { ro.disconnect(); chart.remove() }
  }, [])

  /**
   * 把視圖拉回初始狀態：時間軸從第 0 根開始、右邊留 RIGHT_BARS 根空白，價格軸恢復自動縮放。
   *
   * 預設行為是把資料靠右對齊，K 棒不夠填滿寬度時左邊會空一大塊，所以這裡明確指定範圍。
   * 使用者手動拖過價格軸之後 autoScale 會被關掉，一併打開才會重新貼合價格。
   */
  const fitView = useCallback(() => {
    const n = dataRef.current.length
    if (!n || !chartRef.current) return
    chartRef.current.timeScale().setVisibleLogicalRange({ from: 0, to: n - 1 + RIGHT_BARS })
    chartRef.current.priceScale('right').applyOptions({ autoScale: true })
  }, [])

  useEffect(() => {
    if (resetRef) resetRef.current = fitView
  }, [resetRef, fitView])

  // Trino 回補的歷史 K 棒。API 給的是每分鐘 VWAP，累積曲線需要成交金額，先還原成 pv
  useEffect(() => {
    dataRef.current = candles.map((c) => ({ ...c, pv: c.vwap * c.volume }))
    priceRef.current.setData(dataRef.current.map(({ volume, pv, vwap, ...ohlc }) => ohlc))
    volumeRef.current.setData(dataRef.current.map((c) => ({
      time: c.time, value: c.volume, color: c.close >= c.open ? `${UP}80` : `${DOWN}80`,
    })))
    vwapRef.current.setData(sessionVwap(dataRef.current))
    fitView()
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
    // ponytail: 累積曲線每筆重算整條（最多 ~240 點，微秒等級）。真的嫌貴再改成
    // 存「不含當前這根的累積量」，只 update 最後一點。
    vwapRef.current.setData(sessionVwap(dataRef.current))
  }, [tradeSeq])

  // 異常標記：把告警標在對應的那一分鐘上，讓 K 線圖看得出偵測器為什麼叫。
  //
  // 兩種事件用不同形狀，不是只用顏色區分：價格跳躍往下的那一筆，顏色和位置
  // 都會跟成交量告警一樣，只靠顏色分不出來。
  //   成交量異常 = 紅色箭頭在棒子上方（單邊，沒有方向可言）
  //   價格跳躍   = 圓點，漲在下方綠色、跌在上方紅色
  useEffect(() => {
    if (!markersRef.current) return
    const best = new Map()   // time -> marker，同一分鐘只留最極端的一筆
    const put = (t, m, rank) => {
      const prev = best.get(t)
      if (!prev || rank > prev._rank) best.set(t, { ...m, _rank: rank })
    }
    for (const a of alerts) {
      const t = Math.floor(a.alert_ts_ms / BUCKET_MS) * 60
      put(t, {
        time: t, position: 'aboveBar', color: DOWN, shape: 'arrowDown',
        // size 是等比縮放（預設 1），調小同時把箭頭變細變小，不要壓過 K 棒
        size: 0.5,
        text: a.sigma == null ? '量異常' : `${a.sigma.toFixed(1)}σ`,
      }, Math.abs(a.sigma ?? 0))
    }
    for (const a of priceAlerts) {
      const t = Math.floor(a.alert_ts_ms / BUCKET_MS) * 60
      const up = a.ret >= 0
      // 跳躍比量能異常稀有得多，同一分鐘兩者都有時讓跳躍勝出（+1000）
      put(t, {
        time: t, position: up ? 'belowBar' : 'aboveBar',
        color: up ? cssVar('--up') : cssVar('--down'), shape: 'circle', size: 0.5,
        text: `${up ? '▲' : '▼'}${Math.abs(a.ret * 100).toFixed(2)}%`,
      }, 1000 + Math.abs(a.l_stat ?? 0))
    }
    markersRef.current.setMarkers(
      [...best.values()]
        .sort((x, y) => x.time - y.time)
        .map(({ _rank, ...m }) => m),   // marker 必須依時間遞增
    )
  }, [alerts, priceAlerts])

  return <div className="chart" ref={boxRef} />
}
