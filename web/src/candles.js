export const BUCKET_MS = 60_000

/**
 * Fold one trade into the running 1-minute candle list (mutates `candles`).
 * Returns the candle the chart should update, or null if the tick was ignored.
 *
 * 這是顯示層聚合，不是訊號邏輯：只把真實成交併進所屬的分鐘桶。
 * 沒有成交的分鐘不會產生 K 棒——缺口就畫成缺口，絕不內插補值。
 */
export function mergeTrade(candles, trade) {
  const time = Math.floor(trade.trade_time / BUCKET_MS) * 60 // Lightweight Charts 用 UTC epoch 秒
  const last = candles[candles.length - 1]

  // 遲到的 tick：已經收好的 K 棒不回頭改，寧可少算也不要畫出會變動的歷史
  if (last && time < last.time) return null

  if (!last || time > last.time) {
    const candle = {
      time,
      open: trade.price, high: trade.price, low: trade.price, close: trade.price,
      volume: trade.size,
      pv: trade.price * trade.size,
    }
    candles.push(candle)
    return candle
  }

  last.high = Math.max(last.high, trade.price)
  last.low = Math.min(last.low, trade.price)
  last.close = trade.price
  last.volume += trade.size
  last.pv += trade.price * trade.size
  return last
}

const DAY_SECS = 86_400

/**
 * 累積 VWAP（成交量加權平均價）曲線，逐日重置。
 *
 * 這是「當日到目前為止的平均成交成本」，不是每分鐘各自的 VWAP——後者會貼著
 * K 棒本身跑，畫出來只是把已經看得到的形狀再描一次。跨 UTC 日就歸零：美股
 * 一個交易時段落在同一個 UTC 日內，加密貨幣本來就以 UTC 日為界。
 */
export function sessionVwap(candles) {
  let day = null
  let pv = 0
  let vol = 0
  return candles.map((c) => {
    const d = Math.floor(c.time / DAY_SECS)
    if (d !== day) { day = d; pv = 0; vol = 0 }
    pv += c.pv
    vol += c.volume
    return { time: c.time, value: vol > 0 ? pv / vol : c.close }
  })
}
