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
    }
    candles.push(candle)
    return candle
  }

  last.high = Math.max(last.high, trade.price)
  last.low = Math.min(last.low, trade.price)
  last.close = trade.price
  last.volume += trade.size
  return last
}
