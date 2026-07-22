// 金融數字一律走 Intl，不用字串拼接或 toFixed 之後再運算，避免浮點尾數出現在畫面上。
const qty = new Intl.NumberFormat('en-US', { minimumFractionDigits: 4, maximumFractionDigits: 4 })
const sigma = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
const pct = new Intl.NumberFormat('en-US', { style: 'percent', signDisplay: 'exceptZero',
  minimumFractionDigits: 2, maximumFractionDigits: 2 })

// 美股是 2 位小數，但幣有 DOGE 這種 0.12345 —— 固定 2 位會把資訊直接抹掉，
// 所以小於 1 的價格多給幾位。
const priceFmt = (v) => new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2, maximumFractionDigits: Math.abs(v) < 1 ? 6 : 2,
}).format(v)

export const fmtPrice = (v) => (v == null ? '—' : priceFmt(v))
export const fmtQty = (v) => (v == null ? '—' : qty.format(v))
export const fmtSigma = (v) => (v == null ? 'n/a' : `${sigma.format(v)}σ`)
export const fmtPct = (v) => (v == null || !isFinite(v) ? '—' : pct.format(v))

// 交易所時間一律用 UTC 顯示，並且把時區寫在畫面上，不用瀏覽器本地時區含糊帶過。
const time = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'UTC', hour: '2-digit', minute: '2-digit', second: '2-digit',
})
export const fmtTimeUTC = (ms) => (ms == null ? '—' : time.format(new Date(ms)))
