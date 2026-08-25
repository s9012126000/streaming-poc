// node --test src/candles.test.mjs   （用 node 內建 test runner，不裝測試框架）
import test from 'node:test'
import assert from 'node:assert/strict'
import { mergeTrade, sessionVwap } from './candles.js'

const trade = (ms, price, size = 1) => ({ trade_time: ms, price, size })

test('第一筆成交開出新的 K 棒', () => {
  const candles = []
  const c = mergeTrade(candles, trade(60_000, 100))
  assert.deepEqual(c, { time: 60, open: 100, high: 100, low: 100, close: 100, volume: 1, pv: 100 })
})

test('同一分鐘內滾動更新高低收與量', () => {
  const candles = []
  mergeTrade(candles, trade(60_000, 100))
  mergeTrade(candles, trade(60_500, 105, 2))
  const c = mergeTrade(candles, trade(119_999, 95, 3))
  assert.equal(candles.length, 1)
  // pv = 100*1 + 105*2 + 95*3，累積 VWAP 就是靠這個欄位還原成交金額
  assert.deepEqual(c, { time: 60, open: 100, high: 105, low: 95, close: 95, volume: 6, pv: 595 })
})

test('跨分鐘開新 K 棒，中間沒成交的分鐘不補值', () => {
  const candles = []
  mergeTrade(candles, trade(60_000, 100))
  mergeTrade(candles, trade(300_000, 110))   // 跳過 2~4 分
  assert.deepEqual(candles.map((c) => c.time), [60, 300])
})

test('累積 VWAP 當日累加、跨 UTC 日重置', () => {
  const d1 = 0
  const d2 = 86_400
  const line = sessionVwap([
    { time: d1 + 60,  close: 100, volume: 1, pv: 100 },
    { time: d1 + 120, close: 105, volume: 3, pv: 315 },
    { time: d2 + 60,  close: 200, volume: 2, pv: 400 },
  ])
  assert.deepEqual(line.map((p) => p.time), [d1 + 60, d1 + 120, d2 + 60])
  assert.equal(line[0].value, 100)          // 100/1
  assert.equal(line[1].value, 415 / 4)      // (100+315)/(1+3)：同一天要累加
  assert.equal(line[2].value, 200)          // 跨日歸零，不把昨天的量算進來
})

test('零成交量的分鐘不會除以零', () => {
  assert.equal(sessionVwap([{ time: 60, close: 42, volume: 0, pv: 0 }])[0].value, 42)
})

test('遲到的 tick 不改寫已完成的 K 棒', () => {
  const candles = []
  mergeTrade(candles, trade(120_000, 100))
  assert.equal(mergeTrade(candles, trade(60_000, 999)), null)
  assert.equal(candles.length, 1)
  assert.equal(candles[0].high, 100)
})
