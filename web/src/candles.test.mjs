// node --test src/candles.test.mjs   （用 node 內建 test runner，不裝測試框架）
import test from 'node:test'
import assert from 'node:assert/strict'
import { mergeTrade } from './candles.js'

const trade = (ms, price, size = 1) => ({ trade_time: ms, price, size })

test('第一筆成交開出新的 K 棒', () => {
  const candles = []
  const c = mergeTrade(candles, trade(60_000, 100))
  assert.deepEqual(c, { time: 60, open: 100, high: 100, low: 100, close: 100, volume: 1 })
})

test('同一分鐘內滾動更新高低收與量', () => {
  const candles = []
  mergeTrade(candles, trade(60_000, 100))
  mergeTrade(candles, trade(60_500, 105, 2))
  const c = mergeTrade(candles, trade(119_999, 95, 3))
  assert.equal(candles.length, 1)
  assert.deepEqual(c, { time: 60, open: 100, high: 105, low: 95, close: 95, volume: 6 })
})

test('跨分鐘開新 K 棒，中間沒成交的分鐘不補值', () => {
  const candles = []
  mergeTrade(candles, trade(60_000, 100))
  mergeTrade(candles, trade(300_000, 110))   // 跳過 2~4 分
  assert.deepEqual(candles.map((c) => c.time), [60, 300])
})

test('遲到的 tick 不改寫已完成的 K 棒', () => {
  const candles = []
  mergeTrade(candles, trade(120_000, 100))
  assert.equal(mergeTrade(candles, trade(60_000, 999)), null)
  assert.equal(candles.length, 1)
  assert.equal(candles[0].high, 100)
})
