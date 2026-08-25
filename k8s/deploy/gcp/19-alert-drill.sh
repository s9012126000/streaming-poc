#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 9-5 告警演練 —— 本週唯一能證明「監控真的有用」的一步。
#
# 為什麼需要演練：
#   壞掉的告警系統長得跟正常的一模一樣——畫面一片綠，target 全 UP，
#   規則全部 INACTIVE。你沒辦法從「沒有告警」推論出「系統正常」，
#   因為「告警壞了」也長這樣。**唯一的辦法是製造一次真的故障。**
#
# 這支腳本會：
#   1. 確認起始狀態乾淨（4 個 job、BronzeStalled 是 INACTIVE）
#   2. 停掉 producer
#   3. 每 15 秒輪詢，印出狀態轉換與經過時間
#   4. 等到 FIRING，確認 Alertmanager 真的收到
#   5. 恢復 producer，等到 RESOLVED
#
# 全程約 8-12 分鐘。中途 Ctrl-C 會自動把 producer 開回來。
#
# 用法：bash k8s/deploy/gcp/19-alert-drill.sh
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

NS="$K8S_NAMESPACE"
ALERT="BronzeStalled"
PROM_PORT=59090
AM_PORT=59093

# --- port-forward 與清理 ---------------------------------------------------
# 用不常見的埠，避開你手動開的 9090/9093。
PF_PIDS=""
cleanup() {
  echo
  echo "── 清理 ────────────────────────────────────────────"
  for p in $PF_PIDS; do kill "$p" 2>/dev/null; done
  # ★ 中途中斷時一定要把 producer 開回來，
  #   否則會留下一個「以為只是演練」的真實故障。
  CUR=$(kubectl get deploy producer -n "$NS" -o jsonpath='{.spec.replicas}' 2>/dev/null)
  if [ "${CUR:-1}" = "0" ]; then
    echo "   producer 還是 0，開回 1"
    kubectl scale deploy/producer -n "$NS" --replicas=1 >/dev/null 2>&1
  fi
}
trap cleanup EXIT INT TERM

kubectl port-forward -n "$NS" svc/prometheus   "${PROM_PORT}:9090" >/dev/null 2>&1 &
PF_PIDS="$PF_PIDS $!"
kubectl port-forward -n "$NS" svc/alertmanager "${AM_PORT}:9093"   >/dev/null 2>&1 &
PF_PIDS="$PF_PIDS $!"
sleep 4

# --- 小工具 ---------------------------------------------------------------
# 取得告警目前狀態：firing / pending / inactive
_state() {
  curl -s --max-time 5 "http://localhost:${PROM_PORT}/api/v1/rules" 2>/dev/null \
  | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print('unknown'); sys.exit()
for g in d.get('data', {}).get('groups', []):
    for r in g.get('rules', []):
        if r.get('name') == '${ALERT}':
            print(r.get('state', 'unknown')); sys.exit()
print('notfound')
" 2>/dev/null || echo "unknown"
}

# 告警運算式當下的值——用來分辨「條件成立」和「規則沒在算」
_expr_value() {
  curl -s --max-time 5 --get "http://localhost:${PROM_PORT}/api/v1/query" \
       --data-urlencode 'query=sum(rate(flink_taskmanager_job_task_operator_numRecordsOutPerSecond[1m]))' \
  2>/dev/null | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    r = d['data']['result']
    print(round(float(r[0]['value'][1]), 2) if r else 'no-data')
except Exception:
    print('?')
" 2>/dev/null || echo "?"
}

# Alertmanager 那側收到了什麼——證明「Prometheus 有推過去」
_am_count() {
  curl -s --max-time 5 "http://localhost:${AM_PORT}/api/v2/alerts" 2>/dev/null \
  | python3 -c "
import sys, json
try:
    print(len([a for a in json.load(sys.stdin)
               if a.get('labels', {}).get('alertname') == '${ALERT}']))
except Exception:
    print('?')
" 2>/dev/null || echo "?"
}

_jobs() {
  kubectl exec -n "$NS" deploy/flink-jobmanager -c jobmanager -- \
    curl -s --max-time 5 localhost:8081/jobs 2>/dev/null \
  | grep -o '"status":"RUNNING"' | wc -l | tr -d ' '
}

# 輪詢直到狀態符合，或逾時。每次狀態改變才印一行，避免洗版。
_wait_for() {
  local want="$1" limit="$2" label="$3"
  local t=0 last="" st val am
  while [ "$t" -lt "$limit" ]; do
    st=$(_state); val=$(_expr_value); am=$(_am_count)
    if [ "$st" != "$last" ]; then
      printf "   [%3ds] %-9s  records/s=%-8s  Alertmanager 持有=%s\n" \
             "$t" "$st" "$val" "$am"
      last="$st"
    fi
    [ "$st" = "$want" ] && return 0
    sleep 15; t=$((t + 15))
  done
  echo "   ⏰ 等待 ${label} 逾時（${limit} 秒）"
  return 1
}

# --- 0. 起始狀態 -----------------------------------------------------------
echo "════════════════════════════════════════════════════════"
echo " Week 9-5 告警演練 — ${ALERT}"
echo "════════════════════════════════════════════════════════"
echo
echo "── 0. 起始狀態 ─────────────────────────────────────"

J=$(_jobs)
echo "   Flink RUNNING job：${J}"
if [ "${J:-0}" -lt 4 ]; then
  echo "   ❌ job 不足 4 個，先修好再演練。"
  echo "      演練的前提是「系統本來是健康的」，否則分不出"
  echo "      告警是因為你停了 producer，還是因為別的東西壞了。"
  exit 1
fi

S=$(_state)
echo "   ${ALERT} 目前狀態：${S}"
if [ "$S" = "notfound" ] || [ "$S" = "unknown" ]; then
  echo "   ❌ 讀不到規則。確認 Prometheus 有載入 /etc/prometheus/rules/"
  exit 1
fi
if [ "$S" != "inactive" ]; then
  echo "   ❌ 起始就不是 inactive，代表現在已經有問題。先處理完再演練。"
  exit 1
fi
echo "   records/s：$(_expr_value)"
echo "   ✅ 起始狀態乾淨"

# --- 1. 製造故障 -----------------------------------------------------------
echo
echo "── 1. 停掉 producer ────────────────────────────────"
kubectl scale deploy/producer -n "$NS" --replicas=0 >/dev/null
echo "   producer → 0"
echo
echo "   接下來會看到兩段轉換（規則的 for 是 5m）："
echo "     inactive → pending   條件成立，但還在等 5 分鐘"
echo "     pending  → firing    確認持續，推給 Alertmanager"
echo
echo "   ⚠️ 不會立刻變 pending——rate(...[1m]) 需要約 1 分鐘的資料"
echo "      才會降到 0。這段等待是正常的。"
echo

_wait_for "pending" 240 "PENDING" || { echo "   規則沒有進入 PENDING，條件可能沒成立"; exit 1; }
echo "   ✅ 第一階段：條件成立"

_wait_for "firing" 480 "FIRING" || { echo "   沒有進入 FIRING"; exit 1; }
echo "   ✅ 第二階段：規則判定成立，已推給 Alertmanager"

# --- 2. 通知 ---------------------------------------------------------------
echo
echo "── 2. 通知 ─────────────────────────────────────────"
echo "   等 Alertmanager 分組（group_wait: 10s）..."
sleep 20
echo "   Alertmanager 目前持有的 ${ALERT}：$(_am_count)"
echo
echo "   ★ 現在去看 Discord。"
echo "     沒收到的話，問題在 Alertmanager → Discord 這一段："
echo "       kubectl logs -n ${NS} deploy/alertmanager --tail=50 | grep -i notify"
echo "     最常見的原因是 webhook URL 有問題（Discord 回 404），"
echo "     而日誌只會說「通知失敗」，不會說是 URL 的問題。"
echo
read -r -p "   Discord 收到 FIRING 通知了嗎？(y/n) " GOT
if [ "$GOT" != "y" ]; then
  echo "   ⚠️ 記下來：告警規則正確，但通知路徑有問題。"
  echo "      這比「規則不會觸發」好修，但同樣代表監控是壞的。"
fi

# --- 3. 恢復 ---------------------------------------------------------------
echo
echo "── 3. 恢復 producer ────────────────────────────────"
kubectl scale deploy/producer -n "$NS" --replicas=1 >/dev/null
echo "   producer → 1"
echo

_wait_for "inactive" 420 "INACTIVE" || {
  echo "   ⚠️ 沒有自動恢復。producer 起來了但 Flink 可能沒在處理——"
  echo "      這正是 8/26 那次的情況（catalog 的 SQLITE_BUSY）。"
  echo "      檢查：kubectl exec -n ${NS} deploy/flink-jobmanager -c jobmanager -- \\"
  echo "              curl -s localhost:8081/jobs"
  exit 1
}
echo "   ✅ 告警已解除"

echo
echo "   等 Alertmanager 送出 RESOLVED..."
sleep 30
echo "   Alertmanager 目前持有的 ${ALERT}：$(_am_count)   （應為 0）"
echo
read -r -p "   Discord 收到 RESOLVED 了嗎？(y/n) " GOTR

# --- 總結 -----------------------------------------------------------------
echo
echo "════════════════════════════════════════════════════════"
if [ "$GOT" = "y" ] && [ "$GOTR" = "y" ]; then
  echo " ✅ 演練通過 —— 完整循環：偵測 → 通知 → 恢復 → 解除通知"
  echo
  echo "    「會叫」和「會停」都驗過了。只驗前者的告警系統"
  echo "    最後都會被靜音，然後等於沒有。"
else
  echo " ⚠️ 演練未完整通過，見上面的紀錄"
fi
echo "════════════════════════════════════════════════════════"
