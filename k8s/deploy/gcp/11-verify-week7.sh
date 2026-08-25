#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 7 驗收 —— 對照 week6-9-gcp-direct.md 的驗收清單。
#
# 純讀取，不改任何東西。可以反覆跑。
# 持久化那一項（delete pod 後資料仍在）需要真的砍 pod，
# 所以放在最後、要加 --destructive 才會執行。
#
#   bash k8s/deploy/gcp/11-verify-week7.sh                # 只讀
#   bash k8s/deploy/gcp/11-verify-week7.sh --destructive  # 含 delete pod 測試
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

NS="$K8S_NAMESPACE"
K="kubectl exec -n $NS broker-0 --"
BS="--bootstrap-server localhost:9092"
PASS=0; FAIL=0
_p() { echo "  ✅ $*"; PASS=$((PASS+1)); }
_f() { echo "  ❌ $*"; FAIL=$((FAIL+1)); }

# ⚠️ 為什麼需要這個 helper：
#    `grep -c .` 在找不到任何一行時會「印出 0」**而且**回傳 exit code 1。
#    原本寫成 $(... | grep -c . || echo 0)，兩邊都會輸出，
#    結果變成 "0\n0"，後面的 [ "$X" -eq 0 ] 就報
#    "integer expression expected"。
#    改成 || true 只吞掉 exit code，不多印東西。
_cnt() { grep -c . 2>/dev/null || true; }

# 用 pod 打叢集內的 HTTP 端點，回傳 status code。
_http() {
  kubectl run "httpchk-$RANDOM" -n "$NS" --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.11.1 -- \
    curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$1" 2>/dev/null | tr -dc '0-9'
}
_body() {
  kubectl run "httpbody-$RANDOM" -n "$NS" --rm -i --restart=Never --quiet \
    --image=curlimages/curl:8.11.1 -- \
    curl -s --max-time 10 "$1" 2>/dev/null
}

echo "════════════════════════════════════════════════════════"
echo " Week 7 驗收 — 有狀態服務上 GKE"
echo "════════════════════════════════════════════════════════"

# --- 1. 有狀態服務就緒 ----------------------------------------------------
echo
echo "── 1. 服務狀態 ──────────────────────────────────────"
kubectl get pods -n "$NS" -o wide
echo
for w in "statefulset/broker:3" "statefulset/minio:1" \
         "deployment/schema-registry:1" "deployment/iceberg-rest:1"; do
  obj="${w%%:*}"; want="${w##*:}"
  ready=$(kubectl get "$obj" -n "$NS" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  ready="${ready:-0}"
  [ "$ready" = "$want" ] && _p "$obj  $ready/$want ready" \
                         || _f "$obj  $ready/$want ready"
done

# --- 2. PVC ---------------------------------------------------------------
echo
echo "── 2. PVC（StorageClass 必須是 standard = pd-standard）──"
kubectl get pvc -n "$NS" \
  -o custom-columns='NAME:.metadata.name,SC:.spec.storageClassName,SIZE:.spec.resources.requests.storage,STATUS:.status.phase'
BADSC=$(kubectl get pvc -n "$NS" -o jsonpath='{.items[?(@.spec.storageClassName!="standard")].metadata.name}')
[ -z "$BADSC" ] && _p "全部 PVC 使用 standard" || _f "非 standard 的 PVC：$BADSC"
UNBOUND=$(kubectl get pvc -n "$NS" -o jsonpath='{.items[?(@.status.phase!="Bound")].metadata.name}')
[ -z "$UNBOUND" ] && _p "全部 PVC 已 Bound" || _f "未綁定：$UNBOUND"

# --- 3. Kafka quorum ------------------------------------------------------
echo
echo "── 3. KRaft quorum ─────────────────────────────────"
QUORUM=$($K kafka-metadata-quorum $BS describe --status 2>/dev/null)
echo "$QUORUM" | grep -E "LeaderId|CurrentVoters|MaxFollowerLag" || true
# ⚠️ 用 grep -o | wc -l 而不是 grep -c：
#    grep -c 數的是「符合的行數」，但 CurrentVoters 的 JSON 全在同一行，
#    三個 voter 也只會回 1。grep -o 才是數「出現次數」。
VOTERS=$(echo "$QUORUM" | grep -o '"id":' | wc -l | tr -d ' ')
[ "$VOTERS" -eq 3 ] && _p "3 個 voter" || _f "voter 數 = ${VOTERS}（應為 3）"

# --- 3b. broker 分佈（RF=3 有沒有意義的關鍵）-----------------------------
echo
echo "── 3b. broker 落在幾台 node ────────────────────────"
kubectl get pods -n "$NS" -l app=kafka \
  -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName' --no-headers
DISTINCT=$(kubectl get pods -n "$NS" -l app=kafka \
  -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort -u | _cnt)
NODES_UP=$(kubectl get nodes --no-headers 2>/dev/null | grep -c " Ready " || true)
echo "     （目前叢集有 ${NODES_UP} 台 Ready 的 node）"

# 三段判定，不要只看「有沒有 ≥2」——
# 2+1 分佈雖然合法，但掉「2」那台就失去 quorum，等於一半機率致命。
if [ "$DISTINCT" -ge 3 ]; then
  _p "broker 分佈在 ${DISTINCT} 台 node（1+1+1，單點故障必存活）"
elif [ "$DISTINCT" -eq 2 ]; then
  _p "broker 分佈在 2 台 node —— 合法但降級"
  echo "     ⚠️ 分佈是 2+1：掉「2」那台會一次少 2 個 broker，quorum 消失。"
  echo "        原因通常是第 3 台 node 被 Spot 回收了。"
  echo "        三台都 Ready 之後重排：kubectl delete pods -n $NS -l app=kafka"
else
  _f "全部 broker 擠在 1 台 node —— 那台掛掉就失去 quorum，RF=3 形同虛設"
  echo "     修正：等 node 齊了再 kubectl delete pods -n $NS -l app=kafka"
fi

# --- 4. Topic 與 RF=3 -----------------------------------------------------
echo
echo "── 4. Topic 與副本 ─────────────────────────────────"
TOPICS=$($K kafka-topics $BS --list 2>/dev/null | grep -v '^_' | sort)
echo "$TOPICS" | sed 's/^/     /'
COUNT=$(echo "$TOPICS" | _cnt)
[ "$COUNT" -eq 6 ] && _p "6 個 topic" || _f "topic 數 = ${COUNT}（應為 6）"

# 一次 describe 全部再解析，不要對每個 topic 各跑一次——
# 每次呼叫都要在 pod 裡啟動一個 JVM，六個 topic 就要等半分鐘。
ALLDESC=$($K kafka-topics $BS --describe 2>/dev/null)
BADRF=$(echo "$ALLDESC" | awk '
  /^Topic:/ && /ReplicationFactor:/ {
    name=""; rf=""
    for (i=1; i<=NF; i++) {
      if ($i == "Topic:")             name = $(i+1)
      if ($i == "ReplicationFactor:") rf   = $(i+1)
    }
    if (name !~ /^_/ && rf != "3") printf " %s(RF=%s)", name, rf
  }')
[ -z "$BADRF" ] && _p "全部 topic RF=3" || _f "RF 不是 3：$BADRF"

UNDER=$($K kafka-topics $BS --describe --under-replicated-partitions 2>/dev/null | _cnt)
[ "$UNDER" -eq 0 ] && _p "無 under-replicated 分區" || _f "$UNDER 個分區副本不足"

# --- 5. 儲存讀寫 ----------------------------------------------------------
echo
echo "── 5. MinIO bucket ─────────────────────────────────"
if kubectl exec -n "$NS" minio-0 -- ls /data 2>/dev/null | grep -q warehouse; then
  _p "warehouse bucket 存在"
else
  _f "找不到 warehouse bucket"
fi

# --- 6. iceberg-rest catalog ---------------------------------------------
echo
echo "── 6. Iceberg REST catalog ─────────────────────────"
# ⚠️ 不要 grep "warehouse"——/v1/config 回的是「給 client 的 catalog 設定」，
#    通常是 {"defaults":{},"overrides":{}}，裡面本來就沒有 warehouse 路徑。
#    正確的判準是「HTTP 200 且回得出 JSON」。
CODE=$(_http "http://iceberg-rest.streaming.svc.cluster.local:8181/v1/config")
CFG=$(_body "http://iceberg-rest.streaming.svc.cluster.local:8181/v1/config")
if [ "$CODE" = "200" ]; then
  _p "catalog 回應 200：$(echo "$CFG" | head -c 100)"
else
  _f "catalog HTTP ${CODE:-無回應}"
fi

# --- 7. Schema Registry ---------------------------------------------------
echo
echo "── 7. Schema Registry ──────────────────────────────"
SUBJ=$(_body "http://schema-registry.streaming.svc.cluster.local:8081/subjects")
echo "     subjects: $SUBJ"
echo "$SUBJ" | grep -q "market-trades-value" \
  && _p "market-trades-value 已註冊" \
  || _f "schema 未註冊（跑 k8s/deploy/gcp/10-register-schema.sh）"

# --- 8. Secret 不落地 -----------------------------------------------------
echo
echo "── 8. 憑證不出現在 manifest ────────────────────────"
if grep -rqE "ALPACA_API_KEY=|DISCORD_TOKEN=|MINIO_ROOT_PASSWORD=" k8s/ 2>/dev/null; then
  _f "k8s/ 底下有憑證明文"
else
  _p "k8s/ 底下無憑證明文"
fi
if kubectl exec -n "$NS" deploy/iceberg-rest -- \
     sh -c 'test -n "$AWS_ACCESS_KEY_ID"' 2>/dev/null; then
  _p "iceberg-rest 取得到 Secret"
else
  _f "iceberg-rest 取不到 Secret"
fi

# --- 9. 對外暴露 ----------------------------------------------------------
echo
echo "── 9. 無對外暴露 ───────────────────────────────────"
LB=$(kubectl get svc -n "$NS" --field-selector spec.type=LoadBalancer --no-headers 2>/dev/null | _cnt)
[ "$LB" -eq 0 ] && _p "無 LoadBalancer（無額外計費）" || _f "有 $LB 個 LoadBalancer"

# --- 10. 破壞性測試（選用）------------------------------------------------
if [ "${1:-}" = "--destructive" ]; then
  echo
  echo "════════════════════════════════════════════════════════"
  echo " 10. 持久化測試（會砍掉 broker-2）"
  echo "════════════════════════════════════════════════════════"
  echo "  刪除前的 topic 數：$COUNT"
  kubectl delete pod broker-2 -n "$NS"
  echo "  等待重建（Spot + PVC 重掛可能要 5-10 分鐘）..."
  kubectl wait --for=condition=ready pod/broker-2 -n "$NS" --timeout=900s \
    && _p "broker-2 已重建並就緒" || _f "broker-2 重建逾時"

  echo "  等 ISR 追上..."
  for i in $(seq 1 30); do
    U=$($K kafka-topics $BS --describe --under-replicated-partitions 2>/dev/null | _cnt)
    [ "$U" -eq 0 ] && break
    echo "    還有 $U 個分區副本不足，10 秒後再看"; sleep 10
  done
  [ "$U" -eq 0 ] && _p "ISR 已完全恢復" || _f "仍有 $U 個分區副本不足"

  NEW=$($K kafka-topics $BS --list 2>/dev/null | grep -v '^_' | _cnt)
  [ "$NEW" -eq "$COUNT" ] && _p "topic 資料仍在（$NEW 個）" || _f "topic 數變成 $NEW"
fi

# --- 總結 -----------------------------------------------------------------
echo
echo "════════════════════════════════════════════════════════"
echo " 結果：通過 $PASS 項，失敗 $FAIL 項"
[ "$FAIL" -eq 0 ] && echo " ✅ Week 7 驗收通過" || echo " ❌ 有項目未通過，見上方 ❌"
echo "════════════════════════════════════════════════════════"
echo
echo " 未含破壞性測試？加 --destructive 執行「delete pod 後資料仍在」"
echo " 收工記得：bash k8s/deploy/gcp/06-down.sh"
exit "$FAIL"
