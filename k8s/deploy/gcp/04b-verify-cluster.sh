#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 04 跑完之後的驗收。裝好 gke-gcloud-auth-plugin 再跑這支。
# 對應 week6-9-gcp-direct.md 的 Week 6 驗收清單。
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

if ! command -v gke-gcloud-auth-plugin >/dev/null 2>&1; then
  _die "gke-gcloud-auth-plugin 未安裝，kubectl 連不上。見 01-preflight.sh 的說明"
fi

echo "═══════════════════════════════════════════════════"
echo " Week 6 叢集驗收"
echo "═══════════════════════════════════════════════════"

echo
echo "── 1. node 狀態（STATUS 要 Ready、SPOT 要 true）──"
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,STATUS:.status.conditions[-1].type,MACHINE:.metadata.labels.node\.kubernetes\.io/instance-type,SPOT:.metadata.labels.cloud\.google\.com/gke-spot,ZONE:.metadata.labels.topology\.kubernetes\.io/zone'

echo
echo "── 2. 實際可配置資源（扣掉 GKE 系統保留）──────"
kubectl get nodes -o json | python3 -c '
import json,sys,re
d=json.load(sys.stdin)
def cpu(v):
    return int(v[:-1])/1000 if v.endswith("m") else float(v)
def mem(v):
    m=re.match(r"(\d+)(Ki|Mi|Gi)?",v); n=int(m.group(1))
    return n/(1024*1024) if m.group(2)=="Ki" else n/1024 if m.group(2)=="Mi" else n
tc=ta=mc=ma=0
for n in d["items"]:
    tc+=cpu(n["status"]["capacity"]["cpu"]);  ta+=cpu(n["status"]["allocatable"]["cpu"])
    mc+=mem(n["status"]["capacity"]["memory"]); ma+=mem(n["status"]["allocatable"]["memory"])
print(f"   capacity    : {tc:.2f} vCPU / {mc:.1f} Gi")
print(f"   allocatable : {ta:.2f} vCPU / {ma:.1f} Gi   ← 實際能給 pod 的量")
print(f"   系統保留     : {tc-ta:.2f} vCPU / {mc-ma:.1f} Gi")
print()
print(f"   Week 7-8 調整後需求約 6.0 vCPU → " + ("✅ 放得下" if ta>=6.0 else "❌ 不夠，要再砍"))
'

echo
echo "── 3. PD CSI driver（Week 7 的 PVC 靠它）────────"
kubectl get csidriver pd.csi.storage.gke.io >/dev/null 2>&1 \
  && _ok "pd.csi.storage.gke.io 存在" \
  || echo "   ❌ 沒有 CSI driver —— PVC 會建不起來，叢集要重建"

echo
echo "── 4. StorageClass（PVC 預定用 standard）────────"
kubectl get storageclass

echo
echo "── 5. 確認沒有 LoadBalancer 在背景計費 ──────────"
LB=$(kubectl get svc -A --field-selector spec.type=LoadBalancer --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "$LB" = "0" ] && _ok "沒有 LoadBalancer（符合預期）" || echo "   ⚠️ 有 $LB 個 LB 在計費"

echo
echo "── 6. 對外開放的防火牆規則 ──────────────────────"
gcloud compute firewall-rules list --project="$PROJECT_ID" \
  --filter="sourceRanges:('0.0.0.0/0') AND direction=INGRESS" \
  --format="table(name,allowed[].map().firewall_rule().list())" 2>/dev/null \
  || echo "   （查不到，手動確認）"
echo "   註：GKE 會自建幾條節點間通訊規則，來源是叢集內網段，屬正常。"

echo
echo "═══════════════════════════════════════════════════"
echo " 都過了就可以部署暖身服務："
echo "   kubectl apply -f k8s/base/namespace.yaml"
echo "   bash k8s/deploy/gcp/07-create-secrets.sh"
echo "   kubectl apply -f k8s/_archive/warmup/"
echo "═══════════════════════════════════════════════════"
