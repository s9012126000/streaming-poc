#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 9-6 安全檢查
#
# 檢查的是「有沒有意外對外開放、有沒有權限過寬」。
# 純讀取，不改任何東西，可以反覆跑。
#
# 為什麼這件事在 compose 版本不存在：
#   compose 沒有權限的概念——所有容器同一個網路，誰都連得到誰，
#   沒有 API 要授權，也沒有 IAM。搬上雲端之後，
#   「預設全通」變成「預設全擋，然後你一項一項打開」，
#   而每一項打開都是一次可能開太大的機會。
#
# 用法：bash k8s/deploy/gcp/18-security-check.sh
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

NS="$K8S_NAMESPACE"
PASS=0; FAIL=0; WARN=0
_p() { echo "  ✅ $*"; PASS=$((PASS+1)); }
_f() { echo "  ❌ $*"; FAIL=$((FAIL+1)); }
_w() { echo "  ⚠️  $*"; WARN=$((WARN+1)); }

echo "════════════════════════════════════════════════════════"
echo " Week 9-6 安全檢查"
echo "════════════════════════════════════════════════════════"

# --- 1. 對外暴露的 K8s 服務 ----------------------------------------------
echo
echo "── 1. 有沒有東西對外開放 ───────────────────────────"
# 整個專案的前提是「只用 port-forward，不開任何對外入口」。
# 這既是安全考量，也是成本考量（每條 LB 每月約 $18）。
LB=$(kubectl get svc -A --field-selector spec.type=LoadBalancer \
       --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "${LB:-0}" -eq 0 ] && _p "無 LoadBalancer" || {
  _f "有 ${LB} 個 LoadBalancer"
  kubectl get svc -A --field-selector spec.type=LoadBalancer 2>/dev/null | sed 's/^/     /'
}

# NodePort 比 LoadBalancer 更容易被忽略——它不會出現在帳單上，
# 但只要防火牆規則對，node 的公有 IP 就直接通了。
NP=$(kubectl get svc -A --field-selector spec.type=NodePort \
       --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "${NP:-0}" -eq 0 ] && _p "無 NodePort" || {
  _f "有 ${NP} 個 NodePort"
  kubectl get svc -A --field-selector spec.type=NodePort 2>/dev/null | sed 's/^/     /'
}

ING=$(kubectl get ingress -A --no-headers 2>/dev/null | wc -l | tr -d ' ')
[ "${ING:-0}" -eq 0 ] && _p "無 Ingress" || _w "有 ${ING} 個 Ingress，確認是刻意的"

# --- 2. GCP 防火牆 --------------------------------------------------------
echo
echo "── 2. 防火牆規則（來源 0.0.0.0/0）──────────────────"
# GKE 會自建幾條規則（節點之間、健康檢查），那些是正常的。
# 要看的是「有沒有人為了除錯開了一條全世界都能連的規則然後忘了關」。
OPEN=$(gcloud compute firewall-rules list \
        --filter="sourceRanges:0.0.0.0/0 AND direction=INGRESS AND disabled=false" \
        --format="value(name,allowed[].map().firewall_rule().list())" 2>/dev/null)
if [ -z "$OPEN" ]; then
  _p "沒有來源為 0.0.0.0/0 的 INGRESS 規則"
else
  echo "     以下規則允許來自任何來源的連線："
  echo "$OPEN" | sed 's/^/       /'
  # default-allow-ssh 之類是 GCP 預設就有的，不一定是問題，
  # 但「預設有」不等於「你需要」——這個專案沒有人要 SSH 進 node。
  _w "請逐條確認是否需要（GCP 預設就會有 default-allow-* 幾條）"
fi

# --- 3. IAM 權限過寬 ------------------------------------------------------
echo
echo "── 3. IAM 綁定 ─────────────────────────────────────"
# roles/owner 和 roles/editor 幾乎等於「什麼都能做」。
# 個人專案上自己是 owner 很正常，要看的是**服務帳戶**有沒有拿到。
BROAD=$(gcloud projects get-iam-policy "$PROJECT_ID" \
         --flatten="bindings[].members" \
         --filter="bindings.role:(roles/owner OR roles/editor)" \
         --format="value(bindings.role,bindings.members)" 2>/dev/null \
        | grep "serviceAccount:" || true)
if [ -z "$BROAD" ]; then
  _p "沒有服務帳戶持有 owner / editor"
else
  _f "以下服務帳戶權限過寬："
  echo "$BROAD" | sed 's/^/       /'
  echo "     服務帳戶應該只拿到它實際需要的角色。"
  echo "     本專案的 node SA 只需要："
  echo "       roles/container.defaultNodeServiceAccount"
  echo "       roles/artifactregistry.reader   ← Week 6 踩過，前者不含後者"
fi

# --- 4. K8s RBAC ----------------------------------------------------------
echo
echo "── 4. RBAC ─────────────────────────────────────────"
# 我們刻意只用 namespace 級的 Role。
# 出現 ClusterRole 綁到自己的 ServiceAccount 就是範圍開太大。
OURSA="flink prometheus"
BAD=""
for sa in $OURSA; do
  hit=$(kubectl get clusterrolebinding -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.subjects[*].name}{'\n'}{end}" 2>/dev/null \
        | grep -w "$sa" || true)
  [ -n "$hit" ] && BAD="${BAD}${sa}: ${hit}\n"
done
if [ -z "$BAD" ]; then
  _p "自建的 ServiceAccount 都沒有 ClusterRoleBinding（範圍限於 namespace）"
else
  _f "以下 ServiceAccount 拿到了叢集級權限："
  printf "%b" "$BAD" | sed 's/^/       /'
fi

# 逐項確認實際權限——比看 YAML 可靠，因為它問的是叢集的真實狀態
echo "     實際權限（yes/no）："
for chk in "flink:create:configmaps" "flink:create:leases" \
           "prometheus:list:pods" "prometheus:watch:pods" \
           "prometheus:delete:pods" "flink:delete:secrets"; do
  sa="${chk%%:*}"; rest="${chk#*:}"; verb="${rest%%:*}"; res="${rest##*:}"
  ans=$(kubectl auth can-i "$verb" "$res" \
          --as="system:serviceaccount:${NS}:${sa}" -n "$NS" 2>/dev/null)
  printf "       %-12s %-7s %-12s → %s\n" "$sa" "$verb" "$res" "${ans:-?}"
done
echo "     ↑ 前四項應為 yes，後兩項應為 no（那是我們沒給、也不該給的）"

# --- 5. 憑證 --------------------------------------------------------------
echo
echo "── 5. 憑證處理 ─────────────────────────────────────"
if grep -rqE "ALPACA_API_KEY=|DISCORD_TOKEN=|MINIO_ROOT_PASSWORD=|DISCORD_WEBHOOK=|CATALOG_DB_PASSWORD=" \
     k8s/ 2>/dev/null; then
  _f "k8s/ 底下有憑證明文"
else
  _p "k8s/ 底下無憑證明文"
fi

# .env 進 git 是最常見的意外
if git check-ignore -q .env 2>/dev/null; then
  _p ".env 有被 .gitignore 排除"
else
  _f ".env 沒有被 .gitignore 排除 —— 有進 git 的風險"
fi

# 已經進過 git 的話，加進 .gitignore 也救不回來
if git ls-files --error-unmatch .env >/dev/null 2>&1; then
  _f "★ .env 已經在 git 追蹤中——歷史紀錄裡有憑證，必須輪換"
else
  _p ".env 不在 git 追蹤中"
fi

# Alertmanager 的 webhook 應該來自 Secret 檔案，不是寫死在設定裡
if kubectl get cm alertmanager-config -n "$NS" -o yaml 2>/dev/null \
     | grep -q "webhook_url_file"; then
  _p "Alertmanager 的 webhook 走 Secret 檔案，未落地在 ConfigMap"
else
  _w "Alertmanager 沒有使用 webhook_url_file，確認 URL 沒寫死在 ConfigMap"
fi

# --- 6. 容器權限 ----------------------------------------------------------
echo
echo "── 6. 容器以誰的身分執行 ───────────────────────────"
# 不設 runAsUser 時多數 image 會以 root 跑。
# PoC 不強求全部非 root，但要知道有哪些——這是「知道現況」而不是「必須修」。
ROOTY=$(kubectl get pods -n "$NS" -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.securityContext.runAsUser}{"\n"}{end}' 2>/dev/null \
        | awk -F'\t' '$2=="" {print $1}' || true)
if [ -z "$ROOTY" ]; then
  _p "所有 pod 都明確指定了 runAsUser"
else
  CNT=$(echo "$ROOTY" | wc -l | tr -d ' ')
  _w "${CNT} 個 pod 沒有明確指定 runAsUser（多半以 image 預設身分執行）"
  echo "$ROOTY" | sed 's/^/       /'
  echo "     這不一定是問題——image 自己可能就設了 USER。"
  echo "     要確認實際 uid："
  echo "       kubectl exec -n ${NS} <pod> -- cat /proc/self/status | grep -i uid"
fi

# 特權容器則是明確的紅線
PRIV=$(kubectl get pods -n "$NS" \
        -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[*].securityContext.privileged}{"\n"}{end}' 2>/dev/null \
       | grep -w true || true)
[ -z "$PRIV" ] && _p "沒有特權容器（privileged: true）" || {
  _f "有特權容器："; echo "$PRIV" | sed 's/^/       /'; }

# --- 總結 -----------------------------------------------------------------
echo
echo "════════════════════════════════════════════════════════"
echo " 通過 ${PASS} 項，失敗 ${FAIL} 項，待確認 ${WARN} 項"
[ "$FAIL" -eq 0 ] && echo " ✅ 沒有明確的安全問題" \
                  || echo " ❌ 有項目需要處理"
echo "════════════════════════════════════════════════════════"
cat <<'EOT'

 ⚠️ 這份檢查涵蓋的是「設定層面的常見疏漏」，不是完整的安全稽核。
    明確不在範圍內的：
      - NetworkPolicy（我們沒有設，namespace 內誰都連得到誰）
      - Secret 在 etcd 裡只是 base64，不是加密
        （GKE 有磁碟層級加密，但要更嚴格就得用 Secret Manager，
         已列在 Week 7 的待辦）
      - image 的漏洞掃描
      - Grafana 開了匿名 Admin（因為沒有對外路徑，風險可接受）
EOT
exit "$FAIL"
