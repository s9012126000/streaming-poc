#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 「誰是 control plane、誰是 node」——把叢集的組成攤開來看。
#
# 這支不改任何東西，純讀取。用來建立對 K8s 架構的實感：
# 在 GKE 上，control plane 是**看不到**的，這正是託管服務的定義。
# ---------------------------------------------------------------------------
set -uo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

echo "═══════════════════════════════════════════════════════════"
echo " 1. CONTROL PLANE — 你只看得到一個網址"
echo "═══════════════════════════════════════════════════════════"

echo
echo "kubectl 認為的 API 端點（來自 ~/.kube/config）："
kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'; echo

echo
echo "GCP 認為的端點與版本："
gcloud container clusters describe "$CLUSTER_NAME" \
  --zone="$ZONE" --project="$PROJECT_ID" \
  --format="value[separator='
'](
  'endpoint       : '||endpoint,
  'master version : '||currentMasterVersion,
  'location       : '||location
)" 2>/dev/null

echo
echo "kubectl cluster-info（K8s 自己回報的服務位置）："
kubectl cluster-info 2>/dev/null | head -4

cat <<'EOT'

  ⬆ 以上全部都只是「一個 HTTPS 位址」。
    kube-apiserver / etcd / scheduler / controller-manager 這四個元件
    跑在 Google 自己的專案裡，你沒有帳號、沒有 SSH、看不到 VM。
    這就是「託管」的意思——也是叢集管理費在買的東西。

EOT

echo "═══════════════════════════════════════════════════════════"
echo " 2. NODE — 這些才是你的機器"
echo "═══════════════════════════════════════════════════════════"

echo
echo "K8s 視角（kubectl 看到的 node 物件）："
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,STATUS:.status.conditions[-1].type,ROLES:.metadata.labels.kubernetes\.io/role,VERSION:.status.nodeInfo.kubeletVersion,OS:.status.nodeInfo.osImage' 2>/dev/null

echo
echo "GCP 視角（同一批機器，以 VM 的身分）："
gcloud compute instances list --project="$PROJECT_ID" \
  --filter="name~gke-${CLUSTER_NAME}" \
  --format="table(name,machineType.basename(),status,scheduling.provisioningModel,networkInterfaces[0].networkIP:label=INTERNAL_IP)" 2>/dev/null

echo
echo "  ⬆ 兩份清單是同一批機器。kubectl 看到的是「K8s node 物件」，"
echo "    gcloud 看到的是「Compute Engine VM」——一體兩面。"
echo "    注意沒有 EXTERNAL_IP：node 不對外，你也 SSH 不進去。"

echo
echo "═══════════════════════════════════════════════════════════"
echo " 3. 決定性證據：node 有沒有 control-plane 標記"
echo "═══════════════════════════════════════════════════════════"
echo
echo "自架叢集（kubeadm / kind / k3s）的 control plane node 會有："
echo "  label : node-role.kubernetes.io/control-plane"
echo "  taint : node-role.kubernetes.io/control-plane:NoSchedule"
echo
echo "這個叢集實際查到的："
CP=$(kubectl get nodes -l node-role.kubernetes.io/control-plane \
     --no-headers 2>/dev/null | wc -l | tr -d ' ')
if [ "$CP" = "0" ]; then
  echo "  → 0 個帶 control-plane 標記的 node ✅"
  echo "    證實 control plane 不在你的 node 裡，是 Google 託管的。"
else
  echo "  → 找到 $CP 個（這不該發生在 GKE 上）"
fi

echo
echo "所有 node 的 taint（GKE 的 node 通常只有 Spot 相關的）："
kubectl get nodes -o custom-columns=\
'NAME:.metadata.name,TAINTS:.spec.taints[*].key' 2>/dev/null

echo
echo "═══════════════════════════════════════════════════════════"
echo " 4. kube-system 裡有什麼、沒有什麼"
echo "═══════════════════════════════════════════════════════════"
echo
kubectl get pods -n kube-system -o wide 2>/dev/null \
  | awk '{printf "%-42s %-10s %s\n", $1, $3, $7}'

cat <<'EOT'

  ⬆ 有的是「每個 node 都要跑的代理程式」：
      kube-proxy / fluentbit / gke-metrics-agent / pdcsi-node  → DaemonSet
      kube-dns / metrics-server                                → 一般 Deployment

    ❌ 沒有 kube-apiserver、etcd、kube-scheduler、kube-controller-manager
       在自架叢集上，這四個會以 static pod 出現在這裡。
       在 GKE 上看不到，因為它們根本不在你的叢集裡。

═══════════════════════════════════════════════════════════
 一句話總結
═══════════════════════════════════════════════════════════
  Control plane = 一個你連得到、但摸不到的 HTTPS 端點（Google 管）
  Node          = 你付錢、看得到、會被 Spot 回收的 VM（你管）
  你的 Mac      = 只是拿著 token 的客戶端，關機也不影響叢集
EOT
