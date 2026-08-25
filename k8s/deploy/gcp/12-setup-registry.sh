#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Week 8 / Step 1a — 建立 Artifact Registry 並設定 docker 認證。
#
# 前兩週用的都是公開 image（nginx、cp-kafka、minio…），
# 這是第一次要推自己建的 image。
#
# 費用：儲存約 $0.10/GB/月。四個 image 約 2-3 GB，每月不到 $0.30，
#       但仍然是「縮容也停不掉」的成本，Week 9 成本回顧要記得算進去。
# ---------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")/../../.."
source k8s/deploy/gcp/env.sh

: "${AR_REPO:=streaming-poc}"
: "${AR_LOCATION:=$REGION}"

echo "════════════════════════════════════════════════════"
echo "  專案      : $PROJECT_ID"
echo "  Repository: $AR_REPO"
echo "  位置      : $AR_LOCATION"
echo "════════════════════════════════════════════════════"

# --- 1. 建立 repository ---------------------------------------------------
if gcloud artifacts repositories describe "$AR_REPO" \
     --location="$AR_LOCATION" --project="$PROJECT_ID" >/dev/null 2>&1; then
  _ok "Repository $AR_REPO 已存在"
else
  _info "建立 repository..."
  gcloud artifacts repositories create "$AR_REPO" \
    --repository-format=docker \
    --location="$AR_LOCATION" \
    --project="$PROJECT_ID" \
    --description="streaming-poc 自建 image"
  _ok "Repository 建立完成"
fi

# --- 2. docker 認證 -------------------------------------------------------
# 這一步是在你的 Mac 上設定 ~/.docker/config.json，
# 讓 docker push 時知道要用 gcloud 的憑證。
_info "設定 docker 認證..."
gcloud auth configure-docker "${AR_LOCATION}-docker.pkg.dev" --quiet
_ok "docker 認證完成"

# --- 3. 讓 node 拉得到 image ----------------------------------------------
#
# ⚠️ 這一版之前寫錯了，值得記下來：
#    原本以為 Week 6 授予的 roles/container.defaultNodeServiceAccount
#    已經包含拉取權限，所以只「檢查」不「授權」。
#    實測結果是 403 Forbidden——那個角色**不包含** artifactregistry.reader。
#    Google 的文件也把「拉取私有 registry」列在完全獨立的一節。
#
#    症狀特別容易誤導：image 路徑完全正確、gcloud 列得出來、
#    tag 也在，但 kubelet 就是拉不到。錯誤訊息是
#      failed to authorize: ... 403 Forbidden
#    藏在一長串 URL 後面，很容易看成是路徑問題。
#
# 所以這裡改成直接授權（冪等，已經有就不會重複加）。
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
NODE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

_info "確認 node 服務帳戶的 artifactregistry.reader..."
HAS=$(gcloud projects get-iam-policy "$PROJECT_ID" \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:${NODE_SA} AND bindings.role=roles/artifactregistry.reader" \
  --format="value(bindings.role)" 2>/dev/null | head -1)

if [ -n "$HAS" ]; then
  _ok "已有 roles/artifactregistry.reader"
else
  _info "授予 roles/artifactregistry.reader 給 ${NODE_SA}"
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${NODE_SA}" \
    --role="roles/artifactregistry.reader" \
    --condition=None \
    --format="value(etag)" >/dev/null
  _ok "授權完成（IAM 變更幾分鐘內生效）"
fi

cat <<EOT

────────────────────────────────────────────────────────────
 Image 位址格式：
   ${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/<名稱>:<tag>

 下一步：bash k8s/deploy/gcp/13-build-push.sh
────────────────────────────────────────────────────────────
EOT
