> ⚠️ **已封存。** k8s/ 在 2026-08-26 改成依部署層次分組，
> 這份文件描述的 `k8s/week9/` 目錄已不存在，內容移到 `k8s/50-monitoring/`。
> 對照表與現行部署順序見 [`k8s/README.md`](../README.md)。
> 保留這份是因為它記錄了當時的推理過程。

---

# k8s/week9/ — 監控堆疊

Week 9：把 `monitoring/` 底下的 compose 設定搬上 GKE，並做一次真正的告警演練。

---

## 檔案

| 檔案 | 內容 |
|---|---|
| `kafka-exporter.yaml` | Service + Deployment。**Week 8 漏掉的一塊** |
| `prometheus-rbac.yaml` | ServiceAccount + Role + RoleBinding（服務發現要列 pod） |
| `prometheus.yaml` | PVC + Service + 設定 ConfigMap + 規則 ConfigMap + Deployment |
| `alertmanager.yaml` | Service + 設定 ConfigMap + Deployment（webhook 從 Secret 掛檔案） |
| `grafana.yaml` | Service + 三個 provisioning ConfigMap + Deployment |

---

## 部署順序

```bash
# 前置：確認 Secret 裡有 DISCORD_WEBHOOK
kubectl get secret streaming-secrets -n streaming \
  -o jsonpath='{.data.DISCORD_WEBHOOK}' | wc -c    # 不是 0 就好

# gateway 的 prometheus 註記已移除，要重新套用
kubectl apply -f k8s/week8/app.yaml

kubectl apply -f k8s/week9/kafka-exporter.yaml
kubectl apply -f k8s/week9/prometheus-rbac.yaml
kubectl apply -f k8s/week9/prometheus.yaml
kubectl apply -f k8s/week9/alertmanager.yaml
kubectl apply -f k8s/week9/grafana.yaml
```

`prometheus-rbac.yaml` **必須在** `prometheus.yaml` 之前 —— pod 起來時
ServiceAccount 要已經存在。

---

## 部署前先查（今天的教訓）

三個 image 的 uid 我都是照慣例寫的，**只有 Prometheus 那個特別註明未查證**。
iceberg-rest 那次就是憑印象寫 root、實際是 1000，換來一輪 CrashLoopBackOff。

```bash
for img in prom/prometheus:v3.1.0 prom/alertmanager:v0.28.1 grafana/grafana:11.5.1; do
  echo "--- $img"
  kubectl run tmp-$RANDOM --rm -i --restart=Never --quiet \
    --image="$img" --command -- id
done
```

對不上就改 manifest 裡的 `runAsUser` / `runAsGroup` / `fsGroup`。

RBAC 也可以先驗，不用等 pod 起來：

```bash
kubectl auth can-i list pods \
  --as=system:serviceaccount:streaming:prometheus -n streaming     # 要 yes
kubectl auth can-i watch pods \
  --as=system:serviceaccount:streaming:prometheus -n streaming     # 要 yes
```

---

## 驗證

```bash
kubectl port-forward -n streaming svc/prometheus 9090:9090
```

| 頁面 | 該看到什麼 |
|---|---|
| `/service-discovery` | streaming 底下所有 pod（relabel **之前**） |
| `/targets` | 約 8 個 target 全部 UP |
| `/alerts` | 4 條規則，正常時都是綠的 |

預期的 target：prometheus 自己、alertmanager、grafana、kafka-exporter、
producer、flink-jobmanager、flink-taskmanager ×2。

**排查順序照這三頁走**：

- `/service-discovery` 空的 → 發現階段失敗 = **RBAC 問題**
- 有發現但 `/targets` 空的 → **relabel 規則寫錯**
- `/targets` 有但紅色 → **抓取失敗**（埠錯、網路不通、或端點不是 Prometheus 格式）

```bash
kubectl port-forward -n streaming svc/grafana 3000:3000    # 免登入
kubectl port-forward -n streaming svc/alertmanager 9093:9093
```

---

## ★ 告警演練（本週核心驗收）

**這是唯一能證明「監控真的有用」的一步。** 前面全部部署成功都不算數 ——
壞掉的告警系統長得跟正常的一模一樣，畫面一片綠。

```bash
# 1. 停掉 producer
kubectl scale deploy/producer -n streaming --replicas=0

# 2. 觀察三個階段（BronzeStalled 的 for: 5m）
#    Prometheus /alerts → PENDING（立刻）
#                      → FIRING（5 分鐘後）
#    Discord 收到訊息

# 3. 恢復
kubectl scale deploy/producer -n streaming --replicas=1
#    → Discord 應收到 RESOLVED（send_resolved: true）
```

三個階段都要親眼看到。只看到 Discord 響，不知道是哪一條規則、
延遲多久；只看 Prometheus 的 FIRING，不知道通知有沒有真的送出去。

---

## 已知的坑

**Alertmanager 不展開環境變數。** 寫 `${DISCORD_WEBHOOK}` 會被當字面字串
送出去，Discord 回 404，而日誌只說「通知失敗」。所以用 `webhook_url_file`
指向掛載的 Secret 檔案。`webhook_url` 和 `webhook_url_file` **互斥**，
同時寫會啟動失敗。

**ConfigMap 掛載會蓋掉整個目錄。** Grafana 的 provider 設定和看板 JSON
因此不能放同一個目錄，看板改掛 `/var/lib/grafana/dashboards`。
Alertmanager 的設定檔則用 `subPath` 掛單一檔案，避免蓋掉 `secrets/` 子目錄。

**kafka-exporter 是 Week 8 漏掉的。** 沒有它，`ConsumerLagGrowing` 和
`DLQNotEmpty` 引用的指標根本不存在 —— 而 Prometheus 對不存在的指標
**不報錯，只回空集合**。那兩條告警一直處於「永遠不會觸發」的狀態。
**靜默失效的告警比沒有告警更危險**，因為你以為它在看著。

**gateway 的 `prometheus.io/*` 註記已移除。** `/api/health` 回 JSON，
Prometheus parse 不了，target 會永遠是紅的，而驗收標準是「全部 UP」。
留著等於每次人工忽略一項 —— 告警疲勞的起點。
要恢復得先讓 gateway 真的提供 Prometheus 格式端點（已列待辦）。

---

## 資源

| 服務 | requests | limits | 儲存 |
|---|---|---|---|
| kafka-exporter | 50m / 64Mi | 200m / 192Mi | — |
| prometheus | 100m / 512Mi | 500m / 1Gi | 10Gi PVC |
| alertmanager | 50m / 64Mi | 200m / 192Mi | emptyDir |
| grafana | 50m / 128Mi | 300m / 384Mi | emptyDir |

合計 requests 約 0.25 vCPU / 0.75 Gi —— 3 台 e2-standard-4 放得下。

只有 Prometheus 用 PVC（TSDB 要留 7 天）。Alertmanager 的 silence 和
Grafana 的 plugin 快取用 emptyDir，重建就沒了 —— PoC 可接受，
換來省兩顆磁碟與兩次權限除錯。
