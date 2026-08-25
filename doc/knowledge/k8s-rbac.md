# RBAC

> 起因：Flink HA 要寫 ConfigMap、Prometheus 要列 pod，兩者都需要授權

RBAC = Role-Based Access Control。它管的是**誰能對 K8s API 做什麼**。

注意範圍：它**只管對 API Server 的操作**。「pod 能不能連到 Kafka」不是 RBAC 的事，
那是 NetworkPolicy；「容器能不能寫這個目錄」也不是，那是 Linux 的 uid/gid。

---

## 三個物件，回答三個問題

```
ServiceAccount  →  你是誰？
Role            →  能做什麼？
RoleBinding     →  把兩者綁起來
```

```yaml
kind: ServiceAccount
metadata: { name: prometheus, namespace: streaming }
---
kind: Role
rules:
  - apiGroups: [""]              # "" = 核心 API 群組
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
kind: RoleBinding
subjects: [{ kind: ServiceAccount, name: prometheus, namespace: streaming }]
roleRef:  { kind: Role, name: prometheus-sd, apiGroup: rbac.authorization.k8s.io }
```

**三個缺一不可。** 只建 ServiceAccount 沒建 Role = 有身分證但沒權限；
建了 Role 沒 RoleBinding = 權限躺在那裡沒人拿得到。

---

## pod 怎麼「證明」自己是誰

Deployment 寫 `serviceAccountName: prometheus`，kubelet 就把 token 掛進容器：

```
/var/run/secrets/kubernetes.io/serviceaccount/token
```

呼叫 API 時帶上它，API Server 解出身分是
`system:serviceaccount:streaming:prometheus`，再查有沒有對應的綁定。

---

## namespace 級 vs 叢集級

| | 範圍 |
|---|---|
| `Role` + `RoleBinding` | 單一 namespace |
| `ClusterRole` + `ClusterRoleBinding` | 整個叢集 |

本專案兩個 ServiceAccount 都用 namespace 級。

多數 Prometheus 教學給的是 ClusterRole（因為預設要監控全叢集）。
我們只監控 `streaming`，所以縮成 Role——**但那要求 `prometheus.yml` 裡
也寫 `namespaces: {names: [streaming]}`**：

```yaml
kubernetes_sd_configs:
  - role: pod
    namespaces:
      names: [streaming]    # ← 不寫的話會去列全叢集的 pod，然後被拒
```

**兩邊必須一致。** 只縮 Role 而沒縮 SD 設定，症狀就是 `/targets` 空白。

---

## 兩個容易踩的性質

**1. 純加法，沒有「拒絕」規則。**
RBAC 只能給權限，不能收回。所以實際權限是所有綁定的**聯集**——
排查「為什麼它能做這件事」時要找全部的綁定，不是找一條。

**2. GKE 的 default ServiceAccount 幾乎沒有權限。**
不指定 `serviceAccountName` 時 pod 用的是 `default`，而它連列 pod 都不行。
這就是為什麼 Flink 和 Prometheus 都得自己開一個。

---

## ⚠️ 權限不足的症狀不像權限問題

這是最需要記住的一點。兩個實例：

| 服務 | 權限不足時 |
|---|---|
| **Prometheus** | 正常啟動、UI 打得開，只有 `/targets` 是空的——像「還沒發現目標」 |
| **Flink JM** | pod 起得來，但選不出 leader，REST 一直 503 |

**兩個都不會在畫面上說「你沒權限」。** 真正的訊息只在各自的日誌裡：

```
Failed to list *v1.Pod: pods is forbidden: User
  "system:serviceaccount:streaming:prometheus" cannot list resource "pods"
```

---

## 部署前 3 秒，勝過部署後 30 分鐘

不用等 pod 起來再翻日誌，直接問：

```bash
kubectl auth can-i list pods \
  --as=system:serviceaccount:streaming:prometheus -n streaming
# → yes / no
```

⚠️ 這個指令查的是**叢集裡現有的 RBAC 物件**，所以 Role 和 RoleBinding
要先 apply 才問得到。它省的是「部署 → 發現空白 → 翻日誌」那一輪，不是省掉 apply。

### 也要驗「該擋的有擋住」

只驗成功路徑的話，就算不小心給了 `cluster-admin`，結果一樣是 yes。
`k8s/deploy/gcp/18-security-check.sh` 因此做正反兩面：

```
flink        create  configmaps   → yes    ← 該有的
flink        create  leases       → yes
prometheus   list    pods         → yes
prometheus   watch   pods         → yes
prometheus   delete  pods         → no     ← 該擋的
flink        delete  secrets      → no
```

這跟「驗證工具會騙人」是同一個道理——只檢查成功路徑，失敗會偽裝成成功。

---

## 本專案實際給的權限

### Flink（Kubernetes HA）

```yaml
- apiGroups: [""]
  resources: ["configmaps"]      # HA 中繼資料（job graph 指標、checkpoint 指標）
  verbs: ["create","get","list","watch","update","patch","delete"]
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]          # leader 選舉
  verbs: ["create","get","list","watch","update","patch","delete"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get","list","watch"]
```

⚠️ **`leases` 不能漏。** Flink 1.16 之後 leader 選舉改用
`coordination.k8s.io` 的 Lease，不再是舊的「靠 ConfigMap annotation 搶鎖」。
只給 configmaps 的話，JM 起得來但選不出 leader，**錯誤訊息完全不會提到權限**。

### Prometheus（服務發現）

```yaml
- apiGroups: [""]
  resources: ["pods", "services", "endpoints"]
  verbs: ["get", "list", "watch"]
```

⚠️ **`watch` 不能漏。** `list` 是啟動時抓一次完整清單，
`watch` 是保持長連線讓 API Server 主動推送變化。
只給 `list` 的症狀特別陰險：**一開始一切正常，之後新部署的 pod 都不會被監控到**。

`services` / `endpoints` 其實用不到（我們只用 `role: pod`），
一併給是為了日後改發現方式時不用回來翻文件——但這種「多給一點無害」的判斷
在安全檢查時是會被質疑的，能收就收。

---

## 跟 GCP IAM 的關係

這是兩套獨立的系統：

| 層 | 系統 | 管什麼 |
|---|---|---|
| GCP | **IAM** | 誰能建叢集、拉 image、看帳單 |
| K8s | **RBAC** | 叢集內誰能操作 K8s 物件 |

互不相通。本專案在 IAM 那層也踩過坑：node 的 service account 有
`roles/container.defaultNodeServiceAccount`，**但那個角色不含
`artifactregistry.reader`**，結果 ImagePullBackOff 403。

判斷方法：錯誤訊息提到 `googleapis.com` 或 HTTP 403 通常是 IAM；
提到 `system:serviceaccount:` 或 `is forbidden` 通常是 RBAC。
