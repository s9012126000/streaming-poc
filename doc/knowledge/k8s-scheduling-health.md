# 排程與健康檢查

> 起因：「哪裡設定分散在不同 node」、以及一個 probe 殺了健康的 bot 26 次

---

## pod 怎麼被排到 node 上

Scheduler 做兩輪：

```
① 過濾（filter）    哪些 node 「可以」放
      requests 塞得下？taint 容忍嗎？親和性符合嗎？

② 計分（score）     剩下的裡面哪個「最好」
      資源最寬鬆的、image 已經在本機的、分散度最好的
```

**關鍵是 `requests` 而不是 `limits`。** Scheduler 只看 requests
決定塞不塞得下——limits 是給 kubelet 在執行期限制用的。

```yaml
resources:
  requests: { cpu: 300m, memory: 1792Mi }   # ← 排程看這個
  limits:   { cpu: "1",  memory: 2Gi }      # ← 執行期的上限
```

兩者的行為也不同：

| 資源 | 超過 limits 會怎樣 |
|---|---|
| CPU | **被節流**（throttle），變慢但活著 |
| Memory | **OOMKilled**，exit 137 |

所以記憶體的 limits 要留餘裕。本專案的 Flink JobManager：

```
jobmanager.memory.process.size: 1400m    ← Flink 自己算的
limits.memory: 2Gi                       ← 容器的上限
```

差的那 600MB 是給 RocksDB 的堆外記憶體、執行緒堆疊、glibc 開銷。
設太接近就是週期性 OOMKilled，**而且日誌不會說是記憶體問題**——
process 直接被 SIGKILL，來不及寫任何東西。

---

## 讓副本分散在不同 node

不設的話 scheduler 可能把兩個副本放同一台——實測就發生過。

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    minDomains: 2
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels: { app: kafka }
```

| 欄位 | 意思 |
|---|---|
| `topologyKey` | 用什麼當「域」。`kubernetes.io/hostname` = 每台 node 一個域 |
| `maxSkew: 1` | 任兩個域之間的 pod 數量差最多 1 |
| `minDomains: 2` | **至少要用到 2 個域**——沒有這行，「全部擠在一台」也滿足 maxSkew=1 |
| `whenUnsatisfiable` | `DoNotSchedule`（硬性，寧可 Pending）或 `ScheduleAnyway`（軟性，盡量） |

### 硬性 vs 軟性怎麼選

本專案的兩個例子剛好對照：

```yaml
# Kafka —— DoNotSchedule
# 三個 broker 擠在一台 node，那台掛掉 = quorum 消失 = 整個叢集不可用。
# 這是結構性問題，寧可 Pending 也不能將就。

# TaskManager —— ScheduleAnyway
# Flink TM 是無狀態的，擠在一台只影響容錯速度，不會讓系統不可用。
```

**判準：擠在一起時，壞掉的是「效能」還是「正確性」。**

> ⚠️ `minDomains` 要求叢集真的有那麼多 node。本專案為此把 node
> 從 2 台加到 3 台——node 數量是為 Kafka quorum 決定的，不是為算力。

---

## 三種 probe

| probe | 問的問題 | 失敗時 |
|---|---|---|
| **startup** | 「啟動完成了嗎」 | 繼續等；用完就不再執行 |
| **readiness** | 「現在能接流量嗎」 | 從 Service 的 endpoints 移除，**不重啟** |
| **liveness** | 「這個 process 還活著嗎」 | **殺掉容器重建** |

### startupProbe 的用途

沒有它的話，慢啟動的服務會被 liveness 誤殺——JVM 花 90 秒起來，
而 liveness 在 30 秒就判定失敗。

```yaml
startupProbe:
  httpGet: { path: /, port: ui }
  periodSeconds: 5
  failureThreshold: 36        # 最多 180 秒
livenessProbe:
  httpGet: { path: /, port: ui }
  periodSeconds: 30           # startup 通過後才開始
```

**startupProbe 一旦通過就永久停用**，把「啟動期的寬容」和
「運行期的嚴格」分開，不必為了遷就啟動而把 liveness 調鬆。

### 端點要選對

Prometheus 剛好把兩種語意做成兩個端點，值得參考：

```
/-/healthy  → process 還活著嗎        → liveness
/-/ready    → TSDB 載入完成了嗎       → readiness / startup
```

反例是 readiness 打一個會查資料庫的端點：資料庫慢一下，
所有副本同時被移出 endpoints，服務直接不可用——**而它們其實都好好的**。

---

## ⚠️ 弱訊號的 probe 只有下行風險

這是本專案代價最高的一課。bot 原本的 liveness：

```yaml
livenessProbe:
  exec:
    command: ["sh", "-c", "pgrep -f 'bot.main' > /dev/null"]
```

兩個問題：

**1. `python:3.12-slim` 不含 procps，根本沒有 `pgrep`。**

```
Liveness probe failed: sh: 1: pgrep: not found            (×79)
Container bot failed liveness probe, will be restarted    (×26)
```

時間對得分毫不差：

```
initialDelay 30 + failureThreshold 3 × period 30 = 120s → SIGTERM
+ terminationGracePeriodSeconds 30                = 150s → SIGKILL
實測 03:07:10 → 03:09:39 = 149 秒
```

> 退出碼 **137 配 `Reason: Error`**（不是 `OOMKilled`）就是「被 kubelet 殺掉」的特徵。
> 137 很容易被直覺歸因成記憶體不足，要看 `Reason` 才分得出來。

**2. 更根本的是：就算 `pgrep` 存在，這個 probe 也沒有意義。**

Dockerfile 用 exec form 的 `CMD`，所以 python 就是 PID 1。
**PID 1 死掉時容器本來就會結束**，輪不到 probe 來發現。
也就是說它只有兩種可能結果：永遠通過，或因工具不存在而永遠失敗。

| | 沒有 probe | 壞掉的 probe |
|---|---|---|
| bot 壞了 | 安靜地不工作 | 安靜地不工作 |
| bot 好好的 | 正常運作 | **被反覆殺掉** |

**「聊勝於無」在監控上是錯的判斷。** 已整段移除。

### 寫 probe 前先問三件事

1. **這個檢查會不會失敗？** 只可能成功的檢查沒有資訊量
2. **失敗代表什麼？** liveness 失敗會殺容器，訊號不準就是誤殺
3. **執行環境有這個工具嗎？** exec probe 特別容易踩到（slim image 少一堆東西）

第 3 點的查證方式：

```bash
kubectl run tmp -n streaming --restart=Never --image=<img> --command -- sleep 30
kubectl exec -n streaming tmp -- which pgrep
```

---

## `enableServiceLinks: false`

K8s 預設會把同 namespace 內每個 Service 的位址注入成環境變數：

```
FLINK_JOBMANAGER_PORT=tcp://10.8.4.12:6123
FLINK_JOBMANAGER_SERVICE_HOST=10.8.4.12
```

這是為了相容 docker link 的遠古設計。問題是**它會跟應用程式自己的
設定變數撞名**——Flink 讀 `FLINK_*`，Service 叫 `flink-jobmanager`
就會注入 `FLINK_JOBMANAGER_PORT`。

本專案的 Schema Registry 就是這樣掛掉的（CrashLoopBackOff），
之後所有 Deployment 一律關掉。除了避免撞名，`printenv` 也乾淨很多。

---

## PodDisruptionBudget

保護的是**自願性中斷**（node 升級、`kubectl drain`），不是 pod 自己掛掉。

```yaml
kind: PodDisruptionBudget
spec:
  minAvailable: 2
  selector: { matchLabels: { app: kafka } }
```

意思是「排空 node 時，至少要留 2 個 kafka pod」。
沒有它的話，GKE 自動升級可能一次驅逐兩個 broker，quorum 直接消失。

⚠️ **它擋不住 Spot 回收**——那是非自願中斷，沒有協商餘地。
