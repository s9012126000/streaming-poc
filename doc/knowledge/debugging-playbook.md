# 除錯手冊

> 四週遷移歸納出來的方法。每一條都是踩過才寫的。

---

## 〇、先分清楚三個問題

```
① 系統壞了嗎？
② 我對系統的描述錯了嗎？
③ 我用來觀察系統的工具騙我嗎？
```

本專案的統計：**① 3 次，② 4 次，③ 8 次。**

大多數人只準備了對付 ① 的方法。而 ② 和 ③ 加起來是 ① 的四倍——
**這才是需要方法的地方。**

---

## 一、吻合的敘事不是證據

某天早上全鏈路隔夜斷掉。第一直覺是「Spot node 被回收」——
情境完美吻合，而且 Spot 本來就會這樣。

推翻它只需要一行輸出：

```
flink-jobmanager  RESTARTS 0  AGE 21h
```

一次搶佔都沒有。

> **越合理的解釋越該先驗證，因為它最不會被質疑。**

實務上的做法：想到一個假設時，先問「**哪一個指令能推翻它**」，
而不是「哪一個指令能證實它」。前者通常更快。

---

## 二、不要吞掉 stderr

`2>/dev/null` 是本專案 8 次驗證失效裡出現最多的元凶。它把
**「查詢炸了」和「查詢回 0 筆」變成同一個畫面**。

```bash
# 之前
BRONZE=$($TRINO "SELECT COUNT(*) ..." 2>/dev/null | tail -1 | tr -dc '0-9')
#   查詢失敗 → stderr 被吞 → stdout 空 → tr 給不出數字 → 0
#   驗收畫面顯示「bronze.trades 是空的」

# 真正的訊息
Query ... failed: line 1:22: Schema 'bronze' does not exist
```

那次差別是**「資料沒了」vs「catalog 沒了」**，完全不同的處理方式。

### 相關：HTTP 500 和空結果是不同的訊號

```
/api/symbols → 500      查詢炸了
/api/symbols → []       沒有資料
```

把兩者都當成「沒有資料」，就會往完全錯的方向查。
這一條在 catalog 失憶那次是最快的線索——**表如果只是空的，
查詢會成功並回 `[]`，不會回 500**。

---

## 三、先看 K8s 眼中的狀態，再看應用程式日誌

驗收腳本曾經用這個判斷 bot 健康：

```bash
kubectl logs deploy/bot --tail=50 | grep -qiE "ready|logged in|connected"
```

於是 bot 明明在 `CrashLoopBackOff`（重啟 26 次），驗收照樣印 ✅——
**它曾經連上過，那行日誌還在範圍內**。

> **「曾經成功」不等於「現在健康」。** pod 沒 `Ready`，日誌講什麼都不算數。

順序應該是：

```
kubectl get pods           →  它在跑嗎
kubectl describe pod       →  Events 說了什麼（排程失敗、probe 失敗、OOM）
kubectl logs --previous    →  死掉那次的日誌  ← 不是 kubectl logs
kubectl logs               →  現在這次的日誌
```

`--previous` 特別重要。CrashLoopBackOff 的時候不加它，
你看到的是剛重啟、還沒死的那次，**看不到死因**。

---

## 四、先看時間戳，確認自己在看哪一次啟動

```
2026-08-24 17:19:08  ...granted leadership with leaderSessionID=00000000-...
```

這行看起來是「HA 沒生效」，實際上是 21 小時前舊 pod 的日誌——
新的設定根本還沒 apply。

**對著陳年日誌除錯是很容易發生的事**，尤其在 `kubectl logs deploy/x`
會自動選一個 pod 的情況下。

---

## 五、退出碼會說謊，要配 Reason 一起看

```
Exit Code: 137
Reason:    Error        ← 不是 OOMKilled
```

137 = SIGKILL。直覺會歸因成記憶體不足，但 `Reason: Error`
代表是 **kubelet 因為 liveness probe 失敗而殺掉的**。

驗證方式是算時間：

```
initialDelay 30 + failureThreshold 3 × period 30 = 120s → SIGTERM
+ terminationGracePeriodSeconds 30                = 150s → SIGKILL
實測 03:07:10 → 03:09:39 = 149 秒                  ← 對得分毫不差
```

| Exit Code | Reason | 意思 |
|---|---|---|
| 137 | `OOMKilled` | 真的記憶體不足 |
| 137 | `Error` | 被 kubelet 殺（多半是 probe） |
| 1 | `Error` | 程式自己拋例外 |
| 143 | `Error` | 收到 SIGTERM 後正常結束 |

---

## 六、不要假設回應的結構，先印原始輸出

```bash
# 之前：假設有 root-exception 欄位
curl .../exceptions | python3 -c "print(json.load(sys.stdin)['root-exception'])"
# → （無）        ← 誤以為沒有例外

# 之後：先看原始 JSON
curl .../exceptions | head -c 3000
# → {"exceptionHistory":{"entries":[{"exceptionName":"...CommitStateUnknownException"
```

Flink 較新版本把例外搬到 `exceptionHistory.entries[]` 了。
**用取值失敗當成「沒有東西」，跟吞掉 stderr 是同一種錯誤。**

---

## 七、斷言要對著「性質」，不要對著「實作」

驗證腳本裡曾經寫過：

```bash
case "$CATURI" in
  jdbc:sqlite:file:/data/*) 通過 ;;
  *) 失敗 ;;
esac
```

catalog 後端換成 Postgres 之後，這個守衛自己變成紅燈。

問題在於**它檢查的是特定實作，而該檢查的是要保證的性質**——
「catalog 重啟後不會失憶」，而不是「它必須是放在 `/data` 的 SQLite 檔」。

改成排除法之後就穩定了：

```bash
case "$CATURI" in
  "")                            失敗 ;;   # 沒設 = 預設 in-memory
  *memory*|jdbc:sqlite:*/tmp/*)  失敗 ;;   # 不會存活過重啟
  jdbc:postgresql://*|jdbc:mysql://*) 通過 ;;
  jdbc:sqlite:*)                 通過但警告 ;;
esac
```

> 寫得越具體的斷言，越容易在正常演進時變成雜訊，然後被歸類成
> 「已知可忽略」——**那是告警疲勞的入口，跟真正的告警共用同一個機制。**

---

## 八、驗證「該擋的有擋住」，不只是「該通的有通」

```
prometheus  list    pods  → yes    ← 只驗這些的話…
prometheus  delete  pods  → no     ← …給了 cluster-admin 也會全過
flink       delete  secrets → no
```

只檢查成功路徑時，**失敗會偽裝成成功**。這跟第三條是同一個道理。

---

## 九、`kubectl run --rm -i` 有競態

pod 啟動比 kubectl attach 慢時，容器已經跑完退出，輸出接不到，回傳空字串。
本專案遇到兩次，一次誤報失敗、一次以為 image 沒有 `id`。

穩定的做法：

```bash
kubectl run tmp -n streaming --restart=Never --image=<img> --command -- sleep 30
kubectl exec -n streaming tmp -- cat /proc/self/status | grep -i uid
kubectl delete pod tmp -n streaming
```

`/proc/self/status` 還有個好處：**不依賴容器內有任何工具**，比 `id` 可靠
（slim image 常常什麼都沒有）。

---

## 十、查證的成本永遠低於猜測的成本

| 我寫的 | 事實 | 查證成本 | 猜錯的代價 |
|---|---|---|---|
| iceberg-rest 無狀態 | catalog 指標在 SQLite | 看一次 image 預設值 | 隔天全鏈路斷掉 |
| image 以 root 執行 | uid 1000 | 一行 `id` | 一輪 CrashLoopBackOff |
| probe「聊勝於無」 | 設計上不可能提供資訊 | 想清楚 PID 1 是誰 | 健康的服務被殺 26 次 |
| Grafana gid 472 | gid **0** | 一行 `id` | — **先查了，成本是零** |

同一件事的兩種版本：

```
❌ 部署 → CrashLoopBackOff → 讀 stacktrace → 猜是權限 → 查 uid → 修
✅ 查 uid → 發現不對 → 直接寫對 → 部署 → 過
```

**差別不在於誰比較聰明，只在於查證放在部署前還是部署後。**

### 兩個部署前的 3 秒檢查

```bash
# 容器跑在哪個 uid/gid 底下
kubectl run tmp -n streaming --restart=Never --image=<img> --command -- sleep 30
kubectl exec -n streaming tmp -- cat /proc/self/status | grep -i uid

# ServiceAccount 的權限夠不夠
kubectl auth can-i list pods --as=system:serviceaccount:streaming:prometheus -n streaming
```

各自省下一輪「部署 → 失敗 → 診斷」。

---

## 十一、寫進註解的猜測，會變成明天的事實

四次錯誤判斷的共同點：**憑印象寫進註解，然後註解成為後續決策的依據。**

最典型的一句：

> 限制：只能 1 副本（SQLite 檔案鎖），但我們本來就是 `replicas: 1`

它只考慮了伺服器副本數，忽略了 client 並行數。而**最糟的不是它錯了，
是它讓一個真實的限制看起來像已經被評估過並排除了**——比完全沒提到還糟。

實務上的做法：

- 寫下「因為 X 所以 Y」時，問自己 **X 是查過的還是想當然的**
- 沒查過的就明說「未查證」，並附上查證方式
- 語氣越肯定的註解，後面越沒有人（包括自己）會回頭質疑

---

## 十二、能解釋的和不能解釋的分開放

本專案留了一個沒有結論的紀錄：

> 某個 pod 的 `RESTARTS` 前 21 小時只有 3 次、之後 2 小時衝到 26，
> 而根因（`pgrep: not found`）是確定性的。K8s Events 只保留一小時，查不到了。
> **先記著不編故事。**

把猜測寫成結論，正是前面那些錯誤的起點。
**「我不知道」是一個合法的、而且應該被記錄下來的狀態。**

---

## 快速索引

| 症狀 | 先看 |
|---|---|
| pod `Pending` | `describe` 的 Events——多半是資源不足或 PVC 綁不上 |
| pod `CrashLoopBackOff` | `logs --previous` + `describe` 的 `Last State`（Exit Code 配 Reason） |
| pod `Running` 但 `0/1` | probe 沒過。`describe` 會有 `Unhealthy` 事件 |
| `CreateContainerConfigError` | `secretKeyRef` / `configMapKeyRef` 指向不存在的 key |
| `ImagePullBackOff` 403 | node SA 缺 `artifactregistry.reader`（IAM，不是 RBAC） |
| Prometheus `/targets` 空白 | RBAC。`kubectl auth can-i list pods --as=...` |
| Flink JM 起來但 REST 503 | leader 選不出來——RBAC 少了 `leases` |
| `SQLITE_CANTOPEN` | 權限、路徑、或格式。先 `id` 查 uid |
| `SQLITE_BUSY` | 並行寫入。SQLite 是單寫者模型 |
| `field is immutable` | Job 的 template 改了。先 `delete` 再 `apply` |
| `initdb: directory ... not empty` | PVC 根目錄有 `lost+found`。`PGDATA` 要指子目錄 |
