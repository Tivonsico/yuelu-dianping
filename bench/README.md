# 本地验证与压测

先在仓库根目录执行 `docker compose up -d --wait` 并用 `local` profile 启动普通应用。

```sh
cd bench
npm ci
node smoke.mjs
```

该脚本仅使用专用 Compose 数据库与 Redis，创建测试 Token 和 995xxx 券，验证冷/空缓存、超额争抢有限库存、一人重复提交、Stream 重复投递、消费失败保留 pending、修复后重试。不得修改脚本地址后连接业务数据库。原始记录输出到忽略提交的 `results/`。

## 性能脚本

`run.mjs` 支持 `cache` 与 `stream` 普通接口。对比 `direct` 和 `sync` 需要额外构建压测专用控制器：

1. 临时将 `support/BenchController.java` 放入 `src/main/java/com/hmdp/bench/`。
2. 执行 `mvn clean package`。
3. 用 `--spring.profiles.active=local,bench` 启动该压测 JAR。
4. 设置 `MODE=direct/cache/sync/stream`，然后 `node run.mjs`。

默认每轮3000次请求、并发1/32/128/256、重复三轮，先进行500次成功预热。环境变量 `COUNT`、`ROUNDS`、`CONCURRENCY` 可调整这些参数。PowerShell示例：

```powershell
$env:MODE='stream'
$env:COUNT='3000'
$env:ROUNDS='3'
$env:CONCURRENCY='1,32,128,256'
node run.mjs
```

压测前会重置对应900xxx券的订单及库存。每轮等待SQL完成、记录两侧库存与Stream pending。异步受理QPS与SQL完成吞吐分别保存；历史归档数据来自原始参考Stream，重跑本仓库代码是新一次测量，不能将其视为同一个代码版本。

完成后移除临时压测控制器，执行 `mvn clean package`，确保普通JAR不含该控制器。归档数据及统计口径见根README和 `docs/evidence/`。
