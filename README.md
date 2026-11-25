# 岳麓点评 Yuelu Dianping

面向湖南长沙岳麓区的本地生活点评服务后端，基于黑马点评课程参考实现改造。练习商户缓存、Redis 登录鉴权、优惠券秒杀与社交 Feed，并保留可核查的测试数据。

**技术栈：** Java · Spring Boot · MySQL · Redis · Redisson · Redis Stream

## 核心功能

- **登录鉴权：** Redis + Token 共享登录状态，双拦截器分别负责续期和登录校验。
- **商户缓存：** 空值缓存、逻辑过期、随机 TTL、互斥回源；冷缓存可正常读库，过期锁不会覆盖新锁持有者的数据。
- **异步秒杀：** Lua 原子完成库存/资格判断、预扣和 Stream 写入；消费者提交 MySQL 事务后才确认消息。
- **订单一致性：** 条件扣库存、事务回滚、用户券唯一约束与重复消费幂等，Redisson 用户锁作兜底。
- **社交功能：** ZSet 推模式 Feed、时间戳与累计偏移滚动分页、Set 共同关注；修复同时间戳跨页重复和取关缓存残留。
- **其他功能：** 时间戳 + Redis 序列订单 ID、附近商户与签到等课程功能。

## 下单链路

```text
HTTP 请求
  → Lua 校验库存和购买资格、预扣库存、XADD stream.orders
  → 返回已受理的订单 ID

Redis Stream 消费者
  → Redisson 用户锁
  → 幂等检查 + MySQL 条件扣库存 + 保存订单（同一事务）
  → 提交成功后 XACK / XDEL
```

本仓库最终选择 **Redis Stream**。HTTP 成功表示已受理，数据库随后完成写入；不代表事务已经提交。消费组自动创建，启动时先恢复固定消费者 `g1/c1` 的 pending。失败消息保留并重试，不盲目回补库存。

当前是单消费者学习实现。永久失败可能阻塞后续消费，需要根据日志、`XPENDING` 和业务状态修复；没有宣称多实例故障切换、自动补偿或 exactly-once。Redis AOF 使用每秒刷盘，存在故障丢数据窗口。

## 测试与数据口径

[原始逐请求记录与 SQL 快照](docs/evidence/) · [逐批汇总 CSV](docs/benchmark-summary.csv) · [三轮统计](docs/benchmark-aggregate.json)

历史对照测试：同机、256 并发、每轮 3,000 次请求、重复三轮。以下为各轮指标的中位数：

| 场景 | 接口 QPS | 观测落库单/秒 | 平均响应 |
|---|---:|---:|---:|
| 重建同步落库基线 | 160.38 | 160.34 | 1,528.82 ms |
| 原始课程 Stream 参考版本 | 3,342.19 | 83.20 | 75.66 ms |
| 商户详情直接查询 MySQL | 3,747.73 | — | 67.54 ms |
| 商户详情 Redis 缓存 | 4,442.83 | — | 57.09 ms |

同步返回表示已提交；Stream 返回表示已受理，**受理 QPS 不等于落库吞吐**。原始 Stream 共 36,000 次秒杀请求完成订单数、返回 ID、重复用户和库存核验。缓存均成功返回真实商户，没有把错误响应计作提升。

上述 Stream 性能数字对应归档的原始参考版本；本仓库发布版另补了事务幂等、自动 pending 恢复和 ACK 后清理，**不把历史性能数据冒充当前发布版的同版本压测结果**。当前代码可用下面脚本重新测试；吞吐也会受硬件、刷盘与后台负载影响。

## 本地启动

需要 Java 17、Maven 3.9 和 Docker Compose。课程项目仍以 Java 8 字节码目标编译。

```sh
docker compose up -d --wait
mvn test
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

或执行 `mvn package` 后启动：

```sh
java -jar target/hmdp-1.0-SNAPSHOT.jar --spring.profiles.active=local
```

默认端口：应用 `18081`、MySQL `23316`、Redis `26389`，均面向本地开发。`local` profile 的固定凭据只用于此隔离环境。首次启动 MySQL 自动导入 `db/schema.sql`，只包含表结构、商户示例与分类示例，不附用户手机号样本。

已有数据库可参考 `src/main/resources/db/migration/V1__order_idempotency.sql`，先核查重复数据再执行；首次 Compose 初始化已经包含该唯一约束，不要重复迁移。

检查：`GET http://127.0.0.1:18081/shop/1`；秒杀接口为 `POST /voucher-order/seckill/{voucherId}`，需 Redis Token 登录态。前端静态页面和 Nginx 部署不在本仓库交付范围内。

## 测试

```sh
# 默认只运行无外部依赖的契约测试
mvn test

# 额外运行真实 Redis 测试；专用 DB14 + UUID 前缀
mvn test -Dhmdp.test.redis.port=26389 -Dhmdp.test.redis.password=hmdp-test
```

覆盖冷缓存回源、空值哨兵、逻辑过期、锁所有权、库存扣减失败、重复订单及消费者锁竞争。

压测专用的直查/同步对照控制器位于 `bench/support/`，不会编入普通应用；运行方法见 [bench/README.md](bench/README.md)。

## 来源与个人改造范围

原始业务框架及多数功能来自[黑马程序员 Redis 实战课程](https://www.bilibili.com/video/BV1cr4y1671t)及[课程参考实现](https://github.com/cs001020/hmdp)。保留[原项目说明](docs/upstream-readme.md)和原有作者注释，不将课程代码宣称为全部原创。

本仓库主要补充缓存冷启动/空值/随机 TTL 与锁保护、订单事务及消费幂等、Stream 消费组与 pending 恢复、Feed 分页与取关修复，以及测试脚本和实测证据。它是用于学习与交流的后端工程，不是已上线的生产业务。
