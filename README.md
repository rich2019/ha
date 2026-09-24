# MySQL HA Controller MVP

一个面向实验环境的 Go/MySQL HA 控制器，当前提供：

- MySQL 可用性、只读状态和复制状态检测
- GTID/复制线程/复制延迟信息采集
- Etcd 集群状态、租约锁和 epoch
- 手工主备切换 API
- 自动故障转移 dry-run
- Webhook 告警
- `/metrics` Prometheus 指标
- Docker Compose 实验拓扑

## 运行

需要 Go 1.23 或更高版本：

```powershell
go mod tidy
$env:HA_NODES_JSON='[{"id":"mysql-1","address":"127.0.0.1:3306","dsn":"root:password@tcp(127.0.0.1:3306)/mysql?parseTime=true","expected_role":"primary"}]'
go run ./cmd/controller
```

如果启动时 Etcd 不可用，程序会退回内存状态存储，重启后状态丢失；生产环境应修复 Etcd 配置，而不是依赖该回退。

## API

```text
GET  /healthz
GET  /api/v1/status
POST /api/v1/switchover       {"target":"mysql-2"}
POST /api/v1/auto-failover    {"enabled":true}
GET  /metrics
```

自动倒换默认由 `HA_AUTO_FAILOVER_EXECUTE=false` 保护，只产生 dry-run 任务和告警。只有在隔离的实验环境确认 fencing、路由和复制初始化后，才应设置为 `true`。

## Compose

本仓库提供 Etcd 三节点和 MySQL 一主两备示例，包含 server-id、GTID、ROW Binlog、复制用户和初始复制配置。它用于本地联调，不应直接作为生产部署文件。

```powershell
docker compose up --build
Invoke-RestMethod http://localhost:8080/api/v1/status
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/v1/switchover -ContentType 'application/json' -Body '{"target":"mysql-2"}'
```

## 安全边界

第一版的 fencing 和路由切换是接口化的 noop 实现。真实生产部署必须替换为云实例隔离、IPMI/Redfish、Kubernetes fencing 或经过验证的 ProxySQL/HAProxy 管理器，并补充认证、授权、TLS 和审计存储。
