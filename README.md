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
GET  /api/v1/tasks
POST /api/v1/switchover       {"target":"mysql-2"}
POST /api/v1/auto-failover    {"enabled":true}
GET  /metrics
```

自动倒换默认由 `HA_AUTO_FAILOVER_EXECUTE=false` 保护，只产生 dry-run 任务和告警。只有在隔离的实验环境确认 fencing、路由和复制初始化后，才应设置为 `true`。

多控制器部署必须设置 `HA_REQUIRE_ETCD=true`。管理变更只允许当前 Etcd Leader 执行；切换任务持久化在 Etcd，并可通过 `/api/v1/tasks` 查询。

## Compose

本仓库提供 Etcd 三节点和 MySQL 一主两备示例，包含 server-id、GTID、ROW Binlog、复制用户和初始复制配置。它用于本地联调，不应直接作为生产部署文件。

```powershell
docker compose up --build
Invoke-RestMethod http://localhost:8080/api/v1/status
Invoke-RestMethod -Method Post -Uri http://localhost:8080/api/v1/switchover -ContentType 'application/json' -Body '{"target":"mysql-2"}'
```

## 安全边界

第一版的 fencing 和路由切换是接口化的 noop 实现。真实生产部署必须替换为云实例隔离、IPMI/Redfish、Kubernetes fencing 或经过验证的 ProxySQL/HAProxy 管理器，并补充认证、授权、TLS 和审计存储。

## PVE 三节点验证环境

`deploy/pve` 提供每台 VM 的 MySQL/Etcd Compose、复制初始化脚本、主 Controller 服务和验证告警接收器。每个节点单独设置 `/etc/ha/node.env`，文件权限设为 `0600`；不要把实际密码放进 Git。

1. 设置每台 VM 的 `NODE_IP`、`MYSQL_SERVER_ID`、`ETCD_NAME` 和同一组 `ETCD_INITIAL_CLUSTER`，主节点额外设置 `MYSQL_DATABASE=ha_app`。
2. 启动 `docker compose --env-file /etc/ha/node.env -f /opt/ha/deploy/pve/docker-compose.yml up -d`。在两台备库运行 `bootstrap-replica.sh`，设置 `SOURCE_IP` 后等待复制追平。
3. Controller 通过 systemd 运行，监听 `127.0.0.1:8080`；使用 SSH 隧道访问管理 API。三台 VM 的 Controller 共用 Etcd 集群配置，`HA_REQUIRE_ETCD=true`，`HA_AUTO_FAILOVER_EXECUTE=false`。
4. 首节点运行验证告警接收器，Controller 的 `HA_ALERT_WEBHOOK_URL` 指向 `http://10.56.238.242:9099/webhook`。测试事件可从 `/events` 查询。

MySQL 初始化完成后，在每台 VM 运行 `bash /opt/ha/deploy/pve/setup-semisync.sh` 安装并启用 source/replica 半同步插件；初始化阶段不加载插件，避免 mysqld 初始化系统表时读取尚未安装的插件变量。运行时检查 `Rpl_semi_sync_source_status`、`Rpl_semi_sync_replica_status` 和复制线程状态。备库通过 `SHOW REPLICA STATUS` 确认 IO/SQL 线程运行且延迟为零。

### 2026-09-24 实测记录

| VMID | 名称 | 地址 | vCPU | 内存 | 磁盘 |
| --- | --- | --- | ---: | ---: | ---: |
| 238242 | ha-db-1 | 10.56.238.242 | 4 | 8 GiB | 40 GiB |
| 238243 | ha-db-2 | 10.56.238.243 | 4 | 8 GiB | 40 GiB |
| 238244 | ha-db-3 | 10.56.238.244 | 4 | 8 GiB | 40 GiB |

系统为 Ubuntu 22.04.5，Docker 29.1.3、Compose 2.40.3、MySQL 8.0.39、Etcd 3.5.15。软件包和镜像经本机 Clash `127.0.0.1:7897` 的 SSH 反向隧道下载；隧道结束后已移除 Docker 临时代理配置。

- Etcd 三端点健康，三台 Controller 完成选主；停止 Leader Controller 后，另一 Controller 接任并继续报告 epoch 1。对非 Leader 调用切换 API 返回 HTTP 409。
- 初始主库 `ha-db-1` 手工切到 `ha-db-2` 成功，epoch 从 0 增至 1；全部三台保留 3 行测试数据，当前只有 `ha-db-2` 可写，两台副本只读且延迟为 0。
- 半同步检查：source 状态 ON、2 个副本客户端、确认过的事务计数大于 0；备库复制 IO/SQL 线程正常。
- 停止当前主库 MySQL 容器后，收到节点异常告警和一次自动切换 dry-run 事件，没有提升副本；重新启动后收到恢复告警。告警接收器仅用于本次验证，事件暂存于内存。
- 自动倒换已在测试结束时关闭。当前 Etcd Leader 为 `ha-db-2`；当前主库也是 `ha-db-2`。
- VM SSH 仅接受公钥认证；Controller 管理 API 绑定回环地址，管理变更会拒绝非 Etcd Leader 请求。

实测命令：

```bash
docker compose --env-file /etc/ha/node.env -f /opt/ha/deploy/pve/docker-compose.yml up -d
SOURCE_IP=10.56.238.242 bash /opt/ha/deploy/pve/bootstrap-replica.sh
bash /opt/ha/deploy/pve/setup-semisync.sh
systemctl enable --now ha-controller
```

PVE 只有一台物理宿主机，本轮只验证 VM 和服务故障；fencing 与业务流量路由仍是 noop/预留接口，因此不代表生产级自动故障转移或固定业务入口已验证。
