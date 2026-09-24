# MySQL HA Agent/Controller MVP

Go 实现的 MySQL HA 实验项目。每个数据库节点运行本机 `mysql-agent`；多个 `ha-controller` 通过三节点 Etcd 选出唯一 Leader，负责监控、告警和倒换编排。Controller 不保存 MySQL DSN 或数据库密码。

当前能力：MySQL 可用性/角色/复制线程/延迟/GTID 采集、Webhook 告警、手工倒换、Etcd 租约和 epoch、Controller Leader 选举、Prometheus 指标、自动倒换 dry-run。

## 组件与接口

- Agent HTTPS/mTLS REST：`GET /api/v1/status`、`GET /api/v1/gtid`，以及带 Etcd 切换租约/epoch 授权的 `POST /api/v1/wait-gtid`、`/demote`、`/promote`、`/replica`。
- Controller HTTP API：`GET /healthz`、`GET /api/v1/status`、`GET /api/v1/tasks`、`POST /api/v1/switchover`、`POST /api/v1/auto-failover`、`GET /metrics`。
- 仅 Etcd Leader 可执行集群变更；follower 的切换 API 返回 HTTP 409。Controller API 默认应绑定回环地址并通过 SSH 隧道访问。
- Agent 对每个角色变更向 Etcd 验证当前切换操作、租约和 epoch。Agent 失去 Etcd 访问时 fail closed。自动真实提升默认关闭。
- 当前 fencing 和业务路由管理仍是 noop；本项目没有 ProxySQL/HAProxy 固定业务入口，也不代表生产级自动故障转移。

## 本地 Compose 实验

需要 Go 1.23+、Docker Compose。先生成用于本地实验的 CA、Agent 服务端证书和 Controller 客户端证书：

```powershell
go run ./cmd/pki-gen -out work/dev-pki -agents mysql-agent-1,mysql-agent-2,mysql-agent-3 -controllers controller-1
docker compose up --build
Invoke-RestMethod http://localhost:8080/api/v1/status
```

`work/dev-pki` 被 `.gitignore` 忽略。Compose 实验凭据是演示用固定值，不能用于生产。容器通过 `user: 0:0` 读取本地 0600 PKI 私钥，仅适用于隔离的开发网络。

## PVE 部署拓扑

三台原 MySQL VM 只运行 MySQL 与 Agent；另三台控制 VM 各运行一个 Controller、一个 Etcd 成员和本机告警测试接收器。全部 VM 共用一台 PVE 物理宿主机，因此仅验证 VM/进程故障，不验证宿主机故障。

| VMID | 名称 | 地址 | 角色 | vCPU / 内存 / 磁盘 |
| ---: | --- | --- | --- | --- |
| 238242 | ha-db-1 | 10.56.238.242 | MySQL + Agent | 4 / 8 GiB / 40 GiB |
| 238243 | ha-db-2 | 10.56.238.243 | MySQL + Agent | 4 / 8 GiB / 40 GiB |
| 238244 | ha-db-3 | 10.56.238.244 | MySQL + Agent | 4 / 8 GiB / 40 GiB |
| 238245 | ha-ctrl-1 | 10.56.238.245 | Controller + Etcd | 2 / 4 GiB / 32 GiB |
| 238246 | ha-ctrl-2 | 10.56.238.246 | Controller + Etcd | 2 / 4 GiB / 32 GiB |
| 238247 | ha-ctrl-3 | 10.56.238.247 | Controller + Etcd | 2 / 4 GiB / 32 GiB |

环境为 Ubuntu 22.04.5、Docker 29.1.3、Compose 2.40.3、MySQL 8.0.39、Etcd 3.5.15。Agent 监听 `9443` 并要求 mTLS；Agent 只允许三台控制 VM 访问。Etcd 客户端口仅允许 DB Agent 与控制 VM 来源，peer 端口仅允许控制 VM 互通。Etcd 目前使用受防火墙限制的 HTTP，无 Etcd 用户认证。

Controller 管理 API 绑定 `127.0.0.1:8080`；当前 Leader 为 `ha-ctrl-3` 时可通过以下隧道访问：

```powershell
ssh -L 18080:127.0.0.1:8080 -i C:\Users\13275\.ssh\id_rsa root@10.56.238.247
Invoke-RestMethod http://127.0.0.1:18080/api/v1/status
```

实际 Leader 可在任一 Controller 的 `/api/v1/status` 中查看。follower 上的状态快照仅含 Etcd 集群状态，不保证包含完整节点监控快照；管理变更请求应发往 Leader。

## 部署文件

- `deploy/pve/docker-compose.yml`：仅 MySQL 服务，Etcd 不再与数据库节点共置。
- `deploy/pve/controller-compose.yml`：控制面 VM 的 Etcd 成员。
- `deploy/pve/ha-mysql-agent.service`、`ha-controller.service`：systemd 服务定义。
- `deploy/pve/install-agent-node.sh`、`install-controller-node.sh`、`create-control-vms.sh`：节点准备和 VM 创建脚本。
- `deploy/pve/controller.env.example`、`controller-node.env.example`、`agent.env.example`：配置样例，不含现场密码。

数据库凭据只放在各 DB VM 的 `/etc/ha/agent.env`；Agent 使用本机 `ha_agent` 和 `ha_repl` 账号。`ha_agent` 权限限定为 `SYSTEM_VARIABLES_ADMIN`、`REPLICATION_SLAVE_ADMIN`、`REPLICATION CLIENT` 和 `PROCESS`，不授予 `GRANT OPTION`。切换状态、epoch 与任务存放于 Etcd 的 `/ha/<cluster-name>/` 前缀。

## 2026-09-24 PVE 验证记录

- 从旧三节点 Etcd 迁移 `/ha/mysql-ha-validation/cluster` 与两条任务记录；原来的 Etcd 数据目录保留，DB VM 上旧 Controller 已禁用，旧 Etcd 容器停止且重启策略关闭。
- 新 Etcd 三成员全部 `endpoint health` 通过；三个 Controller 选出唯一 Leader。停止 Leader 后，`ha-ctrl-2` 接任，primary 和 epoch 保持不变。
- 三个 Agent 的 mTLS 状态采集成功；`HA_MYSQL_DSN` 使用无默认数据库的 DSN，避免最小权限账号被要求访问 `mysql` 系统库。
- 手工切换 `ha-db-2` → `ha-db-1` 成功，epoch `1` → `2`。切换后只有 `ha-db-1` 可写，`ha-db-2/3` 为只读副本，复制线程运行且延迟为 0。
- 在新主库写入 Agent/Controller 验证行 `id=4`，并在恢复后写入半同步验证行 `id=5`，三台均读到全部 5 行。半同步 source 状态 ON、2 个 replica 客户端，写入后确认事务计数为 1；两台 replica 线程运行且延迟为 0。非 Leader 切换 API 返回 409；无有效 Etcd 操作租约的 Agent 提升请求返回 403。
- 停止当前主库 MySQL，收到主库 critical 告警、两条 replica IO warning 与一次自动 dry-run 任务，候选为 `ha-db-2`，没有执行提升；重启后收到三台节点恢复通知。演练后 `auto_failover=false`。
- 停止首个 Etcd endpoint、保留其余两节点仲裁时，Controller 和 Agent 可从后续 endpoint 启动；失去 Etcd 仲裁时 Controller 切换请求返回 409，Agent 写请求在 3 秒内 fail closed，MySQL 角色和 epoch 未变。Etcd 恢复后三成员健康。
- 告警接收器事件暂存在各 Controller 本机内存，重启会清空；这不是持久化告警系统。
- PVE 控制 VM 按用户要求将 root 密码设为 `1`，并允许所有 IPv4/IPv6 来源 SSH；VM 确实获得了 `240e:...` 全局 IPv6 地址，虽然本轮未验证其是否能从公网路由到达，但弱密码 + 全来源 SSH 风险极高。DB VM SSH 仍由现有防火墙限定在 `10.56.238.0/24`。

本地验证：`go test ./...`、`go vet ./...` 与 Linux amd64 Controller/Agent/告警接收器/状态迁移工具交叉构建通过。
