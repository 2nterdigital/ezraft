# 底座（ezraft fork）维护说明

本仓库是 [原版 ezraft](https://github.com/drmingdrmer/ezraft) 的完全分叉，作为
[ech0-media](https://github.com/2nterdigital/ech0-media) 的**底座**：在原版之上扩展复制 KV、任意节点读写、
变更订阅、节点间 RPC、集群观测与生产级存储。规划与决策见
[底座扩展 spec：第一个里程碑](https://github.com/2nterdigital/ech0-media/issues/21)；术语以 ech0-media 仓库的
[`CONTEXT.md`](https://github.com/2nterdigital/ech0-media/blob/main/CONTEXT.md) 为准（底座、上游、原版 ezraft、节点、内部端口、集群 token、revision、变更……）。

## 基线

| 项 | 值 |
| --- | --- |
| 原版仓库 | `drmingdrmer/ezraft` |
| 分叉点 | `048f33271ea879636eb8787f4a31acb88288b87a`（`048f332`，原版 v0.2.2 之后） |
| openraft | `=0.10.0-alpha.33`（精确锁定，见下文「openraft 升级流程」） |
| Rust 工具链 | `rust-toolchain` 锁定的 nightly（`rustfmt.toml` 用到 unstable 选项） |
| crate 名 | 保留 `ezraft` 与 `Ez*` 前缀；`publish = false`，不发布到 crates.io |

分支约定：

- `main`：保持原版原样，只用来对照与同步，**不在上面提交**。
- `dev`：底座主干，GitHub 默认分支。每张实现票从 `dev` 拉出分支、完成后合回 `dev`。

## 改动清单

相对原版的每一项改动都在这里登记，按合入 `dev` 的顺序追加。格式：

```text
- [#<票号> <票名>](<票链接>)：新增/修改了什么；删除了原版的什么（如有）。
```

从原版 cherry-pick 的修复同样登记，并注明原版 commit。openraft 升级另记在「openraft 升级记录」。

- [#22 基线：FORK.md、不可发布、默认分支设为 dev](https://github.com/2nterdigital/ech0-media/issues/22)：
  新增本文件；`Cargo.toml` 设 `publish = false`；CI 改为在 `dev` 上触发，`package` 作业与
  `make publish_check` 由"验证可发布"改为"验证发布被拒"；GitHub 默认分支设为 `dev`。顺带修复一个基线
  flake：alpha.33 的写入要求 leader 持有 quorum 确认过的租约，换届或投票集变更后的瞬间会以不带 leader 的
  `ForwardToLeader` 拒绝写入，测试工具的 `http_write` 现在会等过这一瞬间（真正的修复是 #30 的同请求 id 重试）。
- [#23 网络故障注入：仅测试构建的丢包、延迟、分区钩子](https://github.com/2nterdigital/ech0-media/issues/23)：
  新增 `src/transport.rs`——节点对外发请求的唯一出口 `Transport`（知道本节点 id 与目标节点 id；区分
  "不可达/超时"与其他错误），`src/network.rs` 改为在它之上实现 openraft 网络 trait，错误映射不变。新增仅在
  cargo feature `fault-injection` 下存在的 `ezraft::fault::{Faults, Fault}`：按有向节点对 `Drop`、
  `Delay`、`DropResponse`，以及 `partition`/`isolate`/`heal`；句柄按测试集群独立、经 `EzConfig::faults`
  传入，非进程全局。feature 默认关闭，作为依赖被构建时不含任何钩子代码；自身 dev-dependency 让 `cargo test` 无需
  额外参数即可启用——代价是同一仓库里的示例目标（含 #35 的 kvstore 与 #36 基准所测的二进制）也带着这个
  feature 构建：钩子在场但无规则时只是每次出站请求多一次 `Option` 判断。新增 `tests/common/faults.rs` 与验收测试 `tests/partition.rs`。
- [#24 RedbStorage：生产存储替换样例文件存储](https://github.com/2nterdigital/ech0-media/issues/24)：
  新增生产存储 `RedbStorage`（redb 4.3.0）：每节点一个 redb 文件、三张表（log / meta / snapshot），每次
  `persist` 是一个 `Durability::Immediate` 写事务，契约不变（一批 N 条同步 N+1 次、append 同步通知完成）；
  日志截断与压缩用范围删除，快照的 meta 与 data 在同一事务内替换，所有 redb 调用跑在 blocking 线程池。
  `RedbStorage::open` 对"数据库已被占用"做 2 秒有界重试，覆盖同进程内重启时上一实例句柄异步释放的间隙。
  新增 `src/storage/adapter/conformance.rs`：openraft alpha.33 自带的日志存储测试套件经适配层跑在
  `RedbStorage` 上；它暴露并修复了适配层两处既有缺陷（未经 purge、起点高于 0 的日志读取失败；
  `truncate_after` 在截断点超出日志末尾时把 `last_log_id` 推过了实际存储位置）。进程内集群测试与示例全部改用
  `RedbStorage`（临时目录）。**删除**原版样例文件存储 `FileStorage`（`src/storage/file_storage.rs`）及其
  专属测试，以及只有它用到的 tokio `fs`/`io-util` feature。
- [#27 EzKv 核心：条件事务、revision 与本地视图（leader 上写入）](https://github.com/2nterdigital/ech0-media/issues/27)：
  分两步合入。**(a)** 新增纯状态机库 `src/kv/`：`Txn { conditions, ops }` 全有或全无（条件针对事务前状态
  按序求值，首个失败者连同该 key 当前 `mod_rev` 作为 `Conflict` 返回）；KV 自有的 `revision`，条件成立的事务
  一律加一；每个 key 带 `create_rev`/`mod_rev`；`apply` 同时产出按 op 顺序排列、删除项带旧值的变更组；状态存于
  `imbl::OrdMap`，`View`（`revision`/`get`/`get_many`/`prefix`/`page`）是 O(1) 的不可变快照；大小上限由
  `Txn::validate` 检查、**绝不在 `apply` 内检查**（各节点配置不同也不会使副本分叉）；value 为 `Bytes`，
  在 JSON 中编码为 base64、在二进制格式中为原始字节。**(b)** 节点唯一的状态机换成该 KV：crate 去掉
  `T: EzApp` 类型参数（类型配置、`EzEntry`、`Persist`、`EzStorage`、`RedbStorage`、适配层、网络、服务端），
  Raft 日志请求为 crate 内部枚举 `Command`，回执只带 revision；公开句柄由 `EzRaft<T>` 改为 **`EzKv`**，
  新增 `write(txn)`（先校验上限、只在本节点提议；返回 revision / `Conflict` / `NotLeader`；本节点是 leader
  但租约未生效时在约一个选举超时内原地重试）与无锁的 `view()`；适配层持有唯一的 `Publisher`，
  **先发布视图与变更组、再回复写入方**，安装快照后同样发布；`EzConfig` 新增 `kv_limits`；`shutdown()`
  现在同时停止 HTTP 服务并等监听关闭，同进程内重启的节点可在原地址重新上线。快照暂为 JSON 导出，由 #28 替换。
  **删除**（类型不复存在，故先于原定票删除）：`EzApp` 与 `src/app.rs`；`POST /api/write`、`POST /api/read`
  与 `forward_write`（原定 #30）；`timestamp_oracle` 示例及其测试（原定 #34）；测试工具里的样例应用。
  kvstore 示例最小化迁移到 `EzKv`（完整改写见 #35）。新增验收测试 `tests/kv_txn.rs`、`tests/kv_view.rs`。
- [#26 传输补丁：原始二进制快照、可配上限与超时、独立连接池](https://github.com/2nterdigital/ech0-media/issues/26)：
  `POST /raft/snapshot` 改为 `application/octet-stream` 原始快照体，前置 4 字节长度 + 很小的 JSON 头
  （vote、`SnapshotMeta`），不再把快照当作 JSON 数字数组传输；帧格式错误返回带原因的 `400`。`EzConfig` 新增
  `json_body_limit`（8 MiB）、`snapshot_max_bytes`（64 MiB）、`snapshot_timeout`（30 s，映射到 openraft
  `install_snapshot_timeout`——此前整份快照 POST 只有 150 ms 预算）；超限在两端都是明确错误（发送端拒发并
  记日志，接收端 `413`）。openraft 的 `max_payload_entries` 改由 body 上限与 `kv_limits.max_txn_bytes` 推导
  （默认值下为 4），保证单次 `/raft/append` 不会超过上限；body 上限装不下一个最大事务时启动即报错，而不是日后
  卡死复制。以上默认值均待 #36 实测。`Transport` 新增 `TrafficClass`（Heartbeat / Replication / Snapshot），
  经 openraft 的 `new_heartbeat_client` / `new_snapshot_client` 钩子各用独立 `reqwest` 连接池，心跳与
  ReadIndex 探测不会排在大快照或大批量 append 之后。测试钩子新增按流量类别的规则（`set_class` /
  `clear_class`），验收测试据此在快照被延迟数个选举超时期间断言各节点 term 不变。快照构建、发送、安装有结构化
  `tracing` 日志，`tracing` 转为正式依赖。不启用 Pre-Vote（有意为之）。新增 `tests/large_snapshot.rs`、
  `tests/snapshot_limits.rs`、`tests/snapshot_fault.rs`。
- [#29 节点间 RPC 与集群 token 鉴权](https://github.com/2nterdigital/ech0-media/issues/29)：
  内部端口的全部路由（`/raft/*`、`/api/*`、RPC 前缀，以及经 `serve_with` 挂载的路由）要求
  `Authorization: Bearer <集群 token>`：新增 `src/auth.rs`，一个中间件包住整个 `App`，用 `subtle` 做常数时间
  比较，凭证缺失或错误一律 `401` 且不进入任何 handler，凭证从不写日志。`EzConfig::cluster_token` 必填
  （为空则节点拒绝启动）；`Transport` 与 `AdminClient` 发出的每个请求都带 token，对端返回 `401` 时每个
  对端只告警一次。新增 `src/rpc.rs` + `src/rpc/`：`kv.rpc()` 提供
  `register::<Req, Resp>(name, handler)` 与 `call::<Req, Resp>(node_id, name, req, timeout)`——JSON、
  内部端口固定前缀 `POST /rpc/<name>`、按节点 id 经当前成员配置寻址、公开接口不含 actix 类型；错误为
  `Timeout` / `Unreachable` / `NotMember` / `Unauthorized` / `Remote(..)`；**不自动重试**，调用方 drop 只取消
  本端；重名注册报错；名称前缀 `ez.` 保留给底座自身（公开 `register` 拒绝，crate 内部入口接受）；调用自身节点
  走本地直派。RPC 使用独立连接池（`TrafficClass::Rpc`，与 #26 的三类 Raft 流量并列）。成员配置的节点类型由
  openraft 的 `BasicNode` 换成底座自己的 `Node { internal_addr }`。`TransportError` 区分 `Timeout` 与
  `Unreachable` 并新增 `Unauthorized`（对 openraft 的映射不变）；测试钩子里超过预算的延迟现在报告为
  `Timeout`。新增依赖 `subtle`。新增 `tests/rpc.rs`、`tests/auth.rs`。
- [#25 存储崩溃注入与断电恢复测试](https://github.com/2nterdigital/ech0-media/issues/25)：
  仅新增测试代码。`tests/common/crash.rs`：`CrashStorage` 包装真实 `RedbStorage`，在指定的第 N 次 `persist`
  **之前**（该次调用不落盘、不留痕迹）或**之后**（已落盘，但仍向调用方报错）"断电"，此后该实例的每次写入与
  读取都失败，测试再从同一 redb 文件重启节点；同时记录每次 persist 的种类与 Meta 所携带的 vote，使断电点能
  精确落在"记录投票的那次 persist"上而不是猜下标。`tests/crash_recovery.rs`：三节点集群上对一段固定的已确认
  写入序列扫描每个断电点（follower 作受害节点全量扫描 12 个点；leader 作受害节点用较短序列扫描 8 个点，
  因其崩溃还会触发换届），每次重启后断言已确认写入全部存在、三副本的值与 `create_rev`/`mod_rev` 完全一致、
  集群继续服务写入；断电点落在 vote 持久化前后时，磁盘上的 vote 与断电时机精确对应，且任一 term 至多一个
  leader。断电点按 persist 计数选取，无时序依赖。未在 `RedbStorage` 或适配层发现缺陷。
- [#31 任意节点线性一致读（ReadIndex，follower 侧合并）](https://github.com/2nterdigital/ech0-media/issues/31)：
  新增 `EzKv::linearizable_view(timeout) -> Result<View, ReadError>`（`src/read.rs` + `src/read/coalesce.rs`），
  只支持 ReadIndex。leader 上取 read linearizer 并等本地 apply；follower / learner 经内部 RPC `ez.read_index`
  向 leader 取读位置（leader 端确认 leadership，**不等待自身 apply**），再在本地等 applied 追上后返回本节点视图。
  返回的视图不会落后于读位置：状态机在 `apply` 内部先发布视图，openraft 之后才得知该批已应用。follower 侧合并
  由每节点一个经 channel 驱动的任务完成（无锁）：同一时刻最多一轮 RPC 在途，轮次进行中到达的请求只由**下一轮**
  服务，N 个并发读最多两轮；放弃等待的请求不影响其他请求。错误：`NoLeader`（超时内无 leader、leader 不可达或
  无法确认多数派）、`NotMember`、`NotCaughtUp { read_index, applied_index }`。`NotMember` 由 leader 按自己的
  成员配置权威回答——被移出集群的节点可能永远收不到移除它的那条日志，其本地成员视图不可信。测试钩子新增按有向
  节点对、按路径或流量类别的请求计数（`request_count` / `request_count_by_class`），验收测试据此统计 RPC 轮数，
  非生产指标。**删除**原版遗留的 leader-only `linearizable(ReadPolicy)`，`ReadPolicy` 不再从 crate 根重导出。
  新增 `tests/linearizable_read.rs`。
- [#28 快照：不阻塞构建、postcard 编码与格式版本号](https://github.com/2nterdigital/ech0-media/issues/28)：
  新增 `src/kv/snapshot_codec.rs`。快照 = 6 字节头（magic `EZKV` + 小端 `u16` 格式版本，当前为 **1**，手工
  解析，先于任何 postcard 解码校验）+ postcard body（`revision`、每个 key 的 value 字节与
  `create_rev`/`mod_rev`、底座内部状态块 `InternalState`）。未知版本报
  `UnknownVersion { found, supported }`；过短、损坏或带多余尾部字节的输入都是错误，从不 panic、从不部分应用。
  `InternalState` 是独立结构体（当前为空），#30 幂等账本与 #33 注册表在其内部加字段，**本里程碑内版本号保持 1**。
  构建快照时只在状态机锁内克隆持久化 map 的根、内部状态块、`last_applied` 与成员配置（均 O(1)），编码在锁外的
  blocking 线程池进行，`apply` 与 `view()` 不被阻塞（对应测试在"持锁编码"时必然失败）。安装时先解码校验、再落盘与
  替换；未知版本在启动时使 `create`/`join` 失败、在运行时使安装失败（对节点是致命错误），均以 `tracing::error!`
  记录。`EzConfig` 新增 `snapshot_log_to_keep`（对应 openraft `max_in_snapshot_log_to_keep`）与
  `replication_lag_threshold`，均为 `Option`：不设则随 `snapshot_interval` 推导（`/5` 与等于 interval），只改
  interval 不会得到不一致的组合；显式设置的阈值小于 interval 时启动即报错。`postcard` 转为正式依赖。
  **删除** #27 留下的过渡 JSON 快照格式 `KvExport`。
- [#32 watch：分组变更、Reset、Progress 与视图衔接](https://github.com/2nterdigital/ech0-media/issues/32)：
  新增 `src/watch.rs` + `src/watch/`：`EzKv::watch(prefix, Now | After(rev))` 与原子的
  `EzKv::view_and_watch(prefix)`，返回 `futures::Stream` 形式的 `WatchStream`，按 revision 严格递增产出
  `WatchUpdate::Changes`（一个写事务过滤到前缀后的一组变更，按 op 顺序，删除项带旧值）、
  `Reset { view, cause }`、`Progress { revision }`。watch hub 位于唯一的 `Publisher` 内部：**追加到保留窗口、
  投递给各订阅者、替换视图在同一个临界区完成，注册订阅也取同一把锁**，因此"视图 + 其后的一切"无缺口、无重复；
  发布路径不 `.await`、不等待任何消费者。保留窗口是 `imbl::Vector`（组数与字节双上限，不进快照、不复制），
  新订阅者在锁内 O(1) 复制、锁外过滤；每个成功事务都有一组（可为空），窗口覆盖连续的 revision 区间；重启后窗口
  为空，下限为启动时状态的 revision。**不发 Reset 即无缺口**：续订位置早于窗口下限（`ResumeTooOld`）、订阅者
  有界队列溢出（`SubscriberTooSlow`，积压被一条携带最新视图的 Reset 取代）、本节点安装快照
  （`SnapshotInstalled`）时先发 Reset；`After(rev)` 指向尚未到达的 revision 时等待。`Progress` 与 `Changes`
  走同一有序队列、同一把锁，按订阅者限速，保证不会早于 revision 不大于它的 `Changes`，且 revision 停止推进后
  仍会送达。节点 `shutdown()` 后流在交付已有内容后结束（`None`），不会永久挂起。`EzConfig` 新增
  `watch_limits`（默认 10000 组 / 64 MiB / 队列 1024 / 1 秒，待 #36 实测）。每次 Reset 有结构化 `tracing`
  日志。新增 `tests/watch.rs`、`watch_reset.rs`、`watch_progress.rs`、`watch_resume.rs`、`watch_restart.rs`。
- [#30 任意节点写入：转发、同请求 id 重试与幂等](https://github.com/2nterdigital/ech0-media/issues/30)：
  `write(txn)` 在任意节点可用：发起节点先校验上限、分配请求 id（新增公开类型 `RequestId`，ULID），是 leader 则
  本地提议，否则经内部 RPC `ez.write`（`src/write/forward.rs`）转发给 leader；遇到 `ForwardToLeader`
  （带或不带 leader）、远端非 leader、连接失败、RPC 超时、响应丢失、暂无 leader，都用**同一请求 id** 重试
  （等换主或短退避，不空转），直到 `EzConfig::write_deadline`（10 s）；仍无定论返回
  `Indeterminate { request_id }`，`write_with_id(request_id, txn)` 在账本窗口内返回第一次的真实结果。
  Raft 日志命令改为 `Command { request_id, ledger_capacity, body }`，后续内部命令（#33 注册表）复用同一套
  转发、重试与幂等机制。幂等账本 `Ledger`（`src/kv/ledger.rs`）记录"请求 id → 第一次的真实结果（成功或
  冲突）"，存于 `InternalState` 并进快照（v1 布局原地扩展）；`apply` 执行前先查账本，命中则原样返回、不推进
  revision、不产生变更组；按条数先进先出淘汰，**阈值取自提议的 leader 戳进命令里的 `ledger_capacity`
  （默认 100000），绝不读应用节点自己的配置——淘汰是日志的纯函数**。成功后 `write` 等本节点已发布 revision
  追上才返回（由发布事件唤醒，不轮询），否则返回 `LocalLag { revision }`；冲突返回前尽力等本地视图追到
  `current_mod_rev`；新增 `wait_revision(rev, timeout)`。leader 本地提议同样受总时限约束（openraft 的
  `client_write` 自身不超时，被隔离而租约尚在的 leader 否则会无限等待）。`WriteError::NotLeader` 从公开写入
  API 移除；`LimitError` 增加 serde 派生以便经 RPC 回传。重试、`Indeterminate`、`LocalLag` 有结构化
  `tracing` 日志。新增依赖 `ulid`，`imbl` 启用 `serde`。`/api/write`、`/api/read` 已于 #27 删除（确认）。
  `tests/crash_recovery.rs` 改为经从不崩溃的那个 voter 写入（依赖同请求 id 重试跨越 leader 崩溃）。新增
  `tests/write_forward.rs`、`tests/idempotency.rs`。
- [#35 真实进程示例与 SIGKILL 测试](https://github.com/2nterdigital/ech0-media/issues/35)：
  kvstore 示例完全改写到 `EzKv` 公开 API 上。底座跑在内部端口（`--addr`、`--token` 或环境变量
  `EZRAFT_CLUSTER_TOKEN`、`--seed`、`--learner`、`--data-dir`）；示例在**独立的对外端口**（`--api-addr`）
  提供自己的 HTTP API（`examples/kvstore/api.rs`），客户端只与它通信：`POST /tx`（事务，可带 `request_id`）、
  `GET /view`、`GET /linearizable`、`GET /status`（读 `cluster()`、`last_leader_contact()` 与节点注册表，
  列出每个节点的对外地址）、`GET /watch`（按前缀的换行分隔 JSON 流，可带续订 revision）。底座的带类型错误各自
  映射为不同的状态码与响应体（`Conflict`→409、上限→422、`Indeterminate`→504、`LocalLag`→503、`NoLeader`→503、
  `NotMember`→410、`NotCaughtUp`→504）。示例把 `--api-addr` 登记为 `EzConfig::external_addr`。扩展子进程测试
  工具 `examples/harness.rs`，新增测试用客户端 `examples/api_client.rs`。真实进程测试只驱动对外 API：三节点
  冒烟（含"每个节点都列出三个对外地址"）；**写入过程中 SIGKILL leader**——已确认写入在幸存节点上全部保留、选出
  新 leader 后继续服务，被杀瞬间在途的写入用自己的请求 id 重试、恰好生效一次；**SIGKILL follower 并在同一数据
  目录重启**——追上集群，watch 客户端带上次 revision 重连得到 Reset + 增量或无缺口增量，折叠结果等于最终状态；
  测试无论通过、失败或 panic，子进程都被清理。该示例同时是 #36 基准测试的被测二进制。
- [#33 集群观测与节点注册表](https://github.com/2nterdigital/ech0-media/issues/33)：
  新增 `src/cluster.rs`：`EzKv::cluster()` 返回 `watch::Receiver<ClusterState>`（本节点 id、角色、term、当前
  leader、voters、learners、是否仍是成员），由每节点一个任务从 openraft `server_metrics` 派生，**只在这些字段
  变化时推送**；`EzKv::last_leader_contact() -> Option<Duration>`（leader 上为 0，从未收到为 `None`）——
  alpha.33 没有 follower 侧的对应指标，由底座在 `/raft/append` 路由里、请求被承认来自合法 leader 时记录。新增
  节点注册表：`src/kv/registry.rs`（复制部分：`NodeInfo { external_addr, meta, status }`、
  `NodeStatus::{Up, Down}`，存于 `InternalState`，不占上游 key 空间，随 v1 快照布局原地扩展）与
  `src/registry.rs`（句柄侧：`registry()` 本地读、`watch_registry()` 整表订阅、`update_node_meta()`）。
  节点启动时后台任务用 `EzConfig::external_addr` / `node_meta` 登记自身条目，重试到成功为止；`remove_node`
  在成员变更提交后于同一流程删除条目（leader 在两步之间崩溃会留下陈旧条目，由 #34 的内置任务回收）。
  **注册表变更是 Raft 日志里的内部命令**：`CommandBody` 新增 `RegisterNode` / `RemoveNode`，`CommandOutcome`
  改为枚举，同一幂等账本记录两类结果；#30 的任意节点写入路径（本地提议、`ez.write` 转发、同请求 id 重试）从
  "事务"泛化为"命令体"并原样复用，没有长出第二条路径；内部命令不推进 revision、不产生变更组。`status` 本票只存储
  与暴露。合并后将成员管理操作从 `src/raft.rs`（已达 811 行）拆到 `src/membership.rs`。新增
  `tests/cluster_state.rs`、`tests/registry.rs`。
- [#34 leader 专属任务与基于 Raft 心跳的存活判定](https://github.com/2nterdigital/ech0-media/issues/34)：
  新增 `src/leader_task.rs`：`EzKv::spawn_leader_task(name, f)` 为节点生命周期注册任务；每节点一个监督者观察
  `cluster()`，本节点在 term T 成为 leader 时为每个已注册任务运行 `f(LeaderCtx { term: T, .. })`，失去
  leadership 或 term 变化时 abort 并**等它们全部结束后**才允许新一轮开始；已在 leading 时注册的任务立即启动；
  任务返回或 panic 会被记录，同一 term 内不重启。**`ctx.write` 从不转发，且围栏是精确的**：提议前核对角色与
  term（省去一条不可能生效的日志），但 leadership 可能在核对与追加之间变化——因此命令同时被盖上它所属的 term
  （`Command::required_term`），`apply` 拒绝在任何其他 term 追加的条目（结果 `CommandOutcome::StaleTerm`）；
  这只依据日志本身判定，故各副本结论一致。经由过期 context 发出的写入不可能生效，哪怕同一节点先重新赢得了
  leadership。租约未生效时在同一 term 内等待重试。仍在 leading 却超时返回 `LeaderWriteError::Indeterminate`
  而非 `NotLeader`：context 很可能仍然有效，而以此结束的任务要到下个 term 才会再运行。新增内置存活判定任务
  `src/liveness.rs`（经同一机制运行）：leader 读取每个成员最近一次被确认的心跳，注册过的节点静默超过
  `EzConfig::liveness_threshold` **且**本次 leadership 已超过 `liveness_grace`（校验 grace ≥ threshold；
  新 leader 的心跳记录初始为空，从未确认过的节点视为自 leadership 开始起静默）时置 `Down`，恢复确认后置 `Up`，
  leader 自身恒为 `Up`；**只在状态变化时写入**，经 leader 专属内部命令 `CommandBody::SetStatus`，每次变化有
  结构化 `tracing` 日志。同一任务用 `CommandBody::ReconcileRegistry` 清理已不在成员配置中的注册表条目
  （`remove_node` 两步之间 leader 崩溃留下的窗口），`apply` 依据状态机自己已应用的成员配置判定，与节点配置
  无关。`EzConfig` 新增 `liveness_threshold` / `liveness_grace` / `liveness_check_interval`（5 s / 5 s /
  1 s，待 #36 实测）。对转发命令收到形状不符的应答时记录错误并按"无应答"处理，不再 panic（该应答来自对端）。
  原版 `timestamp_oracle` 示例已于 #27 删除（确认）。新增 `tests/leader_task.rs`、`tests/liveness.rs`。
- [#37 底座使用指南](https://github.com/2nterdigital/ech0-media/issues/37)：
  用新 API 重写 `docs/guide.md`（中文），**替换**原版英文指南，并同步重写 `README.md`。覆盖全部公开 API
  及其错误语义：启动集群（create / join / join_as_learner、内部端口与集群 token、`RedbStorage`、
  `EzConfig` 每一项）、条件事务与 revision（冲突后重读重试、上限）、任意节点写入（转发、`Indeterminate`
  与 `write_with_id`、`LocalLag` 与 `wait_revision`）、本地视图与线性读、watch（三种 Reset 原因、
  Progress、按 `mod_rev` 去重、游标续订）、节点间 RPC、集群观测与节点注册表、leader 专属任务与存活判定
  （围栏为何精确、`LeaderWriteError` 每个变体该怎么办）、成员变更与管理协议、存储契约与快照、可观测性，
  以及一张错误参考表。末尾是**给上游的使用约定**一节：热路径读本地视图并以 CAS 兜底、RoomService 读与
  否定判断用线性读、索引在事务里自写、webhook 用 watch + leader 专属任务 + KV 游标自建、软状态由上游
  内存缓存层经 RPC 同步、媒体面致命错误时进程退出。**指南里的每个 Rust 代码块都是 doctest**：`src/lib.rs`
  以 `#[cfg(doctest)]` 模块 `include_str!` 整份指南，`cargo test`（或 `cargo test --doc`）会编译它们，
  API 变了而指南没跟上会直接让测试失败；该模块只在 rustdoc 收集 doctest 时存在，不进入任何构建产物。
- 里程碑收尾评审（对照 [spec](https://github.com/2nterdigital/ech0-media/issues/21) 与各票验收条件、对照编码规范各做一遍）
  发现并修复的问题，按所属票登记。**正确性**：(#28) openraft 把快照构建放在独立任务里、与安装并发；构建在锁外
  编码期间若 leader 发来的更新的快照先装好，构建完成后会把它覆盖成旧的，而日志已清理到新快照的位置——重启后
  中间那段日志被静默跳过。构建拆成「捕获来源 / 存储结果」两步，存储时保留较新的一份（单元测试按该交错逐步
  重放，去掉保护即失败）。(#29/#26) RPC 路由用 `web::Bytes` 取请求体，受 `PayloadConfig` 而非 `JsonConfig`
  约束，实际上限是 actix 默认的 256 KiB：`KvLimits` 允许的大事务在 follower 上写入时每次转发都被 413 拒绝，
  最终以 `Indeterminate` 报告一笔从未发生的写入；现在该上限等于 `json_body_limit`，`tests/write_forward.rs`
  新增「follower 转发一个达到上限的事务」。(#26) alpha.33 的快照发送端只对 `Unreachable` 退避，对 `Network` /
  `Timeout` 会清掉退避立刻重发整份快照；发送端超限拒发、对端 413/401/500、未知快照版本此前都映射成 `Network`，
  形成零延迟重试循环——现在送不出去的 Raft 请求一律报告为 `Unreachable`。(#29) `RpcError::Remote` 此前还
  装着全部 `TransportError::Other`（连接中途断开、非成功状态码、响应解不出来），非幂等 handler 的调用方无法区分
  「handler 拒绝了」与「结果未知」；新增 `RpcError::Transport { node_id, name, message }` 承载后者，`Remote`
  只表示对端 RPC 层的确定答复。(#27/#33) 事务的**条件数**此前没有上限（字节上限只数 key），数十万个单字节 key 的
  条件能通过校验并编码成数 MiB；现在与操作数共用 `max_txn_ops`、各自计数（`LimitError::TooManyConditions`）。
  节点注册表条目（`external_addr` + `meta`）此前完全没有上限，leader 本地提议一个超大 `meta` 会永久卡死复制；
  现在按一个 value 计（`max_value_bytes`，`LimitError::NodeEntryTooLarge`），在提议入口、`update_node_meta`
  发起节点和启动配置三处检查；`RegistryIndeterminate` 改为枚举 `RegistryWriteError { LimitExceeded,
  Indeterminate }`。(#34) 存活判定原先只有两种结论，「没有证据」被当成 `Up`：每次换届，新 leader 的第一次
  检查就把每个已判 `Down` 的节点改回 `Up`、保持整个宽限期、再改回 `Down`——每个死节点每次故障转移两次多余写入
  和一次假恢复。`judge` 现在有第三种结论（无证据，保持原状），`Up` 必须来自本次 leadership 真实收到的确认；
  从未确认过的节点改为从「本次 leadership 第一次在注册表里看到它」起算静默，加入一个已运行很久的 leadership 的
  新节点不再被误判 `Down`；状态变化日志的 `silence_ms` 现在是静默时长而不是 leadership 年龄。顺带修复：
  节点 `shutdown` 之后 `wait_before_next_attempt` 立即返回，使所有重试循环（写入的整个期限内）变成忙等；
  `shutdown` 先中止自注册任务。(#31) 被移出集群的节点只能从它认定的 leader 那里得知 `NotMember`，而它再也收不到
  任何复制、认定永远不更新：那个 leader 一旦不在，读就永远是 `NoLeader`。现在一轮 ReadIndex 在认定的 leader
  问不到（或根本没有认定）时改问它记得的其他成员，并沿 `NotLeader` 的 leader 提示再问一跳；`NotMember` 只由
  刚确认过自己是 leader 的节点回答（follower 的成员视图可能还没追上「把调用方加进来」的变更）；读写应答里从未
  使用的 `leader_addr` 已删除。leader 侧读的确认现在受调用方超时约束，写入/注册表重试循环按截止时刻而不是
  过期的剩余预算等待。(#33) 已被移除但仍在运行的节点能通过转发的 `RegisterNode` 把自己的条目写回去，直到
  下一次对账；现在应用注册命令时按状态机自己已应用的成员配置判定，非成员不产生条目。集群 token 不再出现在
  `EzConfig` / `AdminClient` 的 `Debug` 输出里。**文档与测试**：(#37) 指南里按 key 的 `mod_rev` 去重的示例
  用 `prev.mod_rev` 判断删除，会把每一次删除都当成重复丢掉、也没有墓碑，改为按组的 revision 去重并说明原因；
  `write_until_definite` 不再对 `LimitExceeded` 走 `unreachable!`；线性读的合并只发生在 follower / learner 上
  （指南原写「同一个节点上」）；补充 `node_id()`、`addr()`、`trigger_snapshot()`、`purge_log()`。(#35)
  kill-leader 测试原先只断言已确认写入存活，现在断言两个幸存节点的最终 revision 等于写入次数（恰好一次）。
  (#25) 崩溃试验在三个节点的自注册全部落地之后才取 persist 基线。两处用 sleep 当同步的测试改为条件等待。
  **结构**：命令的语义（term 围栏、幂等账本、每种命令体做什么）从存储适配层的五个分支收拢到
  `Command::apply`，只读命令、条目的 term、已复制状态与已应用成员配置，适配层只负责喂条目和发布变化；
  `src/write.rs` 按模块规则拆出 `write/error.rs` 与 `write/propose.rs`，根文件只留接口与组合；`Transport`
  的 JSON 与原始体发送合并为一条路径（此前快照发送不计入测试钩子的请求计数），响应类型去掉无用的
  `Serialize` 约束；watch hub 在构造时拿到上限，删除事后 `configure` / `reconfigure`；`EzKv` 持有
  `Arc<EzConfig>` 而不是逐字段拷贝，`Clone` 改为 derive；`kv`、`watch`、`server` 改为私有模块（公开项本来就
  都从 crate 根重导出）；RPC 处理器表改名 `rpc/handlers.rs` / `Handlers`，不再与节点注册表重名。

## 验证命令

合入 `dev` 之前全部通过（与 CI 一致）：

```bash
cargo fmt --all -- --check
cargo clippy --no-deps --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --all --no-deps
cargo test
make publish_check        # 断言 cargo publish 被拒（crate 不可发布）
```

`make lint` 会先 `cargo fmt` 再按 CI 的方式跑 clippy；`make check_all` 还需要本机装有
`cargo-machete` 与 `typos`。

## 与原版的同步规则

```bash
git remote add upstream https://github.com/drmingdrmer/ezraft.git   # 只需一次
git fetch upstream
git checkout main && git merge --ff-only upstream/main && git push origin main
```

- `main` 只做快进同步；如果快进失败，说明有人误在 `main` 上提交了，先纠正再同步。
- **不**把 `main` 整体合入 `dev`。原版的修复按需挑选：`git cherry-pick -x <原版 commit>` 到从 `dev`
  拉出的分支，跑完「验证命令」后合回，并在「改动清单」登记（注明原版 commit 与取用理由）。
- 底座核心可以随意改动；原版的样例（自带文件存储、`/api/write`、`/api/read`、示例、原使用指南）在替代品
  合入时删除，删除同样登记在改动清单。

## openraft 升级流程

openraft 处于 alpha 阶段，alpha 之间会有破坏性变更，而底座实现了它的存储、网络与类型配置 trait，所以用
`=` 锁定确切版本，并**有意识地逐次升级**：

1. 一次只升一个版本（如 alpha.33 → alpha.34），单独一个分支、单独一次合并。
2. 先审阅两版本之间所有 `change:` 与 `fix:` 提交，逐条判断是否影响底座（存储契约、网络 trait、
   线性读构件、metrics、租约与写入行为）。
3. 跑完「验证命令」里的全部检查，包含故障注入与真实进程测试。
4. 在下面的「openraft 升级记录」登记：版本、日期、审阅结论、受影响的底座代码。

### openraft 升级记录

| 日期 | 版本 | 结论 |
| --- | --- | --- |
| 2026-09-19 | `=0.10.0-alpha.33`（基线，未升级） | alpha.34 的 `fix: ignore out-of-order log IO completions` 已评估：底座的 `append` 串行执行且同步通知完成，不受该乱序问题影响，暂无升级理由（[ech0-media#11](https://github.com/2nterdigital/ech0-media/issues/11)）。 |

## ech0-media 依赖底座的方式

ech0-media 以 git 依赖使用底座，锁定 `dev` 上的**完整 40 位 SHA**（不用分支名，也不用短 SHA）：

```toml
[dependencies]
ezraft = { git = "https://github.com/2nterdigital/ezraft", rev = "<dev 上的完整 commit SHA>" }
```

两个仓库一起开发时，用 cargo 的 patch 指向本地仓库（`[patch]` 的 URL 必须与依赖声明里的 `git` URL 完全一致）：

```toml
# ech0-media 的 Cargo.toml（或 .cargo/config.toml），不要提交到共享分支
[patch."https://github.com/2nterdigital/ezraft"]
ezraft = { path = "../ezraft" }
```

升级底座 = 把 `rev` 换成新的 `dev` SHA，跑 ech0-media 的全部测试后提交。
