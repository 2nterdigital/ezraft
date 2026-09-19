# 底座（ezraft fork）维护说明

本仓库是 [原版 ezraft](https://github.com/drmingdrmer/ezraft) 的完全分叉，作为
[ech0-media](https://github.com/2nterdigital/ech0-media) 的**底座**：在原版之上扩展复制 KV、任意节点读写、
变更订阅、节点间 RPC、集群观测与生产级存储。规划与决策见
[底座扩展 spec：第一个里程碑](https://github.com/2nterdigital/ech0-media/issues/21)；术语以 ech0-media 的
`CONTEXT.md` 为准（底座、上游、原版 ezraft、节点、内部端口、集群 token、revision、变更……）。

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
  传入，非进程全局。feature 默认关闭，普通构建不含任何钩子代码；自身 dev-dependency 让 `cargo test` 无需
  额外参数即可启用。新增 `tests/common/faults.rs` 与验收测试 `tests/partition.rs`。
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
