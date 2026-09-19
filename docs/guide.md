# 底座使用指南

本指南面向在**底座**（这个 crate，`ezraft`，2nterdigital 对原版 `drmingdrmer/ezraft` 的分叉）之上构建
上游业务的开发者。它按**已实现**的公开 API 写成——凡是与早期设计讨论不一致的地方，以代码为准，本指南只
描述代码实际提供的行为。

底座是分布式能力的地基：复制、任意节点读写、变更订阅、节点间通信、成员与存活由它负责，业务语义（房间、
参与者、事件……）完全交给上游。原版 `ezraft` 是一个「最小 API」的 Raft 教学项目；底座在它之上内置了一个
复制 KV（`EzKv`）作为唯一的状态机，并围绕它扩展出任意节点写入、本地/线性读、watch、节点间 RPC、集群观测
和节点注册表。

**目录**

- 总览：节点、集群与 revision
- 快速开始
- 启动集群
- 条件事务与 revision
- 任意节点写入
- 本地视图与线性读
- watch：订阅复制状态的变更
- 节点间 RPC
- 集群观测与节点注册表
- leader 专属任务与存活判定
- 成员变更
- 存储与快照
- 可观测性
- 错误参考表
- 给上游的使用约定

## 总览：节点、集群与 revision

一个**集群**由若干**节点**组成，每个节点是运行这个 crate 的一个进程。节点之间维持同一份日志（log），日志
里的每一条目在**多数派**节点持久化之后就算*已提交*（committed），此后不会丢失；每个节点再按日志顺序把已提交
的条目*应用*（apply）到自己的一份状态机副本上，所有节点因此得到完全一致的状态。这是 Raft 协议的一般描述，
底座内部处理选举、复制、成员变更、快照与节点间传输，上游不需要了解这些细节。

底座只提供**一个**状态机：一个复制 KV，句柄是 `EzKv`。它是一个扁平的、按字节序排序的 key 空间（约定用 `/`
分隔成路径，例如 `room/{room}/participant/{identity}`，但底座本身只理解前缀，不理解路径结构），value 是
不透明字节（`bytes::Bytes`），上游自己选择编码。这份状态整体称为**复制状态**——经底座的 Raft 复制、每个
节点上的副本都一致。与之相对的是**软状态**：只存在于某个节点内存里、不经底座复制的状态（参见「给上游的
使用约定」）。

复制状态有一个全局的 `revision`：每提交一个写事务（不论是否真的改动了什么 key）就加一，同一事务内的所有
改动共享同一个 revision；它与 Raft 日志的 log index 无关，连续、单调递增。每个 key 额外带
`create_rev`/`mod_rev`，分别是创建它、以及最后一次修改它的事务的 revision。

底座把复制状态的一次改动称为**变更**（`Change`/`Changes`）——`watch` 投递的就是变更，它属于底座层。上游
根据变更推导出的业务通知（例如 `participant_joined`、`room_finished`）称为**事件**，属于上游层；底座不
提供事件日志，也不认识「事件」这个概念，参见「给上游的使用约定」里 webhook 一条。

节点通过两个端口对外提供服务：**内部端口**只供集群内其他节点访问，承载 Raft 流量和全部节点间 RPC，用
**集群 token** 鉴权；**对外端口**由上游自己实现和绑定，供客户端与业务调用方访问。底座完全不知道对外端口
的存在——`examples/kvstore.rs` 演示了如何在同一个进程里各跑一个。

## 快速开始

一个完整的单文件示例：创建集群、写一个 key、立即从本地读回、启动内部端口的 HTTP 服务。这与
`examples/kvstore.rs` 骨架一致，运行 `cargo run --example kvstore` 就能得到一个真实的三节点集群
（该示例额外提供了一个独立的对外端口 API，见后文）。

```rust,no_run
use std::io;

use ezraft::EzConfig;
use ezraft::EzKv;
use ezraft::RedbStorage;
use ezraft::Txn;

#[tokio::main]
async fn main() -> io::Result<()> {
    // 1. 每个节点一个 redb 文件。`RedbStorage` 是底座提供的生产级 `EzStorage` 实现。
    let storage = RedbStorage::open("./data/node1.redb").await?;

    // 2. 同一内部端口上的所有节点必须共享同一个集群 token；留空节点会拒绝启动。
    let config = EzConfig {
        cluster_token: "change-me-to-a-real-secret".to_string(),
        ..EzConfig::default()
    };

    // 3. 集群的第一个节点用 create；其余节点通过集群里任意一个已有节点 join：
    //      EzKv::join("127.0.0.1:8081", "127.0.0.1:8080", storage, config)
    let node = EzKv::create("127.0.0.1:8080", storage, config).await?;

    // 4. 写一个 key，然后从本节点自己的本地视图读回——不经过一轮共识，而且已经
    //    反映了刚才这次写入。
    node.write(Txn::new().put("hello", "world")).await.map_err(io::Error::other)?;
    println!("{:?}", node.view().get("hello"));

    // 5. 提供内部端口的 Raft RPC 与管理 API。这会一直阻塞；有其他工作要做时用
    //    `tokio::spawn` 启动它。
    node.serve().await
}
```

## 启动集群

### create / join / join_as_learner

- `EzKv::create(http_addr, storage, config)`：创建一个新集群，这个节点是唯一成员。一个集群只能有一个
  节点这样调用；分别对两个节点调用 `create` 得到的是两个永远不会合并的独立集群。
- `EzKv::join(http_addr, seed_addr, storage, config)`：通过 `seed_addr`（集群里任意一个已有节点的
  内部端口地址，不必是 leader）加入集群，并请求成为投票者（voter）。节点总是先以*学习者*
  （learner，只复制日志，不参与多数派计票）身份进入，追上进度后才会被提升为投票者；这个提升在后台发起，
  必须由 `EzKv::serve`/`serve_with` 收尾等待——**因此一个正在 join 的节点必须调用 `serve`**，提升失败
  也是从这里返回的。
- `EzKv::join_as_learner(http_addr, seed_addr, storage, config)`：同样通过 `seed_addr` 加入，但不
  请求提升，节点永远是学习者：复制全部日志、能回答读请求，但从不投票，因此不会拖慢写入需要等待的多数派。
  之后可以在 leader 上用 `EzKv::promote` 把它变成投票者。

三者返回的 `EzKv` 都需要调用 `serve()`（或 `serve_with`）之后，这个节点才能被别的节点复制到、也才能真正
完成 join 触发的提升。重启时（同一份存储、同一个 `http_addr`）三者都会读回持久化的节点 id，既不会重新联系
`seed_addr`，也不会重新发起提升——一个节点当前是不是投票者，完全由集群的成员配置决定，重启不会改变它。

节点 id 由集群分配、持久化，不是配置出来的：三节点集群的 id 可能是 `0`、`7`、`2` 这样不连续但唯一的数字。
`EzKv::node_id()` 读出这个节点自己的 id，`EzKv::addr()` 读出它的内部端口地址（就是传给 `create`/`join`
的 `http_addr`）。

### 内部端口与集群 token

`http_addr` 就是内部端口的绑定地址：它承载 Raft 的 append/vote/snapshot/transfer_leader、管理 API
（`/api/node_id`、`/api/membership`、`/api/metrics`）和全部节点间 RPC（`/rpc/<name>`）。这个端口上的
**每一个**请求都必须带 `Authorization: Bearer <cluster_token>`，包括通过 `EzKv::serve_with` 挂载的、
上游自己加的路由——鉴权中间件包住了整个 HTTP 应用，常数时间比较，凭证缺失或错误一律 `401`，且从不进入
任何 handler、从不写日志。

`EzConfig::cluster_token` 是必填项：留空会让 `create`/`join`/`join_as_learner` 直接失败，不存在「先用
默认值跑起来、以后再加鉴权」这种状态。集群目前只支持一个 token，更换它意味着重启整个集群。

内部端口只应该绑定在集群自己的私有网络里；它不做双向 TLS，`examples/kvstore.rs` 也因此把它和上游自己的
对外端口分开监听。

### `RedbStorage`

`RedbStorage::open(path)` 是底座提供的生产级 `EzStorage` 实现：一个节点一个 redb 数据库文件，内部三张
表（日志、meta、快照）。每一次 `persist` 是一个 `Durability::Immediate` 的写事务，调用返回时数据已经能
扛过一次断电；这也是 `EzStorage` 这个 trait 本身要求任何实现都满足的契约（见「存储与快照」）。两个节点绝不
能共享同一个文件——redb 用操作系统文件锁强制这一点，第二次 `open` 会直接失败而不是悄悄损坏第一个节点的
日志。

### `EzConfig`：每一项可调参数

`EzConfig` 收纳了这个节点的全部可调项。除 `cluster_token` 外都有默认值，`EzConfig::default()` 可以直接
起步。下表列出每个字段的作用；**除了少数几个有意留白或按设计固定的字段外，具体默认数值不在本指南里重复
——它们标在 `EzConfig` 对应字段自己的文档注释上；每一项的测定方法、测得数值与取舍记录在 `FORK.md`
「参数定值」一节（ech0-media#36 在真实进程三节点集群上实测，全部默认值维持不变）**。这样做是为了让
本指南不会因为一次调参就过时。

| 字段 | 作用 |
| --- | --- |
| `cluster_token` | 内部端口鉴权用的集群 token。**没有默认值**：留空节点拒绝启动，这是刻意的设计，不是待实测的数值。 |
| `heartbeat_interval` | leader 发心跳的间隔；选举超时由它按 3~6 倍推导。 |
| `snapshot_interval` | 距上一次快照多少条新日志后自动触发下一次快照构建。 |
| `snapshot_log_to_keep` | 快照覆盖之后，磁盘上仍保留多少条日志，好让轻微落后的 follower 靠日志追上而不必装整份快照。留 `None` 按 `snapshot_interval` 推导。 |
| `replication_lag_threshold` | follower 落后多少条日志后，leader 改发整份快照而不是逐条日志。留 `None` 按 `snapshot_interval` 推导；显式设置一个比 `snapshot_interval` 还小的值会在启动时直接报错。 |
| `kv_limits` | 单个 key/value 的大小上限、单个事务的操作数与总字节数上限（`KvLimits`），见「条件事务与 revision」。 |
| `watch_limits` | `watch`/`view_and_watch` 保留窗口的组数与字节上限、每个订阅者的队列长度、`Progress` 的限速间隔（`WatchLimits`），见「watch」一节。 |
| `json_body_limit` | Raft 与管理/RPC 路由（`/raft/snapshot` 除外）的 JSON 请求体上限；必须能装下一个按 `kv_limits.max_txn_bytes` 编码后的最大事务，否则启动即报错。 |
| `snapshot_max_bytes` | `/raft/snapshot` 接受的最大原始字节数，也是这个节点愿意*发送*的最大快照体积；发送端与接收端各自检查，双方配置不一致时以较严格的一方为准。 |
| `snapshot_timeout` | 一整次 `/raft/snapshot` POST 的预算，映射到 openraft 的 `install_snapshot_timeout`。 |
| `ledger_capacity` | 幂等账本（见「任意节点写入」）按条数先进先出淘汰的容量；由提议写入的那个 leader 戳进日志命令里，`apply` 从不读取本节点自己的配置。 |
| `write_deadline` | `EzKv::write`/`write_with_id` 跨越换主重试的总时限，超时返回 `Indeterminate`。 |
| `external_addr` | 这个节点在节点注册表里登记的对外地址（见「集群观测与节点注册表」），完全不透明，底座从不解析或拨号它——只是原样存、原样交还。默认空字符串，即使留空节点也照常登记一条对外地址为空的条目。 |
| `node_meta` | 启动时登记进注册表的初始 `meta`，不透明字节，由上游自己定义含义。默认空。 |
| `liveness_threshold` | 内置存活判定任务用来判断一个成员「静默过久」的门槛，见「leader 专属任务与存活判定」。 |
| `liveness_grace` | 新 leader 上任后、在判定任何节点 `Down` 之前必须先度过的宽限期。**必须不小于 `liveness_threshold`**：`create`/`join` 会拒绝一个比阈值还短的宽限期。 |
| `liveness_check_interval` | 内置存活判定任务重新评估每个成员心跳、以及重新核对注册表与已提交成员配置的节奏。 |
| `faults`（仅 `fault-injection` cargo feature） | 测试专用的网络故障注入句柄，正常构建里这个字段根本不存在。见「存储与快照」末尾的说明。 |

## 条件事务与 revision

写入的唯一入口是一个条件事务 `Txn { conditions: Vec<Condition>, ops: Vec<Op> }`，全有或全无：

- `conditions` 按顺序针对**事务开始前**的状态求值；第一个不满足的条件让整个事务失败，`ops` 一个都不会
  执行。条件有三种：`Condition::ModRevEq { key, mod_rev }`（这个 key 的 `mod_rev` 恰好等于给定值）、
  `Condition::NotExists { key }`（这个 key 不存在，等价于 `create_rev == 0`）、
  `Condition::Exists { key }`（这个 key 存在）。
- `ops` 按顺序应用，后一个能看到前一个的效果（例如先 `DeletePrefix` 清空一个前缀、再往其中 `Put`，这是
  合法的）。三种操作：`Op::Put { key, value }`、`Op::Delete { key }`、
  `Op::DeletePrefix { prefix }`（移除以 `prefix` 开头的每一个 key；`prefix` 不能为空）。删除一个不存在
  的 key（或整个前缀里没有任何 key）不算一次改动。

`Txn::new()` 加上 `.when(condition)`/`.put(key, value)`/`.delete(key)`/`.delete_prefix(prefix)` 是
组装事务的推荐方式；两个字段本身也是公开的，从数据（而不是字面量）组装事务时可以直接推入。

条件全部满足时，事务拿到 `revision + 1`，这次事务改动的每一个 key 的 `mod_rev` 都是这个新 revision（第
一次写入某个 key 时 `create_rev` 与 `mod_rev` 相同，之后只有 `mod_rev` 更新）；revision 因此是连续的
——条件不满足的事务不消耗一个 revision。这些行为在三副本之间逐字节一致，`View::get` 之类的读方法能
观察到的 `create_rev`/`mod_rev` 在每个节点上永远相同。

### 冲突与重读重试

条件不满足时，`EzKv::write`/`write_with_id` 返回 `WriteError::Conflict(Conflict)`。`Conflict` 带着
重试所需的一切：`index`（哪一个条件失败了）、`condition`（那个条件本身）、`key`、`current_mod_rev`（那个
key 此刻真实的 `mod_rev`，key 不存在时是 `0`）。典型用法是**重读、按新状态重建条件、再写一次**——底座本身
不提供 CAS 循环，这段逻辑总是上游写的：

```rust
use ezraft::Condition;
use ezraft::EzKv;
use ezraft::Txn;
use ezraft::WriteError;

/// 单调递增一个计数器：第一次写 `NotExists`，之后按当前 `mod_rev` 用 `ModRevEq` 兜底，
/// 遇到 `Conflict` 就重读、重建事务、再写一次。
async fn increment(node: &EzKv, key: &str) -> Result<u64, WriteError> {
    loop {
        let txn = match node.view().get(key) {
            None => Txn::new().when(Condition::NotExists { key: key.to_string() }).put(key, "1"),
            Some(kv) => {
                let current: u64 = std::str::from_utf8(&kv.value).unwrap_or("0").parse().unwrap_or(0);
                Txn::new()
                    .when(Condition::ModRevEq {
                        key: key.to_string(),
                        mod_rev: kv.mod_rev,
                    })
                    .put(key, (current + 1).to_string())
            }
        };

        match node.write(txn).await {
            Ok(revision) => return Ok(revision),
            Err(WriteError::Conflict(_)) => continue,
            Err(other) => return Err(other),
        }
    }
}
```

这正是「给上游的使用约定」里「热路径读本地视图、写入靠 CAS 条件兜底」的具体形状：读的数据可能已经过时，
但写入时的 `mod_rev` 条件会在数据真的变了的时候拒绝这次写，而不是让旧数据覆盖新数据。

### 上限

`Txn::validate(&self, limits: &KvLimits)` 在事务真正被提议*之前*检查大小与数量：`KvLimits` 有
`max_key_bytes`、`max_value_bytes`、`max_txn_ops`（操作数的上限，同时也是条件数的上限，两者各自计数）、
`max_txn_bytes`（事务里全部 key、value 字节数之和，涵盖 `conditions` 与 `ops` 两边）四项，通过 `EzConfig::kv_limits` 配置（默认值见字段
文档注释）。`EzKv::write`/`write_with_id` 先用**发起节点自己的**限制检查一遍（不满足就地拒绝，不
发出任何请求），转发到 leader 之后 leader 还会用**自己的**限制再检查一遍——两节点配置不同时，不允许因为
检查发生在哪个节点上而让副本产生分歧。超限返回 `WriteError::LimitExceeded(LimitError)`，`LimitError`
的每个变体都带着具体超限的 key/大小/上限，见「错误参考表」。

## 任意节点写入

**任何**节点都能接受写入，不需要先知道谁是 leader：

- `EzKv::write(txn)`：分配一个新的 `RequestId`（一个 ULID，公开类型），然后调用 `write_with_id`。
- `EzKv::write_with_id(request_id, txn)`：如果这个节点自己是 leader，直接本地提议；否则通过内部 RPC
  `ez.write` 转发给它认为的 leader。只要结果不是确定的——`ForwardToLeader`（不论带没带 leader 信息）、
  远端说自己不是 leader、连不上、RPC 超时、丢了响应、暂时不知道谁是 leader——都用**同一个**
  `request_id` 继续重试，直到 `EzConfig::write_deadline` 用完为止，期间不会忙等。

一次成功之后，`write`/`write_with_id` 还会在剩余时限内等待*这个节点自己*已发布的 revision 追上刚才的
写入结果，这样调用返回时 `EzKv::view()` 已经能看到它；等不到就返回 `WriteError::LocalLag(LocalLag {
revision })`——这不是失败，写入已经确定提交了，只是本地视图还没追上，用 `EzKv::wait_revision(revision,
timeout)` 单独等它即可（拿到别的节点的回执、或者从一组 `Changes` 里读出的 revision，同样可以用它来等）。
遇到 `Conflict` 时，`write`/`write_with_id` 也会尽力（best-effort）等本地视图追上冲突里的
`current_mod_rev` 再把冲突返回给调用方，让调用方接下来的重读能看到它刚刚撞上的那个值——等不到也没关系，
冲突照样返回，只是重读可能还差一点点。

deadline 用完了还没有确定结果，返回 `WriteError::Indeterminate { request_id }`——这次写入到底有没有
生效，现在不知道。之后**用同一个 `request_id` 再调用一次 `write_with_id`（带着同一个 `txn`）**，如果这
次写入其实已经在幂等账本的窗口内被记录过，会原样拿回第一次的真实结果（成功的 revision，或者当时的
`Conflict`），不会重复执行。这个窗口按条数先进先出淘汰（`EzConfig::ledger_capacity`），窗口之外重新调用
同一个 `request_id` 就等同于一次全新的写入。**幂等账本记的是「这个 id 第一次的结果」，不是这个 id 对应的
事务本身**——用同一个 id 配一个不同的 `txn` 重试，不会被识别成「不一样」，而是直接拿回第一次那个（很可能
完全不相干的）结果，所以调用方必须保证同一个 `request_id` 每次都配同一个 `txn`。

```rust
use ezraft::EzKv;
use ezraft::LocalLag;
use ezraft::RequestId;
use ezraft::Txn;
use ezraft::WriteError;

/// `Indeterminate` 之后如何收尾：用同一个 request_id、同一个 txn 继续问，直到拿到确定结果；
/// `LocalLag` 当作已经成功、只是本地还没追上来处理。`Conflict` 和 `LimitExceeded` 都是确定结果，
/// 原样交还给调用方——重试不会改变它们。
async fn write_until_definite(node: &EzKv, id: RequestId, txn: Txn) -> Result<u64, WriteError> {
    loop {
        match node.write_with_id(id, txn.clone()).await {
            Ok(revision) => return Ok(revision),
            Err(WriteError::LocalLag(LocalLag { revision })) => return Ok(revision),
            Err(WriteError::Indeterminate { .. }) => continue,
            Err(definite @ (WriteError::Conflict(_) | WriteError::LimitExceeded(_))) => return Err(definite),
        }
    }
}
```

### `RequestId`

`RequestId` 是一个不透明、`Copy`、有序的 ULID，`RequestId::new()` 生成一个几乎不可能与任何其他节点在
任何时候生成的另一个重复的新值。它经由 JSON（转发 RPC、上游自己的对外 API）和二进制格式（Raft 日志、
postcard 快照）不变地往返，`Display` 只输出这个 ULID 本身，从不携带调用方数据。

## 本地视图与线性读

### `EzKv::view()`：本地视图

`EzKv::view()` 返回这个节点*已应用*状态的一个不可变快照 `View`，不加锁、不等待、不发任何请求——它只是
读一次底座内部发布的当前值，读一整份视图的开销是 O(1)（结构共享，不是深拷贝）。同一个 `View` 上的每一次
读都来自完全相同的一个 revision，所以在同一个 `View` 上读多个 key 永远不会读到「半个事务」：

- `view.revision()`：这个视图对应的 revision。
- `view.get(key)`：单个 key 的查找，`None` 表示不存在。
- `view.get_many(keys)`：一次查多个 key，按输入顺序返回等长的 `Vec<Option<KeyValue>>`。
- `view.prefix(prefix)`：按 key 顺序遍历所有以 `prefix` 开头的 key，是一个 `Iterator`。**这是纯字符串
  前缀匹配，不是路径分段匹配**：`"room/1"` 会匹配 `prefix("room/1")`，同时 `"room/10"`、
  `"room/1/participant/x"` 都会匹配，而 `"room0"` 不会；只想要 `room/1` 的真子路径时，查询用带结尾分
  隔符的 `"room/1/"`。
- `view.page(prefix, after, limit)`：分页版本的 `prefix`，返回 `Page { items, next }`；`after` 传上一页
  的 `next` 继续，传 `None` 从头开始，`next` 为 `None` 表示这个前缀已经取完。

```rust
use ezraft::EzKv;
use ezraft::Page;

/// 分页取出一个大房间下的全部成员。
fn list_all_participants(node: &EzKv, room: &str) -> Vec<ezraft::KeyValue> {
    let prefix = format!("room/{room}/participant/");
    let view = node.view();

    let mut items = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let Page { items: page, next } = view.page(&prefix, after.as_deref(), 100);
        items.extend(page);
        match next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    items
}
```

**`view()` 只保证「读己之写」发生在同一个节点上**：`EzKv::write` 返回时，这个节点自己的 `view()` 已经能
看到这次写入，但另一个节点的 `view()` 未必立刻能看到——它是这个节点应用日志的进度，可能落后 leader，也
可能落后另一个刚刚写入成功的节点。要在另一个节点上等到一个已知 revision，用 `EzKv::wait_revision` 之后
再读，或者直接做一次线性读。

### `EzKv::linearizable_view(timeout)`：线性读

线性读回答的是「到目前为止已经确认提交的一切」，而不只是「这个节点目前应用到了哪里」。只支持 ReadIndex，
不提供 LeaseRead，所以正确性从不依赖多个节点的时钟彼此一致：

- 在 leader 上，直接用一轮心跳向多数派确认自己仍然是 leader（不等待本节点自己的 apply），再等本地应用
  追上这个确认位置，返回本地 `View`。
- 在 follower 或学习者上，同样的确认经内部 RPC 在 leader 上发生，follower 本地再等自己的应用追上，返回
  的是 follower 自己的 `View`——线性读永远返回调用节点自己的视图，不是把 leader 的数据搬过来。
- 同一个 follower 或学习者上并发的多个线性读会共享同一轮向 leader 确认的 RPC（至多两轮就能服务任意
  并发量），所以一个节点上大量并发的线性读只给 leader 带来常数量级的负担。**leader 自己身上的线性读不做
  合并**：每次调用各自向多数派确认一轮。需要在 leader 上大量读、又不要求线性一致的路径，用本地视图。
- 一个已经被移出集群的节点再也收不到任何复制，它自己的成员视图不会更新；它靠这条确认路径得知自己已被
  移除（`ReadError::NotMember`）：它认识的 leader 联系不上、或者已经不是 leader 时，会转而询问它记得的
  其他成员，并沿着对方给出的 leader 提示再问一次。只有刚确认过自己是 leader 的节点才会回答
  `NotMember`——follower 的成员视图可能还没追上「把调用方加进来」的那次变更。

`timeout` 限定确认加本地追赶合起来的总时间。失败时是 `ReadError`：

```rust
use ezraft::ReadError;

/// 按错误类型决定接下来怎么办，而不是把三种情况都当成同一种失败处理。
fn handle_read_error(err: ReadError) {
    match err {
        // 目前没有确认到 leader：重试仍在预算内的话值得再等等，选举通常几个心跳内就会结束。
        ReadError::NoLeader => {}
        // 这个节点已经不在集群成员配置里了——重试解决不了，不用再试。
        ReadError::NotMember(_node_id) => {}
        // 读位置已经确认，但本地应用没在超时内追上；写入已经提交，只是这里还没看到。
        ReadError::NotCaughtUp {
            read_index: _,
            applied_index: _,
        } => {}
    }
}
```

RoomService 一类的读接口、以及任何「这个东西不存在」的否定判断，都应该走线性读而不是本地视图——见「给上游
的使用约定」。

## watch：订阅复制状态的变更

`EzKv::watch(prefix, start)` 按 `revision` 严格递增，订阅每一个改动了 `prefix` 下至少一个 key 的事务；
`EzKv::view_and_watch(prefix)` 原子地拿到一个 `View` 和紧接其后、无缺口的变更流——两者共用同一个发布
源，所以「先拿全量、再接增量」永远不会漏掉或重复中间那一刻的变更。空前缀订阅整个 key 空间。返回的
`WatchStream` 实现 `futures::Stream`，drop 它就是取消订阅；一个从不消费的订阅不会拖慢 apply（见下文
`SubscriberTooSlow`）。

`start` 是 `WatchStart::Now`（只看这次调用之后的变更，不做历史重放）或者
`WatchStart::After(revision)`（从某个已知 revision 之后续订，通常是上一次 `Changes`/`Progress` 里保存
下来的那个 revision）。

流上的每一项是 `WatchUpdate` 的一种：

- `WatchUpdate::Changes(Changes)`：一个事务过滤到订阅前缀之后的变更组，`Changes { revision, items }`。
  `items` 按这个事务里 op 的原始顺序列出，不按 op 种类分组；`DeletePrefix` 展开成逐 key 的
  `Change::Delete`，按 key 顺序排列，每一项带着被删掉之前的值（`prev: KeyValue`）。一个事务如果完全没
  碰到这个前缀，不产生任何一项——不是产生一个空的 `Changes`。
- `WatchUpdate::Reset { view, cause }`：这个订阅在 `view.revision()` 之前可能漏掉了什么，丢弃据此推导
  出的一切状态，从 `view` 重新开始——**底座保证：不发 `Reset` 就没有缺口**。之后每一个 `Changes` 的
  revision 都严格大于 `view.revision()`。`cause` 是 `ResetCause` 的一种：
  - `ResumeTooOld { requested, window_floor }`：续订的 revision 早于本节点内存里还保留的最老
    revision（`window_floor`）——这段历史已经不在内存里了。
  - `SubscriberTooSlow`：这个订阅的待投递队列在被消费之前就满了，整个积压被这一条 Reset 取代，绝不会
    因为一个不消费的订阅而拖慢 `apply`。
  - `SnapshotInstalled`：这个节点刚刚安装了一份快照，整个状态被替换，历史变更序列不再适用。
- `WatchUpdate::Progress { revision }`：revision 前进了，但这次没有匹配这个前缀的变更——用来让续订游标
  继续前进，不必等到下一次真正相关的变更才更新。

```rust
use ezraft::EzKv;
use ezraft::ResetCause;
use ezraft::WatchStart;
use ezraft::WatchUpdate;
use futures::StreamExt;

/// 从上次保存的 revision 续订，分别处理三种 Reset 原因，并把新的游标保存下来。
async fn resume_and_consume(node: &EzKv, prefix: &str, after: u64) {
    let mut stream = node.watch(prefix.to_string(), WatchStart::After(after));

    while let Some(update) = stream.next().await {
        match update {
            WatchUpdate::Changes(changes) => {
                let _ = changes.revision; // 转换成业务事件，再把这个 revision 存为新游标
            }
            WatchUpdate::Reset { view, cause } => {
                match cause {
                    ResetCause::ResumeTooOld { .. } => {}
                    ResetCause::SubscriberTooSlow => {}
                    ResetCause::SnapshotInstalled => {}
                }
                let _ = view.revision(); // 丢弃旧状态，从这个 view 重新构建，再存为新游标
            }
            WatchUpdate::Progress { revision } => {
                let _ = revision; // 没有相关变更，但游标可以前进到这里
            }
        }
    }
    // 流在这里结束（返回 None）：本节点已经调用过 shutdown()。
}
```

节点重启之后，保留窗口是空的，下限就是重启时状态的 revision；随后重放日志尾巴的过程会把这些变更重新
投递进窗口，一个跨重启续订的订阅可能因此重复看到一部分变更（重放导致的重复投递，不是 bug）。重复总是
**整组**重复，revision 与第一次投递时完全相同，所以最简单也最可靠的去重单位是组：**只应用 revision 严格
大于上一次已应用的那一组的 `Changes`**。落到单个 key 上这与「按 `mod_rev` 去重」是同一件事——一个
`Change::Put` 的 `mod_rev` 就是它所在那一组的 revision；但 `Change::Delete` 自己没有 `mod_rev`（它带的
`prev.mod_rev` 是*被删掉的那个值*的版本，恰好等于本地已记录的值，拿它来比会把每一次删除都当成重复丢掉），
删除发生的版本只能从组的 revision 读出。按 key 去重的实现因此还必须为已删除的 key 保留一个「墓碑」版本，
否则一次重放的旧 `Put` 会让已删除的 key 复活；按组去重不需要这些：

```rust
use std::collections::BTreeMap;

use ezraft::Change;
use ezraft::Changes;
use ezraft::KeyValue;
use ezraft::View;

/// 把 watch 产出的变更折叠成一份本地状态，按组的 revision 去重：重放导致的重复投递不会造成重复的
/// 业务动作，已删除的 key 也不会被一次重放的旧 `Put` 复活。
#[derive(Default)]
struct Folded {
    /// 已应用的最后一组变更的 revision（或者最近一次 `Reset` 的 `view.revision()`）
    revision: u64,
    entries: BTreeMap<String, KeyValue>,
}

impl Folded {
    /// `WatchUpdate::Reset`：丢弃旧状态，从 `view` 在订阅前缀下的内容重新开始。
    fn reset(&mut self, view: &View, prefix: &str) {
        self.entries = view.prefix(prefix).map(|kv| (kv.key.clone(), kv)).collect();
        self.revision = view.revision();
    }

    /// `WatchUpdate::Changes`：整组应用，或者整组跳过。
    fn apply(&mut self, changes: Changes) {
        if changes.revision <= self.revision {
            return; // 这一组已经应用过：重放造成的重复投递
        }

        for change in changes.items {
            match change {
                Change::Put {
                    key,
                    value,
                    create_rev,
                    mod_rev,
                } => {
                    self.entries.insert(key.clone(), KeyValue {
                        key,
                        value,
                        create_rev,
                        mod_rev,
                    });
                }
                Change::Delete { key, .. } => {
                    self.entries.remove(&key);
                }
            }
        }
        self.revision = changes.revision;
    }
}
```

把状态持久化到别处（数据库、下游系统）的消费者同理：把「已应用到的 revision」和状态放在同一次持久化里
一起提交，恢复后用 `WatchStart::After(那个 revision)` 续订，并按上面的规则跳过不比它新的组。

保留窗口本身是每节点一份的内存环形缓冲，按组数和字节数两个上限截断（`EzConfig::watch_limits` 的
`max_window_groups`/`max_window_bytes`），不进快照、不跨节点复制；每个订阅者的待投递队列长度上限是
`subscriber_queue_len`，`Progress` 的限速间隔是 `progress_interval`——四个默认值经 ech0-media#36 实测
后维持不变（小变更下先触顶的是组数上限，字节上限防的是异常大的 value），见 `FORK.md`「参数定值」。

watch 是**每个节点自己的**订阅：leader 和 follower 各自的订阅者看到的是各自节点应用到的变更，不是从
leader 单点扇出；这正是「给上游的使用约定」里「接入节点信令热路径」应当依赖本地视图与本地 watch 的原因
之一——不需要跨节点转发就能把变更推给连在这个节点上的客户端。

## 节点间 RPC

`EzKv::rpc()` 拿到 `Rpc` 这个句柄，克隆代价很低。它是上游在节点之间发起自定义调用的机制——例如 room 编排
另一个节点上 media node 的 PipeTransport 建连、pause/resume/close——走的是与 Raft 流量相同的内部端口
和集群 token，但有自己独立的连接池，不会排在心跳或大批量复制后面。

```rust
use ezraft::EzKv;
use ezraft::RpcError;
use ezraft::RpcRegisterError;

/// 在这个节点上注册一个处理器，供其他节点按节点 id 调用。名字不能以 `ez.` 开头——
/// 那个前缀留给底座自己的内部机制（转发写入、ReadIndex）。
fn register_handler(node: &EzKv) -> Result<(), RpcRegisterError> {
    node.rpc().register::<u64, String, std::convert::Infallible, _, _>(
        "create_pipe_transport",
        |media_node_port: u64| async move { Ok(format!("transport-on-{media_node_port}")) },
    )
}

/// 在另一个节点上按节点 id 调用它——地址完全由底座根据当前成员配置解析，调用方不需要
/// 知道任何节点的地址。
async fn call_handler(node: &EzKv, target: u64) -> Result<String, RpcError> {
    node.rpc()
        .call::<u64, String>(target, "create_pipe_transport", 5555, std::time::Duration::from_secs(5))
        .await
}
```

- `register::<Req, Resp, E, F, Fut>(name, handler)`：`handler` 是一个 `async fn(Req) -> Result<Resp,
  E>`。同一个名字注册两次会报错而不是悄悄替换第一个注册（两次独立注册撞上同一个名字，更可能是 bug 而
  不是有意的热替换）。
- `call::<Req, Resp>(node_id, name, req, timeout)`：按节点 id 寻址（地址取自这个节点当前的成员配置），
  调用自己的节点 id 会直接走本地分发，不发 HTTP、不检查 token，但错误类型和超时语义与调用别的节点完全
  一样。

**从不自动重试**——一个 handler 未必是幂等的，重试与否是调用方的决定，取决于调用方自己知不知道重试是
安全的。drop `call` 返回的 future 只会取消调用方这一侧的等待，已经开始执行的远端 handler 会照常跑完，
调用方无法阻止它。

`RpcError` 的每个变体：

| 变体 | 含义 | 调用方应当怎么做 |
| --- | --- | --- |
| `Timeout { node_id, name }` | `timeout` 到了还没等到答复 | **结果未知**：远端 handler 可能仍在跑；是否重试取决于它是不是幂等的 |
| `Unreachable { node_id, name }` | 连不上目标节点（测试构建里也可能是被故意丢弃的请求） | 请求没有送达，handler 没有运行；通常等一下再试，或者换一个节点 |
| `NotMember(u64)` | 目标节点不在当前成员配置里 | 重试解决不了，先刷新成员信息 |
| `Unauthorized(u64)` | 目标节点用 `401` 拒绝了，集群 token 不匹配 | 检查配置，不要重试 |
| `Transport { node_id, name, message }` | 请求可能已经送达，但这次交换在途中坏掉了：连接在完整答复到达之前断开，或者答复不是 RPC 层能理解的东西（非成功的 HTTP 状态，例如请求体超过对端上限；或解不出来的响应体） | **结果未知**，和 `Timeout` 一样对待：handler 可能已经运行过 |
| `Remote(String)` | 对端的 RPC 层给出了**确定的**答复，只是它不是一个类型化结果——没有这个名字的处理器、请求解不成 handler 的请求类型、或者 handler 自己返回了错误；消息里会说明具体是哪一种 | 按消息判断：未知名字/解码失败通常是调用方的 bug；handler 自己的错误按业务语义处理 |

区分「确定的答复」和「结果未知」对非幂等的 handler 很重要（例如让另一个节点创建一个 PipeTransport）：
拿到 `Remote` 可以确信对端说的就是发生了的事；拿到 `Timeout` 或 `Transport` 则必须假设 handler 可能已经
执行过，先查询或清理，再决定要不要重试。

`RpcRegisterError`：`Reserved(String)`（名字以 `ez.` 开头）、`AlreadyRegistered(String)`（这个名字已
经注册过）。

## 集群观测与节点注册表

### `cluster()` 与 `last_leader_contact()`

`EzKv::cluster()` 返回一个 `tokio::sync::watch::Receiver<ClusterState>`，只在 `ClusterState` 的某个
字段真的变化时才会推送一次（不是每次心跳都推）。`ClusterState` 包含这个节点自己的 `node_id`、
`role`（复用 openraft 的 `ServerState`：`Learner`/`Follower`/`Candidate`/`Leader`/`Shutdown`）、
`term`、`current_leader`、`voters`、`learners`，以及这个节点是否仍然是当前成员配置的一员
（`is_member`）。

`EzKv::last_leader_contact()` 回答「距离这个节点上次收到一个被承认合法的 leader 消息过了多久」
（`Option<Duration>`）：在 leader 自己身上恒为 `Some(Duration::ZERO)`，从未收到过任何消息时是 `None`。
这条信息 openraft 本身并不提供（它只有 leader 视角的 follower 心跳指标，没有 follower 自己的这一项），
底座在接受 append 请求的路由里自己记录。上游可以据此实现「节点长时间联系不上 leader 时拒绝新的接入」
一类的策略。

`EzKv::is_leader()` 是同一件事的一次性同步版本：只回答「这个节点此刻自认为是不是 leader」。它报告的是
本节点自己的状态，不是保证——一个已经被取代的 leader 要等收到新 leader 的消息才会知道。所以它只适合做
观测和提示（日志、指标、「要不要顺手做点只有 leader 才值得做的事」），不能当作正确性的依据：需要「只在
leader 上执行」的逻辑用 leader 专属任务（见下一节），需要「读到的一定是最新的」用线性读。

### 节点注册表

`EzKv::registry()` 本地读取整份节点注册表（`Registry`），不经共识、不阻塞、不会失败，和 `view()` 一样
可能落后 leader；`EzKv::watch_registry()` 订阅它未来的每一次整体变化——注册表很小（一个集群成员一条），
所以每次变化直接把整份新值发给订阅者，不像 `EzKv::watch` 那样是增量加 Reset。

每条 `NodeInfo` 有三个字段：

- `external_addr`：客户端与业务方用来联系这个节点的地址，和成员配置里 Raft 用的内部地址是两码事；
  底座从不解析或拨号它，原样存、原样交还——完全由 `EzConfig::external_addr` 决定内容。
- `meta`：上游自定义的不透明字节，底座同样不解释；`EzKv::update_node_meta(meta)` 可以随时更新它（保留
  已登记的 `external_addr` 不变），受 `EzConfig::write_deadline` 约束。一条注册表条目和普通 value 一样
  经由日志复制，所以大小也按一个 value 计：`external_addr` 加 `meta` 超过 `KvLimits::max_value_bytes`
  时返回 `RegistryWriteError::LimitExceeded(LimitError::NodeEntryTooLarge { .. })`，什么都不会被提议
  （启动时 `EzConfig::external_addr` 加 `node_meta` 超限则 `create`/`join` 直接失败）；超时返回
  `RegistryWriteError::Indeterminate { request_id }`——更新可能仍会生效，重新调用一次总是安全的，
  因为条目是整条覆盖的。
- `status`：底座对这个节点是否可达的判断，`NodeStatus::Up` 或 `NodeStatus::Down`。

每个节点在启动时都会自己在后台把 `external_addr`/`node_meta` 登记进注册表，失败就一直重试直到成功
（这个节点当时可能还不是成员、集群甚至可能暂时没有 leader）；`EzKv::remove_node` 在对应的成员变更提交
之后，在同一个流程里删除这个节点的注册表条目（best-effort，受 `write_deadline` 约束，失败只是记日志，
不会让一次已经成功的成员变更被回报为失败）。leader 恰好在成员变更提交与注册表删除这两步之间崩溃，会
留下一条陈旧条目——底座内置的存活判定任务（见「leader 专属任务与存活判定」）每一轮都会把注册表里已经
不在**已提交**成员配置中的条目清理掉，这条陈旧条目最迟在它下一轮运行时就会被回收，不需要上游自己处理。

`NodeStatus::Up`/`NodeStatus::Down` 由同一个内置任务驱动，依据的是**leader** 对每个成员最近一次
被确认的心跳；只在状态真的变化时才写入，详见「leader 专属任务与存活判定」。节点自己注册或更新自己的
条目时，状态总是被重置为 `Up`——一个节点正在把自己写进注册表，就说明它此刻是活的。

```rust
use ezraft::EzKv;
use ezraft::NodeStatus;

/// 一个接入节点断线重连时，用本地注册表判断是重定向回原节点还是换一个 `Up` 的节点。
fn choose_reconnect_target(node: &EzKv, original: u64) -> Option<String> {
    let registry = node.registry();
    match registry.get(original) {
        Some(info) if matches!(info.status, NodeStatus::Up) => Some(info.external_addr),
        _ => registry.iter().find(|(_, info)| matches!(info.status, NodeStatus::Up)).map(|(_, info)| info.external_addr),
    }
}
```

## leader 专属任务与存活判定

### `spawn_leader_task` 与 `LeaderCtx`

`EzKv::spawn_leader_task(name, f)` 为这个节点的整个生命周期注册一个任务工厂：这个节点在 term T
成为 leader 时，底座为每一个已注册的工厂调一次 `f`，传入一个携带这个 term 的全新 `LeaderCtx`，运行
它返回的 future；这次运行在这个节点失去 leadership、或者 term 发生变化的那一刻被取消——重新赢得
leadership 得到的永远是一次全新的运行，绝不是被恢复的旧运行。**一次运行自己返回或者 panic 之后，
在同一个 term 内不会被重新启动**：只有一次全新的领导权（新的 term）才会让同一个工厂再跑一次，所以
一个 leader 专属任务通常写成一个循环，在循环体里反复检查状态、决定要不要写，而不是执行一次就退出。
在这个节点已经在 leading 的时候注册的工厂会立即启动，用的是当前这个 epoch，而不是等下一次
`cluster()` 变化——这次 leadership 可能再也不会产生下一次变化了。

`LeaderCtx` 给这次运行三样东西：`term`（这次运行所属的 term，公开字段，直接读）、
`node()`（这个节点的普通句柄，`EzKv::view`、`EzKv::linearizable_view`、`EzKv::registry`、
`EzKv::rpc` 等照常用它）、`write(txn)`（本节介绍的、锁定在这次运行自己 term 上的写入）。

### 围栏：为什么 `ctx.write` 从不转发，而且是精确的

`ctx.write` 从不转发：如果这个节点已经不在 `term` 里继续 leading，它直接返回
`LeaderWriteError::NotLeader`，绝不会去问任何别的节点帮忙提交。

只在提议前检查一次角色和 term 是不够的：那次检查只是读一下本节点自己当前的 metrics（一次同步的
本地读，不发任何请求），代价很低、也能省下一条注定不会生效的日志——但 leadership 完全可能在这次检查
和日志条目真正被追加之间变化，检查通过之后再遇上换届，写入还是可能落到一个已经不对的 term 上。
真正把这条路堵死的是：这次要提议的命令同时被盖上它所要求的 term（`Command::required_term`），而
`apply` 只依据日志条目自己实际被追加在哪个 term 里做判断——不是任何节点自己的配置，也不是任何节点自己
当下的想法——一旦不是 `required_term` 那个 term，就拒绝执行，报告 `CommandOutcome::StaleTerm`。因为
这个判断只看日志本身，每个副本看到的是同一份日志，结论必然一致；又因为 `ctx.write` 从不转发，`txn`
只可能被提议在这个 context 自己的节点上——所以一次通过过期 context 发出的写入不可能在任何节点上生效，
哪怕这个节点后来又在一个新的 term 里重新赢得了 leadership。

围栏只保证**这一个** context 一旦过期就写不进去，不保证换届前后不会有两个不同的、都合法的 context
去做同一件事——旧的一次运行还没被取消完，新的一次运行已经开始，两边都以为自己该做这件事。这种交接期间
可能出现的重复动作，仍然要靠事务自己的 CAS 条件（`Condition`）挡住，跟任意节点写入路径上的重试完全
是同一个道理。

### `LeaderWriteError`：每个变体该怎么办

`ctx.write` 从不转发，所以它没有 `Indeterminate` 之外的「未知」状态，也没有任何跟转发有关的错误：

- `LimitExceeded(LimitError)`：`txn` 超过了配置的上限，纯粹是事务本身的问题，处理方式与
  `EzKv::write` 完全一样。
- `Conflict(Conflict)`：条件不满足。跟「条件事务与 revision」一节的重试循环完全一样：重读、按新状态
  重建事务、再写一次。
- `NotLeader { term }`：**这个 context 已经死了**——这个节点已经不在 `term` 里继续 leading 了，
  不论是彻底失去了 leadership，还是（哪怕是同一个节点）已经在一个更晚的 term 里重新当选。不会被转发，
  也不该被重试：监督者这时通常已经在取消这次运行，或者很快就会取消；这个 context 剩下唯一该做的事就是
  结束这次运行，把后续交给它的下一次运行（新的 term、新的 `LeaderCtx`）去处理。
- `Indeterminate { term }`：**恰恰相反，这个 context 未必已经死了，任务不应该因为它而结束。**
  deadline 用完时这个节点看起来仍然在 `term` 里 leading，只是没等到确定的答案——这次提议可能已经追加
  成功、还在等提交，也可能压根没被接受。一次运行返回之后要等到*下一个* term 才会再运行，所以如果任务
  在这里就退出，即使这次 leadership 完全健康、马上就能给出确定结果，也要白白等到下一次换届才会重新
  尝试。正确的做法是：把这一次当成「再检查一遍」——重读状态，如果这次写入看起来确实还没生效，就在同一次
  运行里、用同样的 CAS 条件再写一次；条件本身保证了即使第一次的提议其实迟一点才提交，重复调用也不会
  造成重复的效果。

一个典型的 leader 专属任务：一个循环，每一轮读本地视图判断要不要写，用 `ctx.write` 在 CAS 条件下
提交，并对每一种错误分别处理：

```rust
use std::time::Duration;

use ezraft::Condition;
use ezraft::LeaderCtx;
use ezraft::LeaderWriteError;
use ezraft::Txn;

/// 每 30 秒检查一次：如果这个房间被标记为「空」且标记从未被别人动过，就把它整个删掉。
async fn expire_empty_room(ctx: LeaderCtx, room: &str) {
    let key = format!("room/{room}/empty_since");
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;

        let Some(kv) = ctx.node().view().get(&key) else {
            continue; // 房间不是空的，或者还没被标记，这一轮没什么要做的
        };

        let txn = Txn::new()
            .when(Condition::ModRevEq {
                key: key.clone(),
                mod_rev: kv.mod_rev,
            })
            .delete_prefix(&format!("room/{room}/"));

        match ctx.write(txn).await {
            Ok(_revision) => {}
            // 读和写之间已经有人动过这个 key 了：按新状态重新判断，放到下一轮再说。
            Err(LeaderWriteError::Conflict(_)) => {}
            // 这个 term 的 context 已经死了：这次运行该结束了，监督者很快会 abort 它。
            Err(LeaderWriteError::NotLeader { .. }) => return,
            // 结果未知，但这个 term 里这个节点可能仍然是 leader：context 未必已经死了，
            // 绝不能把这种情况当成任务该结束——下一轮循环会重读状态，需要的话再写一次。
            Err(LeaderWriteError::Indeterminate { .. }) => {}
            Err(LeaderWriteError::LimitExceeded(_)) => {}
        }
    }
}
```

### 内置的存活判定

底座自带一个存活判定任务，每个节点都会自动注册它（构造节点时就已经装好，上游不需要显式启用），用的
正是上面这套 `spawn_leader_task` 机制，没有任何特殊待遇。

它测量的是**leader** 视角下每个成员最近一次被确认的心跳（或日志复制）——这项数据只在 leader 自己身上
存在，一旦这个节点不再 leading，这一轮判断直接跳过。一个成员的静默时间一旦超过 `liveness_threshold`，
**并且**这次 leadership 本身已经持续超过 `liveness_grace`，这个成员的注册表条目就会被判定为
`NodeStatus::Down`；一旦它的确认心跳恢复，下一轮就会把它改回 `NodeStatus::Up`。leader 自己永远是
`Up`。新 leader 存在宽限期的原因是：一次全新的 leadership 一开始，它对每一个别的成员的心跳记录都是
空的——这和「这个成员已经沉默了很久」在数据上完全无法区分；`liveness_grace`（校验过不小于
`liveness_threshold`）就是用来防止一次换届本身看起来像是所有节点同时失联。状态只在真正变化时才写入
（经由 leader 专属的内部命令 `CommandBody::SetStatus`，与 `ctx.write` 走同一条被围栏保护的路径），所以一个
健康、没有变化的集群不会持续产生任何注册表写入。

同一个任务的每一轮还会核对注册表和状态机自己已经应用的成员配置：任何已经不在已提交成员配置里、却还留在
注册表里的条目都会被清理掉——这正是 `remove_node` 一节提到的、leader 在成员变更提交与注册表删除这两步
之间崩溃可能留下的窗口，靠这里补上。

上游读这份判定结果的方式和读注册表其他字段完全一样，不需要专门对接：`EzKv::registry()` 拿一次当前的
整份注册表，或者 `EzKv::watch_registry()` 订阅它未来的每一次变化——`NodeStatus` 本身就是 `NodeInfo`
的一个字段，见「集群观测与节点注册表」。

`EzConfig` 里跟存活判定有关的三个字段：`liveness_threshold`（判定「静默过久」的门槛）、
`liveness_grace`（新 leader 宽限期，必须不小于 `liveness_threshold`，否则启动即报错）、
`liveness_check_interval`（这个任务重新评估心跳与核对成员配置的节奏）。默认 5 秒 / 5 秒 / 1 秒：
ech0-media#36 实测一个 follower 失联后约 5.2 秒被判 `Down`，leader 被杀后约 5.3–6 秒选出新 leader
（默认心跳间隔下），见 `FORK.md`「参数定值」。

## 成员变更

以下四个方法只能在 leader 上调用成功；在 follower 上调用会直接失败，而不是自动去找 leader——找 leader
是网络协议（HTTP 管理 API）的职责，不是 `EzKv` 这个 Rust 句柄的职责：

- `EzKv::promote(node_id)`：把一个学习者变成投票者。只有投票者计入多数派，所以集群要到加入的节点被
  提升之后，才真正获得对应的容错能力（三个投票者容忍一个节点失效，五个容忍两个；偶数个投票者不额外
  带来容错能力）。返回时这个节点已经是投票者——这一步包含把它追上进度，对一个很大的状态可能是一整次
  快照传输。
- `EzKv::demote(node_id)`：把一个投票者变回学习者，继续复制日志，但不再计入多数派。拒绝把最后一个
  投票者也降级——那样集群永远无法再提交任何东西，包括把投票者加回来的那次变更本身。降级当前 leader 是
  允许的，它不会因此停止领导：leader 不需要是投票者，只是从此按它已经不在其中的那个投票者集合的多数派
  来提交。
- `EzKv::remove_node(node_id)`：把一个节点（投票者或学习者）彻底移出集群，停止复制、忘记它；不会停止
  这个节点自己的进程。成员变更提交之后还会顺带清理它在节点注册表里的条目（见上一节）。
- `EzKv::transfer_leader(node_id)`：让当前 leader 主动把领导权移交给 `node_id`。返回时移交只是「已经
  发起」，目标节点接下来还要真正赢得一次选举，用 `EzKv::wait_metrics` 等 `current_leader` 变成期望的值。

```rust
use ezraft::EzKv;

/// 主动移交领导权，并等待接收方真的成为新 leader。
async fn hand_over_leadership(leader: &EzKv, target: u64) -> std::io::Result<()> {
    leader.transfer_leader(target).await?;
    leader.wait_metrics(None, move |m| m.current_leader == Some(target), "leadership transferred").await?;
    Ok(())
}
```

`EzKv::join`/`join_as_learner` 内部就是用这套机制加入的，只是通过 HTTP 管理协议（`ezraft::admin`
模块：`AdminClient`、`MembershipChange`、`Redirect`）完成——这个协议会自动跟着重定向找到 leader、并且
对「集群刚启动、还没有 leader」这类瞬时状态重试，`EzKv` 自身的 Rust 方法不会。想在 Rust 之外（或者不通过
`EzKv` 句柄）驱动成员变更时，可以直接说这套 HTTP 协议：`POST /api/node_id` 领一个新 id，
`POST /api/membership`（body 是打了 `op` 标签的 `MembershipChange`）做加入/改角色/移除，
`GET /api/metrics` 读某个节点自己的视角。协议里的角色用 `ezraft::NodeRole` 表示，只有两个取值：
`Learner`（复制日志、可以应答读，但不投票、不计入多数派）和 `Voter`（计入多数派）；`SetRole` 把一个
已有节点改成其中之一，对应 Rust 侧的 `demote` / `promote`。节点总是先以 `Learner` 身份进入集群，
`join` 只是在追上进度之后多做一步提升。

进阶用法可以通过 `EzKv::inner()` 直接拿到底层 openraft 的 `Raft` 句柄，类型参数固定为
`ezraft::OpenRaftTypes`；`ezraft::RaftMetrics`、`ezraft::RaftTypeConfig`、`ezraft::ServerState`、
`ezraft::EzVote`、`ezraft::Node` 都是为这类场景重导出的 openraft 类型或底座自己的等价类型。

## 存储与快照

### `EzStorage`：持久化契约

`EzStorage` 是底座唯一要求上游（如果不用 `RedbStorage`）自己实现的 trait：三个方法——`load`（启动时读回
meta 与最近一次快照）、`persist`（写入一次 `Persist` 操作）、`read_logs`（按 `[start, end)` 范围读日志
条目）。Raft 的安全性建立在三条契约之上，任何一条被打破都可能丢失已确认的写入、或者在同一个 term 选出
两个 leader：

- **持久性**：`persist` 返回时，数据要能扛过*机器*断电，不只是进程退出。
- **顺序性**：`persist` 收到的操作按调用顺序变成持久的，后一个不能抢在前一个之前落盘。
- **读己之写**：`read_logs` 要能读到 `persist` 已经写下的一切，包括 `Persist::DeleteLogs` 删掉的部分
  ——删掉的条目此后不能再被读到。

`Persist` 的四个变体覆盖底座会要求持久化的全部内容：`Meta(EzMeta)`（term、投票、日志位置等 Raft 元数据）、
`LogEntry(EzEntry)`（一条日志条目，按 index 写入，同一个 index 再写一次视为替换）、
`Snapshot(EzSnapshot)`（整份快照，替换上一份）、`DeleteLogs { from, to }`（删除 `[from, to)` 范围内的
日志条目——既用于删除与 leader 冲突的尾部，也用于回收已经被快照覆盖的头部）。`Loaded { meta, snapshot }`
是 `load` 的返回值，首次运行时 `meta` 是 `EzMeta::default()`。

实现者会碰到的两个位置类型：`EzLogId` 是 `(term, index)` 二元组，即 `EzEntry::log_id` 的类型——
`read_logs` 与 `DeleteLogs` 的范围都按其中的 `index` 计；`EzSnapshotMeta` 是 `EzSnapshot::meta` 的类型
（openraft 的 `SnapshotMeta`：快照覆盖到的最后一条日志、当时的成员配置、快照 id），存储只需把它和快照
数据作为一个整体原样写入、原样读回，不需要解释其中任何字段。

```rust
use async_trait::async_trait;
use ezraft::EzEntry;
use ezraft::EzStorage;
use ezraft::Loaded;
use ezraft::Persist;

/// `EzStorage` 的最小合法形状——真正落盘请直接用 `RedbStorage`，自己实现只在
/// `RedbStorage` 不适用的场景才需要。
struct MemoryStorage;

#[async_trait]
impl EzStorage for MemoryStorage {
    async fn load(&mut self) -> Result<Loaded, std::io::Error> {
        Ok(Loaded {
            meta: Default::default(),
            snapshot: None,
        })
    }

    async fn persist(&mut self, _op: Persist) -> Result<(), std::io::Error> {
        Ok(())
    }

    async fn read_logs(&mut self, _start: u64, _end: u64) -> Result<Vec<EzEntry>, std::io::Error> {
        Ok(Vec::new())
    }
}
```

`RedbStorage`（见「启动集群」）是这个 trait 的生产实现，同时也是「这个契约具体怎么落实」的完整参考：每次
`persist` 是一个 `Durability::Immediate` 的 redb 写事务，一批 N 条日志加上随后的一次 meta 更新一共
N+1 次 `fsync`；`DeleteLogs` 用一次 redb 范围删除，不是逐条读了再删；快照的 meta 与 data 在同一个事务
里替换，不会有读者看到「新 meta、旧 data」这种半成品。

### 快照

快照把整个复制状态（用户的 KV，加上底座内部状态块）编码成 postcard 格式，前面带一个 6 字节的头（魔数
`EZKV` + 小端 `u16` 格式版本，当前是 `1`）——这个头在解析任何 postcard 内容*之前*先按字节手工检查，因为
postcard 是无自描述标签的定长位置编码，不知道版本就没法安全解出 body。读到一个不认识的版本号是一个
明确的错误：节点启动时读到会让 `create`/`join`/`join_as_learner` 直接失败，运行时安装一份这样的快照
则是该节点的致命错误，两种情况都会记结构化日志——底座绝不会用一个不认识的版本猜着往下解析，更不会
部分应用。

构建快照时，只在状态机的锁内做 O(1) 的操作：复制持久化 map 的根指针、复制内部状态块、记下
`last_applied` 与成员配置；真正的 postcard 编码在锁外、在阻塞线程池上进行，既不阻塞 `apply` 也不阻塞
`view()`。快照按日志条数触发（`EzConfig::snapshot_interval`），什么时候改发整份快照而不是逐条日志由
`replication_lag_threshold` 决定，详见「启动集群」一节里 `EzConfig` 的说明。构建与安装是并发的
（openraft 把构建放在独立任务里）：如果一次构建还在编码时 leader 发来的更新的快照先装好了，构建完成
后会发现已有的快照不比自己旧，于是丢弃自己的结果——存储里的快照永远不会倒退到日志已清理的位置之前。

两个运维用的方法：`EzKv::trigger_snapshot()` 不等阈值、立刻安排一次快照（返回时只是「已安排」，用
`wait_metrics` 等 `snapshot` 到达目标位置）；`EzKv::purge_log(upto_index)` 删除到 `upto_index` 为止、
且已被快照覆盖的日志条目。正常运行不需要调用它们，快照策略会自己做这两件事。

### 测试专用的网络故障注入

`ezraft::fault::{Faults, Fault, TrafficClass}` 只在这个 crate 自己启用了 `fault-injection` cargo
feature 时才存在（默认关闭，正常构建完全不含这部分代码，也不产生任何开销）。它是底座自己的验收测试用来
模拟丢包、延迟、网络分区的钩子，不是给上游生产代码使用的能力——上游需要的软状态同步、心跳一类的可靠性
机制，应当针对真实网络设计，而不是依赖这个测试专属的开关。

## 可观测性

底座在关键路径上用 `tracing` 打结构化日志，自身从不安装订阅者（subscriber）——`examples/kvstore.rs`
演示了怎么装一个：设置 `RUST_LOG=info`（或 `debug`）环境变量，再用 `tracing_subscriber` 初始化，就能看到
选举与换主（openraft 自身的日志）、写转发重试的每一次尝试、`Indeterminate`、`LocalLag`、watch 的每一次
`Reset`、快照的构建/发送/安装、以及内部端口上被拒绝的集群 token。库本身永远不替调用方安装订阅者，是否
输出、输出到哪里完全由上游决定。

## 错误参考表

| 类型 / 变体 | 含义 | 调用方应当怎么做 |
| --- | --- | --- |
| `LimitError::EmptyKey` | 某个条件或操作的 key 是空字符串 | 修正调用方自己传入的数据，不要重试 |
| `LimitError::EmptyPrefix` | `DeletePrefix` 的 `prefix` 是空字符串 | 同上 |
| `LimitError::KeyTooLarge { key, actual, max }` | 一个 key（或 `DeletePrefix` 的 `prefix`）超过 `max_key_bytes` | 同上，消息里带着具体大小 |
| `LimitError::ValueTooLarge { key, actual, max }` | 一个 `Put` 的 value 超过 `max_value_bytes` | 同上 |
| `LimitError::TooManyOps { actual, max }` | 事务的操作数超过 `max_txn_ops` | 拆成多个事务，或减少操作数 |
| `LimitError::TooManyConditions { actual, max }` | 事务的条件数超过 `max_txn_ops`（条件和操作各自计数，共用同一个上限） | 减少条件数 |
| `LimitError::NodeEntryTooLarge { actual, max }` | 节点注册表条目（`external_addr` 加 `meta`）超过 `max_value_bytes` | 见下方 `RegistryWriteError::LimitExceeded` |
| `LimitError::TooLarge { actual, max }` | 事务的总字节数超过 `max_txn_bytes` | 同上 |
| `Conflict { index, condition, key, current_mod_rev }` | 第 `index` 个条件不满足 | 重读 `key`（拿到 `current_mod_rev`），按新状态重建事务再写一次 |
| `WriteError::LimitExceeded(LimitError)` | 见上面几行 | 见上面几行 |
| `WriteError::Conflict(Conflict)` | 见上面一行 | 见上面一行 |
| `WriteError::Indeterminate { request_id }` | 到 `write_deadline` 还没有确定结果 | 用同一个 `request_id`（同一个 `txn`）再问，直到拿到确定结果 |
| `WriteError::LocalLag(LocalLag { revision })` | 已经提交，本节点应用还没追上 | 不是失败；用 `wait_revision(revision, timeout)` 等，或者接受「暂时读不到」 |
| `LeaderWriteError::LimitExceeded(LimitError)` | 见上面几行 | 见上面几行 |
| `LeaderWriteError::Conflict(Conflict)` | 见上面一行 | 见上面一行 |
| `LeaderWriteError::NotLeader { term }` | 这个 `LeaderCtx` 已经不在 `term` 里继续 leading 了——context 已经死了 | 不要重试；结束这次运行，交给下一次全新的 `LeaderCtx`（监督者通常已经在取消它） |
| `LeaderWriteError::Indeterminate { term }` | 仍在 `term` 里 leading，只是 deadline 内没等到确定答案 | **不要因此结束任务**：重读状态，需要的话用同样的 CAS 条件再写一次 |
| `ReadError::NoLeader` | 超时内没能确认到一个 leader | 值得在预算内重试，选举通常几个心跳就结束 |
| `ReadError::NotMember(u64)` | 这个节点已经不在当前成员配置里 | 重试解决不了 |
| `ReadError::NotCaughtUp { read_index, applied_index }` | 读位置已确认，但本地应用没在超时内追上 | 已提交，加大超时或稍后重试 |
| `RpcError::Timeout { node_id, name }` | 调用超时未获得答复 | 结果未知：远端 handler 可能仍在跑；是否重试取决于它是否幂等 |
| `RpcError::Unreachable { node_id, name }` | 连不上目标节点 | 请求没有送达；等一下再试或换节点 |
| `RpcError::NotMember(u64)` | 目标不在当前成员配置里 | 重试解决不了，先刷新成员信息 |
| `RpcError::Unauthorized(u64)` | 目标拒绝了集群 token | 检查配置，不要重试 |
| `RpcError::Transport { node_id, name, message }` | 请求可能已送达，但交换在途中坏掉（连接中断、非成功状态码、响应解不出来） | 结果未知，与 `Timeout` 同样对待 |
| `RpcError::Remote(String)` | 对端 RPC 层的确定答复，但不是类型化结果（未知名字/请求解码失败/handler 自身错误） | 按消息判断 |
| `RpcRegisterError::Reserved(String)` | 名字以 `ez.` 开头 | 换一个名字 |
| `RpcRegisterError::AlreadyRegistered(String)` | 这个名字已经注册过 | 检查是不是重复注册，不要用同名覆盖 |
| `RegistryWriteError::LimitExceeded(LimitError)` | `update_node_meta`：`external_addr` 加新的 `meta` 超过 `max_value_bytes`，没有提议任何东西 | 缩小 `meta`；大块数据放进 KV，`meta` 里只放指向它的 key |
| `RegistryWriteError::Indeterminate { request_id }` | `update_node_meta` 在 `write_deadline` 内没有确认结果 | 更新可能仍会生效；条目是整条覆盖的，重新调用一次总是安全的 |
| 其余方法（`create`/`join`/`serve`/`promote`/`demote`/`remove_node`/`transfer_leader`/`metrics`/`wait_metrics`/`trigger_snapshot`/`purge_log`/`shutdown` 等） | 返回 `std::io::Error`，包括与 I/O 本身无关的失败（例如在 follower 上调用 `promote`） | 消息里说明了具体原因；这些方法没有更细的类型化错误 |

## 给上游的使用约定

以下每一条都是决定，不是随意的风格建议——理由和它们各自的取舍写在旁边。

**接入节点信令热路径的读一律用本地视图（`EzKv::view`），旧数据的风险由写入时的 CAS 条件兜底。**
线性读要经过一轮向 leader 确认，延迟和吞吐都不如本地视图；热路径（参与者的信令交互）追求的是低延迟，
偶尔读到几十到几百毫秒之前的状态是可以接受的，只要写入的那一刻用 `mod_rev` 条件保护关键不变量（例如
「同一个 identity 在同一个房间只能有一个参与者」），旧数据就不会造成错误的写入，只会造成一次可以重试的
`Conflict`。

**RoomService 一类的读接口、以及任何「这个东西不存在」的否定判断，用线性读（`EzKv::linearizable_view`）。**
本地视图可能落后：在 A 节点写入、紧接着在 B 节点用本地视图读，B 未必已经看到。这对「是否存在」这类否定
判断尤其危险——把刚刚在别处创建好的房间误判为不存在，后果比多等一轮 RPC 严重得多，所以这类判断值得付出
线性读的代价。

**需要索引时，在写入数据的同一个事务里把索引 key 一并写进去，读侧用前缀扫描。** 底座本身不提供声明式
二级索引，`Txn` 的「全有或全无」保证了索引和它索引的数据永远不会不同步——不存在「数据写完了、索引还没写」
的中间状态，因为它们本来就是同一个事务的不同 `Op`。

**webhook 由上游基于 watch、一个 leader 专属任务和保存在 KV 里的续订游标一起搭建。** 用
`EzKv::spawn_leader_task` 注册一个任务：这个节点每次成为 leader 都会启动它一次，先从 KV 里读出上次
保存的游标 revision，用 `watch(prefix, After(cursor))`（第一次、还没有游标时用 `view_and_watch`）拿到
从那里续接、无缺口的变更流；每收到一组 `Changes`，按 revision 顺序把它们转换成业务事件（例如 LiveKit
标准签名的 POST）发出去，发送成功后用 `ctx.write` 把新游标（这组变更的 revision）写回 KV。发送逻辑
本身必须只在集群里的**一处**运行，否则同一个变更会被多个副本各自的 watch 各发一次；leader 专属任务
换届时自动迁移、旧 leader 上的运行自动取消，是「只有一处在跑」的第一层保证，但换届前后仍有一个旧运行
还没被取消完、新运行已经开始的短暂窗口——这个窗口靠游标本身的 CAS 条件收尾：把游标写回 KV 时带上
一个针对游标 key 的 `ModRevEq` 条件，这样哪怕两个 context 在这条窗口里都发出了同一次事件，游标也只会
被推进一次，旧运行迟来的一次写入会因为条件不再满足而变成 `Conflict`，不会把游标错误地写回旧值。
底座不提供事件日志或持久游标消费者：这条游标完全是上游自己的复制状态。

**媒体资源、负载、健康等 TTL 软状态，由上游在底座之上再建一层内存缓存，通过节点间 RPC 同步，不经 Raft。**
这类数据变化频繁、容忍短暂不一致、节点重启后可以重新上报重建——把它们塞进复制状态只会拖慢每一次写入的
共识延迟，却换不来它们本来就不需要的强一致性。这层缓存完全建立在已有的机制之上（`EzKv::rpc()`、
`EzKv::cluster()`、节点注册表），底座不内置这样一个「软状态汇总」组件。

**媒体面发生致命错误时，让进程直接退出。** 节点存活以 Raft 心跳为准——一个节点还在参与心跳，底座就认为
它是活的；如果 mediasoup 这类媒体面组件挂掉却让进程继续跑着回应心跳，底座会一直认为这个节点健康，直到
上游自己的存活探测（目前尚不存在）发现问题，而在那之前它可能一直在错误地接受本该失败的请求。让进程退出
把「节点存活」和「Raft 存活」重新对齐成同一件事，底座内置的存活判定（见「leader 专属任务与存活判定」）才能
如实反映这个节点是否真的还能干活。
