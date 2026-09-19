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
