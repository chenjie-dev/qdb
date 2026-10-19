---
name: commit
description: Analyze staged/unstaged changes, auto-detect commit type, generate semantic commit message, and execute git commit. Use when the user says "/commit", "提交", or requests a code commit.
---

> **中文说明**：仓库自定义技能。分析暂存/未暂存改动，自动判定类型并生成 `type: 中文描述` 的提交信息，确认后执行 git commit；对应 `/commit` 和「提交」。规则与根目录 AGENTS.md 一致，不默认 push、不打 Tag。勿提交含真实密钥的 `.env`。

# 智能代码提交

## 触发方式

- `/commit` — 自动分析变更，生成 commit message 并提交
- `/commit 简要描述` — 结合用户描述 + 自动分析生成 commit message

## 完整流程

### Step 1: 检查变更

```bash
git status
git diff --stat
git diff --staged --stat
```

- 如果没有任何变更，告知用户无需提交并中止
- 如果有未暂存的变更，先执行 `git add -A`（排除 node_modules 等）

### Step 2: 分析变更内容

```bash
git diff --staged
```

阅读 diff 内容，识别：
- 修改了哪些文件
- 变更的性质（新功能、修复、优化、重构、文档等）
- 影响范围（哪个模块/功能）

### Step 3: 生成 Commit Message

根据变更内容自动选择类型前缀：

| 前缀 | 适用场景 | 示例 |
|------|---------|------|
| `feat:` | 新增功能、新页面、新接口 | `feat: 用户充值页面` |
| `fix:` | 修复 bug、修复显示异常 | `fix: 修复余额精度丢失` |
| `perf:` | 性能优化、缓存、索引 | `perf: Redis 缓存模型列表` |
| `refactor:` | 重构代码，不改变功能 | `refactor: 提取公共鉴权中间件` |
| `chore:` | 配置、依赖、脚手架、清理 | `chore: 更新 Docker 配置` |
| `docs:` | 文档、注释、更新日志 | `docs: 更新日志 v1.11.0` |
| `style:` | 样式调整、UI 微调 | `style: 调整侧边栏间距` |

**格式规范：**

```
{type}: {简洁中文描述}
```

- 标题行不超过 72 字符
- 使用中文描述
- 如果涉及多个变更，用 ` + ` 连接关键点（最多 3 个）
- 例：`feat: API转发超时控制 + 登录注册安全优化 + 手机号柔性绑定`

**多类型变更时**，选择最主要的类型作为前缀。优先级：`feat` > `fix` > `perf` > `refactor` > `chore`

### Step 4: 确认并提交

展示生成的 commit message 供用户确认，用户确认后执行：

```bash
git commit -m "{commit message}"
```

### Step 5: 推送

仅在用户明确要求 push 时执行。硬性约定见根目录 `AGENTS.md`。

```bash
git push
```

## 重要规则

1. **禁止打 Tag**
2. **中文描述** — commit message 使用中文
3. **确认后提交** — 生成 message 后须等用户确认（用户已说「提交一次」且未改文案时可直接提交）
4. **不默认 push** — 未明确要求则只 commit
5. **排除密钥** — 勿 `git add` 含真实 API Key 的 `.env`；可提交 `.env.example`
