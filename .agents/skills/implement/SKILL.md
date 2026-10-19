---
name: implement
description: "Implement a piece of work based on a spec or set of tickets."
disable-model-invocation: true
---

> **中文说明**：按 spec 或工单实现。在事先约定的 seam 上用 tdd 逐片推进，过程中频繁 typecheck、经常跑单文件测试，最后跑全量测试并用 code-review 复查，再提交到当前分支。

Implement the work described by the user in the spec or tickets.

Use /tdd where possible, at pre-agreed seams.

Run typechecking regularly, single test files regularly, and the full test suite once at the end.

Once done, use /code-review to review the work.

Commit your work to the current branch.
