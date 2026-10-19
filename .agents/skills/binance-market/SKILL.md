---
name: binance-market
description: >-
  Prerequisite knowledge for Binance-style global 24-hour markets: UTC klines,
  continuous sessions, trade and depth streams, and snapshot resync. Use when
  modeling crypto or other round-the-clock venues, or when the user mentions
  币安、Binance、24小时、全球行情、BTCUSDT、深度、bookTicker.
---

> **中文说明**：做全球连续交易行情时先读。A 股时段不适用于这些市场。与 `docs/design.md` 冲突时以设计正文为准。

# 币安式连续行情

完成判据：能指出日线在 UTC 午夜切换、24 小时行情不是日线、深度断档要重新要快照，以及本地 `seq` 不是币安的 update id。

## 交易日

币安现货 7×24 成交，没有集合竞价、午休和收盘。进程不因为当地时间到 15:00 而停止写入。

K 线对齐 UTC：

| 周期 | 桶 |
|---|---|
| `1s` `1m` `5m` `15m` `1h` `4h` | 从 Unix 纪元按周期长度切 |
| `1d` | UTC 当天 00:00 到次日 00:00 |
| 周线 | UTC 周一 00:00。第一版不建周线 |
| 月线 | UTC 自然月。第一版不建月线 |

一根日线收盘，不等于过去 24 小时的涨跌。24 小时行情是随当前时刻滑动的窗口：最新价、窗口内高低、成交量、成交额。两者分开存。

没有下一笔成交时，写入者仍要按 UTC 水位写 `ClockTick`。水位越过桶结束时间，当前 K 线闭合。

## 代码

对外标识是 `BINANCE:BTCUSDT`。`BTCUSDT` 是基础币加报价币，不是 A 股数字代码。同一名字在不同交易所不是同一标的。

价格仍是定点整数。成交量用币的数量，不换成「手」。库内不保存复权。

## 币安推送里要接的流

| 流 | 本库 |
|---|---|
| `trade` / `aggTrade` | 成交，推动 K 线 |
| `bookTicker` | 最优买卖，只更新行情快照 |
| `depth` | 档位增量。断档后先取快照再续 |
| `kline` | 可用来对照。本库自己聚合，不把对方未闭合 K 线写进已闭合段 |
| `ticker` | 24 小时滚动行情，不是 `1d` K 线 |

深度增量带一组更新号。接入时先缓冲增量，再取快照。第一条增量必须盖住快照的最后更新号，之后每条都要接上上一条的末号。接不上就丢弃这段增量，重新要快照。这个更新号留在币安载荷里。QDB 的 `seq` 仍由本库写入者分配。

## 和 A 股的分界

`SSE` / `SZSE` / `BSE` 继续按上海交易日做日线，时段只用于空窗告警。`BINANCE` 的安静不是休市。空窗告警只在预期仍有成交时打开，不对连续市场套用 09:30–15:00。
