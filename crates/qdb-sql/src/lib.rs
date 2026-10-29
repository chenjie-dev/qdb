//! 只读 SQL 子集，以及 `CREATE TABLE`、`INSERT`（含 `EXPECT`）和按主键的 `DELETE`。
//!
//! 语法边界见 `docs/manual/sql.md`。`WHERE`、表达式、`CASE`、`GROUP BY`、`HAVING`、`DISTINCT`、`UNION` 和 `ORDER BY` 都会生效。
#![forbid(unsafe_code)]

use qdb_abi::{KlineBody, Period};
use qdb_store::{Cell, ColumnDef, ColumnType, Engine, IndexDef, StoreError, TableSchema};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SqlError {
    #[error("syntax: {0}")]
    Syntax(String),
    #[error("symbol not found")]
    NotFound,
    #[error("ambiguous symbol")]
    Ambiguous,
    #[error("limit {0} exceeds 100000")]
    Limit(u64),
    #[error("{0}")]
    Store(String),
}

/// 在一个 [`Engine`] 上跑 SQL。
///
/// [`SqlEngine::query`] 只读。`CREATE TABLE`、`INSERT` 和 `DELETE` 走 [`SqlEngine::execute`]，需要可写引擎。
/// 语法以 `docs/manual/sql.md` 为准。
pub struct SqlEngine<'a> {
    engine: &'a Engine,
}

/// 一次查询的文本结果。
///
/// 价格和时间保持定点整数的十进制文本，不转成浮点。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    /// 列名，顺序与 `rows` 里每一行一致。
    pub columns: Vec<String>,
    /// 每一行按列顺序排好的文本。没有命中时为空。
    pub rows: Vec<Vec<String>>,
}

impl<'a> SqlEngine<'a> {
    /// 绑定一个引擎，用来执行只读语句。
    ///
    /// # Arguments
    ///
    /// * `engine` - 可写或只读打开都可以。查询不会 `fsync`。
    pub fn new(engine: &'a Engine) -> Self {
        Self { engine }
    }

    /// 执行一条语句。`CREATE`、`INSERT` 和 `DELETE` 写入 WAL，其余交给 [`SqlEngine::query`]。
    ///
    /// 末尾分号会被去掉。写入语句在默认组提交下会 `fsync`。`INSERT ... EXPECT n` 在序号不符时不写 WAL。`DELETE ... IN` 在任一键不可见或列表有重复时也不写。
    ///
    /// # Arguments
    ///
    /// * `engine` - 可写引擎。只读打开时建表、插入、条件覆盖和删除都会失败。
    /// * `sql` - 一条语句。
    ///
    /// # Returns
    ///
    /// 查询返回行。建表成功时 `rows` 是 `created`，插入和条件覆盖是 `inserted`，删除是 `deleted`。
    ///
    /// # Errors
    ///
    /// * [`SqlError::Syntax`] - 语句无法解析，主键没写全，删的是引擎登记的表，或 `EXPECT` 的序号对不上（`syntax: seq conflict`）。
    /// * [`SqlError::Store`] - 引擎只读，或存储层返回了没有单独映射的错误。
    /// * [`SqlError::NotFound`] - 表不存在。主键当前没有可见行是 `syntax: key not found`，不是这个错误。
    pub fn execute(engine: &mut Engine, sql: &str) -> Result<QueryResult, SqlError> {
        let sql = sql.trim().trim_end_matches(';').trim();
        let tokens = tokenize(sql);
        let head = tokens
            .first()
            .map(|token| token.to_ascii_lowercase())
            .unwrap_or_default();
        if head == "create" {
            return exec_create(engine, &tokens);
        }
        if head == "insert" {
            return exec_insert(engine, &tokens);
        }
        if head == "delete" {
            return exec_delete(engine, &tokens);
        }
        if head == "expire" {
            return exec_expire(engine, &tokens);
        }
        SqlEngine::new(engine).query(sql)
    }

    /// 执行 `SELECT`、`SHOW` 或 `DESCRIBE`。不写 WAL。
    ///
    /// `WHERE` 在扫描之后过滤，可以写比较、四则运算、`CASE`、`IN` 和 `LIKE`。`GROUP BY` 每个分组一行，`HAVING` 再过滤分组。`DISTINCT` 去重。`UNION` 拼接结果后再排序。`ORDER BY` 在 `LIMIT` 之前排序。`LIMIT` 不能超过 100000。
    /// 不带 `symbol` 的 `ticks` 会多一列 `symbol`。`JOIN` 按 PostgreSQL 的内连接和外连接匹配，多表时列名是 `别名.列名`。
    ///
    /// # Arguments
    ///
    /// * `sql` - 一条只读语句。末尾分号会被去掉。
    ///
    /// # Errors
    ///
    /// * [`SqlError::Syntax`] - 语句无法解析，末尾带有 `SUBSCRIBE`，或 `CREATE`/`INSERT` 走到了这里。
    /// * [`SqlError::NotFound`] - `symbol` 不存在。
    /// * [`SqlError::Ambiguous`] - 裸代码对应多只证券。
    /// * [`SqlError::Limit`] - `LIMIT` 大于 100000。
    /// * [`SqlError::Store`] - 用户表不存在，或过滤类型不对。
    pub fn query(&self, sql: &str) -> Result<QueryResult, SqlError> {
        let sql = sql.trim().trim_end_matches(';').trim();
        if split_subscribe(sql)?.is_some() {
            return Err(SqlError::Syntax(
                "SUBSCRIBE follows in the client; use split_subscribe".into(),
            ));
        }
        let tokens = tokenize(sql);
        if tokens
            .first()
            .is_some_and(|token| token.eq_ignore_ascii_case("show"))
        {
            let mut listed = query_show(&tokens)?;
            for name in self.engine.table_names() {
                listed.rows.push(vec![name, String::new()]);
            }
            return Ok(listed);
        }
        if tokens.first().is_some_and(|token| {
            token.eq_ignore_ascii_case("describe") || token.eq_ignore_ascii_case("desc")
        }) {
            return self.query_user_describe(&tokens);
        }
        let stmt = parse(sql)?;
        self.execute_query(&stmt)
    }

    fn execute_query(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        if stmt.limit > 100_000 {
            return Err(SqlError::Limit(stmt.limit));
        }
        if stmt.union.is_empty() {
            return self.execute_stmt(stmt);
        }
        let mut combined = self.execute_stmt(&union_branch(stmt))?;
        for (all, branch) in &stmt.union {
            let next = self.execute_stmt(&union_branch(branch))?;
            if next.columns.len() != combined.columns.len() {
                return Err(SqlError::Syntax(
                    "each UNION query must have the same number of columns".into(),
                ));
            }
            combined.rows.extend(next.rows);
            if !all {
                let mut seen = std::collections::HashSet::new();
                combined.rows.retain(|row| seen.insert(row.clone()));
            }
        }
        if !stmt.order.is_empty() {
            sort_rows(&mut combined, &stmt.order)?;
        }
        combined.rows.truncate(stmt.limit as usize);
        Ok(combined)
    }

    fn execute_stmt(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        if stmt.limit > 100_000 {
            return Err(SqlError::Limit(stmt.limit));
        }
        if !stmt.joins.is_empty() && !stmt.aggregates.is_empty() {
            return Err(SqlError::Syntax(
                "aggregate is not supported on join".into(),
            ));
        }
        let mut fetch = stmt.clone();
        fetch.limit = 100_000;
        fetch.columns = vec!["*".into()];
        fetch.headings = vec!["*".into()];
        fetch.select_values = vec![None];
        fetch.aggregates.clear();
        fetch.union.clear();
        fetch.distinct = false;
        fetch.order.clear();
        fetch.group.clear();
        fetch.having = None;
        fetch.where_expr = None;
        let raw = if !fetch.joins.is_empty() {
            self.query_joined(&fetch)?
        } else {
            match fetch.table.as_str() {
                "klines" => self.query_klines(&fetch)?,
                "snapshots" => self.query_snapshots(&fetch)?,
                "symbols" => self.query_symbols(&fetch)?,
                "ticks" => self.query_ticks(&fetch)?,
                other => self.query_user(other, &fetch)?,
            }
        };
        self.finish(stmt, raw)
    }

    fn finish(&self, stmt: &Stmt, mut result: QueryResult) -> Result<QueryResult, SqlError> {
        if let Some(expr) = &stmt.where_expr {
            let mut kept = Vec::new();
            for row in result.rows {
                if eval_expr(expr, &result.columns, &row)?.is_true() {
                    kept.push(row);
                }
            }
            result.rows = kept;
        }
        if !stmt.aggregates.is_empty() || !stmt.group.is_empty() {
            result = group_rows(stmt, result)?;
        } else {
            result = project_rows(stmt, result)?;
        }
        if let Some(expr) = &stmt.having {
            let mut kept = Vec::new();
            for row in result.rows {
                if eval_expr(expr, &result.columns, &row)?.is_true() {
                    kept.push(row);
                }
            }
            result.rows = kept;
        }
        if stmt.distinct {
            let mut seen = std::collections::HashSet::new();
            result.rows.retain(|row| seen.insert(row.clone()));
        }
        if !stmt.order.is_empty() {
            sort_rows(&mut result, &stmt.order)?;
        }
        result.rows.truncate(stmt.limit as usize);
        Ok(result)
    }
}

/// 若 `SELECT` 以 `SUBSCRIBE` 结尾，返回去掉该关键字后的语句。
///
/// 关键字不区分大小写，必须是最后一个词。这是客户端的订阅读取，不是网络推送。
///
/// # Arguments
///
/// * `sql` - 一条语句。末尾分号会被去掉。
///
/// # Returns
///
/// 没有 `SUBSCRIBE` 时返回 `None`。有则返回剩余的 `SELECT` 文本。
///
/// # Errors
///
/// [`SqlError::Syntax`] - 末尾是 `SUBSCRIBE`，但语句不是 `SELECT`。
pub fn split_subscribe(sql: &str) -> Result<Option<String>, SqlError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let Some(pos) = trimmed.rfind(char::is_whitespace) else {
        return Ok(None);
    };
    let word = trimmed[pos..].trim();
    if !word.eq_ignore_ascii_case("subscribe") {
        return Ok(None);
    }
    let head = trimmed[..pos].trim();
    if !head
        .split_whitespace()
        .next()
        .is_some_and(|token| token.eq_ignore_ascii_case("select"))
    {
        return Err(SqlError::Syntax("subscribe requires select".into()));
    }
    Ok(Some(head.to_string()))
}

/// 这条 `SELECT` 的增量是否只由 [`Engine::poll`] 的变更决定。
///
/// 成交、报价和 K 线的列投影返回 `Ok(true)`。`symbols`、用户表、聚合、表达式、`UNION` 和连接要重算整表，返回 `Ok(false)`。
///
/// # Errors
///
/// 语句无法解析时返回 [`SqlError::Syntax`]。
pub fn follows_changes(sql: &str) -> Result<bool, SqlError> {
    Ok(incremental(&parse(sql)?))
}

fn incremental(stmt: &Stmt) -> bool {
    stmt.joins.is_empty()
        && stmt.aggregates.is_empty()
        && stmt.union.is_empty()
        && stmt.select_values.iter().all(|value| {
            value
                .as_ref()
                .is_none_or(|expr| matches!(expr, ValExpr::Column(_)))
        })
        && stmt.table != "symbols"
        && builtin_table(&stmt.table)
}

/// 把新变更投影成与这条 `SELECT` 相同的列。
///
/// 成交、报价和 K 线按 `changes` 逐条投影，同一价格的新成交也会返回。`symbols`、用户表、聚合、表达式和 `UNION` 不看单条变更的列，而是重算后返回 `seen` 里没有的行。`LIMIT` 不截断这里的增量。
///
/// # Arguments
///
/// * `engine` - 已经 `poll` 过这些变更的引擎。
/// * `sql` - 不含 `SUBSCRIBE` 的 `SELECT`。
/// * `changes` - [`Engine::poll`] 刚返回的变更。
/// * `seen` - 已经打印过的行。只用于 `symbols`、用户表和聚合。
///
/// # Returns
///
/// 这一批要追加打印的行，列顺序与 [`SqlEngine::query`] 一致。
///
/// # Errors
///
/// 与 [`SqlEngine::query`] 相同。过滤不成匹配时返回空向量，而不是 `NotFound`。
pub fn live_rows(
    engine: &Engine,
    sql: &str,
    changes: &[qdb_store::Change],
    seen: &[Vec<String>],
) -> Result<Vec<Vec<String>>, SqlError> {
    let stmt = parse(sql)?;
    if !incremental(&stmt) {
        let mut wide = stmt.clone();
        wide.limit = 100_000;
        let rows = SqlEngine::new(engine).execute_query(&wide)?.rows;
        return Ok(rows
            .into_iter()
            .filter(|row| !seen.iter().any(|old| old == row))
            .collect());
    }
    let mut out = Vec::new();
    for change in changes {
        if !change_matches(engine, &stmt, change) {
            continue;
        }
        for row in project_change(engine, &stmt, change) {
            if stmt.table == "snapshots" && out.last() == Some(&row) {
                continue;
            }
            out.push(row);
        }
    }
    Ok(out)
}

/// 这条 `SELECT` 当前能返回的行，最多 100000 行。
///
/// 订阅读取用它记住已经看见的 `symbols`、用户表和聚合，避免 `LIMIT` 把旧行再打印一遍。
///
/// # Arguments
///
/// * `engine` - 只读或可写引擎。本函数不写 WAL。
/// * `sql` - 不含 `SUBSCRIBE` 的 `SELECT`。
///
/// # Errors
///
/// 与 [`SqlEngine::query`] 相同。
pub fn captured_rows(engine: &Engine, sql: &str) -> Result<Vec<Vec<String>>, SqlError> {
    let mut stmt = parse(sql)?;
    stmt.limit = 100_000;
    Ok(SqlEngine::new(engine).execute_query(&stmt)?.rows)
}

fn builtin_table(table: &str) -> bool {
    matches!(table, "symbols" | "ticks" | "snapshots" | "klines")
}

fn change_matches(engine: &Engine, stmt: &Stmt, change: &qdb_store::Change) -> bool {
    if change.table != stmt.table {
        return false;
    }
    if let Some(symbol) = &stmt.symbol {
        if !symbol_matches(engine, symbol, &change.symbol) {
            return false;
        }
    }
    if stmt.table == "klines" {
        let Some(period) = stmt.period else {
            return false;
        };
        if change.period != period.as_str() {
            return false;
        }
    }
    if stmt.ts_from.is_some_and(|from| change.ts < from)
        || stmt.ts_to.is_some_and(|to| change.ts > to)
    {
        return false;
    }
    true
}

fn symbol_matches(engine: &Engine, filter: &str, change_symbol: &str) -> bool {
    if filter.eq_ignore_ascii_case(change_symbol) {
        return true;
    }
    match (engine.resolve(filter), engine.resolve(change_symbol)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

fn project_change(engine: &Engine, stmt: &Stmt, change: &qdb_store::Change) -> Vec<Vec<String>> {
    match stmt.table.as_str() {
        "ticks" => vec![tick_live_row(engine, stmt, change)],
        "snapshots" => snapshot_live_row(engine, stmt, change)
            .into_iter()
            .collect(),
        "klines" => kline_live_rows(engine, stmt, change),
        _ => Vec::new(),
    }
}

fn tick_live_row(engine: &Engine, stmt: &Stmt, change: &qdb_store::Change) -> Vec<String> {
    let columns = listed_columns(engine, "ticks", &stmt.columns).unwrap_or_default();
    let symbol_id = engine.resolve(&change.symbol).unwrap_or(0);
    let amount = engine
        .ticks(symbol_id)
        .iter()
        .find(|tick| tick.seq == change.seq)
        .map(|tick| tick.amount)
        .unwrap_or_else(|| change.price.saturating_mul(change.volume));
    project(
        &columns,
        &[
            ("seq", change.seq.to_string()),
            ("symbol_id", symbol_id.to_string()),
            ("symbol", change.symbol.clone()),
            ("ts", change.ts.to_string()),
            ("price", change.price.to_string()),
            ("volume", change.volume.to_string()),
            ("amount", amount.to_string()),
        ],
    )
}

fn snapshot_live_row(
    engine: &Engine,
    stmt: &Stmt,
    change: &qdb_store::Change,
) -> Option<Vec<String>> {
    let symbol_id = engine.resolve(&change.symbol).ok()?;
    let quote = engine.quote(symbol_id)?;
    let columns = listed_columns(engine, "snapshots", &stmt.columns).ok()?;
    Some(project(
        &columns,
        &snapshot_fields(symbol_id, &change.symbol, quote),
    ))
}

fn kline_live_rows(engine: &Engine, stmt: &Stmt, change: &qdb_store::Change) -> Vec<Vec<String>> {
    let Some(period) = stmt.period else {
        return Vec::new();
    };
    let Ok(symbol_id) = engine.resolve(&change.symbol) else {
        return Vec::new();
    };
    let bars = engine.klines(symbol_id, period);
    let columns = listed_columns(engine, "klines", &stmt.columns).unwrap_or_default();
    let symbol_text = change.symbol.clone();
    let period_id = i64::from(period.id());
    let period_name = period.as_str();
    let project_bar = |bar: &qdb_abi::KlineBody| -> Vec<String> {
        project(
            &columns,
            &[
                ("symbol_id", symbol_id.to_string()),
                ("period_id", period_id.to_string()),
                ("ts", bar.bucket_start_ns.to_string()),
                ("symbol", symbol_text.clone()),
                ("period", period_name.into()),
                ("open", bar.open.to_string()),
                ("high", bar.high.to_string()),
                ("low", bar.low.to_string()),
                ("close", bar.close.to_string()),
                ("volume", bar.volume.to_string()),
                ("amount", bar.amount.to_string()),
                ("seq", "0".into()),
            ],
        )
    };
    let mut rows = Vec::new();
    if let Some(bar) = bars.iter().find(|row| row.bucket_start_ns == change.ts) {
        rows.push(project_bar(bar));
    }
    // 跨桶时这一条变更只带上被闭合的桶。同一笔成交新开的桶也要打印。
    if let Some(open) = bars.last() {
        if open.bucket_start_ns != change.ts {
            rows.push(project_bar(open));
        }
    }
    rows
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Clone)]
struct JoinItem {
    kind: JoinKind,
    table: String,
    alias: String,
    on: Vec<(String, String)>,
}

#[derive(Clone)]
struct Stmt {
    columns: Vec<String>,
    headings: Vec<String>,
    select_values: Vec<Option<ValExpr>>,
    aggregates: Vec<Agg>,
    distinct: bool,
    table: String,
    alias: String,
    joins: Vec<JoinItem>,
    where_expr: Option<Expr>,
    group: Vec<ValExpr>,
    having: Option<Expr>,
    order: Vec<OrderKey>,
    union: Vec<(bool, Stmt)>,
    symbol: Option<String>,
    period: Option<Period>,
    ts_from: Option<i64>,
    ts_to: Option<i64>,
    eq_column: Option<String>,
    eq_value: Option<String>,
    limit: u64,
}

#[derive(Clone)]
struct Agg {
    func: String,
    column: String,
}

fn union_branch(stmt: &Stmt) -> Stmt {
    let mut bare = stmt.clone();
    bare.union.clear();
    bare.order.clear();
    bare.limit = 100_000;
    bare
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Copy)]
enum Arith {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Truth {
    True,
    False,
    Unknown,
}

impl Truth {
    fn is_true(self) -> bool {
        matches!(self, Truth::True)
    }

    fn not(self) -> Self {
        match self {
            Truth::True => Truth::False,
            Truth::False => Truth::True,
            Truth::Unknown => Truth::Unknown,
        }
    }
}

#[derive(Clone)]
enum Scalar {
    Null,
    Int(i128),
    Text(String),
}

#[derive(Clone)]
enum ValExpr {
    Column(String),
    Literal(String),
    Neg(Box<ValExpr>),
    Bin {
        op: Arith,
        left: Box<ValExpr>,
        right: Box<ValExpr>,
    },
    Case {
        arms: Vec<(Expr, ValExpr)>,
        else_arm: Option<Box<ValExpr>>,
    },
    Pred(Box<Expr>),
}

#[derive(Clone)]
enum Expr {
    Value(ValExpr),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    Cmp {
        left: Box<ValExpr>,
        op: Cmp,
        right: Box<ValExpr>,
    },
    Between {
        value: Box<ValExpr>,
        low: Box<ValExpr>,
        high: Box<ValExpr>,
    },
    IsNull {
        value: Box<ValExpr>,
        negated: bool,
    },
    In {
        value: Box<ValExpr>,
        list: Vec<ValExpr>,
        negated: bool,
    },
    Like {
        value: Box<ValExpr>,
        pattern: Box<ValExpr>,
        negated: bool,
    },
}

#[derive(Clone)]
struct OrderKey {
    name: String,
    desc: bool,
}

fn parse(sql: &str) -> Result<Stmt, SqlError> {
    let tokens = tokenize(sql);
    let mut index = 0;
    let mut stmt = parse_select(&tokens, &mut index)?;
    while tokens
        .get(index)
        .is_some_and(|token| token.eq_ignore_ascii_case("union"))
    {
        index += 1;
        let all = if tokens
            .get(index)
            .is_some_and(|token| token.eq_ignore_ascii_case("all"))
        {
            index += 1;
            true
        } else {
            false
        };
        stmt.union.push((all, parse_select(&tokens, &mut index)?));
    }
    parse_order_and_limit(&tokens, &mut index, &mut stmt)?;
    if let Some(token) = tokens.get(index) {
        return Err(SqlError::Syntax(format!("unexpected {token}")));
    }
    Ok(stmt)
}

fn parse_select(tokens: &[String], index: &mut usize) -> Result<Stmt, SqlError> {
    expect(tokens, index, "select")?;
    let distinct = if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("distinct"))
    {
        *index += 1;
        true
    } else {
        false
    };
    let mut columns = Vec::new();
    let mut headings = Vec::new();
    let mut select_values = Vec::new();
    let mut aggregates = Vec::new();
    if tokens.get(*index).map(String::as_str) == Some("*") {
        columns.push("*".to_string());
        headings.push("*".to_string());
        select_values.push(None);
        *index += 1;
    } else {
        loop {
            if agg_call(tokens, *index) {
                let func = tokens[*index].to_ascii_lowercase();
                *index += 2;
                let column = tokens
                    .get(*index)
                    .ok_or_else(|| SqlError::Syntax("agg arg".into()))?
                    .clone();
                *index += 1;
                expect(tokens, index, ")")?;
                let source = format!("{func}({})", column.to_ascii_lowercase());
                let heading = take_heading(tokens, index, &source)?;
                aggregates.push(Agg {
                    func,
                    column: column.to_ascii_lowercase(),
                });
                columns.push(source);
                headings.push(heading);
                select_values.push(None);
            } else {
                let value = parse_add(tokens, index)?;
                let source = render_val(&value);
                let heading = take_heading(tokens, index, &render_val_outer(&value))?;
                columns.push(source);
                headings.push(heading);
                select_values.push(Some(value));
            }
            if tokens.get(*index).map(String::as_str) == Some(",") {
                *index += 1;
                continue;
            }
            break;
        }
    }
    expect(tokens, index, "from")?;
    let table = tokens
        .get(*index)
        .ok_or_else(|| SqlError::Syntax("table".into()))?
        .to_ascii_lowercase();
    *index += 1;
    let alias = take_alias(tokens, index, &table)?;
    let mut joins = Vec::new();
    while let Some(kind) = take_join_kind(tokens, index)? {
        let join_table = tokens
            .get(*index)
            .ok_or_else(|| SqlError::Syntax("table".into()))?
            .to_ascii_lowercase();
        *index += 1;
        let join_alias = take_alias(tokens, index, &join_table)?;
        if kind == JoinKind::Cross
            && tokens
                .get(*index)
                .is_some_and(|token| token.eq_ignore_ascii_case("on"))
        {
            return Err(SqlError::Syntax("cross join cannot have on".into()));
        }
        let mut on = Vec::new();
        if kind != JoinKind::Cross {
            expect(tokens, index, "on")?;
            loop {
                let left = tokens
                    .get(*index)
                    .ok_or_else(|| SqlError::Syntax("on".into()))?
                    .to_ascii_lowercase();
                *index += 1;
                expect(tokens, index, "=")?;
                let right = tokens
                    .get(*index)
                    .ok_or_else(|| SqlError::Syntax("on".into()))?
                    .to_ascii_lowercase();
                *index += 1;
                on.push((left, right));
                if tokens
                    .get(*index)
                    .is_some_and(|token| token.eq_ignore_ascii_case("and"))
                {
                    *index += 1;
                    continue;
                }
                break;
            }
            if on.is_empty() {
                return Err(SqlError::Syntax("on".into()));
            }
        }
        joins.push(JoinItem {
            kind,
            table: join_table,
            alias: join_alias,
            on,
        });
    }
    let mut symbol = None;
    let mut period = None;
    let mut ts_from = None;
    let mut ts_to = None;
    let mut eq_column = None;
    let mut eq_value = None;
    let mut where_expr = None;
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("where"))
    {
        *index += 1;
        let parsed = parse_expr(tokens, index)?;
        if joins.is_empty() {
            let (residual, simple) = peel_scan_predicates(parsed);
            symbol = simple.symbol;
            period = simple.period;
            ts_from = simple.ts_from;
            ts_to = simple.ts_to;
            eq_column = simple.eq_column;
            eq_value = simple.eq_value;
            where_expr = residual;
        } else {
            where_expr = Some(parsed);
        }
    }
    let mut group = Vec::new();
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("group"))
    {
        *index += 1;
        expect(tokens, index, "by")?;
        loop {
            group.push(parse_add(tokens, index)?);
            if tokens.get(*index).map(String::as_str) == Some(",") {
                *index += 1;
                continue;
            }
            break;
        }
    }
    let mut having = None;
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("having"))
    {
        *index += 1;
        having = Some(parse_expr(tokens, index)?);
    }
    Ok(Stmt {
        columns,
        headings,
        select_values,
        aggregates,
        distinct,
        table,
        alias,
        joins,
        where_expr,
        group,
        having,
        order: Vec::new(),
        union: Vec::new(),
        symbol,
        period,
        ts_from,
        ts_to,
        eq_column,
        eq_value,
        limit: 1000,
    })
}

fn parse_order_and_limit(
    tokens: &[String],
    index: &mut usize,
    stmt: &mut Stmt,
) -> Result<(), SqlError> {
    let mut saw_limit = false;
    loop {
        if tokens
            .get(*index)
            .is_some_and(|token| token.eq_ignore_ascii_case("order"))
        {
            if !stmt.order.is_empty() {
                return Err(SqlError::Syntax("order".into()));
            }
            *index += 1;
            expect(tokens, index, "by")?;
            loop {
                let name = tokens
                    .get(*index)
                    .ok_or_else(|| SqlError::Syntax("order".into()))?
                    .to_ascii_lowercase();
                *index += 1;
                let desc = if tokens
                    .get(*index)
                    .is_some_and(|token| token.eq_ignore_ascii_case("desc"))
                {
                    *index += 1;
                    true
                } else if tokens
                    .get(*index)
                    .is_some_and(|token| token.eq_ignore_ascii_case("asc"))
                {
                    *index += 1;
                    false
                } else {
                    false
                };
                stmt.order.push(OrderKey { name, desc });
                if tokens.get(*index).map(String::as_str) == Some(",") {
                    *index += 1;
                    continue;
                }
                break;
            }
        } else if tokens
            .get(*index)
            .is_some_and(|token| token.eq_ignore_ascii_case("limit"))
        {
            if saw_limit {
                return Err(SqlError::Syntax("limit".into()));
            }
            *index += 1;
            stmt.limit = tokens
                .get(*index)
                .ok_or_else(|| SqlError::Syntax("limit".into()))?
                .parse()
                .map_err(|_| SqlError::Syntax("limit".into()))?;
            *index += 1;
            saw_limit = true;
        } else {
            break;
        }
    }
    Ok(())
}

impl<'a> SqlEngine<'a> {
    fn query_klines(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let symbol = stmt
            .symbol
            .clone()
            .ok_or_else(|| SqlError::Syntax("symbol required".into()))?;
        let symbol_id = map_resolve(self.engine.resolve(&symbol))?;
        let period = stmt
            .period
            .ok_or_else(|| SqlError::Syntax("period required".into()))?;
        if stmt.aggregates.is_empty() {
            let columns = listed_columns(self.engine, "klines", &stmt.columns)?;
            let (names, mut stored) = self.stored("klines")?;
            let id_text = symbol_id.to_string();
            stored.retain(|row| {
                field(&names, row, "symbol_id") == id_text
                    && field(&names, row, "period") == period.as_str()
                    && stmt
                        .ts_from
                        .map(|from| field_i64(&names, row, "ts") >= from)
                        .unwrap_or(true)
                    && stmt
                        .ts_to
                        .map(|to| field_i64(&names, row, "ts") <= to)
                        .unwrap_or(true)
            });
            stored.truncate(stmt.limit as usize);
            let rows = stored
                .iter()
                .map(|row| project_stored(&names, row, &columns))
                .collect();
            return Ok(QueryResult { columns, rows });
        }
        let mut rows = self.engine.klines(symbol_id, period);
        if let Some(from) = stmt.ts_from {
            rows.retain(|row| row.bucket_start_ns >= from);
        }
        if let Some(to) = stmt.ts_to {
            rows.retain(|row| row.bucket_start_ns <= to);
        }
        rows.truncate(stmt.limit as usize);
        {
            let columns = stmt
                .aggregates
                .iter()
                .map(|agg| format!("{}({})", agg.func, agg.column))
                .collect();
            let values = stmt
                .aggregates
                .iter()
                .map(|agg| aggregate(&rows, agg))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(QueryResult {
                columns,
                rows: vec![values],
            })
        }
    }

    fn query_snapshots(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let symbol = stmt
            .symbol
            .clone()
            .ok_or_else(|| SqlError::Syntax("symbol required".into()))?;
        let symbol_id = map_resolve(self.engine.resolve(&symbol))?;
        let columns = listed_columns(self.engine, "snapshots", &stmt.columns)?;
        let (names, mut stored) = self.stored("snapshots")?;
        let id_text = symbol_id.to_string();
        stored.retain(|row| field(&names, row, "symbol_id") == id_text);
        let Some(row) = stored.into_iter().next() else {
            return Err(SqlError::NotFound);
        };
        Ok(QueryResult {
            columns: columns.clone(),
            rows: vec![project_stored(&names, &row, &columns)],
        })
    }

    fn query_symbols(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let columns = listed_columns(self.engine, "symbols", &stmt.columns)?;
        let (names, mut stored) = self.stored("symbols")?;
        stored.truncate(stmt.limit as usize);
        let rows = stored
            .iter()
            .map(|row| project_stored(&names, row, &columns))
            .collect();
        Ok(QueryResult { columns, rows })
    }

    fn query_ticks(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        if let Some(symbol) = &stmt.symbol {
            map_resolve(self.engine.resolve(symbol))?;
        }
        let columns = listed_columns(self.engine, "ticks", &stmt.columns)?;
        let (names, mut stored) = self.stored("ticks")?;
        if let Some(symbol) = &stmt.symbol {
            let id_text = map_resolve(self.engine.resolve(symbol))?.to_string();
            stored.retain(|row| field(&names, row, "symbol_id") == id_text);
        }
        stored.retain(|row| {
            stmt.ts_from
                .map(|from| field_i64(&names, row, "ts") >= from)
                .unwrap_or(true)
                && stmt
                    .ts_to
                    .map(|to| field_i64(&names, row, "ts") <= to)
                    .unwrap_or(true)
        });
        stored.truncate(stmt.limit as usize);
        let rows = stored
            .iter()
            .map(|row| project_stored(&names, row, &columns))
            .collect();
        Ok(QueryResult { columns, rows })
    }

    fn stored(&self, table: &str) -> Result<(Vec<String>, Vec<Vec<String>>), SqlError> {
        self.engine
            .scan_table(table, None, None, 100_000)
            .map_err(map_store)
    }

    fn query_user(&self, table: &str, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let schema = self
            .engine
            .table_schema(table)
            .ok_or_else(|| SqlError::Syntax(format!("unknown table {table}")))?
            .clone();
        let filter = match (&stmt.eq_column, &stmt.eq_value) {
            (Some(column), Some(raw)) if schema.columns.iter().any(|item| item.name == *column) => {
                let definition = schema
                    .columns
                    .iter()
                    .find(|item| item.name == *column)
                    .ok_or_else(|| SqlError::Syntax(format!("unknown column {column}")))?;
                Some((column.clone(), parse_cell(definition.ty, raw)?))
            }
            _ => None,
        };
        let (columns, rows) = self
            .engine
            .scan_table(
                table,
                filter.as_ref().map(|(column, _)| column.as_str()),
                filter.as_ref().map(|(_, cell)| cell),
                stmt.limit as usize,
            )
            .map_err(map_store)?;
        Ok(QueryResult { columns, rows })
    }

    fn query_joined(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let mut names = Vec::new();
        names.push(stmt.alias.clone());
        for join in &stmt.joins {
            if names.iter().any(|name| name == &join.alias) {
                return Err(SqlError::Syntax(format!(
                    "table name \"{}\" specified more than once",
                    join.alias
                )));
            }
            names.push(join.alias.clone());
        }
        let base = self.load_rel(&stmt.table)?;
        let mut cols = qualify(&stmt.alias, &base.columns);
        let mut rows = base.rows;
        for join in &stmt.joins {
            let right = self.load_rel(&join.table)?;
            let right_cols = qualify(&join.alias, &right.columns);
            let joined = apply_join(&cols, &rows, join, &right_cols, &right.rows)?;
            cols.extend(right_cols);
            rows = joined;
        }
        Ok(QueryResult {
            columns: cols,
            rows,
        })
    }

    fn load_rel(&self, table: &str) -> Result<LoadedRel, SqlError> {
        if self.engine.table_schema(table).is_none() {
            return Err(SqlError::Syntax(format!("unknown table {table}")));
        }
        let (columns, rows) = self
            .engine
            .scan_table(table, None, None, 100_000)
            .map_err(map_store)?;
        Ok(LoadedRel { columns, rows })
    }

    fn query_user_describe(&self, tokens: &[String]) -> Result<QueryResult, SqlError> {
        if tokens.len() != 2 {
            return Err(SqlError::Syntax("describe table".into()));
        }
        let table = tokens[1].trim_matches('\'').to_ascii_lowercase();
        if let Some(schema) = self.engine.table_schema(&table) {
            let rows = schema
                .columns
                .iter()
                .map(|column| {
                    let mut note = column.ty.as_str().to_string();
                    if column.primary_key {
                        note.push_str(" PRIMARY KEY");
                    }
                    if let Some(index) = schema
                        .indexes
                        .iter()
                        .find(|index| index.column == column.name)
                    {
                        note.push_str(" INDEX ");
                        note.push_str(&index.name);
                    }
                    row(&column.name, &note)
                })
                .collect();
            return Ok(QueryResult {
                columns: vec!["column".into(), "note".into()],
                rows,
            });
        }
        query_describe(tokens)
    }
}

fn aggregate(rows: &[KlineBody], agg: &Agg) -> Result<String, SqlError> {
    if rows.is_empty() {
        return Ok(String::new());
    }
    let nums: Vec<i128> = rows
        .iter()
        .map(|row| kline_i128(row, &agg.column))
        .collect();
    let ordered = rows;
    match agg.func.as_str() {
        "max" => Ok(nums.iter().copied().max().unwrap_or(0).to_string()),
        "min" => Ok(nums.iter().copied().min().unwrap_or(0).to_string()),
        "sum" => Ok(nums.iter().sum::<i128>().to_string()),
        "count" => Ok(rows.len().to_string()),
        "avg" => {
            let sum: i128 = nums.iter().sum();
            let count = nums.len() as i128;
            Ok(format_avg(sum, count))
        }
        "first" => Ok(kline_value(&ordered[0], &agg.column)),
        "last" => Ok(kline_value(ordered.last().unwrap(), &agg.column)),
        other => Err(SqlError::Syntax(format!("aggregate {other}"))),
    }
}

fn format_avg(sum: i128, count: i128) -> String {
    if count == 0 {
        return "0".into();
    }
    let whole = sum / count;
    let rem = (sum % count).abs();
    if rem == 0 {
        return whole.to_string();
    }
    let mut digits = String::new();
    let mut rest = rem;
    for _ in 0..4 {
        rest *= 10;
        digits.push(char::from(b'0' + (rest / count) as u8));
        rest %= count;
        if rest == 0 {
            break;
        }
    }
    format!("{whole}.{digits}")
}

fn listed_columns(
    engine: &Engine,
    table: &str,
    requested: &[String],
) -> Result<Vec<String>, SqlError> {
    let schema = engine
        .table_schema(table)
        .ok_or_else(|| SqlError::Syntax(format!("unknown table {table}")))?;
    if requested == ["*"] {
        return Ok(schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect());
    }
    for name in requested {
        if !schema.columns.iter().any(|column| &column.name == name) {
            return Err(SqlError::Syntax(format!("unknown column {name}")));
        }
    }
    Ok(requested.to_vec())
}

fn field<'a>(names: &[String], row: &'a [String], name: &str) -> &'a str {
    names
        .iter()
        .position(|column| column == name)
        .and_then(|index| row.get(index))
        .map(String::as_str)
        .unwrap_or("")
}

fn field_i64(names: &[String], row: &[String], name: &str) -> i64 {
    field(names, row, name).parse().unwrap_or(0)
}

fn project_stored(names: &[String], row: &[String], requested: &[String]) -> Vec<String> {
    requested
        .iter()
        .map(|name| field(names, row, name).to_string())
        .collect()
}

fn project(columns: &[String], fields: &[(&str, String)]) -> Vec<String> {
    columns
        .iter()
        .map(|name| {
            fields
                .iter()
                .find(|(field, _)| *field == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        })
        .collect()
}

fn snapshot_fields(
    symbol_id: u32,
    symbol: &str,
    quote: &qdb_store::QuoteRow,
) -> Vec<(&'static str, String)> {
    let mut fields = vec![
        ("symbol_id", symbol_id.to_string()),
        ("symbol", symbol.to_string()),
        ("last", quote.last.to_string()),
        ("seq", quote.seq.to_string()),
    ];
    fields.extend([
        ("bid_px1", quote.bid_px[0].to_string()),
        ("bid_qty1", quote.bid_qty[0].to_string()),
        ("ask_px1", quote.ask_px[0].to_string()),
        ("ask_qty1", quote.ask_qty[0].to_string()),
        ("bid_px2", quote.bid_px[1].to_string()),
        ("bid_qty2", quote.bid_qty[1].to_string()),
        ("ask_px2", quote.ask_px[1].to_string()),
        ("ask_qty2", quote.ask_qty[1].to_string()),
        ("bid_px3", quote.bid_px[2].to_string()),
        ("bid_qty3", quote.bid_qty[2].to_string()),
        ("ask_px3", quote.ask_px[2].to_string()),
        ("ask_qty3", quote.ask_qty[2].to_string()),
        ("bid_px4", quote.bid_px[3].to_string()),
        ("bid_qty4", quote.bid_qty[3].to_string()),
        ("ask_px4", quote.ask_px[3].to_string()),
        ("ask_qty4", quote.ask_qty[3].to_string()),
        ("bid_px5", quote.bid_px[4].to_string()),
        ("bid_qty5", quote.bid_qty[4].to_string()),
        ("ask_px5", quote.ask_px[4].to_string()),
        ("ask_qty5", quote.ask_qty[4].to_string()),
    ]);
    fields
}

fn kline_value(row: &KlineBody, column: &str) -> String {
    match column {
        "ts" | "bucket_start_ns" => row.bucket_start_ns.to_string(),
        "open" => row.open.to_string(),
        "high" => row.high.to_string(),
        "low" => row.low.to_string(),
        "close" => row.close.to_string(),
        "volume" => row.volume.to_string(),
        "amount" => row.amount.to_string(),
        "seq" => "0".into(),
        _ => String::new(),
    }
}

fn kline_i128(row: &KlineBody, column: &str) -> i128 {
    match column {
        "open" => row.open as i128,
        "high" => row.high as i128,
        "low" => row.low as i128,
        "close" => row.close as i128,
        "volume" => row.volume as i128,
        "amount" => row.amount as i128,
        _ => 0,
    }
}

fn exec_create(engine: &mut Engine, tokens: &[String]) -> Result<QueryResult, SqlError> {
    let mut index = 0;
    expect(tokens, &mut index, "create")?;
    expect(tokens, &mut index, "table")?;
    let name = tokens
        .get(index)
        .ok_or_else(|| SqlError::Syntax("table".into()))?
        .to_ascii_lowercase();
    index += 1;
    expect(tokens, &mut index, "(")?;
    let mut columns = Vec::new();
    let mut indexes = Vec::new();
    while tokens.get(index).is_some_and(|token| token != ")") {
        if tokens[index].eq_ignore_ascii_case("index") {
            index += 1;
            let index_name = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("index".into()))?
                .to_ascii_lowercase();
            index += 1;
            expect(tokens, &mut index, "(")?;
            let column = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("index column".into()))?
                .to_ascii_lowercase();
            index += 1;
            expect(tokens, &mut index, ")")?;
            indexes.push(IndexDef {
                name: index_name,
                column,
            });
        } else {
            let column_name = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("column".into()))?
                .to_ascii_lowercase();
            index += 1;
            let type_name = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("type".into()))?;
            let ty = ColumnType::parse(&type_name.to_ascii_uppercase())
                .ok_or_else(|| SqlError::Syntax(format!("unknown type {type_name}")))?;
            index += 1;
            let mut primary_key = false;
            if tokens
                .get(index)
                .is_some_and(|token| token.eq_ignore_ascii_case("primary"))
            {
                index += 1;
                expect(tokens, &mut index, "key")?;
                primary_key = true;
            }
            columns.push(ColumnDef {
                name: column_name,
                ty,
                primary_key,
            });
        }
        if tokens.get(index).is_some_and(|token| token == ",") {
            index += 1;
        }
    }
    expect(tokens, &mut index, ")")?;
    engine
        .create_table(TableSchema {
            name,
            columns,
            indexes,
        })
        .map_err(map_store)?;
    Ok(QueryResult {
        columns: vec!["result".into()],
        rows: vec![vec!["created".into()]],
    })
}

fn exec_insert(engine: &mut Engine, tokens: &[String]) -> Result<QueryResult, SqlError> {
    let mut index = 0;
    expect(tokens, &mut index, "insert")?;
    expect(tokens, &mut index, "into")?;
    let name = tokens
        .get(index)
        .ok_or_else(|| SqlError::Syntax("table".into()))?
        .to_ascii_lowercase();
    index += 1;
    expect(tokens, &mut index, "values")?;
    expect(tokens, &mut index, "(")?;
    let schema = engine
        .table_schema(&name)
        .cloned()
        .ok_or(SqlError::NotFound)?;
    let mut raws = Vec::new();
    while tokens.get(index).is_some_and(|token| token != ")") {
        let token = tokens
            .get(index)
            .ok_or_else(|| SqlError::Syntax("value".into()))?;
        if token == "-" {
            index += 1;
            let number = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("value".into()))?;
            raws.push(format!("-{}", unquote(number)));
            index += 1;
        } else {
            raws.push(unquote(token));
            index += 1;
        }
        if tokens.get(index).is_some_and(|token| token == ",") {
            index += 1;
        }
    }
    expect(tokens, &mut index, ")")?;
    let mut expected = None;
    let mut expire_at = None;
    loop {
        let Some(token) = tokens.get(index) else {
            break;
        };
        if token.eq_ignore_ascii_case("expect") {
            index += 1;
            expected = Some(read_u64(tokens, &mut index)?);
            continue;
        }
        if token.eq_ignore_ascii_case("expire") {
            index += 1;
            expect(tokens, &mut index, "at")?;
            expire_at = Some(read_i64(tokens, &mut index)?);
            continue;
        }
        return Err(SqlError::Syntax("value".into()));
    }
    if raws.len() != schema.columns.len() {
        return Err(SqlError::Syntax("column count".into()));
    }
    let values = raws
        .iter()
        .zip(&schema.columns)
        .map(|(raw, column)| parse_cell(column.ty, raw))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(seq) = expected {
        engine
            .insert_timed(&name, &values, Some(seq), expire_at)
            .map_err(map_store)?;
    } else if expire_at.is_some() {
        engine
            .insert_timed(&name, &values, None, expire_at)
            .map_err(map_store)?;
    } else {
        engine.insert_row(&name, &values).map_err(map_store)?;
    }
    Ok(QueryResult {
        columns: vec!["result".into()],
        rows: vec![vec!["inserted".into()]],
    })
}

fn read_u64(tokens: &[String], index: &mut usize) -> Result<u64, SqlError> {
    let raw = tokens
        .get(*index)
        .ok_or_else(|| SqlError::Syntax("seq".into()))?;
    let seq = raw
        .parse::<u64>()
        .map_err(|_| SqlError::Syntax("seq".into()))?;
    *index += 1;
    Ok(seq)
}

fn read_i64(tokens: &[String], index: &mut usize) -> Result<i64, SqlError> {
    let token = tokens
        .get(*index)
        .ok_or_else(|| SqlError::Syntax("expire".into()))?;
    let raw = if token == "-" {
        *index += 1;
        let number = tokens
            .get(*index)
            .ok_or_else(|| SqlError::Syntax("expire".into()))?;
        format!("-{}", unquote(number))
    } else {
        unquote(token)
    };
    *index += 1;
    raw.parse::<i64>()
        .map_err(|_| SqlError::Syntax("expire".into()))
}

fn exec_expire(engine: &mut Engine, tokens: &[String]) -> Result<QueryResult, SqlError> {
    let mut index = 0;
    expect(tokens, &mut index, "expire")?;
    expect(tokens, &mut index, "at")?;
    let deadline = read_i64(tokens, &mut index)?;
    if index != tokens.len() {
        return Err(SqlError::Syntax("expire".into()));
    }
    engine.expire_at(deadline).map_err(map_store)?;
    Ok(QueryResult {
        columns: vec!["result".into()],
        rows: vec![vec!["expired".into()]],
    })
}

fn exec_delete(engine: &mut Engine, tokens: &[String]) -> Result<QueryResult, SqlError> {
    let mut index = 0;
    expect(tokens, &mut index, "delete")?;
    expect(tokens, &mut index, "from")?;
    let name = tokens
        .get(index)
        .ok_or_else(|| SqlError::Syntax("table".into()))?
        .to_ascii_lowercase();
    index += 1;
    expect(tokens, &mut index, "where")?;
    let schema = engine
        .table_schema(&name)
        .cloned()
        .ok_or(SqlError::NotFound)?;
    if tokens
        .get(index + 1)
        .is_some_and(|token| token.eq_ignore_ascii_case("in"))
    {
        return exec_delete_in(engine, tokens, &name, &schema, index);
    }
    let mut pairs = Vec::new();
    loop {
        let column = tokens
            .get(index)
            .ok_or_else(|| SqlError::Syntax("column".into()))?
            .to_ascii_lowercase();
        index += 1;
        expect(tokens, &mut index, "=")?;
        let token = tokens
            .get(index)
            .ok_or_else(|| SqlError::Syntax("value".into()))?;
        let raw = if token == "-" {
            index += 1;
            let number = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("value".into()))?;
            index += 1;
            format!("-{}", unquote(number))
        } else {
            index += 1;
            unquote(token)
        };
        pairs.push((column, raw));
        if tokens
            .get(index)
            .is_some_and(|token| token.eq_ignore_ascii_case("and"))
        {
            index += 1;
            continue;
        }
        break;
    }
    if index != tokens.len() {
        return Err(SqlError::Syntax("primary key required".into()));
    }
    let mut seen = std::collections::HashSet::new();
    if pairs.iter().any(|(name, _)| !seen.insert(name.clone())) {
        return Err(SqlError::Syntax("primary key required".into()));
    }
    let pk: Vec<&ColumnDef> = schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .collect();
    if pk.is_empty() || pairs.len() != pk.len() {
        return Err(SqlError::Syntax("primary key required".into()));
    }
    let mut key = Vec::with_capacity(pk.len());
    for column in &pk {
        let raw = pairs
            .iter()
            .find(|(name, _)| name == &column.name)
            .map(|(_, raw)| raw.as_str())
            .ok_or_else(|| SqlError::Syntax("primary key required".into()))?;
        key.push(parse_cell(column.ty, raw)?);
    }
    if pairs
        .iter()
        .any(|(name, _)| pk.iter().all(|column| column.name != *name))
    {
        return Err(SqlError::Syntax("primary key required".into()));
    }
    engine.delete_row(&name, &key).map_err(map_store)?;
    Ok(QueryResult {
        columns: vec!["result".into()],
        rows: vec![vec!["deleted".into()]],
    })
}

fn exec_delete_in(
    engine: &mut Engine,
    tokens: &[String],
    name: &str,
    schema: &TableSchema,
    mut index: usize,
) -> Result<QueryResult, SqlError> {
    let column = tokens
        .get(index)
        .ok_or_else(|| SqlError::Syntax("column".into()))?
        .to_ascii_lowercase();
    index += 1;
    expect(tokens, &mut index, "in")?;
    expect(tokens, &mut index, "(")?;
    let pk: Vec<&ColumnDef> = schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .collect();
    if pk.len() != 1 || pk[0].name != column {
        return Err(SqlError::Syntax("primary key required".into()));
    }
    let mut raws = Vec::new();
    while tokens.get(index).is_some_and(|token| token != ")") {
        let token = tokens
            .get(index)
            .ok_or_else(|| SqlError::Syntax("value".into()))?;
        let raw = if token == "-" {
            index += 1;
            let number = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("value".into()))?;
            index += 1;
            format!("-{}", unquote(number))
        } else {
            index += 1;
            unquote(token)
        };
        raws.push(raw);
        if tokens.get(index).is_some_and(|token| token == ",") {
            index += 1;
        }
    }
    expect(tokens, &mut index, ")")?;
    if index != tokens.len() || raws.is_empty() {
        return Err(SqlError::Syntax("primary key required".into()));
    }
    let keys = raws
        .iter()
        .map(|raw| parse_cell(pk[0].ty, raw).map(|cell| vec![cell]))
        .collect::<Result<Vec<_>, _>>()?;
    engine.delete_rows(name, &keys).map_err(map_store)?;
    Ok(QueryResult {
        columns: vec!["result".into()],
        rows: vec![vec!["deleted".into()]],
    })
}

fn parse_cell(ty: ColumnType, raw: &str) -> Result<Cell, SqlError> {
    match ty {
        ColumnType::Int64 => raw
            .parse::<i64>()
            .map(Cell::Int64)
            .map_err(|_| SqlError::Syntax("type mismatch".into())),
        ColumnType::Bool => match raw.to_ascii_lowercase().as_str() {
            "true" => Ok(Cell::Bool(true)),
            "false" => Ok(Cell::Bool(false)),
            _ => Err(SqlError::Syntax("type mismatch".into())),
        },
        ColumnType::String => Ok(Cell::String(raw.to_string())),
    }
}

fn map_store(err: StoreError) -> SqlError {
    match err {
        StoreError::NotFound => SqlError::NotFound,
        StoreError::TableExists => SqlError::Syntax("table already exists".into()),
        StoreError::DuplicateKey => SqlError::Syntax("duplicate key".into()),
        StoreError::KeyNotFound => SqlError::Syntax("key not found".into()),
        StoreError::Conflict => SqlError::Syntax("seq conflict".into()),
        StoreError::TypeMismatch => SqlError::Syntax("type mismatch".into()),
        StoreError::Schema(message) => SqlError::Syntax(message),
        other => SqlError::Store(other.to_string()),
    }
}

fn query_show(tokens: &[String]) -> Result<QueryResult, SqlError> {
    if tokens.len() != 2 || !tokens[1].eq_ignore_ascii_case("tables") {
        return Err(SqlError::Syntax("show tables".into()));
    }
    Ok(QueryResult {
        columns: vec!["table".into(), "filter".into()],
        rows: vec![
            vec!["symbols".into(), String::new()],
            vec!["ticks".into(), String::new()],
            vec!["snapshots".into(), "symbol".into()],
            vec!["klines".into(), "symbol, period".into()],
        ],
    })
}

fn query_describe(tokens: &[String]) -> Result<QueryResult, SqlError> {
    if tokens.len() != 2 {
        return Err(SqlError::Syntax("describe table".into()));
    }
    let table = tokens[1].trim_matches('\'').to_ascii_lowercase();
    let rows = match table.as_str() {
        "symbols" => vec![
            row("symbol", "交易所:代码"),
            row("exchange", "内部交易所编号"),
        ],
        "ticks" => vec![
            row("ts", "事件时间，UTC 纳秒"),
            row("price", "成交价，定点整数"),
            row("volume", "成交量"),
            row("seq", "成交序号"),
        ],
        "snapshots" => vec![
            row("last", "最新价"),
            row("bid_px1", "买一价"),
            row("ask_px1", "卖一价"),
            row("seq", "报价序号"),
        ],
        "klines" => vec![
            row("ts", "桶起点，UTC 纳秒"),
            row("open", "开盘价"),
            row("high", "最高价"),
            row("low", "最低价"),
            row("close", "收盘价"),
            row("volume", "成交量"),
            row("amount", "成交额"),
            row("seq", "当前查询恒为 0"),
        ],
        _ => return Err(SqlError::Syntax(format!("unknown table {table}"))),
    };
    Ok(QueryResult {
        columns: vec!["column".into(), "note".into()],
        rows,
    })
}

fn row(column: &str, note: &str) -> Vec<String> {
    vec![column.into(), note.into()]
}

fn map_resolve(result: Result<u32, StoreError>) -> Result<u32, SqlError> {
    match result {
        Ok(id) => Ok(id),
        Err(StoreError::NotFound) => Err(SqlError::NotFound),
        Err(StoreError::Ambiguous) => Err(SqlError::Ambiguous),
        Err(err) => Err(SqlError::Store(err.to_string())),
    }
}

/// 连接补出来的空列。文本结果里显示成空字符串，但不再和真正的空字符串相等。
const SQL_NULL: &str = "\u{0}";

struct LoadedRel {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

fn take_alias(tokens: &[String], index: &mut usize, table: &str) -> Result<String, SqlError> {
    let Some(token) = tokens.get(*index) else {
        return Ok(table.to_string());
    };
    if token.eq_ignore_ascii_case("as") {
        *index += 1;
        let alias = tokens
            .get(*index)
            .ok_or_else(|| SqlError::Syntax("alias".into()))?
            .to_ascii_lowercase();
        *index += 1;
        return Ok(alias);
    }
    if clause_word(token) {
        return Ok(table.to_string());
    }
    *index += 1;
    Ok(token.to_ascii_lowercase())
}

fn clause_word(token: &str) -> bool {
    matches!(
        token.to_ascii_lowercase().as_str(),
        "where"
            | "join"
            | "inner"
            | "left"
            | "right"
            | "full"
            | "cross"
            | "order"
            | "group"
            | "limit"
            | "on"
            | "outer"
            | "union"
            | "having"
            | "distinct"
    )
}

fn take_join_kind(tokens: &[String], index: &mut usize) -> Result<Option<JoinKind>, SqlError> {
    let Some(token) = tokens.get(*index) else {
        return Ok(None);
    };
    match token.to_ascii_lowercase().as_str() {
        "join" => {
            *index += 1;
            Ok(Some(JoinKind::Inner))
        }
        "inner" => {
            *index += 1;
            expect(tokens, index, "join")?;
            Ok(Some(JoinKind::Inner))
        }
        "left" => {
            *index += 1;
            skip_outer(tokens, index);
            expect(tokens, index, "join")?;
            Ok(Some(JoinKind::Left))
        }
        "right" => {
            *index += 1;
            skip_outer(tokens, index);
            expect(tokens, index, "join")?;
            Ok(Some(JoinKind::Right))
        }
        "full" => {
            *index += 1;
            skip_outer(tokens, index);
            expect(tokens, index, "join")?;
            Ok(Some(JoinKind::Full))
        }
        "cross" => {
            *index += 1;
            expect(tokens, index, "join")?;
            Ok(Some(JoinKind::Cross))
        }
        _ => Ok(None),
    }
}

fn skip_outer(tokens: &[String], index: &mut usize) {
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("outer"))
    {
        *index += 1;
    }
}

fn qualify(alias: &str, columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|column| format!("{alias}.{column}"))
        .collect()
}

fn show_cell(cell: &str) -> String {
    if cell == SQL_NULL {
        String::new()
    } else {
        cell.to_string()
    }
}

fn resolve_column(columns: &[String], name: &str) -> Result<usize, SqlError> {
    let name = name.to_ascii_lowercase();
    if let Some(index) = columns.iter().position(|column| column == &name) {
        return Ok(index);
    }
    let hits: Vec<usize> = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| {
            column
                .rsplit_once('.')
                .is_some_and(|(_, bare)| bare == name)
        })
        .map(|(index, _)| index)
        .collect();
    match hits.as_slice() {
        [index] => Ok(*index),
        [] => Err(SqlError::Syntax(format!(
            "column \"{name}\" does not exist"
        ))),
        _ => Err(SqlError::Syntax(format!("column \"{name}\" is ambiguous"))),
    }
}

fn apply_join(
    left_cols: &[String],
    left_rows: &[Vec<String>],
    join: &JoinItem,
    right_cols: &[String],
    right_rows: &[Vec<String>],
) -> Result<Vec<Vec<String>>, SqlError> {
    if join.kind == JoinKind::Cross {
        let mut out = Vec::new();
        for left in left_rows {
            for right in right_rows {
                let mut row = left.clone();
                row.extend(right.clone());
                out.push(row);
            }
        }
        return Ok(out);
    }
    let (left_at, right_at) = on_sides(left_cols, right_cols, &join.on)?;
    let mut index: std::collections::HashMap<Vec<String>, Vec<usize>> =
        std::collections::HashMap::new();
    for (row_index, row) in right_rows.iter().enumerate() {
        if let Some(key) = key_of(row, &right_at) {
            index.entry(key).or_default().push(row_index);
        }
    }
    let mut matched = vec![false; right_rows.len()];
    let mut out = Vec::new();
    for left in left_rows {
        let hits = key_of(left, &left_at).and_then(|key| index.get(&key).cloned());
        if let Some(hits) = hits {
            for hit in hits {
                matched[hit] = true;
                let mut row = left.clone();
                row.extend(right_rows[hit].clone());
                out.push(row);
            }
        } else if join.kind == JoinKind::Left || join.kind == JoinKind::Full {
            let mut row = left.clone();
            row.extend(vec![SQL_NULL.to_string(); right_cols.len()]);
            out.push(row);
        }
    }
    if join.kind == JoinKind::Right || join.kind == JoinKind::Full {
        for (row_index, right) in right_rows.iter().enumerate() {
            if !matched[row_index] {
                let mut row = vec![SQL_NULL.to_string(); left_cols.len()];
                row.extend(right.clone());
                out.push(row);
            }
        }
    }
    Ok(out)
}

fn on_sides(
    left_cols: &[String],
    right_cols: &[String],
    on: &[(String, String)],
) -> Result<(Vec<usize>, Vec<usize>), SqlError> {
    let mut left_at = Vec::new();
    let mut right_at = Vec::new();
    for (left_name, right_name) in on {
        match (
            place(left_cols, right_cols, left_name)?,
            place(left_cols, right_cols, right_name)?,
        ) {
            (Side::Left(left), Side::Right(right)) | (Side::Right(right), Side::Left(left)) => {
                left_at.push(left);
                right_at.push(right);
            }
            _ => {
                return Err(SqlError::Syntax(
                    "on must compare one column from each side".into(),
                ))
            }
        }
    }
    Ok((left_at, right_at))
}

enum Side {
    Left(usize),
    Right(usize),
}

fn place(left_cols: &[String], right_cols: &[String], name: &str) -> Result<Side, SqlError> {
    let left = resolve_column(left_cols, name).ok();
    let right = resolve_column(right_cols, name).ok();
    match (left, right) {
        (Some(index), None) => Ok(Side::Left(index)),
        (None, Some(index)) => Ok(Side::Right(index)),
        (Some(_), Some(_)) => Err(SqlError::Syntax(format!("column \"{name}\" is ambiguous"))),
        (None, None) => Err(SqlError::Syntax(format!(
            "column \"{name}\" does not exist"
        ))),
    }
}

fn key_of(row: &[String], indexes: &[usize]) -> Option<Vec<String>> {
    let mut key = Vec::with_capacity(indexes.len());
    for index in indexes {
        let cell = &row[*index];
        if cell == SQL_NULL {
            return None;
        }
        key.push(cell.clone());
    }
    Some(key)
}

struct ScanPush {
    symbol: Option<String>,
    period: Option<Period>,
    ts_from: Option<i64>,
    ts_to: Option<i64>,
    eq_column: Option<String>,
    eq_value: Option<String>,
}

fn peel_scan_predicates(expr: Expr) -> (Option<Expr>, ScanPush) {
    let mut push = ScanPush {
        symbol: None,
        period: None,
        ts_from: None,
        ts_to: None,
        eq_column: None,
        eq_value: None,
    };
    let residual = peel_expr(&expr, &mut push);
    (residual, push)
}

fn peel_expr(expr: &Expr, push: &mut ScanPush) -> Option<Expr> {
    match expr {
        Expr::And(items) => {
            let mut kept = Vec::new();
            for item in items {
                if let Some(part) = peel_expr(item, push) {
                    kept.push(part);
                }
            }
            match kept.len() {
                0 => None,
                1 => kept.pop(),
                _ => Some(Expr::And(kept)),
            }
        }
        Expr::Cmp {
            left,
            op: Cmp::Eq,
            right,
        } => {
            if let (ValExpr::Column(column), ValExpr::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                if column == "symbol" {
                    push.symbol = Some(value.clone());
                    return None;
                }
                if column == "period" {
                    push.period = Period::parse(value);
                    return None;
                }
                if push.eq_column.is_none() {
                    push.eq_column = Some(column.clone());
                    push.eq_value = Some(value.clone());
                }
            }
            Some(expr.clone())
        }
        Expr::Between { value, low, high } => {
            if let (ValExpr::Column(column), Some(low_n), Some(high_n)) =
                (value.as_ref(), literal_i64(low), literal_i64(high))
            {
                if column == "ts" {
                    push.ts_from = Some(low_n);
                    push.ts_to = Some(high_n);
                }
            }
            Some(expr.clone())
        }
        other => Some(other.clone()),
    }
}

fn literal_i64(expr: &ValExpr) -> Option<i64> {
    match expr {
        ValExpr::Literal(value) => value.parse().ok(),
        ValExpr::Neg(inner) => literal_i64(inner).and_then(i64::checked_neg),
        _ => None,
    }
}

fn agg_call(tokens: &[String], index: usize) -> bool {
    let Some(name) = tokens.get(index) else {
        return false;
    };
    matches!(
        name.to_ascii_lowercase().as_str(),
        "max" | "min" | "sum" | "avg" | "first" | "last" | "count"
    ) && tokens.get(index + 1).map(String::as_str) == Some("(")
}

fn take_heading(tokens: &[String], index: &mut usize, fallback: &str) -> Result<String, SqlError> {
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("as"))
    {
        *index += 1;
        let heading = tokens
            .get(*index)
            .ok_or_else(|| SqlError::Syntax("alias".into()))?
            .to_ascii_lowercase();
        *index += 1;
        Ok(heading)
    } else {
        Ok(fallback.to_string())
    }
}

fn parse_expr(tokens: &[String], index: &mut usize) -> Result<Expr, SqlError> {
    let mut items = vec![parse_and(tokens, index)?];
    while tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("or"))
    {
        *index += 1;
        items.push(parse_and(tokens, index)?);
    }
    if items.len() == 1 {
        Ok(items.pop().unwrap())
    } else {
        Ok(Expr::Or(items))
    }
}

fn parse_and(tokens: &[String], index: &mut usize) -> Result<Expr, SqlError> {
    let mut items = vec![parse_not(tokens, index)?];
    while tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("and"))
    {
        *index += 1;
        items.push(parse_not(tokens, index)?);
    }
    if items.len() == 1 {
        Ok(items.pop().unwrap())
    } else {
        Ok(Expr::And(items))
    }
}

fn parse_not(tokens: &[String], index: &mut usize) -> Result<Expr, SqlError> {
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("not"))
    {
        *index += 1;
        return Ok(Expr::Not(Box::new(parse_not(tokens, index)?)));
    }
    parse_comparison(tokens, index)
}

fn parse_comparison(tokens: &[String], index: &mut usize) -> Result<Expr, SqlError> {
    let left = parse_add(tokens, index)?;
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("is"))
    {
        *index += 1;
        let negated = if tokens
            .get(*index)
            .is_some_and(|token| token.eq_ignore_ascii_case("not"))
        {
            *index += 1;
            true
        } else {
            false
        };
        expect(tokens, index, "null")?;
        return Ok(Expr::IsNull {
            value: Box::new(left),
            negated,
        });
    }
    let negated = if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("not"))
        && tokens.get(*index + 1).is_some_and(|token| {
            token.eq_ignore_ascii_case("in") || token.eq_ignore_ascii_case("like")
        }) {
        *index += 1;
        true
    } else {
        false
    };
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("between"))
    {
        *index += 1;
        let low = parse_add(tokens, index)?;
        expect(tokens, index, "and")?;
        let high = parse_add(tokens, index)?;
        return Ok(Expr::Between {
            value: Box::new(left),
            low: Box::new(low),
            high: Box::new(high),
        });
    }
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("in"))
    {
        *index += 1;
        expect(tokens, index, "(")?;
        let mut list = Vec::new();
        loop {
            list.push(parse_add(tokens, index)?);
            if tokens.get(*index).map(String::as_str) == Some(",") {
                *index += 1;
                continue;
            }
            break;
        }
        if list.is_empty() {
            return Err(SqlError::Syntax("in".into()));
        }
        expect(tokens, index, ")")?;
        return Ok(Expr::In {
            value: Box::new(left),
            list,
            negated,
        });
    }
    if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("like"))
    {
        *index += 1;
        let pattern = parse_add(tokens, index)?;
        return Ok(Expr::Like {
            value: Box::new(left),
            pattern: Box::new(pattern),
            negated,
        });
    }
    if negated {
        return Err(SqlError::Syntax("not".into()));
    }
    if let Some(op) = cmp_op(tokens.get(*index).map(String::as_str)) {
        *index += 1;
        let right = parse_add(tokens, index)?;
        return Ok(Expr::Cmp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        });
    }
    Ok(Expr::Value(left))
}

fn cmp_op(token: Option<&str>) -> Option<Cmp> {
    match token? {
        "=" => Some(Cmp::Eq),
        "<>" | "!=" => Some(Cmp::Ne),
        "<" => Some(Cmp::Lt),
        "<=" => Some(Cmp::Le),
        ">" => Some(Cmp::Gt),
        ">=" => Some(Cmp::Ge),
        _ => None,
    }
}

fn parse_add(tokens: &[String], index: &mut usize) -> Result<ValExpr, SqlError> {
    let mut left = parse_mul(tokens, index)?;
    while let Some(op) = match tokens.get(*index).map(String::as_str) {
        Some("+") => Some(Arith::Add),
        Some("-") => Some(Arith::Sub),
        _ => None,
    } {
        *index += 1;
        let right = parse_mul(tokens, index)?;
        left = ValExpr::Bin {
            op,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_mul(tokens: &[String], index: &mut usize) -> Result<ValExpr, SqlError> {
    let mut left = parse_unary(tokens, index)?;
    while let Some(op) = match tokens.get(*index).map(String::as_str) {
        Some("*") => Some(Arith::Mul),
        Some("/") => Some(Arith::Div),
        _ => None,
    } {
        *index += 1;
        let right = parse_unary(tokens, index)?;
        left = ValExpr::Bin {
            op,
            left: Box::new(left),
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_unary(tokens: &[String], index: &mut usize) -> Result<ValExpr, SqlError> {
    if tokens.get(*index).map(String::as_str) == Some("-") {
        *index += 1;
        return Ok(ValExpr::Neg(Box::new(parse_unary(tokens, index)?)));
    }
    parse_primary(tokens, index)
}

fn parse_primary(tokens: &[String], index: &mut usize) -> Result<ValExpr, SqlError> {
    let token = tokens
        .get(*index)
        .ok_or_else(|| SqlError::Syntax("value".into()))?
        .clone();
    if token == "(" {
        *index += 1;
        let expr = parse_expr(tokens, index)?;
        expect(tokens, index, ")")?;
        return Ok(match expr {
            Expr::Value(value) => value,
            other => ValExpr::Pred(Box::new(other)),
        });
    }
    if token.eq_ignore_ascii_case("case") {
        return parse_case(tokens, index);
    }
    *index += 1;
    if token.starts_with('\'') {
        return Ok(ValExpr::Literal(unquote(&token)));
    }
    if token.eq_ignore_ascii_case("null") {
        return Ok(ValExpr::Literal(SQL_NULL.to_string()));
    }
    if token.eq_ignore_ascii_case("true") || token.eq_ignore_ascii_case("false") {
        return Ok(ValExpr::Literal(token.to_ascii_lowercase()));
    }
    if token.parse::<i128>().is_ok() {
        return Ok(ValExpr::Literal(token));
    }
    if token
        .chars()
        .any(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
    {
        return Ok(ValExpr::Column(token.to_ascii_lowercase()));
    }
    Err(SqlError::Syntax(format!("value {token}")))
}

fn parse_case(tokens: &[String], index: &mut usize) -> Result<ValExpr, SqlError> {
    expect(tokens, index, "case")?;
    let mut arms = Vec::new();
    while tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("when"))
    {
        *index += 1;
        let cond = parse_expr(tokens, index)?;
        expect(tokens, index, "then")?;
        let value = parse_add(tokens, index)?;
        arms.push((cond, value));
    }
    if arms.is_empty() {
        return Err(SqlError::Syntax("case".into()));
    }
    let else_arm = if tokens
        .get(*index)
        .is_some_and(|token| token.eq_ignore_ascii_case("else"))
    {
        *index += 1;
        Some(Box::new(parse_add(tokens, index)?))
    } else {
        None
    };
    expect(tokens, index, "end")?;
    Ok(ValExpr::Case { arms, else_arm })
}

fn eval_expr(expr: &Expr, columns: &[String], row: &[String]) -> Result<Truth, SqlError> {
    match expr {
        Expr::Value(value) => truth_of_scalar(&eval_val(value, columns, row)?),
        Expr::And(items) => {
            let mut acc = Truth::True;
            for item in items {
                acc = and_truth(acc, eval_expr(item, columns, row)?);
                if acc == Truth::False {
                    return Ok(Truth::False);
                }
            }
            Ok(acc)
        }
        Expr::Or(items) => {
            let mut acc = Truth::False;
            for item in items {
                acc = or_truth(acc, eval_expr(item, columns, row)?);
                if acc == Truth::True {
                    return Ok(Truth::True);
                }
            }
            Ok(acc)
        }
        Expr::Not(inner) => Ok(eval_expr(inner, columns, row)?.not()),
        Expr::Cmp { left, op, right } => {
            let ordering = scalar_ord(
                &eval_val(left, columns, row)?,
                &eval_val(right, columns, row)?,
            );
            Ok(match ordering {
                None => Truth::Unknown,
                Some(ordering) => match op {
                    Cmp::Eq => ordering.is_eq(),
                    Cmp::Ne => ordering.is_ne(),
                    Cmp::Lt => ordering.is_lt(),
                    Cmp::Le => ordering.is_le(),
                    Cmp::Gt => ordering.is_gt(),
                    Cmp::Ge => ordering.is_ge(),
                }
                .then_some(Truth::True)
                .unwrap_or(Truth::False),
            })
        }
        Expr::Between { value, low, high } => {
            let value = eval_val(value, columns, row)?;
            let low = eval_val(low, columns, row)?;
            let high = eval_val(high, columns, row)?;
            if matches!(value, Scalar::Null)
                || matches!(low, Scalar::Null)
                || matches!(high, Scalar::Null)
            {
                return Ok(Truth::Unknown);
            }
            let Some(value) = number_of(&value) else {
                return Ok(Truth::False);
            };
            let Some(low) = number_of(&low) else {
                return Ok(Truth::False);
            };
            let Some(high) = number_of(&high) else {
                return Ok(Truth::False);
            };
            Ok(if value >= low && value <= high {
                Truth::True
            } else {
                Truth::False
            })
        }
        Expr::IsNull { value, negated } => {
            let is_null = matches!(eval_val(value, columns, row)?, Scalar::Null);
            Ok(if is_null == *negated {
                Truth::False
            } else {
                Truth::True
            })
        }
        Expr::In {
            value,
            list,
            negated,
        } => {
            let value = eval_val(value, columns, row)?;
            if matches!(value, Scalar::Null) {
                return Ok(Truth::Unknown);
            }
            let mut unknown = false;
            for item in list {
                match scalar_ord(&value, &eval_val(item, columns, row)?) {
                    Some(ordering) if ordering.is_eq() => {
                        return Ok(if *negated { Truth::False } else { Truth::True });
                    }
                    None => unknown = true,
                    _ => {}
                }
            }
            if unknown {
                Ok(Truth::Unknown)
            } else if *negated {
                Ok(Truth::True)
            } else {
                Ok(Truth::False)
            }
        }
        Expr::Like {
            value,
            pattern,
            negated,
        } => {
            let value = eval_val(value, columns, row)?;
            let pattern = eval_val(pattern, columns, row)?;
            if matches!(value, Scalar::Null) || matches!(pattern, Scalar::Null) {
                return Ok(Truth::Unknown);
            }
            let matched = like_match(&scalar_text(&value), &scalar_text(&pattern));
            Ok(if matched == *negated {
                Truth::False
            } else {
                Truth::True
            })
        }
    }
}

fn and_truth(left: Truth, right: Truth) -> Truth {
    match (left, right) {
        (Truth::False, _) | (_, Truth::False) => Truth::False,
        (Truth::Unknown, _) | (_, Truth::Unknown) => Truth::Unknown,
        _ => Truth::True,
    }
}

fn or_truth(left: Truth, right: Truth) -> Truth {
    match (left, right) {
        (Truth::True, _) | (_, Truth::True) => Truth::True,
        (Truth::Unknown, _) | (_, Truth::Unknown) => Truth::Unknown,
        _ => Truth::False,
    }
}

fn truth_of_scalar(scalar: &Scalar) -> Result<Truth, SqlError> {
    match scalar {
        Scalar::Null => Ok(Truth::Unknown),
        Scalar::Text(text) if text.eq_ignore_ascii_case("true") => Ok(Truth::True),
        Scalar::Text(text) if text.eq_ignore_ascii_case("false") => Ok(Truth::False),
        Scalar::Int(0) => Ok(Truth::False),
        Scalar::Int(_) => Ok(Truth::True),
        Scalar::Text(_) => Err(SqlError::Syntax("boolean".into())),
    }
}

fn eval_val(expr: &ValExpr, columns: &[String], row: &[String]) -> Result<Scalar, SqlError> {
    match expr {
        ValExpr::Column(name) => {
            let cell = row
                .get(resolve_column(columns, name)?)
                .map(String::as_str)
                .unwrap_or("");
            if cell == SQL_NULL {
                Ok(Scalar::Null)
            } else {
                Ok(Scalar::Text(cell.to_string()))
            }
        }
        ValExpr::Literal(value) => {
            if value == SQL_NULL {
                Ok(Scalar::Null)
            } else {
                Ok(Scalar::Text(value.clone()))
            }
        }
        ValExpr::Neg(inner) => match eval_val(inner, columns, row)? {
            Scalar::Null => Ok(Scalar::Null),
            other => {
                let Some(number) = number_of(&other) else {
                    return Err(SqlError::Syntax("numeric".into()));
                };
                number
                    .checked_neg()
                    .map(Scalar::Int)
                    .ok_or_else(|| SqlError::Syntax("overflow".into()))
            }
        },
        ValExpr::Bin { op, left, right } => {
            let left = eval_val(left, columns, row)?;
            let right = eval_val(right, columns, row)?;
            if matches!(left, Scalar::Null) || matches!(right, Scalar::Null) {
                return Ok(Scalar::Null);
            }
            let Some(left) = number_of(&left) else {
                return Err(SqlError::Syntax("numeric".into()));
            };
            let Some(right) = number_of(&right) else {
                return Err(SqlError::Syntax("numeric".into()));
            };
            let value = match op {
                Arith::Add => left.checked_add(right),
                Arith::Sub => left.checked_sub(right),
                Arith::Mul => left.checked_mul(right),
                Arith::Div => {
                    if right == 0 {
                        return Err(SqlError::Syntax("division by zero".into()));
                    }
                    left.checked_div(right)
                }
            };
            value
                .map(Scalar::Int)
                .ok_or_else(|| SqlError::Syntax("overflow".into()))
        }
        ValExpr::Case { arms, else_arm } => {
            for (cond, value) in arms {
                if eval_expr(cond, columns, row)?.is_true() {
                    return eval_val(value, columns, row);
                }
            }
            else_arm
                .as_ref()
                .map_or(Ok(Scalar::Null), |value| eval_val(value, columns, row))
        }
        ValExpr::Pred(inner) => Ok(match eval_expr(inner, columns, row)? {
            Truth::True => Scalar::Text("true".into()),
            Truth::False => Scalar::Text("false".into()),
            Truth::Unknown => Scalar::Null,
        }),
    }
}

fn number_of(scalar: &Scalar) -> Option<i128> {
    match scalar {
        Scalar::Null => None,
        Scalar::Int(number) => Some(*number),
        Scalar::Text(text) => text.parse().ok(),
    }
}

fn scalar_text(scalar: &Scalar) -> String {
    match scalar {
        Scalar::Null => String::new(),
        Scalar::Int(number) => number.to_string(),
        Scalar::Text(text) => text.clone(),
    }
}

fn show_scalar(scalar: &Scalar) -> String {
    match scalar {
        Scalar::Null => String::new(),
        Scalar::Int(number) => number.to_string(),
        Scalar::Text(text) => show_cell(text),
    }
}

fn scalar_group_key(scalar: &Scalar) -> String {
    match scalar {
        Scalar::Null => SQL_NULL.to_string(),
        Scalar::Int(number) => number.to_string(),
        Scalar::Text(text) if text == SQL_NULL => SQL_NULL.to_string(),
        Scalar::Text(text) => text.clone(),
    }
}

fn scalar_ord(left: &Scalar, right: &Scalar) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Scalar::Null, _) | (_, Scalar::Null) => None,
        (Scalar::Int(left), Scalar::Int(right)) => Some(left.cmp(right)),
        (Scalar::Int(left), Scalar::Text(right)) => Some(match right.parse::<i128>() {
            Ok(right) => left.cmp(&right),
            Err(_) => left.to_string().cmp(right),
        }),
        (Scalar::Text(left), Scalar::Int(right)) => Some(match left.parse::<i128>() {
            Ok(left) => left.cmp(right),
            Err(_) => left.cmp(&right.to_string()),
        }),
        (Scalar::Text(left), Scalar::Text(right)) => {
            Some(match (left.parse::<i128>(), right.parse::<i128>()) {
                (Ok(left), Ok(right)) => left.cmp(&right),
                _ => left.cmp(right),
            })
        }
    }
}

fn render_val(expr: &ValExpr) -> String {
    match expr {
        ValExpr::Column(name) => name.clone(),
        ValExpr::Literal(value) => {
            if value == SQL_NULL {
                "null".to_string()
            } else {
                value.clone()
            }
        }
        ValExpr::Neg(inner) => format!("-({})", render_val(inner)),
        ValExpr::Bin { op, left, right } => format!(
            "({} {} {})",
            render_val(left),
            arith_text(*op),
            render_val(right)
        ),
        ValExpr::Case { arms, else_arm } => {
            let mut text = String::from("case");
            for (cond, value) in arms {
                text.push_str(" when ");
                text.push_str(&render_expr(cond));
                text.push_str(" then ");
                text.push_str(&render_val(value));
            }
            if let Some(value) = else_arm {
                text.push_str(" else ");
                text.push_str(&render_val(value));
            }
            text.push_str(" end");
            text
        }
        ValExpr::Pred(inner) => format!("({})", render_expr(inner)),
    }
}

fn render_val_outer(expr: &ValExpr) -> String {
    match expr {
        ValExpr::Bin { op, left, right } => format!(
            "{} {} {}",
            render_val(left),
            arith_text(*op),
            render_val(right)
        ),
        ValExpr::Neg(inner) => format!("-{}", render_val(inner)),
        other => render_val(other),
    }
}

fn render_expr(expr: &Expr) -> String {
    match expr {
        Expr::Value(value) => render_val(value),
        Expr::And(items) => items
            .iter()
            .map(render_expr)
            .collect::<Vec<_>>()
            .join(" and "),
        Expr::Or(items) => items
            .iter()
            .map(render_expr)
            .collect::<Vec<_>>()
            .join(" or "),
        Expr::Not(inner) => format!("not {}", render_expr(inner)),
        Expr::Cmp { left, op, right } => format!(
            "{} {} {}",
            render_val(left),
            cmp_text(*op),
            render_val(right)
        ),
        Expr::Between { value, low, high } => format!(
            "{} between {} and {}",
            render_val(value),
            render_val(low),
            render_val(high)
        ),
        Expr::IsNull { value, negated } => format!(
            "{} is {}null",
            render_val(value),
            if *negated { "not " } else { "" }
        ),
        Expr::In {
            value,
            list,
            negated,
        } => format!(
            "{} {}in ({})",
            render_val(value),
            if *negated { "not " } else { "" },
            list.iter().map(render_val).collect::<Vec<_>>().join(", ")
        ),
        Expr::Like {
            value,
            pattern,
            negated,
        } => format!(
            "{} {}like {}",
            render_val(value),
            if *negated { "not " } else { "" },
            render_val(pattern)
        ),
    }
}

fn arith_text(op: Arith) -> &'static str {
    match op {
        Arith::Add => "+",
        Arith::Sub => "-",
        Arith::Mul => "*",
        Arith::Div => "/",
    }
}

fn cmp_text(op: Cmp) -> &'static str {
    match op {
        Cmp::Eq => "=",
        Cmp::Ne => "<>",
        Cmp::Lt => "<",
        Cmp::Le => "<=",
        Cmp::Gt => ">",
        Cmp::Ge => ">=",
    }
}

enum LikeTok {
    Any,
    One,
    Char(char),
}

fn like_match(text: &str, pattern: &str) -> bool {
    let text: Vec<char> = text.chars().collect();
    let pattern = like_tokens(pattern);
    let mut dp = vec![vec![false; pattern.len() + 1]; text.len() + 1];
    dp[0][0] = true;
    for (index, token) in pattern.iter().enumerate() {
        if matches!(token, LikeTok::Any) {
            dp[0][index + 1] = dp[0][index];
        } else {
            break;
        }
    }
    for row in 1..=text.len() {
        for column in 1..=pattern.len() {
            dp[row][column] = match pattern[column - 1] {
                LikeTok::Any => dp[row][column - 1] || dp[row - 1][column],
                LikeTok::One => dp[row - 1][column - 1],
                LikeTok::Char(ch) => dp[row - 1][column - 1] && text[row - 1] == ch,
            };
        }
    }
    dp[text.len()][pattern.len()]
}

fn like_tokens(pattern: &str) -> Vec<LikeTok> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] == '\\' && index + 1 < chars.len() {
            tokens.push(LikeTok::Char(chars[index + 1]));
            index += 2;
        } else if chars[index] == '%' {
            tokens.push(LikeTok::Any);
            index += 1;
        } else if chars[index] == '_' {
            tokens.push(LikeTok::One);
            index += 1;
        } else {
            tokens.push(LikeTok::Char(chars[index]));
            index += 1;
        }
    }
    tokens
}
fn project_rows(stmt: &Stmt, result: QueryResult) -> Result<QueryResult, SqlError> {
    if stmt.columns == ["*"] {
        return Ok(show_result(result));
    }
    let mut rows = Vec::new();
    for row in result.rows {
        let mut out = Vec::new();
        for (name, value) in stmt.columns.iter().zip(&stmt.select_values) {
            if let Some(expr) = value {
                out.push(show_scalar(&eval_val(expr, &result.columns, &row)?));
            } else {
                let index = resolve_column(&result.columns, name)?;
                out.push(show_cell(row.get(index).map(String::as_str).unwrap_or("")));
            }
        }
        rows.push(out);
    }
    Ok(QueryResult {
        columns: stmt.headings.clone(),
        rows,
    })
}

fn show_result(mut result: QueryResult) -> QueryResult {
    for row in &mut result.rows {
        for cell in row {
            if cell == SQL_NULL {
                cell.clear();
            }
        }
    }
    result
}

fn group_rows(stmt: &Stmt, result: QueryResult) -> Result<QueryResult, SqlError> {
    for (name, value) in stmt.columns.iter().zip(&stmt.select_values) {
        let aggregated = value.is_none() && name.contains('(');
        if aggregated {
            continue;
        }
        if stmt.group.iter().all(|group| render_val(group) != *name) {
            return Err(SqlError::Syntax(format!(
                "column \"{name}\" must appear in GROUP BY"
            )));
        }
    }
    let mut groups: Vec<(Vec<String>, Vec<Vec<String>>)> = Vec::new();
    for row in result.rows {
        let key = stmt
            .group
            .iter()
            .map(|expr| {
                eval_val(expr, &result.columns, &row).map(|scalar| scalar_group_key(&scalar))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some((_, bucket)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            bucket.push(row);
        } else {
            groups.push((key, vec![row]));
        }
    }
    if groups.is_empty() && stmt.group.is_empty() && !stmt.aggregates.is_empty() {
        groups.push((Vec::new(), Vec::new()));
    }
    let mut rows = Vec::new();
    for (_, bucket) in groups {
        let mut out = Vec::new();
        let mut aggs = stmt.aggregates.iter();
        let sample = bucket.first().cloned().unwrap_or_default();
        for (name, value) in stmt.columns.iter().zip(&stmt.select_values) {
            if value.is_none() && name.contains('(') {
                let agg = aggs
                    .next()
                    .ok_or_else(|| SqlError::Syntax("aggregate".into()))?;
                out.push(aggregate_text(&result.columns, &bucket, agg)?);
            } else if let Some(expr) = value {
                out.push(show_scalar(&eval_val(expr, &result.columns, &sample)?));
            } else {
                let index = resolve_column(&result.columns, name)?;
                out.push(show_cell(
                    sample.get(index).map(String::as_str).unwrap_or(""),
                ));
            }
        }
        rows.push(out);
    }
    Ok(QueryResult {
        columns: stmt.headings.clone(),
        rows,
    })
}

fn aggregate_text(columns: &[String], rows: &[Vec<String>], agg: &Agg) -> Result<String, SqlError> {
    if rows.is_empty() {
        return Ok(String::new());
    }
    let index = resolve_column(columns, &agg.column)?;
    let values: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get(index).map(String::as_str))
        .filter(|value| *value != SQL_NULL)
        .collect();
    if agg.func == "count" {
        return Ok(values.len().to_string());
    }
    if values.is_empty() {
        return Ok(String::new());
    }
    let numbers: Vec<i128> = values
        .iter()
        .map(|value| {
            value
                .parse::<i128>()
                .map_err(|_| SqlError::Syntax(format!("aggregate {}", agg.func)))
        })
        .collect::<Result<_, _>>()?;
    match agg.func.as_str() {
        "max" => Ok(numbers.iter().copied().max().unwrap_or(0).to_string()),
        "min" => Ok(numbers.iter().copied().min().unwrap_or(0).to_string()),
        "sum" => Ok(numbers.iter().sum::<i128>().to_string()),
        "avg" => Ok(format_avg(numbers.iter().sum(), numbers.len() as i128)),
        "first" => Ok(values[0].to_string()),
        "last" => Ok(values[values.len() - 1].to_string()),
        other => Err(SqlError::Syntax(format!("aggregate {other}"))),
    }
}

fn sort_rows(result: &mut QueryResult, order: &[OrderKey]) -> Result<(), SqlError> {
    let keys = order
        .iter()
        .map(|key| Ok((resolve_column(&result.columns, &key.name)?, key.desc)))
        .collect::<Result<Vec<_>, _>>()?;
    result.rows.sort_by(|left, right| {
        for (index, desc) in &keys {
            let cmp = cmp_cells(
                left.get(*index).map(String::as_str).unwrap_or(""),
                right.get(*index).map(String::as_str).unwrap_or(""),
            );
            let cmp = if *desc { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

fn cmp_cells(left: &str, right: &str) -> std::cmp::Ordering {
    match (left.parse::<i64>(), right.parse::<i64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '\'' {
            let mut value = String::from("'");
            for inner in chars.by_ref() {
                value.push(inner);
                if inner == '\'' {
                    break;
                }
            }
            tokens.push(value);
            continue;
        }
        if ch == ','
            || ch == '('
            || ch == ')'
            || ch == '='
            || ch == '+'
            || ch == '-'
            || ch == '*'
            || ch == '/'
        {
            tokens.push(ch.to_string());
            continue;
        }
        if ch == '<' || ch == '>' || ch == '!' {
            let mut op = String::from(ch);
            if let Some(next) = chars.peek() {
                if (*next == '=' && (ch == '<' || ch == '>' || ch == '!'))
                    || (*next == '>' && ch == '<')
                {
                    op.push(chars.next().unwrap());
                }
            }
            tokens.push(op);
            continue;
        }
        let mut word = String::from(ch);
        while let Some(next) = chars.peek() {
            if next.is_whitespace()
                || matches!(
                    *next,
                    ',' | '(' | ')' | '=' | '+' | '-' | '*' | '/' | '<' | '>' | '!'
                )
            {
                break;
            }
            word.push(chars.next().unwrap());
        }
        tokens.push(word);
    }
    tokens
}

fn expect(tokens: &[String], index: &mut usize, word: &str) -> Result<(), SqlError> {
    let got = tokens
        .get(*index)
        .ok_or_else(|| SqlError::Syntax(word.into()))?;
    if !got.eq_ignore_ascii_case(word) {
        return Err(SqlError::Syntax(format!("expected {word}, got {got}")));
    }
    *index += 1;
    Ok(())
}

fn unquote(value: &str) -> String {
    value.trim_matches('\'').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdb_abi::TickQuote;

    fn engine() -> (std::path::PathBuf, Engine) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("qdb-sql-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut engine = Engine::open(&dir).unwrap();
        let base = ts(100_000_000);
        engine.write_trade("SZSE", "000001", base, 10, 100).unwrap();
        engine
            .write_trade("SZSE", "000001", base + 100_000_000, 12, 50)
            .unwrap();
        engine
            .write_trade("SZSE", "000001", base + 900_000_000, 11, 10)
            .unwrap();
        engine.write_trade("SSE", "000001", base, 9, 1).unwrap();
        engine
            .write_quote(
                "SZSE",
                "000001",
                base,
                &TickQuote {
                    last: 11,
                    volume: 160,
                    amount: 0,
                    bid_px: [10, 0, 0, 0, 0],
                    bid_qty: [1, 0, 0, 0, 0],
                    ask_px: [12, 0, 0, 0, 0],
                    ask_qty: [1, 0, 0, 0, 0],
                },
            )
            .unwrap();
        (dir, engine)
    }

    fn ts(extra: i64) -> i64 {
        let days = days(2024, 1, 2);
        (days * 86_400 + 9 * 3600 + 30 * 60 - 8 * 3600) * 1_000_000_000 + extra
    }

    fn days(mut y: i32, m: u32, d: u32) -> i64 {
        y -= i32::from(m <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146_097 + doe as i64 - 719_468
    }

    #[test]
    fn example_queries_and_ambiguous_code() {
        let (_dir, engine) = engine();
        let sql = SqlEngine::new(&engine);
        let bars = sql
            .query("SELECT ts, open, high, low, close, volume, amount, seq FROM klines WHERE symbol = 'SZSE:000001' AND period = '1m' AND ts BETWEEN 0 AND 2000000000000000000 ORDER BY ts ASC LIMIT 500")
            .unwrap();
        assert_eq!(bars.rows.len(), 1);
        assert_eq!(bars.rows[0][1], "10");
        assert_eq!(bars.rows[0][4], "11");
        let snap = sql
            .query("SELECT last, bid_px1, ask_px1, seq FROM snapshots WHERE symbol = 'SZSE:000001'")
            .unwrap();
        assert_eq!(snap.rows[0][0], "11");
        assert_eq!(snap.rows[0][1], "10");
        assert_eq!(snap.rows[0][2], "12");
        let agg = sql
            .query("SELECT max(high), min(low), sum(volume), avg(close), first(open), last(close) FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s'")
            .unwrap();
        assert_eq!(agg.rows[0][0], "12");
        assert_eq!(agg.rows[0][1], "10");
        assert_eq!(agg.rows[0][2], "160");
        assert_eq!(agg.rows[0][4], "10");
        assert_eq!(agg.rows[0][5], "11");
        assert!(sql
            .query("SELECT last FROM snapshots WHERE symbol = '000001'")
            .is_err());
    }

    #[test]
    fn user_table_keeps_its_index_and_survives_reopen() {
        let (dir, mut engine) = engine();
        let created = SqlEngine::execute(
            &mut engine,
            "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, active BOOL, INDEX idx_code (code))",
        )
        .unwrap();
        assert_eq!(created.rows[0][0], "created");
        SqlEngine::execute(
            &mut engine,
            "INSERT INTO note VALUES (1, 'SSE:600000', true)",
        )
        .unwrap();
        SqlEngine::execute(
            &mut engine,
            "INSERT INTO note VALUES (2, 'SZSE:000001', false)",
        )
        .unwrap();
        assert!(
            SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'again', true)").is_err()
        );
        assert!(SqlEngine::execute(&mut engine, "CREATE TABLE note (id INT64)").is_err());
        assert!(SqlEngine::execute(&mut engine, "CREATE TABLE ticks (id INT64)").is_err());
        assert!(SqlEngine::execute(&mut engine, "CREATE TABLE bad (id FLOAT)").is_err());
        drop(engine);
        let engine = Engine::open(&dir).unwrap();
        let sql = SqlEngine::new(&engine);
        let rows = sql
            .query("SELECT * FROM note WHERE code = 'SSE:600000'")
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0][0], "1");
        assert_eq!(rows.rows[0][2], "true");
        let described = sql.query("DESCRIBE note").unwrap();
        assert!(described
            .rows
            .iter()
            .any(|row| row[0] == "id" && row[1].contains("PRIMARY KEY")));
        let listed = sql.query("SHOW TABLES").unwrap();
        assert!(listed.rows.iter().any(|row| row[0] == "note"));
    }

    #[test]
    fn ticks_without_a_symbol_return_every_trade() {
        let (_dir, engine) = engine();
        let sql = SqlEngine::new(&engine);
        let all = sql.query("SELECT * FROM ticks;").unwrap();
        assert_eq!(
            all.columns,
            [
                "seq",
                "symbol_id",
                "symbol",
                "ts",
                "price",
                "volume",
                "amount"
            ]
        );
        let symbol_at = all
            .columns
            .iter()
            .position(|name| name == "symbol")
            .unwrap();
        let price_at = all.columns.iter().position(|name| name == "price").unwrap();
        assert!(all
            .rows
            .iter()
            .any(|row| row[symbol_at] == "SZSE:000001" && row[price_at] == "10"));
        assert!(all.rows.iter().any(|row| row[symbol_at] == "SSE:000001"));
        let one = sql
            .query("SELECT price, volume FROM ticks WHERE symbol = 'SZSE:000001'")
            .unwrap();
        assert_eq!(one.columns, ["price", "volume"]);
        assert!(one.rows.iter().all(|row| row.len() == 2));
    }

    #[test]
    fn show_tables_and_describe_list_the_query_catalog() {
        let (_dir, engine) = engine();
        let sql = SqlEngine::new(&engine);
        let tables = sql.query("SHOW TABLES").unwrap();
        assert_eq!(tables.columns, ["table", "filter"]);
        assert_eq!(tables.rows.len(), 4);
        assert_eq!(tables.rows[3][0], "klines");
        let described = sql.query("DESCRIBE klines;").unwrap();
        assert_eq!(described.rows[0][0], "symbol_id");
        assert!(described.rows[0][1].contains("PRIMARY KEY"));
        assert!(described.rows.iter().any(|row| row[0] == "ts"));
        let symbols = sql.query("DESCRIBE symbols").unwrap();
        assert!(symbols.rows.iter().any(|row| row[0] == "symbol_id"));
        assert!(sql.query("DESC nope").is_err());
    }

    #[test]
    fn subscribe_projects_only_the_matching_new_rows() {
        let (dir, mut writer) = engine();
        let sql = "SELECT last, seq FROM snapshots WHERE symbol = 'SZSE:000001'";
        assert_eq!(
            split_subscribe(&format!("{sql} SUBSCRIBE;"))
                .unwrap()
                .as_deref(),
            Some(sql)
        );
        assert!(split_subscribe("SHOW TABLES SUBSCRIBE").is_err());
        assert!(SqlEngine::new(&writer)
            .query(&format!("{sql} SUBSCRIBE"))
            .is_err());
        let mut reader = Engine::open_read(&dir).unwrap();
        let seen = captured_rows(&reader, sql).unwrap();
        writer
            .write_trade("SSE", "000001", ts(2_000_000_000), 99, 1)
            .unwrap();
        writer
            .write_trade("SZSE", "000001", ts(2_000_000_000), 20, 3)
            .unwrap();
        let changes = reader.poll().unwrap();
        let snap = live_rows(&reader, sql, &changes, &seen).unwrap();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0][0], "20");
        let ticks = live_rows(
            &reader,
            "SELECT price, volume FROM ticks WHERE symbol = 'SZSE:000001'",
            &changes,
            &[],
        )
        .unwrap();
        assert_eq!(ticks, vec![vec![String::from("20"), String::from("3")]]);
        let bars = live_rows(
            &reader,
            "SELECT close FROM klines WHERE symbol = 'SZSE:000001' AND period = '1m'",
            &changes,
            &[],
        )
        .unwrap();
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0][0], "20");
        let other = live_rows(
            &reader,
            "SELECT close FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s'",
            &changes,
            &[],
        )
        .unwrap();
        assert!(other.iter().any(|row| row[0] == "20"));
        assert!(other.iter().all(|row| row[0] != "99"));
        let _ = std::fs::remove_dir_all(dir);
    }
}
