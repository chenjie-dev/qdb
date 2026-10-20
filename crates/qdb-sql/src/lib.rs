//! 只读 SQL 子集，以及 `CREATE TABLE` 和 `INSERT`。
//!
//! 语法边界见 `docs/manual/sql.md`。`ORDER BY` 可以被解析，结果仍按存储顺序返回。
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
/// [`SqlEngine::query`] 只读。`CREATE TABLE` 和 `INSERT` 走 [`SqlEngine::execute`]，需要可写引擎。
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

    /// 执行一条语句。`CREATE` 和 `INSERT` 写入 WAL，其余交给 [`SqlEngine::query`]。
    ///
    /// 末尾分号会被去掉。`CREATE`/`INSERT` 在默认组提交下会 `fsync`。
    ///
    /// # Arguments
    ///
    /// * `engine` - 可写引擎。只读引擎上的建表和插入会失败。
    /// * `sql` - 一条语句。
    ///
    /// # Returns
    ///
    /// 查询返回行。建表和插入成功时 `rows` 为空。
    ///
    /// # Errors
    ///
    /// * [`SqlError::Syntax`] - 语句无法解析。
    /// * [`SqlError::Store`] - 表已存在、主键冲突、类型不对，或引擎只读。
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
        SqlEngine::new(engine).query(sql)
    }

    /// 执行 `SELECT`、`SHOW` 或 `DESCRIBE`。不写 WAL。
    ///
    /// `ORDER BY` 可以被解析，结果仍按存储顺序返回。`LIMIT` 不能超过 100000。
    /// 不带 `symbol` 的 `ticks` 会多一列 `symbol`。
    ///
    /// # Arguments
    ///
    /// * `sql` - 一条只读语句。末尾分号会被去掉。
    ///
    /// # Errors
    ///
    /// * [`SqlError::Syntax`] - 语句无法解析，或 `CREATE`/`INSERT` 走到了这里。
    /// * [`SqlError::NotFound`] - `symbol` 不存在。
    /// * [`SqlError::Ambiguous`] - 裸代码对应多只证券。
    /// * [`SqlError::Limit`] - `LIMIT` 大于 100000。
    /// * [`SqlError::Store`] - 用户表不存在，或过滤类型不对。
    pub fn query(&self, sql: &str) -> Result<QueryResult, SqlError> {
        let sql = sql.trim().trim_end_matches(';').trim();
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
        if stmt.limit > 100_000 {
            return Err(SqlError::Limit(stmt.limit));
        }
        match stmt.table.as_str() {
            "klines" => self.query_klines(&stmt),
            "snapshots" => self.query_snapshots(&stmt),
            "symbols" => self.query_symbols(&stmt),
            "ticks" => self.query_ticks(&stmt),
            other => self.query_user(other, &stmt),
        }
    }
}

struct Stmt {
    columns: Vec<String>,
    aggregates: Vec<Agg>,
    table: String,
    symbol: Option<String>,
    period: Option<Period>,
    ts_from: Option<i64>,
    ts_to: Option<i64>,
    eq_column: Option<String>,
    eq_value: Option<String>,
    limit: u64,
}

struct Agg {
    func: String,
    column: String,
}

fn parse(sql: &str) -> Result<Stmt, SqlError> {
    let tokens = tokenize(sql);
    let mut index = 0;
    expect(&tokens, &mut index, "select")?;
    let mut columns = Vec::new();
    let mut aggregates = Vec::new();
    if tokens.get(index).map(|t| t.as_str()) == Some("*") {
        columns.push("*".to_string());
        index += 1;
    } else {
        loop {
            let name = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("column".into()))?
                .clone();
            index += 1;
            if tokens.get(index).map(|t| t.as_str()) == Some("(") {
                index += 1;
                let column = tokens
                    .get(index)
                    .ok_or_else(|| SqlError::Syntax("agg arg".into()))?
                    .clone();
                index += 1;
                expect(&tokens, &mut index, ")")?;
                aggregates.push(Agg {
                    func: name.to_ascii_lowercase(),
                    column,
                });
                columns.push(format!(
                    "{}({})",
                    aggregates.last().unwrap().func,
                    aggregates.last().unwrap().column
                ));
            } else {
                columns.push(name);
            }
            if tokens.get(index).map(|t| t.as_str()) == Some(",") {
                index += 1;
                continue;
            }
            break;
        }
    }
    expect(&tokens, &mut index, "from")?;
    let table = tokens
        .get(index)
        .ok_or_else(|| SqlError::Syntax("table".into()))?
        .to_ascii_lowercase();
    index += 1;
    let mut symbol = None;
    let mut period = None;
    let mut ts_from = None;
    let mut ts_to = None;
    let mut eq_column = None;
    let mut eq_value = None;
    if tokens
        .get(index)
        .map(|t| t.eq_ignore_ascii_case("where"))
        .unwrap_or(false)
    {
        index += 1;
        while index < tokens.len()
            && !tokens[index].eq_ignore_ascii_case("order")
            && !tokens[index].eq_ignore_ascii_case("limit")
            && !tokens[index].eq_ignore_ascii_case("group")
        {
            let field = tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("where".into()))?
                .to_ascii_lowercase();
            index += 1;
            if tokens
                .get(index)
                .map(|t| t.eq_ignore_ascii_case("between"))
                .unwrap_or(false)
            {
                index += 1;
                let from = parse_i64(
                    tokens
                        .get(index)
                        .ok_or_else(|| SqlError::Syntax("between".into()))?,
                )?;
                index += 1;
                expect(&tokens, &mut index, "and")?;
                let to = parse_i64(
                    tokens
                        .get(index)
                        .ok_or_else(|| SqlError::Syntax("between".into()))?,
                )?;
                index += 1;
                if field == "ts" {
                    ts_from = Some(from);
                    ts_to = Some(to);
                }
            } else {
                expect(&tokens, &mut index, "=")?;
                let value = tokens
                    .get(index)
                    .ok_or_else(|| SqlError::Syntax("value".into()))?
                    .clone();
                index += 1;
                if field == "symbol" {
                    symbol = Some(unquote(&value));
                } else if field == "period" {
                    period = Period::parse(&unquote(&value));
                } else {
                    eq_column = Some(field);
                    eq_value = Some(unquote(&value));
                }
            }
            if tokens
                .get(index)
                .map(|t| t.eq_ignore_ascii_case("and"))
                .unwrap_or(false)
            {
                index += 1;
            }
        }
    }
    let mut limit = 1000;
    if let Some(pos) = tokens.iter().position(|t| t.eq_ignore_ascii_case("limit")) {
        limit = tokens
            .get(pos + 1)
            .ok_or_else(|| SqlError::Syntax("limit".into()))?
            .parse()
            .map_err(|_| SqlError::Syntax("limit".into()))?;
    }
    Ok(Stmt {
        columns,
        aggregates,
        table,
        symbol,
        period,
        ts_from,
        ts_to,
        eq_column,
        eq_value,
        limit,
    })
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
        let mut rows = self.engine.klines(symbol_id, period);
        if let Some(from) = stmt.ts_from {
            rows.retain(|row| row.bucket_start_ns >= from);
        }
        if let Some(to) = stmt.ts_to {
            rows.retain(|row| row.bucket_start_ns <= to);
        }
        rows.truncate(stmt.limit as usize);
        if stmt.aggregates.is_empty() {
            let columns = if stmt.columns == ["*"] {
                vec![
                    "ts".into(),
                    "open".into(),
                    "high".into(),
                    "low".into(),
                    "close".into(),
                    "volume".into(),
                    "amount".into(),
                    "seq".into(),
                ]
            } else {
                stmt.columns.clone()
            };
            let out = rows
                .iter()
                .map(|row| columns.iter().map(|col| kline_value(row, col)).collect())
                .collect();
            Ok(QueryResult { columns, rows: out })
        } else {
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
        let quote = self.engine.quote(symbol_id).ok_or(SqlError::NotFound)?;
        let columns = if stmt.columns == ["*"] {
            vec![
                "last".into(),
                "bid_px1".into(),
                "ask_px1".into(),
                "seq".into(),
            ]
        } else {
            stmt.columns.clone()
        };
        let row = columns
            .iter()
            .map(|col| match col.as_str() {
                "last" => quote.last.to_string(),
                "bid_px1" => quote.bid_px[0].to_string(),
                "bid_px2" => quote.bid_px[1].to_string(),
                "bid_px3" => quote.bid_px[2].to_string(),
                "bid_px4" => quote.bid_px[3].to_string(),
                "bid_px5" => quote.bid_px[4].to_string(),
                "ask_px1" => quote.ask_px[0].to_string(),
                "ask_px2" => quote.ask_px[1].to_string(),
                "ask_px3" => quote.ask_px[2].to_string(),
                "ask_px4" => quote.ask_px[3].to_string(),
                "ask_px5" => quote.ask_px[4].to_string(),
                "bid_qty1" => quote.bid_qty[0].to_string(),
                "ask_qty1" => quote.ask_qty[0].to_string(),
                "seq" => quote.seq.to_string(),
                _ => String::new(),
            })
            .collect();
        Ok(QueryResult {
            columns,
            rows: vec![row],
        })
    }

    fn query_symbols(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let columns = vec!["symbol".into(), "exchange".into()];
        let mut rows = Vec::new();
        for symbol in self.engine.symbols() {
            let name = format!("{}:{}", symbol.exchange, symbol.code);
            rows.push(vec![name, symbol.exchange_id.to_string()]);
        }
        rows.truncate(stmt.limit as usize);
        Ok(QueryResult { columns, rows })
    }

    fn query_ticks(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let targets = if let Some(symbol) = &stmt.symbol {
            vec![(symbol.clone(), map_resolve(self.engine.resolve(symbol))?)]
        } else {
            self.engine
                .symbols()
                .iter()
                .map(|symbol| (format!("{}:{}", symbol.exchange, symbol.code), symbol.id))
                .collect()
        };
        let columns = if stmt.symbol.is_none() {
            vec![
                "symbol".into(),
                "ts".into(),
                "price".into(),
                "volume".into(),
                "seq".into(),
            ]
        } else {
            vec!["ts".into(), "price".into(), "volume".into(), "seq".into()]
        };
        let mut rows = Vec::new();
        for (name, symbol_id) in targets {
            for tick in self.engine.ticks(symbol_id) {
                if stmt.ts_from.is_some_and(|from| tick.ts < from)
                    || stmt.ts_to.is_some_and(|to| tick.ts > to)
                {
                    continue;
                }
                if stmt.symbol.is_none() {
                    rows.push(vec![
                        name.clone(),
                        tick.ts.to_string(),
                        tick.price.to_string(),
                        tick.volume.to_string(),
                        tick.seq.to_string(),
                    ]);
                } else {
                    rows.push(vec![
                        tick.ts.to_string(),
                        tick.price.to_string(),
                        tick.volume.to_string(),
                        tick.seq.to_string(),
                    ]);
                }
            }
        }
        rows.truncate(stmt.limit as usize);
        Ok(QueryResult { columns, rows })
    }

    fn query_user(&self, table: &str, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let schema = self
            .engine
            .table_schema(table)
            .ok_or_else(|| SqlError::Syntax(format!("unknown table {table}")))?
            .clone();
        let filter = match (&stmt.eq_column, &stmt.eq_value) {
            (Some(column), Some(raw)) => {
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
        raws.push(unquote(
            tokens
                .get(index)
                .ok_or_else(|| SqlError::Syntax("value".into()))?,
        ));
        index += 1;
        if tokens.get(index).is_some_and(|token| token == ",") {
            index += 1;
        }
    }
    expect(tokens, &mut index, ")")?;
    if raws.len() != schema.columns.len() {
        return Err(SqlError::Syntax("column count".into()));
    }
    let values = raws
        .iter()
        .zip(&schema.columns)
        .map(|(raw, column)| parse_cell(column.ty, raw))
        .collect::<Result<Vec<_>, _>>()?;
    engine.insert_row(&name, &values).map_err(map_store)?;
    Ok(QueryResult {
        columns: vec!["result".into()],
        rows: vec![vec!["inserted".into()]],
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
        if ch == ',' || ch == '(' || ch == ')' || ch == '=' {
            tokens.push(ch.to_string());
            continue;
        }
        let mut word = String::from(ch);
        while let Some(next) = chars.peek() {
            if next.is_whitespace() || *next == ',' || *next == '(' || *next == ')' || *next == '='
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

fn parse_i64(value: &str) -> Result<i64, SqlError> {
    unquote(value)
        .parse()
        .map_err(|_| SqlError::Syntax(value.into()))
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
        assert_eq!(all.columns, ["symbol", "ts", "price", "volume", "seq"]);
        assert!(all
            .rows
            .iter()
            .any(|row| row[0] == "SZSE:000001" && row[2] == "10"));
        assert!(all.rows.iter().any(|row| row[0] == "SSE:000001"));
        let one = sql
            .query("SELECT * FROM ticks WHERE symbol = 'SZSE:000001'")
            .unwrap();
        assert_eq!(one.columns, ["ts", "price", "volume", "seq"]);
        assert!(one.rows.iter().all(|row| row.len() == 4));
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
        assert_eq!(described.rows[0][0], "ts");
        assert_eq!(described.rows.len(), 8);
        assert!(sql.query("DESC nope").is_err());
    }
}
