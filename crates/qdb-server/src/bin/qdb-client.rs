//! 只读命令行客户端。查看表结构，并查询已经落盘的数据。

use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use qdb_abi::Period;
use qdb_sql::{QueryResult, SqlEngine};
use qdb_store::{Change, Engine};

fn main() -> ExitCode {
    let args = match Args::parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };
    if args.help {
        println!("{}", help_text());
        return ExitCode::SUCCESS;
    }
    if let Some(command) = args.command {
        return finish(&args.data, &command);
    }
    repl(&args.data)
}

struct Args {
    data: PathBuf,
    command: Option<String>,
    help: bool,
}

impl Args {
    fn parse<I>(input: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut data = PathBuf::from("data");
        let mut command = None;
        let mut help = false;
        let mut tokens = input.into_iter();
        while let Some(token) = tokens.next() {
            match token.as_str() {
                "-h" | "--help" => help = true,
                "-c" | "--command" => {
                    command = Some(
                        tokens
                            .next()
                            .ok_or_else(|| "missing statement after -c".to_string())?,
                    );
                }
                other if other.starts_with('-') => return Err(format!("unknown argument {other}")),
                other => data = PathBuf::from(other),
            }
        }
        Ok(Self {
            data,
            command,
            help,
        })
    }
}

fn repl(data: &Path) -> ExitCode {
    let interactive = io::stdin().is_terminal();
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    loop {
        if interactive {
            eprint!("qdb> ");
            let _ = io::stderr().flush();
        }
        let Some(line) = lines.next() else {
            break;
        };
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                eprintln!("io: {err}");
                return ExitCode::from(1);
            }
        };
        if line.trim() == ".quit" || line.trim() == ".exit" {
            break;
        }
        let code = finish(data, &line);
        if code != ExitCode::SUCCESS && !interactive {
            return code;
        }
    }
    ExitCode::SUCCESS
}

fn finish(data: &Path, line: &str) -> ExitCode {
    match parse_watch(line) {
        Some(Ok(watch)) => return follow(data, &watch),
        Some(Err(message)) => {
            eprintln!("{message}");
            return ExitCode::from(1);
        }
        None => {}
    }
    match execute(data, line) {
        Ok(text) => {
            if !text.is_empty() {
                print!("{text}");
            }
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(1)
        }
    }
}

fn execute(data: &Path, line: &str) -> Result<String, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(String::new());
    }
    let statement = if let Some(rest) = line.strip_prefix('.') {
        match rest.split_whitespace().collect::<Vec<_>>().as_slice() {
            ["tables"] => "SHOW TABLES".to_string(),
            ["schema"] => return describe_all(data),
            ["schema", table] => format!("DESCRIBE {table}"),
            ["help"] => return Ok(help_text()),
            ["quit"] | ["exit"] => return Ok(String::new()),
            _ => return Err("unknown command. try .help".into()),
        }
    } else {
        line.to_string()
    };
    let engine = Engine::open_read(data).map_err(|err| err.to_string())?;
    let result = SqlEngine::new(&engine)
        .query(&statement)
        .map_err(|err| err.to_string())?;
    Ok(format_result(&result))
}

fn describe_all(data: &Path) -> Result<String, String> {
    let engine = Engine::open_read(data).map_err(|err| err.to_string())?;
    let sql = SqlEngine::new(&engine);
    let mut out = String::new();
    for table in ["symbols", "ticks", "snapshots", "klines"] {
        let result = sql
            .query(&format!("DESCRIBE {table}"))
            .map_err(|err| err.to_string())?;
        out.push_str(table);
        out.push('\n');
        out.push_str(&format_result(&result));
        out.push('\n');
    }
    Ok(out)
}

fn format_result(result: &QueryResult) -> String {
    let widths: Vec<usize> = (0..result.columns.len())
        .map(|index| {
            result
                .columns
                .get(index)
                .map(|cell| display_width(cell))
                .unwrap_or(0)
                .max(
                    result
                        .rows
                        .iter()
                        .filter_map(|row| row.get(index))
                        .map(|cell| display_width(cell))
                        .max()
                        .unwrap_or(0),
                )
        })
        .collect();
    let mut out = String::new();
    out.push_str(&format_row(&result.columns, &widths));
    out.push('\n');
    out.push_str(&separator(&widths));
    out.push('\n');
    for row in &result.rows {
        out.push_str(&format_row(row, &widths));
        out.push('\n');
    }
    out.push_str(&format!("({} rows)\n", result.rows.len()));
    out
}

fn format_row(cells: &[String], widths: &[usize]) -> String {
    cells
        .iter()
        .enumerate()
        .map(|(index, cell)| {
            let width = widths.get(index).copied().unwrap_or(0);
            format!(
                "{cell}{}",
                " ".repeat(width.saturating_sub(display_width(cell)))
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn separator(widths: &[usize]) -> String {
    widths
        .iter()
        .map(|width| "-".repeat((*width).max(1)))
        .collect::<Vec<_>>()
        .join("-+-")
}

fn display_width(text: &str) -> usize {
    text.chars()
        .map(|ch| if (ch as u32) < 128 { 1 } else { 2 })
        .sum()
}

struct Watch {
    table: &'static str,
    symbol: String,
    period: Option<String>,
}

fn parse_watch(line: &str) -> Option<Result<Watch, String>> {
    let line = line.trim().trim_end_matches(';').trim();
    let rest = line
        .strip_prefix(".watch")
        .or_else(|| line.strip_prefix(".sub"))
        .or_else(|| line.strip_prefix("SUBSCRIBE"))
        .or_else(|| line.strip_prefix("subscribe"))?;
    if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let parts: Vec<&str> = rest.split_whitespace().collect();
    let watch = match parts.as_slice() {
        [symbol] => Watch {
            table: "snapshots",
            symbol: (*symbol).to_string(),
            period: None,
        },
        ["snapshots" | "ticks", symbol] => Watch {
            table: if parts[0] == "ticks" {
                "ticks"
            } else {
                "snapshots"
            },
            symbol: (*symbol).to_string(),
            period: None,
        },
        ["klines", symbol, period] => Watch {
            table: "klines",
            symbol: (*symbol).to_string(),
            period: Some((*period).to_string()),
        },
        _ => {
            return Some(Err(
                "usage: .watch [snapshots|ticks|klines] SYMBOL [period]".into(),
            ))
        }
    };
    if watch.table == "klines" && Period::parse(watch.period.as_deref().unwrap_or("")).is_none() {
        return Some(Err("unknown period".into()));
    }
    Some(Ok(watch))
}

fn follow(data: &Path, watch: &Watch) -> ExitCode {
    let mut engine = match Engine::open_read(data) {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(1);
        }
    };
    print!("{}", current_line(&engine, watch));
    let _ = io::stdout().flush();
    loop {
        match engine.poll() {
            Ok(changes) => {
                for change in changes {
                    if watch_matches(&change, watch) {
                        println!("{}", format_change(&change));
                    }
                }
                let _ = io::stdout().flush();
            }
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(1);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

fn current_line(engine: &Engine, watch: &Watch) -> String {
    let Ok(id) = engine.resolve(&watch.symbol) else {
        return format!("waiting {}\n", watch.symbol);
    };
    match watch.table {
        "ticks" => match engine.ticks(id).last() {
            Some(tick) => format!(
                "ticks\t{}\tseq={}\tts={}\tprice={}\tvolume={}\n",
                watch.symbol, tick.seq, tick.ts, tick.price, tick.volume
            ),
            None => format!("waiting {}\n", watch.symbol),
        },
        "klines" => {
            let period = Period::parse(watch.period.as_deref().unwrap_or("")).unwrap();
            match engine.klines(id, period).last() {
                Some(bar) => format!(
                    "klines\t{}\t{}\tts={}\topen={}\thigh={}\tlow={}\tclose={}\tvolume={}\n",
                    watch.symbol,
                    period.as_str(),
                    bar.bucket_start_ns,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume
                ),
                None => format!("waiting {}\n", watch.symbol),
            }
        }
        _ => match engine.quote(id) {
            Some(quote) => format!(
                "snapshots\t{}\tseq={}\tlast={}\tbid={}\task={}\n",
                watch.symbol, quote.seq, quote.last, quote.bid_px[0], quote.ask_px[0]
            ),
            None => format!("waiting {}\n", watch.symbol),
        },
    }
}

fn watch_matches(change: &Change, watch: &Watch) -> bool {
    if change.table != watch.table || change.symbol != watch.symbol {
        return false;
    }
    if watch.table == "klines" {
        return change.period == watch.period.as_deref().unwrap_or("");
    }
    true
}

fn format_change(change: &Change) -> String {
    match change.table {
        "ticks" => format!(
            "ticks\t{}\tseq={}\tts={}\tprice={}\tvolume={}",
            change.symbol, change.seq, change.ts, change.price, change.volume
        ),
        "klines" => format!(
            "klines\t{}\t{}\tts={}\topen={}\thigh={}\tlow={}\tclose={}\tvolume={}",
            change.symbol,
            change.period,
            change.ts,
            change.open,
            change.high,
            change.low,
            change.close,
            change.volume
        ),
        _ => format!(
            "snapshots\t{}\tseq={}\tlast={}\tbid={}\task={}",
            change.symbol, change.seq, change.last, change.bid, change.ask
        ),
    }
}

fn help_text() -> String {
    "\
qdb-client [数据目录] [-c 语句]

只读。查询重放已经 fsync 的 WAL。订阅接着读新写入的记录。不修改 region.bin。

  SHOW TABLES
  DESCRIBE 表名
  SELECT ...
  .watch [snapshots|ticks|klines] 交易所:代码 [周期]

  .tables          列出表
  .schema [表名]   列出列
  .help
  .quit

.watch 先打印当前行，再持续打印变更。Ctrl-C 结束。
"
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PathBuf {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("qdb-client-{n}"));
        let _ = std::fs::remove_dir_all(&dir);
        let mut engine = qdb_store::Engine::open(&dir).unwrap();
        engine
            .write_trade("SSE", "600000", 1_704_160_200_000_000_000, 1_050_000, 100)
            .unwrap();
        drop(engine);
        dir
    }

    #[test]
    fn client_lists_tables_and_reads_a_trade() {
        let dir = sample();
        let tables = execute(&dir, ".tables").unwrap();
        assert!(tables.contains("klines"));
        assert!(tables.contains("symbol, period"));
        let described = execute(&dir, ".schema ticks").unwrap();
        assert!(described.contains("price"));
        let rows = execute(
            &dir,
            "SELECT price, volume FROM ticks WHERE symbol = 'SSE:600000'",
        )
        .unwrap();
        assert!(rows.contains("1050000"));
        assert!(rows.contains("100"));
        let region = std::fs::read(dir.join("region.bin")).unwrap();
        execute(&dir, "SHOW TABLES").unwrap();
        assert_eq!(std::fs::read(dir.join("region.bin")).unwrap(), region);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn watch_prints_the_current_price_and_later_changes() {
        let dir = sample();
        let watch = parse_watch(".watch SSE:600000").unwrap().unwrap();
        assert_eq!(watch.table, "snapshots");
        let engine = qdb_store::Engine::open_read(&dir).unwrap();
        let current = current_line(&engine, &watch);
        assert!(current.contains("last=1050000"));
        let change = qdb_store::Change {
            seq: 9,
            table: "snapshots",
            symbol: "SSE:600000".into(),
            ts: 1,
            price: 1060000,
            volume: 1,
            last: 1060000,
            bid: 1,
            ask: 2,
            period: "",
            open: 0,
            high: 0,
            low: 0,
            close: 1060000,
        };
        assert!(watch_matches(&change, &watch));
        assert!(format_change(&change).contains("last=1060000"));
        let ticks = parse_watch("SUBSCRIBE ticks SSE:600000").unwrap().unwrap();
        assert!(!watch_matches(&change, &ticks));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_directory_is_an_error() {
        let missing = std::env::temp_dir().join("qdb-client-missing-dir");
        let _ = std::fs::remove_dir_all(&missing);
        assert!(execute(&missing, "SHOW TABLES").is_err());
    }
}
