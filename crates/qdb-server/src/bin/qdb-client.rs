//! 命令行客户端。查询、订阅和转储走只读打开；建表和插入会短暂成为该库的写入者。

use std::cell::RefCell;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::rc::Rc;

use qdb_abi::Period;
use qdb_sql::{QueryResult, SqlEngine};
use qdb_store::{playback, Catalog, Change, Engine};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{CompletionType, Config, Context, Editor, Helper};

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
        let mut session = Session::new(args.data);
        return finish(&mut session, &command);
    }
    repl(&args.data)
}

struct Session {
    instance: PathBuf,
    database: String,
}

impl Session {
    fn new(instance: PathBuf) -> Self {
        Self {
            instance,
            database: "default".into(),
        }
    }

    fn path(&self) -> Result<PathBuf, String> {
        Catalog::open_read(&self.instance)
            .and_then(|catalog| catalog.path(&self.database))
            .map_err(|err| err.to_string())
    }
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

fn repl(instance: &Path) -> ExitCode {
    if io::stdin().is_terminal() {
        return repl_tty(instance);
    }
    repl_lines(instance)
}

fn repl_tty(instance: &Path) -> ExitCode {
    let session = Rc::new(RefCell::new(Session::new(instance.to_path_buf())));
    let config = Config::builder()
        .completion_type(CompletionType::List)
        .build();
    let mut editor = match Editor::with_config(config) {
        Ok(editor) => editor,
        Err(err) => {
            eprintln!("io: {err}");
            return ExitCode::from(1);
        }
    };
    editor.set_helper(Some(QdbHelper {
        session: session.clone(),
    }));
    loop {
        let prompt = prompt(&session.borrow());
        match editor.readline(&prompt) {
            Ok(line) => {
                if line.trim() == ".quit"
                    || line.trim() == ".exit"
                    || line.trim() == "quit"
                    || line.trim() == "exit"
                {
                    break;
                }
                let _ = editor.add_history_entry(line.as_str());
                let _ = finish(&mut session.borrow_mut(), &line);
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("io: {err}");
                return ExitCode::from(1);
            }
        }
    }
    ExitCode::SUCCESS
}

fn repl_lines(instance: &Path) -> ExitCode {
    let mut session = Session::new(instance.to_path_buf());
    let stdin = io::stdin();
    let mut lines = stdin.lock().lines();
    loop {
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
        if line.trim() == ".quit"
            || line.trim() == ".exit"
            || line.trim() == "quit"
            || line.trim() == "exit"
        {
            break;
        }
        let code = finish(&mut session, &line);
        if code != ExitCode::SUCCESS {
            return code;
        }
    }
    ExitCode::SUCCESS
}

fn prompt(session: &Session) -> String {
    if session.database == "default" {
        "qdb> ".into()
    } else {
        format!("qdb {}> ", session.database)
    }
}

struct CompletionWords {
    commands: Vec<String>,
    databases: Vec<String>,
    tables: Vec<String>,
    symbols: Vec<String>,
    periods: Vec<String>,
}

fn completion_words(session: &Session) -> CompletionWords {
    let databases = Catalog::open_read(&session.instance)
        .and_then(|catalog| catalog.list())
        .unwrap_or_default();
    let symbols = session
        .path()
        .ok()
        .and_then(|path| Engine::open_read(&path).ok())
        .map(|engine| {
            engine
                .symbols()
                .iter()
                .map(|symbol| format!("{}:{}", symbol.exchange, symbol.code))
                .collect()
        })
        .unwrap_or_default();
    CompletionWords {
        commands: [
            "help",
            "ll",
            "ls",
            "quit",
            "exit",
            "SHOW",
            "SELECT",
            "DESCRIBE",
            "DESC",
            "CREATE",
            "USE",
            "SUBSCRIBE",
            ".dump",
            ".restore",
            ".replay",
            ".watch",
            ".sub",
            ".tables",
            ".schema",
            ".help",
            ".quit",
            ".exit",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        databases,
        tables: ["symbols", "ticks", "snapshots", "klines"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        symbols,
        periods: ["1s", "1m", "5m", "15m", "1h", "4h", "1d"]
            .into_iter()
            .map(str::to_string)
            .collect(),
    }
}

fn complete_line(line: &str, pos: usize, words: &CompletionWords) -> (usize, Vec<String>) {
    let pos = pos.min(line.len());
    let start = line[..pos]
        .rfind(char::is_whitespace)
        .map(|index| index + 1)
        .unwrap_or(0);
    let current = &line[start..pos];
    let before: Vec<String> = line[..start]
        .split_whitespace()
        .map(|token| token.to_ascii_uppercase())
        .collect();
    let last = before.last().map(String::as_str);
    let options = match before.as_slice() {
        [] => words.commands.clone(),
        [watch, kind, _]
            if (watch == ".WATCH" || watch == ".SUB" || watch == "SUBSCRIBE")
                && kind == "KLINES" =>
        {
            words.periods.clone()
        }
        [watch, kind]
            if (watch == ".WATCH" || watch == ".SUB" || watch == "SUBSCRIBE")
                && (kind == "SNAPSHOTS" || kind == "TICKS" || kind == "KLINES") =>
        {
            words.symbols.clone()
        }
        _ if last == Some("USE") => words.databases.clone(),
        _ if last == Some("DESCRIBE") || last == Some("DESC") || last == Some(".SCHEMA") => {
            words.tables.clone()
        }
        _ if last == Some("SHOW") => vec!["TABLES".into(), "DATABASES".into()],
        _ if last == Some("CREATE") => vec!["DATABASE".into(), "TABLE".into()],
        _ if last == Some("FROM") => words.tables.clone(),
        _ if last == Some(".WATCH") || last == Some(".SUB") || last == Some("SUBSCRIBE") => {
            let mut options = vec!["snapshots".into(), "ticks".into(), "klines".into()];
            options.extend(words.symbols.clone());
            options
        }
        _ if before.len() >= 2 && before[before.len() - 2] == "SYMBOL" && last == Some("=") => {
            quote_values(&words.symbols)
        }
        _ if before.len() >= 2 && before[before.len() - 2] == "PERIOD" && last == Some("=") => {
            quote_values(&words.periods)
        }
        _ if before.first().is_some_and(|word| word == "SELECT") => {
            vec!["SUBSCRIBE".into()]
        }
        _ => Vec::new(),
    };
    let typed = current.trim_start_matches('\'').to_ascii_lowercase();
    let matches = options
        .into_iter()
        .filter(|option| {
            option
                .to_ascii_lowercase()
                .starts_with(&current.to_ascii_lowercase())
                || option
                    .trim_matches('\'')
                    .to_ascii_lowercase()
                    .starts_with(&typed)
        })
        .collect();
    (start, matches)
}

fn quote_values(values: &[String]) -> Vec<String> {
    values.iter().map(|value| format!("'{value}'")).collect()
}

struct QdbHelper {
    session: Rc<RefCell<Session>>,
}

impl Completer for QdbHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let session = self.session.borrow();
        let words = completion_words(&session);
        let (start, matches) = complete_line(line, pos, &words);
        let pairs = matches
            .into_iter()
            .map(|word| Pair {
                display: word.clone(),
                replacement: word,
            })
            .collect();
        Ok((start, pairs))
    }
}

impl Hinter for QdbHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None
    }
}

impl Highlighter for QdbHelper {}

impl Validator for QdbHelper {}

impl Helper for QdbHelper {}

fn finish(session: &mut Session, line: &str) -> ExitCode {
    if let Some(result) = catalog_command(session, line) {
        return match result {
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
        };
    }
    match parse_watch(line) {
        Some(Ok(watch)) => return follow(session, &watch),
        Some(Err(message)) => {
            eprintln!("{message}");
            return ExitCode::from(1);
        }
        None => {}
    }
    match qdb_sql::split_subscribe(line) {
        Ok(Some(sql)) => return follow_select(session, &sql),
        Ok(None) => {}
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::from(1);
        }
    }
    match execute(session, line) {
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

fn catalog_command(session: &mut Session, line: &str) -> Option<Result<String, String>> {
    let text = line.trim().trim_end_matches(';').trim();
    let parts: Vec<&str> = text.split_whitespace().collect();
    let upper: Vec<String> = parts.iter().map(|part| part.to_ascii_uppercase()).collect();
    match upper.as_slice() {
        [command, what] if command == "SHOW" && what == "DATABASES" => {
            Some(show_databases(&session.instance))
        }
        [command, what, name] if command == "CREATE" && what == "DATABASE" => Some(
            create_database(&session.instance, parts[2])
                .map(|()| format!("created {}\n", parts[2])),
        ),
        [command, name] if command == "USE" => {
            Some(use_database(session, parts[1]).map(|name| format!("database {name}\n")))
        }
        [command] if command == "HELP" || command == "?" => Some(Ok(help_text())),
        [command] if command == "LL" || command == "LS" => Some(list_here(session)),
        _ if upper
            .first()
            .is_some_and(|word| word == ".DUMP" || word == ".RESTORE" || word == ".REPLAY") =>
        {
            Some(file_command(session, &upper[0], parts.get(1).copied()))
        }
        _ => None,
    }
}

fn list_here(session: &Session) -> Result<String, String> {
    let mut out = String::from("databases\n");
    out.push_str(&show_databases(&session.instance)?);
    out.push_str(&format!("\ntables in {}\n", session.database));
    out.push_str(&execute(session, "SHOW TABLES")?);
    Ok(out)
}

fn show_databases(instance: &Path) -> Result<String, String> {
    let names = Catalog::open_read(instance)
        .and_then(|catalog| catalog.list())
        .map_err(|err| err.to_string())?;
    Ok(format_result(&QueryResult {
        columns: vec!["database".into()],
        rows: names.into_iter().map(|name| vec![name]).collect(),
    }))
}

fn create_database(instance: &Path, name: &str) -> Result<(), String> {
    let catalog = Catalog::open_read(instance).map_err(|err| err.to_string())?;
    catalog
        .create(name)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

fn file_command(session: &Session, command: &str, file: Option<&str>) -> Result<String, String> {
    let Some(file) = file else {
        return Err("missing file path".into());
    };
    let path = Path::new(file);
    let catalog = Catalog::open_read(&session.instance).map_err(|err| err.to_string())?;
    match command {
        ".DUMP" => {
            let n = catalog
                .dump(&session.database, path)
                .map_err(|err| err.to_string())?;
            Ok(format!("dumped {n} records\n"))
        }
        ".RESTORE" => {
            let n = catalog
                .restore(&session.database, path)
                .map_err(|err| err.to_string())?;
            Ok(format!("restored {n} records\n"))
        }
        ".REPLAY" => {
            let changes = playback(path).map_err(|err| err.to_string())?;
            let mut out = String::new();
            for change in changes
                .iter()
                .filter(|change| change.table == "ticks" || change.table == "snapshots")
            {
                out.push_str(&format_change(change));
                out.push('\n');
            }
            Ok(out)
        }
        _ => Err("unknown command. try help".into()),
    }
}

fn use_database(session: &mut Session, name: &str) -> Result<String, String> {
    let catalog = Catalog::open_read(&session.instance).map_err(|err| err.to_string())?;
    catalog.path(name).map_err(|err| err.to_string())?;
    session.database = name.to_string();
    Ok(name.to_string())
}

fn execute(session: &Session, line: &str) -> Result<String, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(String::new());
    }
    let statement = if let Some(rest) = line.strip_prefix('.') {
        match rest.split_whitespace().collect::<Vec<_>>().as_slice() {
            ["tables"] => "SHOW TABLES".to_string(),
            ["schema"] => return describe_all(session),
            ["schema", table] => format!("DESCRIBE {table}"),
            ["help"] => return Ok(help_text()),
            ["quit"] | ["exit"] => return Ok(String::new()),
            _ => return Err("unknown command. try help".into()),
        }
    } else {
        line.to_string()
    };
    let head = statement
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if head == "create" || head == "insert" {
        let catalog = Catalog::open_read(&session.instance).map_err(|err| err.to_string())?;
        let mut engine = catalog
            .open_database(&session.database)
            .map_err(|err| err.to_string())?;
        let result = SqlEngine::execute(&mut engine, &statement).map_err(|err| err.to_string())?;
        return Ok(format_result(&result));
    }
    let engine = Engine::open_read(&session.path()?).map_err(|err| err.to_string())?;
    let result = SqlEngine::new(&engine)
        .query(&statement)
        .map_err(|err| err.to_string())?;
    Ok(format_result(&result))
}

fn describe_all(session: &Session) -> Result<String, String> {
    let engine = Engine::open_read(&session.path()?).map_err(|err| err.to_string())?;
    let sql = SqlEngine::new(&engine);
    let mut out = String::new();
    let mut names = vec![
        "symbols".to_string(),
        "ticks".to_string(),
        "snapshots".to_string(),
        "klines".to_string(),
    ];
    names.extend(engine.table_names());
    for table in names {
        let result = sql
            .query(&format!("DESCRIBE {table}"))
            .map_err(|err| err.to_string())?;
        out.push_str(&table);
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

fn follow_select(session: &Session, sql: &str) -> ExitCode {
    let mut engine = match session
        .path()
        .and_then(|path| Engine::open_read(&path).map_err(|err| err.to_string()))
    {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(1);
        }
    };
    let mut seen = Vec::new();
    let mut primed = false;
    match SqlEngine::new(&engine).query(sql) {
        Ok(result) => {
            print!("{}", format_result(&result));
            seen = qdb_sql::captured_rows(&engine, sql).unwrap_or(result.rows);
            primed = true;
        }
        Err(err) if err.to_string().contains("symbol not found") => {
            println!("waiting");
        }
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(1);
        }
    }
    let _ = io::stdout().flush();
    loop {
        let before = engine.applied_seq();
        let changes = match engine.poll() {
            Ok(changes) => changes,
            Err(err) => {
                eprintln!("{err}");
                return ExitCode::from(1);
            }
        };
        if changes.is_empty() && engine.applied_seq() == before {
            std::thread::sleep(std::time::Duration::from_millis(50));
            continue;
        }
        if !primed {
            if let Ok(result) = SqlEngine::new(&engine).query(sql) {
                print!("{}", format_result(&result));
                seen = qdb_sql::captured_rows(&engine, sql).unwrap_or(result.rows);
                primed = true;
                let _ = io::stdout().flush();
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
            continue;
        }
        match qdb_sql::live_rows(&engine, sql, &changes, &seen) {
            Ok(rows) => {
                for row in rows {
                    println!("{}", row.join("\t"));
                    seen.push(row);
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

fn follow(session: &Session, watch: &Watch) -> ExitCode {
    let mut engine = match session
        .path()
        .and_then(|path| Engine::open_read(&path).map_err(|err| err.to_string()))
    {
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

  CREATE DATABASE 库名
  SHOW DATABASES
  USE 库名
  SHOW TABLES
  DESCRIBE 表名
  SELECT ...
  SELECT ... SUBSCRIBE   先打印当前结果，再持续打印匹配的新行
  .dump 文件
  .restore 文件
  .replay 文件
  .watch [snapshots|ticks|klines] 交易所:代码 [周期]

  help             显示本页
  ll               列出库和当前库的表
  Tab              补全命令、库名、表名和代码
  .tables          列出表
  .schema [表名]   列出列
  .help
  .quit

.watch 先打印当前行，再持续打印变更。SELECT ... SUBSCRIBE 按查询列做同样的事。Ctrl-C 结束。
交互时按 Tab 补全。
"
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("qdb-client-{}-{n}", std::process::id()));
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
        let session = Session::new(dir.clone());
        let tables = execute(&session, ".tables").unwrap();
        assert!(tables.contains("klines"));
        assert!(tables.contains("symbol, period"));
        let described = execute(&session, ".schema ticks").unwrap();
        assert!(described.contains("price"));
        let rows = execute(
            &session,
            "SELECT price, volume FROM ticks WHERE symbol = 'SSE:600000'",
        )
        .unwrap();
        assert!(rows.contains("1050000"));
        assert!(rows.contains("100"));
        let region = std::fs::read(dir.join("region.bin")).unwrap();
        execute(&session, "SHOW TABLES").unwrap();
        assert_eq!(std::fs::read(dir.join("region.bin")).unwrap(), region);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn watch_prints_the_current_price_and_later_changes() {
        let dir = sample();
        let watch = parse_watch(".watch SSE:600000").unwrap().unwrap();
        assert_eq!(watch.table, "snapshots");
        let engine =
            qdb_store::Engine::open_read(&Session::new(dir.clone()).path().unwrap()).unwrap();
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
    fn tab_completes_commands_databases_tables_and_symbols() {
        let words = CompletionWords {
            commands: vec!["help".into(), "SHOW".into(), "USE".into(), ".watch".into()],
            databases: vec!["default".into(), "ashare".into()],
            tables: vec!["klines".into(), "ticks".into()],
            symbols: vec!["SSE:600000".into()],
            periods: vec!["1m".into(), "1d".into()],
        };
        let (start, commands) = complete_line("he", 2, &words);
        assert_eq!(start, 0);
        assert_eq!(commands, vec!["help".to_string()]);
        let (_, databases) = complete_line("USE ", 4, &words);
        assert_eq!(databases, vec!["default".to_string(), "ashare".to_string()]);
        let (_, tables) = complete_line("DESCRIBE k", 11, &words);
        assert_eq!(tables, vec!["klines".to_string()]);
        let (start, symbols) = complete_line("SELECT * FROM klines WHERE symbol = 'SS", 39, &words);
        assert!(symbols == vec!["'SSE:600000'".to_string()], "{symbols:?}");
        assert_eq!(&"SELECT * FROM klines WHERE symbol = 'SS"[start..], "'SS");
        let (_, periods) = complete_line(".watch klines SSE:600000 ", 25, &words);
        assert_eq!(periods, vec!["1m".to_string(), "1d".to_string()]);
        let (_, subscribe) = complete_line("SELECT * FROM ticks ", 20, &words);
        assert_eq!(subscribe, vec!["SUBSCRIBE".to_string()]);
    }

    #[test]
    fn help_and_ll_are_client_commands() {
        let dir = sample();
        let mut session = Session::new(dir.clone());
        let help = catalog_command(&mut session, "help").unwrap().unwrap();
        assert!(help.contains("SHOW TABLES"));
        assert!(help.contains("SUBSCRIBE"));
        assert!(help.contains("ll"));
        let listed = catalog_command(&mut session, "ll").unwrap().unwrap();
        assert!(listed.contains("default"));
        assert!(listed.contains("klines"));
        assert!(listed.contains("ticks"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn missing_directory_is_an_error() {
        let missing = std::env::temp_dir().join("qdb-client-missing-dir");
        let _ = std::fs::remove_dir_all(&missing);
        let session = Session::new(missing);
        assert!(execute(&session, "SHOW TABLES").is_err());
    }

    #[test]
    fn create_database_keeps_trades_apart() {
        let dir = sample();
        let mut session = Session::new(dir.clone());
        catalog_command(&mut session, "CREATE DATABASE crypto")
            .unwrap()
            .unwrap();
        let listed = catalog_command(&mut session, "SHOW DATABASES")
            .unwrap()
            .unwrap();
        assert!(listed.contains("default"));
        assert!(listed.contains("crypto"));
        catalog_command(&mut session, "USE crypto")
            .unwrap()
            .unwrap();
        let rows = execute(
            &session,
            "SELECT price FROM ticks WHERE symbol = 'SSE:600000'",
        );
        assert!(rows.is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn dump_restores_into_an_empty_database_and_replay_does_not_require_it() {
        let dir = sample();
        let mut session = Session::new(dir.clone());
        let dump = dir.join("tape.qdump");
        let dumped = catalog_command(&mut session, &format!(".dump {}", dump.display()))
            .unwrap()
            .unwrap();
        assert!(dumped.contains("dumped"));
        catalog_command(&mut session, "CREATE DATABASE copy")
            .unwrap()
            .unwrap();
        catalog_command(&mut session, "USE copy").unwrap().unwrap();
        catalog_command(&mut session, &format!(".restore {}", dump.display()))
            .unwrap()
            .unwrap();
        let rows = execute(
            &session,
            "SELECT price FROM ticks WHERE symbol = 'SSE:600000'",
        )
        .unwrap();
        assert!(rows.contains("1050000"));
        catalog_command(&mut session, "USE default")
            .unwrap()
            .unwrap();
        assert!(
            catalog_command(&mut session, &format!(".restore {}", dump.display()))
                .unwrap()
                .is_err()
        );
        let replay = catalog_command(&mut session, &format!(".replay {}", dump.display()))
            .unwrap()
            .unwrap();
        assert!(replay.contains("1050000"));
        let still = execute(
            &session,
            "SELECT price FROM ticks WHERE symbol = 'SSE:600000'",
        )
        .unwrap();
        assert!(still.contains("1050000"));
        std::fs::write(&dump, b"NOTADUMP").unwrap();
        catalog_command(&mut session, "USE copy").unwrap().unwrap();
        assert!(
            catalog_command(&mut session, &format!(".restore {}", dump.display()))
                .unwrap()
                .is_err()
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
