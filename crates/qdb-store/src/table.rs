//! 普通表。类型只有 `INT64`、`STRING`、`BOOL`。主键可以多列，合起来唯一。二级索引用于等值查找。
//!
//! 删除不改已经 `fsync` 的字节，只把行标成不可见。调用方建成的表在扫描结果最后附带可见 `seq`，这个 `seq` 不是建表时的列。
//! 引擎登记的四张表仍由写入路径维护，不走这里的删除和条件覆盖。
#![forbid(unsafe_code)]

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use crate::StoreError;

pub const BUILTIN_TABLES: [&str; 4] = ["symbols", "ticks", "snapshots", "klines"];
const MAX_STRING: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Int64 = 1,
    String = 2,
    Bool = 3,
}

impl ColumnType {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "INT64" => Some(Self::Int64),
            "STRING" => Some(Self::String),
            "BOOL" => Some(Self::Bool),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "INT64",
            Self::String => "STRING",
            Self::Bool => "BOOL",
        }
    }

    fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Int64),
            2 => Some(Self::String),
            3 => Some(Self::Bool),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    Int64(i64),
    String(String),
    Bool(bool),
}

impl Cell {
    pub fn display(&self) -> String {
        match self {
            Self::Int64(value) => value.to_string(),
            Self::String(value) => value.clone(),
            Self::Bool(true) => "true".into(),
            Self::Bool(false) => "false".into(),
        }
    }

    fn matches(&self, ty: ColumnType) -> bool {
        matches!(
            (self, ty),
            (Self::Int64(_), ColumnType::Int64)
                | (Self::String(_), ColumnType::String)
                | (Self::Bool(_), ColumnType::Bool)
        )
    }

    fn key_bytes(&self) -> Vec<u8> {
        match self {
            Self::Int64(value) => value.to_le_bytes().to_vec(),
            Self::String(value) => value.as_bytes().to_vec(),
            Self::Bool(value) => vec![u8::from(*value)],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: ColumnType,
    pub primary_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDef {
    pub name: String,
    pub column: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
}

#[derive(Default)]
pub struct TableSet {
    schemas: HashMap<String, TableSchema>,
    rows: HashMap<String, Vec<Vec<Cell>>>,
    primary: HashMap<String, HashMap<Vec<u8>, usize>>,
    secondary: HashMap<String, HashMap<String, HashMap<Vec<u8>, Vec<usize>>>>,
    /// 墓碑行的下标。快照只写出仍然可见的行。
    deleted: HashMap<String, HashSet<usize>>,
    /// 与 `rows` 对齐。可见行的值是写入它的 `seq`。0 表示这份快照没有记下序号。
    seqs: HashMap<String, Vec<u64>>,
    /// 与 `rows` 对齐。非 0 表示该行在这个 UTC 纳秒及之前可以被 `EXPIRE AT` 收成墓碑。
    expires: HashMap<String, Vec<i64>>,
}

impl TableSet {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let names: Vec<String> = self
            .names()
            .into_iter()
            .filter(|name| !BUILTIN_TABLES.contains(&name.as_str()))
            .collect();
        push_u16(&mut out, names.len() as u16);
        for name in &names {
            let schema = &self.schemas[name];
            let encoded = Self::encode_schema(schema);
            out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            out.extend_from_slice(&encoded);
            let rows = &self.rows[name];
            let dead = self.deleted.get(name);
            let live: Vec<(usize, &Vec<Cell>)> = rows
                .iter()
                .enumerate()
                .filter(|(index, _)| !dead.is_some_and(|set| set.contains(index)))
                .map(|(index, row)| (index, row))
                .collect();
            out.extend_from_slice(&(live.len() as u32).to_le_bytes());
            for (_, row) in &live {
                for cell in *row {
                    match cell {
                        Cell::Int64(value) => out.extend_from_slice(&value.to_le_bytes()),
                        Cell::Bool(value) => out.push(u8::from(*value)),
                        Cell::String(value) => push_str(&mut out, value),
                    }
                }
            }
        }
        out.extend_from_slice(b"QSEQ");
        for name in &names {
            let rows = &self.rows[name];
            let dead = self.deleted.get(name);
            for (index, _) in rows.iter().enumerate() {
                if dead.is_some_and(|set| set.contains(&index)) {
                    continue;
                }
                let seq = self
                    .seqs
                    .get(name)
                    .and_then(|seqs| seqs.get(index))
                    .copied()
                    .unwrap_or(0);
                out.extend_from_slice(&seq.to_le_bytes());
            }
        }
        out.extend_from_slice(b"QEXP");
        for name in &names {
            let rows = &self.rows[name];
            let dead = self.deleted.get(name);
            for (index, _) in rows.iter().enumerate() {
                if dead.is_some_and(|set| set.contains(&index)) {
                    continue;
                }
                let expire = self
                    .expires
                    .get(name)
                    .and_then(|expires| expires.get(index))
                    .copied()
                    .unwrap_or(0);
                out.extend_from_slice(&expire.to_le_bytes());
            }
        }
        out
    }

    pub fn from_bytes(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        let mut cursor = 0;
        let count = take_u16(bytes, &mut cursor)? as usize;
        let mut loaded = Vec::new();
        for _ in 0..count {
            let len = take_u32(bytes, &mut cursor)? as usize;
            let end = cursor + len;
            let schema = Self::decode_schema(
                bytes
                    .get(cursor..end)
                    .ok_or_else(|| StoreError::Schema("truncated".into()))?,
            )?;
            cursor = end;
            let row_count = take_u32(bytes, &mut cursor)? as usize;
            self.insert_schema(schema.clone())?;
            for _ in 0..row_count {
                let mut values = Vec::new();
                for column in &schema.columns {
                    values.push(match column.ty {
                        ColumnType::Int64 => Cell::Int64(take_i64(bytes, &mut cursor)?),
                        ColumnType::Bool => Cell::Bool(take_u8(bytes, &mut cursor)? == 1),
                        ColumnType::String => Cell::String(take_str(bytes, &mut cursor)?),
                    });
                }
                let payload = self.encode_row(&schema.name, &values)?;
                self.insert_encoded(&payload, 0)?;
            }
            loaded.push((schema.name, row_count));
        }
        if bytes.get(cursor..cursor + 4) == Some(b"QSEQ") {
            cursor += 4;
            for (name, count) in &loaded {
                for index in 0..*count {
                    let seq = take_u64(bytes, &mut cursor)?;
                    if let Some(slot) = self.seqs.get_mut(name).and_then(|seqs| seqs.get_mut(index))
                    {
                        *slot = seq;
                    }
                }
            }
        }
        if bytes.get(cursor..cursor + 4) == Some(b"QEXP") {
            cursor += 4;
            for (name, count) in &loaded {
                for index in 0..*count {
                    let expire = take_i64(bytes, &mut cursor)?;
                    if let Some(slot) = self
                        .expires
                        .get_mut(name)
                        .and_then(|expires| expires.get_mut(index))
                    {
                        *slot = expire;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.schemas.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn schema(&self, name: &str) -> Option<&TableSchema> {
        self.schemas.get(name)
    }

    pub fn validate_new(schema: &TableSchema) -> Result<(), StoreError> {
        if BUILTIN_TABLES.contains(&schema.name.as_str()) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        Self::validate_shape(schema)?;
        let mut seen = HashMap::new();
        for column in &schema.columns {
            seen.insert(column.name.clone(), ());
        }
        let mut index_names = HashMap::new();
        for index in &schema.indexes {
            if index.name.is_empty() || index_names.insert(index.name.clone(), ()).is_some() {
                return Err(StoreError::Schema("duplicate index".into()));
            }
            if !seen.contains_key(&index.column) {
                return Err(StoreError::Schema(format!(
                    "unknown column {}",
                    index.column
                )));
            }
        }
        Ok(())
    }

    pub fn encode_schema(schema: &TableSchema) -> Vec<u8> {
        let mut out = Vec::new();
        push_str(&mut out, &schema.name);
        push_u16(&mut out, schema.columns.len() as u16);
        for column in &schema.columns {
            push_str(&mut out, &column.name);
            out.push(column.ty as u8);
            out.push(u8::from(column.primary_key));
        }
        push_u16(&mut out, schema.indexes.len() as u16);
        for index in &schema.indexes {
            push_str(&mut out, &index.name);
            push_str(&mut out, &index.column);
        }
        out
    }

    pub fn decode_schema(bytes: &[u8]) -> Result<TableSchema, StoreError> {
        let mut cursor = 0;
        let name = take_str(bytes, &mut cursor)?;
        let ncol = take_u16(bytes, &mut cursor)? as usize;
        let mut columns = Vec::with_capacity(ncol);
        for _ in 0..ncol {
            let column_name = take_str(bytes, &mut cursor)?;
            let ty = ColumnType::from_u8(take_u8(bytes, &mut cursor)?)
                .ok_or_else(|| StoreError::Schema("bad column type".into()))?;
            let primary_key = take_u8(bytes, &mut cursor)? == 1;
            columns.push(ColumnDef {
                name: column_name,
                ty,
                primary_key,
            });
        }
        let nindex = take_u16(bytes, &mut cursor)? as usize;
        let mut indexes = Vec::with_capacity(nindex);
        for _ in 0..nindex {
            let index_name = take_str(bytes, &mut cursor)?;
            let column = take_str(bytes, &mut cursor)?;
            indexes.push(IndexDef {
                name: index_name,
                column,
            });
        }
        let schema = TableSchema {
            name,
            columns,
            indexes,
        };
        Self::validate_new(&schema)?;
        Ok(schema)
    }

    /// 登记一张表。已经存在则保持原定义。
    ///
    /// 引擎用它登记内置表。调用方的 `CREATE TABLE` 仍不能占用这些名字。
    ///
    /// # Errors
    ///
    /// 列定义不合法时返回 [`StoreError::Schema`]。
    pub fn ensure(&mut self, schema: TableSchema) -> Result<(), StoreError> {
        if self.schemas.contains_key(&schema.name) {
            return Ok(());
        }
        Self::validate_shape(&schema)?;
        self.insert_new(schema)
    }

    pub fn clear_rows(&mut self, table: &str) {
        if let Some(rows) = self.rows.get_mut(table) {
            rows.clear();
        }
        if let Some(index) = self.primary.get_mut(table) {
            index.clear();
        }
        if let Some(indexes) = self.secondary.get_mut(table) {
            for index in indexes.values_mut() {
                index.clear();
            }
        }
    }

    /// 按主键写入一行。主键已存在时覆盖该行。
    ///
    /// # Errors
    ///
    /// 表不存在、列数或类型不对时返回错误。没有主键的表不能覆盖，只能追加。
    pub fn upsert(&mut self, table: &str, values: Vec<Cell>) -> Result<(), StoreError> {
        let schema = self
            .schemas
            .get(table)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        check_cells(&schema, &values)?;
        let Some(key) = primary_key(&schema, &values) else {
            return self.insert_values(table, values);
        };
        if let Some(index) = self
            .primary
            .get(table)
            .and_then(|index| index.get(&key).copied())
        {
            self.replace_row(table, &schema, index, values);
            return Ok(());
        }
        self.insert_values(table, values)
    }

    /// 把表名编码成删除载荷。
    ///
    /// # Arguments
    ///
    /// * `table` - 要去掉的调用方表名。
    ///
    /// # Returns
    ///
    /// 小端长度加表名字节。
    pub fn encode_drop(table: &str) -> Vec<u8> {
        let mut out = Vec::new();
        push_str(&mut out, table);
        out
    }

    /// 按 [`TableSet::encode_drop`] 的载荷去掉表。
    ///
    /// 定义、行、主键、二级索引和截止时间都从内存里去掉。WAL 字节不变。表已经不在时失败，避免同一条记录被应用两次。
    ///
    /// # Arguments
    ///
    /// * `bytes` - [`TableSet::encode_drop`] 的输出。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 表名是引擎登记的四张表之一，或载荷被截断。
    pub fn drop_encoded(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        let mut cursor = 0;
        let name = take_str(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(StoreError::Schema("truncated".into()));
        }
        if BUILTIN_TABLES.contains(&name.as_str()) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        if self.schemas.remove(&name).is_none() {
            return Err(StoreError::NotFound);
        }
        // 行和索引必须一起去掉，否则同名再建时会撞上旧下标。
        self.rows.remove(&name);
        self.primary.remove(&name);
        self.secondary.remove(&name);
        self.deleted.remove(&name);
        self.seqs.remove(&name);
        self.expires.remove(&name);
        Ok(())
    }

    /// 把表名编码成清空载荷。
    ///
    /// # Arguments
    ///
    /// * `table` - 要清空的调用方表名。
    ///
    /// # Returns
    ///
    /// 小端长度加表名字节。格式与 [`TableSet::encode_drop`] 相同，消息类型不同。
    pub fn encode_truncate(table: &str) -> Vec<u8> {
        Self::encode_drop(table)
    }

    /// 按 [`TableSet::encode_truncate`] 的载荷清掉可见行。
    ///
    /// 表定义留下。行、主键下标、截止时间和墓碑下标清掉，二级索引按原列重建成空表。WAL 字节不变。表不在时失败。
    ///
    /// # Arguments
    ///
    /// * `bytes` - [`TableSet::encode_truncate`] 的输出。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 表名是引擎登记的四张表之一，或载荷被截断。
    pub fn truncate_encoded(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        let mut cursor = 0;
        let name = take_str(bytes, &mut cursor)?;
        if cursor != bytes.len() {
            return Err(StoreError::Schema("truncated".into()));
        }
        if BUILTIN_TABLES.contains(&name.as_str()) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        let schema = self
            .schemas
            .get(&name)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        self.rows.insert(name.clone(), Vec::new());
        self.primary.insert(name.clone(), HashMap::new());
        self.deleted.remove(&name);
        self.seqs.remove(&name);
        self.expires.remove(&name);
        // 二级索引按原列重建成空表，下次插入不会用到清空前的下标。
        let mut secondary = HashMap::new();
        for index in &schema.indexes {
            secondary.insert(index.column.clone(), HashMap::new());
        }
        self.secondary.insert(name, secondary);
        Ok(())
    }

    pub fn insert_schema(&mut self, schema: TableSchema) -> Result<(), StoreError> {
        if self.schemas.contains_key(&schema.name) {
            return Err(StoreError::TableExists);
        }
        Self::validate_new(&schema)?;
        let name = schema.name.clone();
        self.rows.insert(name.clone(), Vec::new());
        self.primary.insert(name.clone(), HashMap::new());
        let mut secondary = HashMap::new();
        for index in &schema.indexes {
            secondary.insert(index.column.clone(), HashMap::new());
        }
        self.secondary.insert(name.clone(), secondary);
        self.schemas.insert(name, schema);
        Ok(())
    }

    pub fn encode_row(&self, table: &str, values: &[Cell]) -> Result<Vec<u8>, StoreError> {
        self.encode_row_at(table, values, false)
    }

    /// 把多行都编码成插入载荷，但不改内存。
    ///
    /// 某一行类型不对、主键已经可见，或同一批里主键重复时，一行载荷都不返回。没有主键的表不比较行与行。
    ///
    /// # Arguments
    ///
    /// * `table` - 已经存在的表。
    /// * `rows` - 每行的值与列顺序一致。至少一行。
    ///
    /// # Returns
    ///
    /// 与 `rows` 顺序相同的载荷。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::DuplicateKey`] - 主键已经可见，或这一批里出现了两次。
    /// * [`StoreError::TypeMismatch`] - 某个值的类型和列定义不一致。
    /// * [`StoreError::Schema`] - 没有行、列数不对，或字符串超过 1024 字节。
    pub fn encode_inserts(
        &self,
        table: &str,
        rows: &[Vec<Cell>],
    ) -> Result<Vec<Vec<u8>>, StoreError> {
        if rows.is_empty() {
            return Err(StoreError::Schema("column count".into()));
        }
        let schema = self.schemas.get(table).ok_or(StoreError::NotFound)?.clone();
        let mut seen = Vec::new();
        let mut payloads = Vec::with_capacity(rows.len());
        for values in rows {
            let payload = self.encode_row_at(table, values, false)?;
            if let Some(key) = primary_key(&schema, values) {
                if seen.iter().any(|earlier| earlier == &key) {
                    return Err(StoreError::DuplicateKey);
                }
                seen.push(key);
            }
            payloads.push(payload);
        }
        Ok(payloads)
    }

    fn encode_row_at(
        &self,
        table: &str,
        values: &[Cell],
        allow_present: bool,
    ) -> Result<Vec<u8>, StoreError> {
        let schema = self.schemas.get(table).ok_or(StoreError::NotFound)?;
        if values.len() != schema.columns.len() {
            return Err(StoreError::Schema("column count".into()));
        }
        for (value, column) in values.iter().zip(&schema.columns) {
            if !value.matches(column.ty) {
                return Err(StoreError::TypeMismatch);
            }
            if let Cell::String(text) = value {
                if text.len() > MAX_STRING {
                    return Err(StoreError::Schema("string longer than 1024 bytes".into()));
                }
            }
        }
        if let Some(key) = primary_key(schema, values) {
            if !allow_present
                && self
                    .primary
                    .get(table)
                    .is_some_and(|index| index.contains_key(&key))
            {
                return Err(StoreError::DuplicateKey);
            }
        }
        let mut out = Vec::new();
        push_str(&mut out, table);
        for value in values {
            match value {
                Cell::Int64(value) => out.extend_from_slice(&value.to_le_bytes()),
                Cell::Bool(value) => out.push(u8::from(*value)),
                Cell::String(value) => push_str(&mut out, value),
            }
        }
        Ok(out)
    }

    pub fn insert_encoded(&mut self, bytes: &[u8], seq: u64) -> Result<(), StoreError> {
        let mut cursor = 0;
        let table = take_str(bytes, &mut cursor)?;
        let schema = self
            .schemas
            .get(&table)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        let mut values = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let cell = match column.ty {
                ColumnType::Int64 => Cell::Int64(take_i64(bytes, &mut cursor)?),
                ColumnType::Bool => Cell::Bool(take_u8(bytes, &mut cursor)? == 1),
                ColumnType::String => {
                    let text = take_str(bytes, &mut cursor)?;
                    if text.len() > MAX_STRING {
                        return Err(StoreError::Schema("string longer than 1024 bytes".into()));
                    }
                    Cell::String(text)
                }
            };
            values.push(cell);
        }
        let row_index = self.rows.get(&table).map(Vec::len).unwrap_or(0);
        if let Some(key) = primary_key(&schema, &values) {
            let index = self.primary.entry(table.clone()).or_default();
            if index.contains_key(&key) {
                return Err(StoreError::DuplicateKey);
            }
            index.insert(key, row_index);
        }
        for index in &schema.indexes {
            let position = schema
                .columns
                .iter()
                .position(|column| column.name == index.column)
                .unwrap();
            let key = values[position].key_bytes();
            self.secondary
                .entry(table.clone())
                .or_default()
                .entry(index.column.clone())
                .or_default()
                .entry(key)
                .or_default()
                .push(row_index);
        }
        self.rows.entry(table.clone()).or_default().push(values);
        self.seqs.entry(table.clone()).or_default().push(seq);
        let expire = if cursor < bytes.len() {
            take_i64(bytes, &mut cursor)?
        } else {
            0
        };
        self.expires.entry(table).or_default().push(expire);
        Ok(())
    }

    /// 读取这个主键当前可见行的提交序号。
    ///
    /// 只查内存里的主键索引，不读 WAL，也不分配新序号。墓碑之后的键返回 `None`，和从来没插入过一样。旧快照若没记下序号，可见行的值可以是 0；0 不是「键不存在」。
    ///
    /// # Arguments
    ///
    /// * `table` - 已登记的表名。
    /// * `key` - 主键列的值，顺序和个数都要与建表时的主键列一致。
    ///
    /// # Returns
    ///
    /// `Some(seq)` 是当前可见行的序号。`None` 表示这个键现在没有可见行。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 没有主键，或 `key` 的个数不对。
    /// * [`StoreError::TypeMismatch`] - 某个值的类型和主键列不一致。
    pub fn visible_seq(&self, table: &str, key: &[Cell]) -> Result<Option<u64>, StoreError> {
        let schema = self.schemas.get(table).ok_or(StoreError::NotFound)?;
        let pk: Vec<&ColumnDef> = schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .collect();
        if pk.is_empty() {
            return Err(StoreError::Schema("primary key required".into()));
        }
        if key.len() != pk.len() {
            return Err(StoreError::Schema("column count".into()));
        }
        for (value, column) in key.iter().zip(&pk) {
            if !value.matches(column.ty) {
                return Err(StoreError::TypeMismatch);
            }
        }
        let Some(index) = self
            .primary
            .get(table)
            .and_then(|index| index.get(&key_bytes(key)).copied())
        else {
            return Ok(None);
        };
        Ok(Some(
            self.seqs
                .get(table)
                .and_then(|seqs| seqs.get(index))
                .copied()
                .unwrap_or(0),
        ))
    }

    /// 读取这个主键当前可见的整行。
    ///
    /// 只查内存里的主键索引，不读 WAL，也不分配新序号。墓碑之后返回 `None`。单元格按建表时的列顺序排列，不含查询结果末尾的 `seq`。
    ///
    /// # Arguments
    ///
    /// * `table` - 已登记的表名。
    /// * `key` - 主键列的值，顺序和个数都要与建表时的主键列一致。
    ///
    /// # Returns
    ///
    /// `Some` 是当前可见行。`None` 表示这个键现在没有可见行。
    ///
    /// # Errors
    ///
    /// 与 [`TableSet::visible_seq`] 相同。主键索引和行槽不一致时返回 [`StoreError::KeyNotFound`]。
    pub fn visible_row(&self, table: &str, key: &[Cell]) -> Result<Option<Vec<Cell>>, StoreError> {
        if self.visible_seq(table, key)?.is_none() {
            return Ok(None);
        }
        let index = self
            .primary
            .get(table)
            .and_then(|index| index.get(&key_bytes(key)).copied())
            .ok_or(StoreError::KeyNotFound)?;
        let row = self
            .rows
            .get(table)
            .and_then(|rows| rows.get(index))
            .cloned()
            .ok_or(StoreError::KeyNotFound)?;
        Ok(Some(row))
    }

    /// 把一次条件覆盖编码成 WAL 载荷。
    ///
    /// 不写盘。载荷前 8 字节是期望序号，后面是与 [`TableSet::encode_row`] 相同的行。`expected` 必须大于 0，并且等于这个主键的可见序号。
    ///
    /// # Arguments
    ///
    /// * `table` - 带主键的调用方表。
    /// * `values` - 覆盖后的整行，列顺序与建表时一致。主键列必须仍指向当前那一行。
    /// * `expected` - 调用方看见的可见 `seq`，不能为 0。
    ///
    /// # Returns
    ///
    /// 可以交给 [`TableSet::replace_encoded`] 的字节。
    ///
    /// # Errors
    ///
    /// * [`StoreError::Conflict`] - `expected` 为 0，或不是当前可见序号。
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 没有主键，列数不对，或字符串超过 1024 字节。
    /// * [`StoreError::TypeMismatch`] - 某个值的类型和列定义不一致。
    pub fn encode_expect(
        &self,
        table: &str,
        values: &[Cell],
        expected: u64,
        expire_at: Option<i64>,
    ) -> Result<Vec<u8>, StoreError> {
        if expected == 0 {
            return Err(StoreError::Conflict);
        }
        let schema = self.schemas.get(table).ok_or(StoreError::NotFound)?;
        let key: Vec<Cell> = values
            .iter()
            .zip(&schema.columns)
            .filter(|(_, column)| column.primary_key)
            .map(|(value, _)| value.clone())
            .collect();
        if key.is_empty() {
            return Err(StoreError::Schema("primary key required".into()));
        }
        match self.visible_seq(table, &key)? {
            Some(seq) if seq == expected => {}
            _ => return Err(StoreError::Conflict),
        }
        let row = self.encode_row_at(table, values, true)?;
        let mut out = expected.to_le_bytes().to_vec();
        out.extend(row);
        if let Some(expire_at) = expire_at {
            out.extend_from_slice(&expire_at.to_le_bytes());
        }
        Ok(out)
    }

    /// 把一条条件覆盖应用到内存表。
    ///
    /// 行留在原来的槽位，主键索引的指向不变，只换单元格和可见 `seq`。重放时期望序号必须仍然对得上，否则拒绝这条记录。
    ///
    /// # Arguments
    ///
    /// * `bytes` - [`TableSet::encode_expect`] 的输出。短于 8 字节会失败。
    /// * `seq` - 这次提交分配到的新序号，会成为该主键下一次的期望值。
    ///
    /// # Errors
    ///
    /// * [`StoreError::Conflict`] - 载荷里的期望序号不是当前可见序号。
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 载荷截断或列对不上。
    pub fn replace_encoded(&mut self, bytes: &[u8], seq: u64) -> Result<(), StoreError> {
        if bytes.len() < 8 {
            return Err(StoreError::Schema("truncated".into()));
        }
        let expected = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let mut cursor = 8;
        let table = take_str(bytes, &mut cursor)?;
        let schema = self
            .schemas
            .get(&table)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        let mut values = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            values.push(match column.ty {
                ColumnType::Int64 => Cell::Int64(take_i64(bytes, &mut cursor)?),
                ColumnType::Bool => Cell::Bool(take_u8(bytes, &mut cursor)? == 1),
                ColumnType::String => Cell::String(take_str(bytes, &mut cursor)?),
            });
        }
        let expire_at = if cursor < bytes.len() {
            Some(take_i64(bytes, &mut cursor)?)
        } else {
            None
        };
        let key: Vec<Cell> = values
            .iter()
            .zip(&schema.columns)
            .filter(|(_, column)| column.primary_key)
            .map(|(value, _)| value.clone())
            .collect();
        if self.visible_seq(&table, &key)? != Some(expected) {
            return Err(StoreError::Conflict);
        }
        let index = self
            .primary
            .get(&table)
            .and_then(|index| index.get(&key_bytes(&key)).copied())
            .ok_or(StoreError::Conflict)?;
        self.replace_row(&table, &schema, index, values);
        // 覆盖不新开槽位，所以序号写回原来的下标，而不是再 push 一条。
        if let Some(slot) = self
            .seqs
            .get_mut(&table)
            .and_then(|seqs| seqs.get_mut(index))
        {
            *slot = seq;
        }
        if let Some(expire_at) = expire_at {
            if let Some(slot) = self
                .expires
                .get_mut(&table)
                .and_then(|expires| expires.get_mut(index))
            {
                *slot = expire_at;
            }
        }
        Ok(())
    }

    /// 收集截止时间不晚于 `deadline_ns`、并且仍然可见的主键。
    ///
    /// 截止时间为 0 的行不算到期。没有主键的表跳过。顺序是表名，然后是主键字节序。
    ///
    /// # Arguments
    ///
    /// * `deadline_ns` - UTC 纳秒水位。
    ///
    /// # Returns
    ///
    /// 表名和主键列的值。没有命中时为空。
    pub fn keys_expiring_by(&self, deadline_ns: i64) -> Vec<(String, Vec<Cell>)> {
        let mut names: Vec<String> = self
            .schemas
            .keys()
            .filter(|name| !BUILTIN_TABLES.contains(&name.as_str()))
            .cloned()
            .collect();
        names.sort();
        let mut out = Vec::new();
        for name in names {
            let Some(schema) = self.schemas.get(&name) else {
                continue;
            };
            let pk: Vec<usize> = schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, column)| column.primary_key)
                .map(|(index, _)| index)
                .collect();
            if pk.is_empty() {
                continue;
            }
            let Some(rows) = self.rows.get(&name) else {
                continue;
            };
            let mut hits = Vec::new();
            for (index, row) in rows.iter().enumerate() {
                if self.is_deleted(&name, index) {
                    continue;
                }
                let expire = self
                    .expires
                    .get(&name)
                    .and_then(|expires| expires.get(index))
                    .copied()
                    .unwrap_or(0);
                if expire == 0 || expire > deadline_ns {
                    continue;
                }
                let key: Vec<Cell> = pk.iter().map(|position| row[*position].clone()).collect();
                hits.push((key_bytes(&key), key));
            }
            hits.sort_by(|left, right| left.0.cmp(&right.0));
            for (_, key) in hits {
                out.push((name.clone(), key));
            }
        }
        out
    }

    /// 扫描一张表里仍然可见的行。
    ///
    /// 墓碑行会被跳过，并且不占用 `limit`。调用方建成的表在列尾追加文本列 `seq`。引擎登记的四张表不加这一列。
    ///
    /// # Arguments
    ///
    /// * `table` - 已登记的表名。
    /// * `column` - 等值过滤的列。`None` 表示不过滤。
    /// * `expected` - 与 `column` 一起给出的值。类型必须和列一致。
    /// * `limit` - 最多返回的可见行数。
    ///
    /// # Returns
    ///
    /// 列名，以及每行的文本。调用方表的最后一列是可见 `seq`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 过滤列不存在。
    /// * [`StoreError::TypeMismatch`] - 过滤值的类型不对。
    pub fn scan(
        &self,
        table: &str,
        column: Option<&str>,
        expected: Option<&Cell>,
        limit: usize,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), StoreError> {
        let schema = self.schemas.get(table).ok_or(StoreError::NotFound)?;
        let mut columns: Vec<String> = schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        let rows = self.rows.get(table).map(Vec::as_slice).unwrap_or(&[]);
        let indexes = match (column, expected) {
            (Some(column), Some(expected)) => {
                Some(self.matching_rows(table, schema, column, expected)?)
            }
            _ => None,
        };
        let mut out: Vec<Vec<String>> = Vec::new();
        let mut emitted = Vec::new();
        if let Some(indexes) = indexes {
            for index in indexes {
                if out.len() >= limit {
                    break;
                }
                if self.is_deleted(table, index) {
                    continue;
                }
                if let Some(row) = rows.get(index) {
                    out.push(row.iter().map(Cell::display).collect());
                    emitted.push(self.row_seq(table, index));
                }
            }
        } else {
            for (index, row) in rows.iter().enumerate() {
                if out.len() >= limit {
                    break;
                }
                if self.is_deleted(table, index) {
                    continue;
                }
                if let Some(expected) = expected {
                    let position = schema
                        .columns
                        .iter()
                        .position(|item| Some(item.name.as_str()) == column);
                    let Some(position) = position else {
                        return Err(StoreError::Schema("unknown column".into()));
                    };
                    if &row[position] != expected {
                        continue;
                    }
                }
                out.push(row.iter().map(Cell::display).collect());
                emitted.push(self.row_seq(table, index));
            }
        }
        if !BUILTIN_TABLES.contains(&table) {
            // seq 不是建表列，只附在调用方表的结果末尾，给 EXPECT 读取。
            columns.push("seq".into());
            for (row, seq) in out.iter_mut().zip(emitted.iter()) {
                row.push(seq.to_string());
            }
        }
        Ok((columns, out))
    }

    fn matching_rows(
        &self,
        table: &str,
        schema: &TableSchema,
        column: &str,
        expected: &Cell,
    ) -> Result<Vec<usize>, StoreError> {
        let definition = schema
            .columns
            .iter()
            .find(|item| item.name == column)
            .ok_or_else(|| StoreError::Schema("unknown column".into()))?;
        if !expected.matches(definition.ty) {
            return Err(StoreError::TypeMismatch);
        }
        let key = expected.key_bytes();
        let primary_columns = schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .count();
        if definition.primary_key && primary_columns == 1 {
            return Ok(self
                .primary
                .get(table)
                .and_then(|index| index.get(&key).copied())
                .into_iter()
                .collect());
        }
        if schema.indexes.iter().any(|index| index.column == column) {
            return Ok(self
                .secondary
                .get(table)
                .and_then(|indexes| indexes.get(column))
                .and_then(|index| index.get(&key))
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|index| !self.is_deleted(table, *index))
                .collect());
        }
        let position = schema
            .columns
            .iter()
            .position(|item| item.name == column)
            .unwrap();
        let rows = self.rows.get(table).map(Vec::as_slice).unwrap_or(&[]);
        Ok(rows
            .iter()
            .enumerate()
            .filter(|(index, row)| !self.is_deleted(table, *index) && row[position] == *expected)
            .map(|(index, _)| index)
            .collect())
    }

    /// 把主键编码成墓碑载荷。
    ///
    /// 不写盘。键必须当前可见。载荷是表名加上按主键列顺序排列的值。
    ///
    /// # Arguments
    ///
    /// * `table` - 已登记、并且有主键的表。
    /// * `key` - 主键列的值，顺序和个数与建表时的主键列一致。
    ///
    /// # Returns
    ///
    /// 可以交给 [`TableSet::delete_encoded`] 的字节。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 没有主键，或给出的值个数不对。
    /// * [`StoreError::TypeMismatch`] - 值的类型和主键列不一致。
    /// * [`StoreError::KeyNotFound`] - 这个主键当前没有可见行。
    pub fn encode_delete(&self, table: &str, key: &[Cell]) -> Result<Vec<u8>, StoreError> {
        let schema = self.schemas.get(table).ok_or(StoreError::NotFound)?;
        let pk: Vec<&ColumnDef> = schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .collect();
        if pk.is_empty() {
            return Err(StoreError::Schema("primary key required".into()));
        }
        if key.len() != pk.len() {
            return Err(StoreError::Schema("column count".into()));
        }
        for (value, column) in key.iter().zip(&pk) {
            if !value.matches(column.ty) {
                return Err(StoreError::TypeMismatch);
            }
        }
        let bytes = key_bytes(key);
        if !self
            .primary
            .get(table)
            .is_some_and(|index| index.contains_key(&bytes))
        {
            return Err(StoreError::KeyNotFound);
        }
        let mut out = Vec::new();
        push_str(&mut out, table);
        for value in key {
            push_cell(&mut out, value);
        }
        Ok(out)
    }

    /// 应用一条墓碑，使主键从查询和二级索引里消失。
    ///
    /// 行的字节留在表槽里，只从主键索引去掉，并记入不可见集合。重放时键已经不可见就失败，避免同一条墓碑被应用两次。
    ///
    /// # Arguments
    ///
    /// * `bytes` - [`TableSet::encode_delete`] 的输出。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::KeyNotFound`] - 主键没有可见行。
    /// * [`StoreError::Schema`] - 载荷截断或类型对不上。
    pub fn delete_encoded(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
        let mut cursor = 0;
        let table = take_str(bytes, &mut cursor)?;
        let schema = self
            .schemas
            .get(&table)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        let pk: Vec<ColumnDef> = schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .cloned()
            .collect();
        let mut key = Vec::with_capacity(pk.len());
        for column in &pk {
            key.push(match column.ty {
                ColumnType::Int64 => Cell::Int64(take_i64(bytes, &mut cursor)?),
                ColumnType::Bool => Cell::Bool(take_u8(bytes, &mut cursor)? == 1),
                ColumnType::String => Cell::String(take_str(bytes, &mut cursor)?),
            });
        }
        let bytes = key_bytes(&key);
        let row_index = self
            .primary
            .get(&table)
            .and_then(|index| index.get(&bytes).copied())
            .ok_or(StoreError::KeyNotFound)?;
        let values = self
            .rows
            .get(&table)
            .and_then(|rows| rows.get(row_index))
            .cloned()
            .ok_or(StoreError::KeyNotFound)?;
        self.primary.get_mut(&table).unwrap().remove(&bytes);
        for index in &schema.indexes {
            let position = schema
                .columns
                .iter()
                .position(|column| column.name == index.column)
                .unwrap();
            let secondary_key = values[position].key_bytes();
            if let Some(list) = self
                .secondary
                .get_mut(&table)
                .and_then(|indexes| indexes.get_mut(&index.column))
                .and_then(|index| index.get_mut(&secondary_key))
            {
                list.retain(|item| *item != row_index);
            }
        }
        self.deleted.entry(table).or_default().insert(row_index);
        Ok(())
    }

    fn is_deleted(&self, table: &str, index: usize) -> bool {
        self.deleted
            .get(table)
            .is_some_and(|set| set.contains(&index))
    }

    fn row_seq(&self, table: &str, index: usize) -> u64 {
        self.seqs
            .get(table)
            .and_then(|seqs| seqs.get(index))
            .copied()
            .unwrap_or(0)
    }
}

/// 行情四表的字典。调用方不能用 `CREATE TABLE` 替换它们。
///
/// # Arguments
///
/// * `name` - `symbols`、`ticks`、`snapshots` 或 `klines`。
///
/// # Returns
///
/// 表不在这四张里时返回 `None`。
pub fn quote_schema(name: &str) -> Option<&'static TableSchema> {
    static SCHEMAS: OnceLock<Vec<TableSchema>> = OnceLock::new();
    let schemas = SCHEMAS.get_or_init(quote_schemas);
    schemas.iter().find(|schema| schema.name == name)
}

fn quote_schemas() -> Vec<TableSchema> {
    vec![
        TableSchema {
            name: "symbols".into(),
            columns: vec![
                col("symbol_id", ColumnType::Int64, true),
                col("symbol", ColumnType::String, false),
                col("exchange", ColumnType::String, false),
                col("code", ColumnType::String, false),
                col("exchange_id", ColumnType::Int64, false),
            ],
            indexes: vec![idx("idx_code", "code")],
        },
        TableSchema {
            name: "ticks".into(),
            columns: vec![
                col("seq", ColumnType::Int64, true),
                col("symbol_id", ColumnType::Int64, false),
                col("symbol", ColumnType::String, false),
                col("ts", ColumnType::Int64, false),
                col("price", ColumnType::Int64, false),
                col("volume", ColumnType::Int64, false),
                col("amount", ColumnType::Int64, false),
            ],
            indexes: vec![idx("idx_symbol", "symbol_id")],
        },
        TableSchema {
            name: "snapshots".into(),
            columns: snapshot_columns(),
            indexes: vec![idx("idx_symbol", "symbol")],
        },
        TableSchema {
            name: "klines".into(),
            columns: vec![
                col("symbol_id", ColumnType::Int64, true),
                col("period_id", ColumnType::Int64, true),
                col("ts", ColumnType::Int64, true),
                col("symbol", ColumnType::String, false),
                col("period", ColumnType::String, false),
                col("open", ColumnType::Int64, false),
                col("high", ColumnType::Int64, false),
                col("low", ColumnType::Int64, false),
                col("close", ColumnType::Int64, false),
                col("volume", ColumnType::Int64, false),
                col("amount", ColumnType::Int64, false),
                col("seq", ColumnType::Int64, false),
            ],
            indexes: vec![idx("idx_symbol", "symbol_id")],
        },
    ]
}

fn snapshot_columns() -> Vec<ColumnDef> {
    let mut columns = vec![
        col("symbol_id", ColumnType::Int64, true),
        col("symbol", ColumnType::String, false),
        col("last", ColumnType::Int64, false),
    ];
    for level in 1..=5 {
        columns.push(col(&format!("bid_px{level}"), ColumnType::Int64, false));
        columns.push(col(&format!("bid_qty{level}"), ColumnType::Int64, false));
        columns.push(col(&format!("ask_px{level}"), ColumnType::Int64, false));
        columns.push(col(&format!("ask_qty{level}"), ColumnType::Int64, false));
    }
    columns.push(col("seq", ColumnType::Int64, false));
    columns
}

fn col(name: &str, ty: ColumnType, primary_key: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        primary_key,
    }
}

fn idx(name: &str, column: &str) -> IndexDef {
    IndexDef {
        name: name.into(),
        column: column.into(),
    }
}

impl TableSet {
    fn validate_shape(schema: &TableSchema) -> Result<(), StoreError> {
        if schema.name.is_empty() || schema.columns.is_empty() {
            return Err(StoreError::Schema("table needs a name and a column".into()));
        }
        let mut seen = HashMap::new();
        for column in &schema.columns {
            if column.name.is_empty() || seen.insert(column.name.clone(), ()).is_some() {
                return Err(StoreError::Schema("duplicate column".into()));
            }
        }
        Ok(())
    }

    fn insert_new(&mut self, schema: TableSchema) -> Result<(), StoreError> {
        let name = schema.name.clone();
        self.rows.insert(name.clone(), Vec::new());
        self.primary.insert(name.clone(), HashMap::new());
        let mut secondary = HashMap::new();
        for index in &schema.indexes {
            secondary.insert(index.column.clone(), HashMap::new());
        }
        self.secondary.insert(name.clone(), secondary);
        self.schemas.insert(name, schema);
        Ok(())
    }

    fn insert_values(&mut self, table: &str, values: Vec<Cell>) -> Result<(), StoreError> {
        let schema = self
            .schemas
            .get(table)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        let row_index = self.rows.get(table).map(Vec::len).unwrap_or(0);
        if let Some(key) = primary_key(&schema, &values) {
            let index = self.primary.entry(table.to_string()).or_default();
            if index.contains_key(&key) {
                return Err(StoreError::DuplicateKey);
            }
            index.insert(key, row_index);
        }
        self.index_secondary(table, &schema, row_index, &values);
        self.rows.entry(table.to_string()).or_default().push(values);
        self.seqs.entry(table.to_string()).or_default().push(0);
        self.expires.entry(table.to_string()).or_default().push(0);
        Ok(())
    }

    fn replace_row(&mut self, table: &str, schema: &TableSchema, index: usize, values: Vec<Cell>) {
        if let Some(old) = self
            .rows
            .get(table)
            .and_then(|rows| rows.get(index))
            .cloned()
        {
            self.unindex_secondary(table, schema, index, &old);
        }
        if let Some(rows) = self.rows.get_mut(table) {
            if let Some(slot) = rows.get_mut(index) {
                *slot = values.clone();
            }
        }
        self.index_secondary(table, schema, index, &values);
    }

    fn index_secondary(
        &mut self,
        table: &str,
        schema: &TableSchema,
        row_index: usize,
        values: &[Cell],
    ) {
        for index in &schema.indexes {
            let Some(position) = schema
                .columns
                .iter()
                .position(|column| column.name == index.column)
            else {
                continue;
            };
            let key = values[position].key_bytes();
            self.secondary
                .entry(table.to_string())
                .or_default()
                .entry(index.column.clone())
                .or_default()
                .entry(key)
                .or_default()
                .push(row_index);
        }
    }

    fn unindex_secondary(
        &mut self,
        table: &str,
        schema: &TableSchema,
        row_index: usize,
        values: &[Cell],
    ) {
        for index in &schema.indexes {
            let Some(position) = schema
                .columns
                .iter()
                .position(|column| column.name == index.column)
            else {
                continue;
            };
            let key = values[position].key_bytes();
            let Some(bucket) = self
                .secondary
                .get_mut(table)
                .and_then(|indexes| indexes.get_mut(&index.column))
                .and_then(|index| index.get_mut(&key))
            else {
                continue;
            };
            bucket.retain(|found| *found != row_index);
        }
    }
}

fn check_cells(schema: &TableSchema, values: &[Cell]) -> Result<(), StoreError> {
    if values.len() != schema.columns.len() {
        return Err(StoreError::Schema("column count".into()));
    }
    for (value, column) in values.iter().zip(&schema.columns) {
        if !value.matches(column.ty) {
            return Err(StoreError::TypeMismatch);
        }
        if let Cell::String(text) = value {
            if text.len() > MAX_STRING {
                return Err(StoreError::Schema("string longer than 1024 bytes".into()));
            }
        }
    }
    Ok(())
}

fn key_bytes(values: &[Cell]) -> Vec<u8> {
    if values.len() == 1 {
        return values[0].key_bytes();
    }
    let mut key = Vec::new();
    for value in values {
        let bytes = value.key_bytes();
        key.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        key.extend_from_slice(&bytes);
    }
    key
}

fn push_cell(out: &mut Vec<u8>, value: &Cell) {
    match value {
        Cell::Int64(value) => out.extend_from_slice(&value.to_le_bytes()),
        Cell::Bool(value) => out.push(u8::from(*value)),
        Cell::String(value) => push_str(out, value),
    }
}

fn primary_key(schema: &TableSchema, values: &[Cell]) -> Option<Vec<u8>> {
    let marked: Vec<Cell> = values
        .iter()
        .zip(&schema.columns)
        .filter(|(_, column)| column.primary_key)
        .map(|(value, _)| value.clone())
        .collect();
    if marked.is_empty() {
        return None;
    }
    Some(key_bytes(&marked))
}

fn push_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_str(out: &mut Vec<u8>, value: &str) {
    push_u16(out, value.len() as u16);
    out.extend_from_slice(value.as_bytes());
}

fn take_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, StoreError> {
    let value = *bytes
        .get(*cursor)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?;
    *cursor += 1;
    Ok(value)
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, StoreError> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?;
    let raw: [u8; 8] = bytes
        .get(*cursor..end)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?
        .try_into()
        .unwrap();
    *cursor = end;
    Ok(u64::from_le_bytes(raw))
}

fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, StoreError> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?;
    let raw: [u8; 4] = bytes
        .get(*cursor..end)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?
        .try_into()
        .unwrap();
    *cursor = end;
    Ok(u32::from_le_bytes(raw))
}

fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, StoreError> {
    let end = cursor
        .checked_add(2)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?;
    let raw: [u8; 2] = bytes
        .get(*cursor..end)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?
        .try_into()
        .unwrap();
    *cursor = end;
    Ok(u16::from_le_bytes(raw))
}

fn take_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64, StoreError> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?;
    let raw: [u8; 8] = bytes
        .get(*cursor..end)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?
        .try_into()
        .unwrap();
    *cursor = end;
    Ok(i64::from_le_bytes(raw))
}

fn take_str(bytes: &[u8], cursor: &mut usize) -> Result<String, StoreError> {
    let len = take_u16(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?;
    let raw = bytes
        .get(*cursor..end)
        .ok_or_else(|| StoreError::Schema("truncated".into()))?;
    *cursor = end;
    String::from_utf8(raw.to_vec()).map_err(|_| StoreError::Schema("bad utf-8".into()))
}
