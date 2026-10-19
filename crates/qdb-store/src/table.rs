//! 用户表。类型只有 INT64、STRING、BOOL。主键唯一，二级索引用于等值查找。
#![forbid(unsafe_code)]

use std::collections::HashMap;

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
}

impl TableSet {
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
        if schema.name.is_empty() || schema.columns.is_empty() {
            return Err(StoreError::Schema("table needs a name and a column".into()));
        }
        let mut seen = HashMap::new();
        let mut primary = 0;
        for column in &schema.columns {
            if column.name.is_empty() || seen.insert(column.name.clone(), ()).is_some() {
                return Err(StoreError::Schema("duplicate column".into()));
            }
            if column.primary_key {
                primary += 1;
            }
        }
        if primary > 1 {
            return Err(StoreError::Schema("one primary key".into()));
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
            if column.primary_key {
                let key = value.key_bytes();
                if self
                    .primary
                    .get(table)
                    .is_some_and(|index| index.contains_key(&key))
                {
                    return Err(StoreError::DuplicateKey);
                }
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

    pub fn insert_encoded(&mut self, bytes: &[u8]) -> Result<(), StoreError> {
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
        for (value, column) in values.iter().zip(&schema.columns) {
            if column.primary_key {
                let key = value.key_bytes();
                let index = self.primary.entry(table.clone()).or_default();
                if index.contains_key(&key) {
                    return Err(StoreError::DuplicateKey);
                }
                index.insert(key, row_index);
            }
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
        self.rows.entry(table).or_default().push(values);
        Ok(())
    }

    pub fn scan(
        &self,
        table: &str,
        column: Option<&str>,
        expected: Option<&Cell>,
        limit: usize,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), StoreError> {
        let schema = self.schemas.get(table).ok_or(StoreError::NotFound)?;
        let columns: Vec<String> = schema
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
        let mut out = Vec::new();
        if let Some(indexes) = indexes {
            for index in indexes {
                if out.len() >= limit {
                    break;
                }
                if let Some(row) = rows.get(index) {
                    out.push(row.iter().map(Cell::display).collect());
                }
            }
        } else {
            for row in rows.iter().take(limit) {
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
        if definition.primary_key {
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
                .unwrap_or_default());
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
            .filter(|(_, row)| row[position] == *expected)
            .map(|(index, _)| index)
            .collect())
    }
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
