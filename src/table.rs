use crate::search::{SearchEngine, SearchType};
use crate::storage::{RowId, Storage, Value};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Column {
    pub name: Box<str>,
    pub col_type: ColumnType,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ColumnType {
    Int,
    Float,
    String,
    Bytes,
}

pub struct Table {
    name: Box<str>,
    columns: Vec<Column>,
    storage: Storage,
    search_engine: SearchEngine,
}

impl Table {
    pub fn new(name: &str, columns: Vec<Column>) -> Self {
        let num_cols = columns.len();
        Self {
            name: name.into(),
            columns,
            storage: Storage::new(),
            search_engine: SearchEngine::new(num_cols),
        }
    }

    pub fn with_capacity(name: &str, columns: Vec<Column>, capacity: usize) -> Self {
        let num_cols = columns.len();
        Self {
            name: name.into(),
            columns,
            storage: Storage::with_capacity(capacity),
            search_engine: SearchEngine::with_capacity(num_cols, capacity),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| &*c.name == name)
    }

    #[inline(always)]
    pub fn insert(&mut self, values: Vec<Value>) -> Result<RowId, &'static str> {
        if values.len() != self.columns.len() {
            return Err("column count mismatch");
        }

        let row_id = self.storage.insert(values.clone());
        self.search_engine.index_row(row_id, &values);
        Ok(row_id)
    }

    #[inline(always)]
    pub fn insert_batch(&mut self, rows: Vec<Vec<Value>>) -> Vec<Result<RowId, &'static str>> {
        rows.into_iter().map(|values| self.insert(values)).collect()
    }

    #[inline(always)]
    pub fn get(&self, row_id: RowId) -> Option<&[Value]> {
        self.storage.get(row_id).map(|r| r.columns.as_slice())
    }

    pub fn get_many(&self, row_ids: &[RowId]) -> Vec<(RowId, &[Value])> {
        self.storage
            .get_many(row_ids)
            .into_iter()
            .map(|r| (r.id, r.columns.as_slice()))
            .collect()
    }

    #[inline(always)]
    pub fn delete(&mut self, row_id: RowId) -> bool {
        if let Some(row) = self.storage.delete(row_id) {
            self.search_engine.remove_row(row_id, &row.columns);
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn update(&mut self, row_id: RowId, values: Vec<Value>) -> Result<bool, &'static str> {
        if values.len() != self.columns.len() {
            return Err("column count mismatch");
        }

        if let Some(old_row) = self.storage.get(row_id) {
            let old_columns = old_row.columns.clone();
            self.search_engine.remove_row(row_id, &old_columns);
            self.storage.update(row_id, values.clone());
            self.search_engine.index_row(row_id, &values);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    #[inline(always)]
    pub fn search_exact(&self, column: usize, value: &Value) -> Vec<RowId> {
        self.search_engine.search_exact(column, value).row_ids
    }

    #[inline(always)]
    pub fn search_exact_by_name(&self, column_name: &str, value: &Value) -> Vec<RowId> {
        match self.column_index(column_name) {
            Some(idx) => self.search_exact(idx, value),
            None => Vec::new(),
        }
    }

    #[inline(always)]
    pub fn search_prefix(&self, column: usize, prefix: &str) -> Vec<RowId> {
        self.search_engine.search_prefix(column, prefix).row_ids
    }

    #[inline(always)]
    pub fn search_prefix_by_name(&self, column_name: &str, prefix: &str) -> Vec<RowId> {
        match self.column_index(column_name) {
            Some(idx) => self.search_prefix(idx, prefix),
            None => Vec::new(),
        }
    }

    #[inline(always)]
    pub fn search_fulltext(&self, column: usize, query: &str) -> Vec<RowId> {
        self.search_engine.search_fulltext(column, query).row_ids
    }

    #[inline(always)]
    pub fn search_fulltext_by_name(&self, column_name: &str, query: &str) -> Vec<RowId> {
        match self.column_index(column_name) {
            Some(idx) => self.search_fulltext(idx, query),
            None => Vec::new(),
        }
    }

    #[inline(always)]
    pub fn search_range(&mut self, column: usize, min: i64, max: i64) -> Vec<RowId> {
        self.search_engine.search_range(column, min, max).row_ids
    }

    #[inline(always)]
    pub fn search(&mut self, column: usize, search_type: SearchType) -> Vec<RowId> {
        self.search_engine.search(column, search_type).row_ids
    }

    pub fn len(&self) -> usize {
        self.storage.len()
    }

    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    pub fn stats(&self) -> TableStats {
        TableStats {
            name: self.name.to_string(),
            row_count: self.storage.len(),
            column_count: self.columns.len(),
        }
    }
}

#[derive(Debug)]
pub struct TableStats {
    pub name: String,
    pub row_count: usize,
    pub column_count: usize,
}

pub struct Database {
    tables: HashMap<Box<str>, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<(), &'static str> {
        if self.tables.contains_key(name) {
            return Err("table already exists");
        }
        self.tables.insert(name.into(), Table::new(name, columns));
        Ok(())
    }

    pub fn create_table_with_capacity(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        capacity: usize,
    ) -> Result<(), &'static str> {
        if self.tables.contains_key(name) {
            return Err("table already exists");
        }
        self.tables
            .insert(name.into(), Table::with_capacity(name, columns, capacity));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> bool {
        self.tables.remove(name).is_some()
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|k| &**k).collect()
    }

    pub fn stats(&self) -> Vec<TableStats> {
        self.tables.values().map(|t| t.stats()).collect()
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_table() -> Table {
        Table::new(
            "users",
            vec![
                Column { name: "name".into(), col_type: ColumnType::String },
                Column { name: "age".into(), col_type: ColumnType::Int },
            ],
        )
    }

    #[test]
    fn test_table_insert_and_get() {
        let mut table = create_test_table();
        
        let id = table.insert(vec![
            Value::String("alice".into()),
            Value::Int(30),
        ]).unwrap();
        
        let row = table.get(id).unwrap();
        assert_eq!(row[0], Value::String("alice".into()));
        assert_eq!(row[1], Value::Int(30));
    }

    #[test]
    fn test_table_search_exact() {
        let mut table = create_test_table();
        
        table.insert(vec![Value::String("alice".into()), Value::Int(30)]).unwrap();
        table.insert(vec![Value::String("bob".into()), Value::Int(25)]).unwrap();
        
        let results = table.search_exact_by_name("name", &Value::String("alice".into()));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_table_delete() {
        let mut table = create_test_table();
        
        let id = table.insert(vec![
            Value::String("alice".into()),
            Value::Int(30),
        ]).unwrap();
        
        assert!(table.delete(id));
        assert!(table.get(id).is_none());
    }

    #[test]
    fn test_table_update() {
        let mut table = create_test_table();
        
        let id = table.insert(vec![
            Value::String("alice".into()),
            Value::Int(30),
        ]).unwrap();
        
        table.update(id, vec![
            Value::String("alice".into()),
            Value::Int(31),
        ]).unwrap();
        
        let row = table.get(id).unwrap();
        assert_eq!(row[1], Value::Int(31));
    }

    #[test]
    fn test_database_operations() {
        let mut db = Database::new();
        
        db.create_table("users", vec![
            Column { name: "name".into(), col_type: ColumnType::String },
        ]).unwrap();
        
        assert!(db.get_table("users").is_some());
        assert!(db.drop_table("users"));
        assert!(db.get_table("users").is_none());
    }

    #[test]
    fn test_column_mismatch() {
        let mut table = create_test_table();
        
        let result = table.insert(vec![Value::String("alice".into())]);
        assert!(result.is_err());
    }
}
