use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub type RowId = u64;

#[derive(Clone, Debug)]
pub struct Row {
    pub id: RowId,
    pub columns: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    String(Box<str>),
    Bytes(Box<[u8]>),
}

impl Value {
    #[inline(always)]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
}

pub struct Storage {
    rows: HashMap<RowId, Row>,
    next_id: AtomicU64,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            rows: HashMap::with_capacity(1_000_000),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: HashMap::with_capacity(capacity),
            next_id: AtomicU64::new(1),
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, columns: Vec<Value>) -> RowId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let row = Row { id, columns };
        self.rows.insert(id, row);
        id
    }

    #[inline(always)]
    pub fn get(&self, id: RowId) -> Option<&Row> {
        self.rows.get(&id)
    }

    #[inline(always)]
    pub fn delete(&mut self, id: RowId) -> Option<Row> {
        self.rows.remove(&id)
    }

    #[inline(always)]
    pub fn update(&mut self, id: RowId, columns: Vec<Value>) -> bool {
        if let Some(row) = self.rows.get_mut(&id) {
            row.columns = columns;
            true
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Row> {
        self.rows.values()
    }

    #[inline(always)]
    pub fn get_many(&self, ids: &[RowId]) -> Vec<&Row> {
        ids.iter().filter_map(|id| self.rows.get(id)).collect()
    }
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_insert_and_get() {
        let mut storage = Storage::new();
        let id = storage.insert(vec![Value::String("test".into())]);
        
        let row = storage.get(id).unwrap();
        assert_eq!(row.columns[0], Value::String("test".into()));
    }

    #[test]
    fn test_storage_delete() {
        let mut storage = Storage::new();
        let id = storage.insert(vec![Value::Int(42)]);
        
        assert!(storage.get(id).is_some());
        storage.delete(id);
        assert!(storage.get(id).is_none());
    }

    #[test]
    fn test_storage_update() {
        let mut storage = Storage::new();
        let id = storage.insert(vec![Value::Int(1)]);
        
        storage.update(id, vec![Value::Int(2)]);
        assert_eq!(storage.get(id).unwrap().columns[0], Value::Int(2));
    }

    #[test]
    fn test_storage_len() {
        let mut storage = Storage::new();
        assert_eq!(storage.len(), 0);
        
        storage.insert(vec![Value::Null]);
        assert_eq!(storage.len(), 1);
    }

    #[test]
    fn test_value_accessors() {
        let s = Value::String("hello".into());
        let i = Value::Int(42);
        let f = Value::Float(3.14);

        assert_eq!(s.as_str(), Some("hello"));
        assert_eq!(i.as_int(), Some(42));
        assert_eq!(f.as_float(), Some(3.14));
        assert_eq!(s.as_int(), None);
    }
}
