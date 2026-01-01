use crate::index::{BloomFilter, HashIndex, InvertedIndex, SortedIndex, TrieIndex};
use crate::storage::{RowId, Value};

pub enum SearchType {
    Exact(Value),
    Prefix(String),
    FullText(String),
    Range { min: i64, max: i64 },
    Contains(String),
}

pub struct SearchResult {
    pub row_ids: Vec<RowId>,
    pub total: usize,
}

impl SearchResult {
    pub fn new(row_ids: Vec<RowId>) -> Self {
        let total = row_ids.len();
        Self { row_ids, total }
    }

    pub fn empty() -> Self {
        Self {
            row_ids: Vec::new(),
            total: 0,
        }
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.row_ids.truncate(n);
        self
    }

    pub fn offset(mut self, n: usize) -> Self {
        if n < self.row_ids.len() {
            self.row_ids = self.row_ids[n..].to_vec();
        } else {
            self.row_ids.clear();
        }
        self
    }
}

pub struct SearchEngine {
    hash_indexes: Vec<HashIndex>,
    inverted_indexes: Vec<InvertedIndex>,
    trie_indexes: Vec<TrieIndex>,
    sorted_indexes: Vec<SortedIndex>,
    bloom_filters: Vec<BloomFilter>,
}

impl SearchEngine {
    pub fn new(num_columns: usize) -> Self {
        Self {
            hash_indexes: (0..num_columns).map(|_| HashIndex::new()).collect(),
            inverted_indexes: (0..num_columns).map(|_| InvertedIndex::new()).collect(),
            trie_indexes: (0..num_columns).map(|_| TrieIndex::new()).collect(),
            sorted_indexes: (0..num_columns).map(|_| SortedIndex::new()).collect(),
            bloom_filters: (0..num_columns)
                .map(|_| BloomFilter::new(1_000_000, 0.01))
                .collect(),
        }
    }

    pub fn with_capacity(num_columns: usize, capacity: usize) -> Self {
        Self {
            hash_indexes: (0..num_columns)
                .map(|_| HashIndex::with_capacity(capacity))
                .collect(),
            inverted_indexes: (0..num_columns)
                .map(|_| InvertedIndex::with_capacity(capacity / 10))
                .collect(),
            trie_indexes: (0..num_columns).map(|_| TrieIndex::new()).collect(),
            sorted_indexes: (0..num_columns)
                .map(|_| SortedIndex::with_capacity(capacity))
                .collect(),
            bloom_filters: (0..num_columns)
                .map(|_| BloomFilter::new(capacity, 0.001))
                .collect(),
        }
    }

    #[inline(always)]
    pub fn index_row(&mut self, row_id: RowId, columns: &[Value]) {
        for (col_idx, value) in columns.iter().enumerate() {
            if col_idx >= self.hash_indexes.len() {
                break;
            }

            self.hash_indexes[col_idx].insert(value, row_id);

            match value {
                Value::String(s) => {
                    self.inverted_indexes[col_idx].index_text(s, row_id);
                    self.trie_indexes[col_idx].insert(s, row_id);
                    self.bloom_filters[col_idx].insert(s.as_bytes());
                }
                Value::Int(i) => {
                    self.sorted_indexes[col_idx].insert(*i, row_id);
                }
                _ => {}
            }
        }
    }

    #[inline(always)]
    pub fn remove_row(&mut self, row_id: RowId, columns: &[Value]) {
        for (col_idx, value) in columns.iter().enumerate() {
            if col_idx >= self.hash_indexes.len() {
                break;
            }

            self.hash_indexes[col_idx].remove(value, row_id);

            match value {
                Value::String(s) => {
                    self.inverted_indexes[col_idx].remove_text(s, row_id);
                    self.trie_indexes[col_idx].remove(s, row_id);
                }
                Value::Int(i) => {
                    self.sorted_indexes[col_idx].remove(*i, row_id);
                }
                _ => {}
            }
        }
    }

    #[inline(always)]
    pub fn search(&mut self, column: usize, search_type: SearchType) -> SearchResult {
        if column >= self.hash_indexes.len() {
            return SearchResult::empty();
        }

        let row_ids = match search_type {
            SearchType::Exact(ref value) => {
                // use bloom filter for early rejection on strings
                if let Value::String(s) = value {
                    if !self.bloom_filters[column].may_contain(s.as_bytes()) {
                        return SearchResult::empty();
                    }
                }
                self.hash_indexes[column].search(value).to_vec()
            }
            SearchType::Prefix(ref prefix) => {
                self.trie_indexes[column].search_prefix(prefix)
            }
            SearchType::FullText(ref text) => {
                let terms: Vec<&str> = text.split_whitespace().collect();
                if terms.len() == 1 {
                    self.inverted_indexes[column].search_term(terms[0]).to_vec()
                } else {
                    self.inverted_indexes[column].search_terms(&terms)
                }
            }
            SearchType::Range { min, max } => {
                self.sorted_indexes[column].search_range(min, max)
            }
            SearchType::Contains(ref substr) => {
                // fallback to inverted index term search
                self.inverted_indexes[column].search_term(substr).to_vec()
            }
        };

        SearchResult::new(row_ids)
    }

    #[inline(always)]
    pub fn search_exact(&self, column: usize, value: &Value) -> SearchResult {
        if column >= self.hash_indexes.len() {
            return SearchResult::empty();
        }
        
        if let Value::String(s) = value {
            if !self.bloom_filters[column].may_contain(s.as_bytes()) {
                return SearchResult::empty();
            }
        }
        
        SearchResult::new(self.hash_indexes[column].search(value).to_vec())
    }

    #[inline(always)]
    pub fn search_prefix(&self, column: usize, prefix: &str) -> SearchResult {
        if column >= self.trie_indexes.len() {
            return SearchResult::empty();
        }
        SearchResult::new(self.trie_indexes[column].search_prefix(prefix))
    }

    #[inline(always)]
    pub fn search_fulltext(&self, column: usize, query: &str) -> SearchResult {
        if column >= self.inverted_indexes.len() {
            return SearchResult::empty();
        }
        
        let terms: Vec<&str> = query.split_whitespace().collect();
        let row_ids = if terms.len() == 1 {
            self.inverted_indexes[column].search_term(terms[0]).to_vec()
        } else {
            self.inverted_indexes[column].search_terms(&terms)
        };
        
        SearchResult::new(row_ids)
    }

    #[inline(always)]
    pub fn search_range(&mut self, column: usize, min: i64, max: i64) -> SearchResult {
        if column >= self.sorted_indexes.len() {
            return SearchResult::empty();
        }
        SearchResult::new(self.sorted_indexes[column].search_range(min, max))
    }
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_engine_exact() {
        let mut engine = SearchEngine::new(2);
        
        engine.index_row(1, &[Value::String("hello".into()), Value::Int(100)]);
        engine.index_row(2, &[Value::String("world".into()), Value::Int(200)]);
        
        let result = engine.search_exact(0, &Value::String("hello".into()));
        assert_eq!(result.row_ids.len(), 1);
        assert!(result.row_ids.contains(&1));
    }

    #[test]
    fn test_search_engine_prefix() {
        let mut engine = SearchEngine::new(1);
        
        engine.index_row(1, &[Value::String("hello".into())]);
        engine.index_row(2, &[Value::String("help".into())]);
        engine.index_row(3, &[Value::String("world".into())]);
        
        let result = engine.search_prefix(0, "hel");
        assert_eq!(result.row_ids.len(), 2);
    }

    #[test]
    fn test_search_engine_fulltext() {
        let mut engine = SearchEngine::new(1);
        
        engine.index_row(1, &[Value::String("rust programming language".into())]);
        engine.index_row(2, &[Value::String("rust systems programming".into())]);
        
        let result = engine.search_fulltext(0, "rust");
        assert_eq!(result.row_ids.len(), 2);
    }

    #[test]
    fn test_search_engine_range() {
        let mut engine = SearchEngine::new(1);
        
        engine.index_row(1, &[Value::Int(10)]);
        engine.index_row(2, &[Value::Int(20)]);
        engine.index_row(3, &[Value::Int(30)]);
        
        let result = engine.search_range(0, 15, 25);
        assert_eq!(result.row_ids.len(), 1);
        assert!(result.row_ids.contains(&2));
    }

    #[test]
    fn test_search_result_pagination() {
        let result = SearchResult::new(vec![1, 2, 3, 4, 5]);
        
        let limited = result.limit(3);
        assert_eq!(limited.row_ids, vec![1, 2, 3]);
        
        let result = SearchResult::new(vec![1, 2, 3, 4, 5]);
        let offset = result.offset(2);
        assert_eq!(offset.row_ids, vec![3, 4, 5]);
    }
}
