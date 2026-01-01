use crate::storage::{RowId, Value};
use std::collections::HashMap;

// exact match index using hash table for o(1) lookup
pub struct HashIndex {
    map: HashMap<u64, Vec<RowId>>,
}

impl HashIndex {
    pub fn new() -> Self {
        Self {
            map: HashMap::with_capacity(1_000_000),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
        }
    }

    #[inline(always)]
    fn hash_value(value: &Value) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        match value {
            Value::Null => 0u8.hash(&mut hasher),
            Value::Int(i) => i.hash(&mut hasher),
            Value::Float(f) => f.to_bits().hash(&mut hasher),
            Value::String(s) => s.hash(&mut hasher),
            Value::Bytes(b) => b.hash(&mut hasher),
        }
        hasher.finish()
    }

    #[inline(always)]
    pub fn insert(&mut self, value: &Value, row_id: RowId) {
        let hash = Self::hash_value(value);
        self.map.entry(hash).or_insert_with(Vec::new).push(row_id);
    }

    #[inline(always)]
    pub fn search(&self, value: &Value) -> &[RowId] {
        let hash = Self::hash_value(value);
        self.map.get(&hash).map(|v| v.as_slice()).unwrap_or(&[])
    }

    #[inline(always)]
    pub fn remove(&mut self, value: &Value, row_id: RowId) {
        let hash = Self::hash_value(value);
        if let Some(ids) = self.map.get_mut(&hash) {
            ids.retain(|&id| id != row_id);
            if ids.is_empty() {
                self.map.remove(&hash);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for HashIndex {
    fn default() -> Self {
        Self::new()
    }
}

// inverted index for full-text search
pub struct InvertedIndex {
    terms: HashMap<Box<str>, Vec<RowId>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            terms: HashMap::with_capacity(100_000),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            terms: HashMap::with_capacity(capacity),
        }
    }

    #[inline(always)]
    pub fn index_text(&mut self, text: &str, row_id: RowId) {
        for token in Self::tokenize(text) {
            self.terms
                .entry(token.into())
                .or_insert_with(Vec::new)
                .push(row_id);
        }
    }

    #[inline(always)]
    fn tokenize(text: &str) -> impl Iterator<Item = &str> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
    }

    #[inline(always)]
    pub fn search_term(&self, term: &str) -> &[RowId] {
        self.terms.get(term).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn search_terms(&self, terms: &[&str]) -> Vec<RowId> {
        if terms.is_empty() {
            return Vec::new();
        }

        let mut result: Option<Vec<RowId>> = None;
        
        for term in terms {
            let ids = self.search_term(term);
            match &mut result {
                None => result = Some(ids.to_vec()),
                Some(r) => {
                    r.retain(|id| ids.contains(id));
                }
            }
        }
        
        result.unwrap_or_default()
    }

    pub fn remove_text(&mut self, text: &str, row_id: RowId) {
        for token in Self::tokenize(text) {
            if let Some(ids) = self.terms.get_mut(token) {
                ids.retain(|&id| id != row_id);
                if ids.is_empty() {
                    self.terms.remove(token);
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        self.terms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }
}

impl Default for InvertedIndex {
    fn default() -> Self {
        Self::new()
    }
}

// trie for prefix search
#[derive(Default)]
pub struct TrieNode {
    children: HashMap<u8, Box<TrieNode>>,
    row_ids: Vec<RowId>,
}

pub struct TrieIndex {
    root: TrieNode,
}

impl TrieIndex {
    pub fn new() -> Self {
        Self {
            root: TrieNode::default(),
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, key: &str, row_id: RowId) {
        let mut node = &mut self.root;
        for byte in key.bytes() {
            node = node.children.entry(byte).or_insert_with(|| Box::new(TrieNode::default()));
        }
        node.row_ids.push(row_id);
    }

    #[inline(always)]
    pub fn search_prefix(&self, prefix: &str) -> Vec<RowId> {
        let mut node = &self.root;
        for byte in prefix.bytes() {
            match node.children.get(&byte) {
                Some(child) => node = child,
                None => return Vec::new(),
            }
        }
        self.collect_all_ids(node)
    }

    fn collect_all_ids(&self, node: &TrieNode) -> Vec<RowId> {
        let mut result = node.row_ids.clone();
        for child in node.children.values() {
            result.extend(self.collect_all_ids(child));
        }
        result
    }

    pub fn remove(&mut self, key: &str, row_id: RowId) {
        let mut node = &mut self.root;
        for byte in key.bytes() {
            match node.children.get_mut(&byte) {
                Some(child) => node = child,
                None => return,
            }
        }
        node.row_ids.retain(|&id| id != row_id);
    }
}

impl Default for TrieIndex {
    fn default() -> Self {
        Self::new()
    }
}

// sorted index for range queries
pub struct SortedIndex {
    entries: Vec<(i64, RowId)>,
    sorted: bool,
}

impl SortedIndex {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(1_000_000),
            sorted: true,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            sorted: true,
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, value: i64, row_id: RowId) {
        self.entries.push((value, row_id));
        self.sorted = false;
    }

    #[inline(always)]
    fn ensure_sorted(&mut self) {
        if !self.sorted {
            self.entries.sort_unstable_by_key(|(v, _)| *v);
            self.sorted = true;
        }
    }

    pub fn search_range(&mut self, min: i64, max: i64) -> Vec<RowId> {
        self.ensure_sorted();
        
        let start = self.entries.partition_point(|(v, _)| *v < min);
        let end = self.entries.partition_point(|(v, _)| *v <= max);
        
        self.entries[start..end].iter().map(|(_, id)| *id).collect()
    }

    pub fn search_exact(&mut self, value: i64) -> Vec<RowId> {
        self.ensure_sorted();
        
        let start = self.entries.partition_point(|(v, _)| *v < value);
        let end = self.entries.partition_point(|(v, _)| *v <= value);
        
        self.entries[start..end].iter().map(|(_, id)| *id).collect()
    }

    pub fn remove(&mut self, value: i64, row_id: RowId) {
        self.entries.retain(|(v, id)| !(*v == value && *id == row_id));
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for SortedIndex {
    fn default() -> Self {
        Self::new()
    }
}

// bloom filter for fast existence checks
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: usize,
    num_bits: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        // use proper bloom filter sizing formula
        let ln2 = std::f64::consts::LN_2;
        let ln2_sq = ln2 * ln2;
        let bits_needed = (-(expected_items as f64) * false_positive_rate.ln() / ln2_sq).ceil() as usize;
        let num_bits = ((bits_needed + 63) / 64) * 64;
        let num_hashes = ((num_bits as f64 / expected_items as f64) * ln2).ceil() as usize;
        
        Self {
            bits: vec![0; num_bits / 64],
            num_hashes: num_hashes.max(1).min(16),
            num_bits,
        }
    }

    #[inline(always)]
    fn hash(&self, value: &[u8], seed: usize) -> usize {
        // use fnv-1a inspired hash with better distribution
        let mut h: u64 = 14695981039346656037u64.wrapping_add(seed as u64 * 31);
        for &byte in value {
            h ^= byte as u64;
            h = h.wrapping_mul(1099511628211);
        }
        (h as usize) % self.num_bits
    }

    #[inline(always)]
    pub fn insert(&mut self, value: &[u8]) {
        for i in 0..self.num_hashes {
            let idx = self.hash(value, i);
            self.bits[idx / 64] |= 1 << (idx % 64);
        }
    }

    #[inline(always)]
    pub fn may_contain(&self, value: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let idx = self.hash(value, i);
            if self.bits[idx / 64] & (1 << (idx % 64)) == 0 {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_index() {
        let mut index = HashIndex::new();
        let value = Value::String("test".into());
        
        index.insert(&value, 1);
        index.insert(&value, 2);
        
        let results = index.search(&value);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_hash_index_remove() {
        let mut index = HashIndex::new();
        let value = Value::Int(42);
        
        index.insert(&value, 1);
        index.insert(&value, 2);
        index.remove(&value, 1);
        
        let results = index.search(&value);
        assert_eq!(results.len(), 1);
        assert!(results.contains(&2));
    }

    #[test]
    fn test_inverted_index() {
        let mut index = InvertedIndex::new();
        
        index.index_text("hello world", 1);
        index.index_text("hello rust", 2);
        
        let results = index.search_term("hello");
        assert_eq!(results.len(), 2);
        
        let results = index.search_term("world");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_inverted_index_multi_term() {
        let mut index = InvertedIndex::new();
        
        index.index_text("rust programming language", 1);
        index.index_text("rust systems programming", 2);
        index.index_text("python programming", 3);
        
        let results = index.search_terms(&["rust", "programming"]);
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_trie_index() {
        let mut index = TrieIndex::new();
        
        index.insert("hello", 1);
        index.insert("help", 2);
        index.insert("world", 3);
        
        let results = index.search_prefix("hel");
        assert_eq!(results.len(), 2);
        
        let results = index.search_prefix("wor");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_sorted_index() {
        let mut index = SortedIndex::new();
        
        index.insert(10, 1);
        index.insert(20, 2);
        index.insert(15, 3);
        index.insert(25, 4);
        
        let results = index.search_range(10, 20);
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_bloom_filter() {
        let mut bloom = BloomFilter::new(1000, 0.01);
        
        bloom.insert(b"hello");
        bloom.insert(b"world");
        
        assert!(bloom.may_contain(b"hello"));
        assert!(bloom.may_contain(b"world"));
        // note: bloom filters can have false positives but not false negatives
    }
}
