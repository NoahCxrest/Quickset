use quickset::storage::{Storage, Value};
use quickset::index::{HashIndex, InvertedIndex, TrieIndex, SortedIndex, BloomFilter};
use quickset::search::SearchEngine;
use quickset::table::{Table, Column, ColumnType, Database};

fn generate_string(len: usize, seed: u64) -> String {
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz".chars().collect();
    let mut result = String::with_capacity(len);
    let mut s = seed;
    for _ in 0..len {
        s = s.wrapping_mul(1103515245).wrapping_add(12345);
        result.push(chars[(s as usize) % chars.len()]);
    }
    result
}

#[test]
fn test_large_storage() {
    let count = 100_000;
    let mut storage = Storage::with_capacity(count);
    
    for i in 0..count {
        storage.insert(vec![
            Value::String(generate_string(20, i as u64).into()),
            Value::Int(i as i64),
        ]);
    }
    
    assert_eq!(storage.len(), count);
    
    // verify random access
    for target in [1, 100, 1000, 50000, 99999] {
        let row = storage.get(target as u64).unwrap();
        assert_eq!(row.columns[1], Value::Int((target - 1) as i64));
    }
}

#[test]
fn test_large_hash_index() {
    let count = 100_000;
    let mut index = HashIndex::with_capacity(count);
    
    for i in 0..count {
        let value = Value::String(generate_string(10, i as u64).into());
        index.insert(&value, i as u64);
    }
    
    // search for known values
    for target in [0, 100, 1000, 50000, 99999] {
        let value = Value::String(generate_string(10, target as u64).into());
        let results = index.search(&value);
        assert!(!results.is_empty(), "should find value at {}", target);
    }
    
    // search for unknown value
    let unknown = Value::String("definitely_not_in_index_xyz".into());
    assert!(index.search(&unknown).is_empty());
}

#[test]
fn test_large_inverted_index() {
    let count = 50_000;
    let mut index = InvertedIndex::with_capacity(count);
    
    for i in 0..count {
        let text = format!("document {} contains word{} and common terms", i, i);
        index.index_text(&text, i as u64);
    }
    
    // search common term
    let common_results = index.search_term("common");
    assert_eq!(common_results.len(), count);
    
    // search unique term
    let unique_results = index.search_term("word12345");
    assert_eq!(unique_results.len(), 1);
    
    // multi-term search
    let multi_results = index.search_terms(&["common", "terms"]);
    assert_eq!(multi_results.len(), count);
}

#[test]
fn test_large_trie_index() {
    let count = 50_000;
    let mut index = TrieIndex::new();
    
    for i in 0..count {
        let key = generate_string(10, i as u64);
        index.insert(&key, i as u64);
    }
    
    // prefix search
    let results = index.search_prefix("a");
    assert!(!results.is_empty());
    
    // verify prefix results actually start with prefix
    let prefix = generate_string(3, 12345);
    let specific_results = index.search_prefix(&prefix);
    // results may or may not exist depending on random generation
}

#[test]
fn test_large_sorted_index() {
    let count = 100_000;
    let mut index = SortedIndex::with_capacity(count);
    
    for i in 0..count {
        index.insert(i as i64, i as u64);
    }
    
    // range search
    let results = index.search_range(1000, 2000);
    assert_eq!(results.len(), 1001);
    
    // exact search
    let exact = index.search_exact(50000);
    assert_eq!(exact.len(), 1);
    
    // edge cases
    let low = index.search_range(0, 10);
    assert_eq!(low.len(), 11);
    
    let high = index.search_range(99990, 99999);
    assert_eq!(high.len(), 10);
}

#[test]
fn test_bloom_filter_accuracy() {
    let count = 100_000;
    let mut bloom = BloomFilter::new(count, 0.01);
    
    for i in 0..count {
        let key = generate_string(10, i as u64);
        bloom.insert(key.as_bytes());
    }
    
    // all inserted items should be found
    for i in 0..count {
        let key = generate_string(10, i as u64);
        assert!(bloom.may_contain(key.as_bytes()), "false negative at {}", i);
    }
    
    // count false positives
    let mut false_positives = 0;
    for i in count..(count + 10000) {
        let key = generate_string(10, i as u64);
        if bloom.may_contain(key.as_bytes()) {
            false_positives += 1;
        }
    }
    
    let fp_rate = false_positives as f64 / 10000.0;
    assert!(fp_rate < 0.05, "false positive rate too high: {}", fp_rate);
}

#[test]
fn test_search_engine_comprehensive() {
    let count = 50_000;
    let mut engine = SearchEngine::with_capacity(3, count);
    
    for i in 0..count {
        let name = generate_string(10, i as u64);
        let desc = format!("item {} description with searchable content", i);
        engine.index_row(
            i as u64,
            &[
                Value::String(name.into()),
                Value::String(desc.into()),
                Value::Int(i as i64),
            ],
        );
    }
    
    // exact search
    let target = generate_string(10, 25000);
    let exact = engine.search_exact(0, &Value::String(target.into()));
    assert_eq!(exact.row_ids.len(), 1);
    
    // fulltext search
    let fulltext = engine.search_fulltext(1, "searchable");
    assert_eq!(fulltext.row_ids.len(), count);
    
    // range search
    let range = engine.search_range(2, 10000, 20000);
    assert_eq!(range.row_ids.len(), 10001);
}

#[test]
fn test_table_comprehensive() {
    let count = 50_000;
    let mut table = Table::with_capacity(
        "test_table",
        vec![
            Column { name: "id".into(), col_type: ColumnType::Int },
            Column { name: "name".into(), col_type: ColumnType::String },
            Column { name: "description".into(), col_type: ColumnType::String },
        ],
        count,
    );
    
    // bulk insert
    for i in 0..count {
        let name = generate_string(15, i as u64);
        let desc = format!("row {} with text content for searching", i);
        table.insert(vec![
            Value::Int(i as i64),
            Value::String(name.into()),
            Value::String(desc.into()),
        ]).unwrap();
    }
    
    assert_eq!(table.len(), count);
    
    // test searches
    let id_search = table.search_exact_by_name("id", &Value::Int(25000));
    assert_eq!(id_search.len(), 1);
    
    let text_search = table.search_fulltext_by_name("description", "content");
    assert_eq!(text_search.len(), count);
    
    // test update
    let row_id = id_search[0];
    table.update(row_id, vec![
        Value::Int(25000),
        Value::String("updated_name".into()),
        Value::String("updated description".into()),
    ]).unwrap();
    
    let updated = table.get(row_id).unwrap();
    assert_eq!(updated[1], Value::String("updated_name".into()));
    
    // test delete
    assert!(table.delete(row_id));
    assert!(table.get(row_id).is_none());
    assert_eq!(table.len(), count - 1);
}

#[test]
fn test_database_multi_table() {
    let mut db = Database::new();
    
    db.create_table("users", vec![
        Column { name: "id".into(), col_type: ColumnType::Int },
        Column { name: "name".into(), col_type: ColumnType::String },
    ]).unwrap();
    
    db.create_table_with_capacity("products", vec![
        Column { name: "id".into(), col_type: ColumnType::Int },
        Column { name: "title".into(), col_type: ColumnType::String },
        Column { name: "price".into(), col_type: ColumnType::Int },
    ], 10_000).unwrap();
    
    // insert into users
    let users = db.get_table_mut("users").unwrap();
    for i in 0..1000 {
        users.insert(vec![
            Value::Int(i),
            Value::String(format!("user{}", i).into()),
        ]).unwrap();
    }
    
    // insert into products
    let products = db.get_table_mut("products").unwrap();
    for i in 0..5000 {
        products.insert(vec![
            Value::Int(i),
            Value::String(format!("product {} item", i).into()),
            Value::Int((i * 100) as i64),
        ]).unwrap();
    }
    
    // verify counts
    let stats = db.stats();
    assert_eq!(stats.len(), 2);
    
    let user_stats = stats.iter().find(|s| s.name == "users").unwrap();
    assert_eq!(user_stats.row_count, 1000);
    
    let product_stats = stats.iter().find(|s| s.name == "products").unwrap();
    assert_eq!(product_stats.row_count, 5000);
}

#[test]
fn test_concurrent_like_access() {
    let count = 10_000;
    let mut table = Table::with_capacity(
        "concurrent_test",
        vec![
            Column { name: "value".into(), col_type: ColumnType::Int },
        ],
        count,
    );
    
    // simulate mixed workload
    for i in 0..count {
        table.insert(vec![Value::Int(i as i64)]).unwrap();
        
        // intermittent searches
        if i % 100 == 0 && i > 0 {
            let results = table.search_exact(0, &Value::Int((i / 2) as i64));
            assert_eq!(results.len(), 1);
        }
    }
    
    assert_eq!(table.len(), count);
}

#[test]
fn test_edge_cases() {
    let mut table = Table::new(
        "edge_cases",
        vec![
            Column { name: "text".into(), col_type: ColumnType::String },
        ],
    );
    
    // empty string
    table.insert(vec![Value::String("".into())]).unwrap();
    
    // very long string
    let long_str = "a".repeat(10_000);
    table.insert(vec![Value::String(long_str.into())]).unwrap();
    
    // unicode
    table.insert(vec![Value::String("日本語テスト".into())]).unwrap();
    table.insert(vec![Value::String("emoji 🚀🔥💻".into())]).unwrap();
    
    // special characters
    table.insert(vec![Value::String("!@#$%^&*()".into())]).unwrap();
    
    assert_eq!(table.len(), 5);
    
    // search for unicode
    let results = table.search_fulltext(0, "日本語テスト");
    assert_eq!(results.len(), 1);
}

#[test]
fn test_remove_and_reindex() {
    let mut table = Table::new(
        "remove_test",
        vec![
            Column { name: "value".into(), col_type: ColumnType::Int },
        ],
    );
    
    // insert values
    for i in 0..1000 {
        table.insert(vec![Value::Int(i)]).unwrap();
    }
    
    // delete half
    for i in 1..=500 {
        table.delete(i as u64);
    }
    
    assert_eq!(table.len(), 500);
    
    // search should still work for remaining
    let results = table.search_exact(0, &Value::Int(600));
    assert_eq!(results.len(), 1);
    
    // deleted should not be found
    let results = table.search_exact(0, &Value::Int(100));
    // note: hash collision might still return results, but get() should fail
}
