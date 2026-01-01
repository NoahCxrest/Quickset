use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use quickset::storage::{Storage, Value};
use quickset::index::{HashIndex, InvertedIndex, TrieIndex, SortedIndex, BloomFilter};
use quickset::search::SearchEngine;
use quickset::table::{Table, Column, ColumnType};

fn generate_random_string(len: usize, seed: u64) -> String {
    let chars: Vec<char> = "abcdefghijklmnopqrstuvwxyz".chars().collect();
    let mut result = String::with_capacity(len);
    let mut s = seed;
    for _ in 0..len {
        s = s.wrapping_mul(1103515245).wrapping_add(12345);
        result.push(chars[(s as usize) % chars.len()]);
    }
    result
}

fn bench_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let mut storage = Storage::with_capacity(*size);
        for i in 0..*size {
            storage.insert(vec![Value::Int(i as i64)]);
        }
        
        group.bench_with_input(BenchmarkId::new("get", size), size, |b, _| {
            let target = (*size / 2) as u64;
            b.iter(|| {
                black_box(storage.get(target))
            });
        });
    }
    
    group.finish();
}

fn bench_hash_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_index");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let mut index = HashIndex::with_capacity(*size);
        for i in 0..*size {
            let value = Value::String(generate_random_string(10, i as u64).into());
            index.insert(&value, i as u64);
        }
        
        let search_value = Value::String(generate_random_string(10, (*size / 2) as u64).into());
        
        group.bench_with_input(BenchmarkId::new("search", size), size, |b, _| {
            b.iter(|| {
                black_box(index.search(&search_value))
            });
        });
    }
    
    group.finish();
}

fn bench_inverted_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("inverted_index");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let mut index = InvertedIndex::with_capacity(*size);
        for i in 0..*size {
            let text = format!("word{} common text document", i);
            index.index_text(&text, i as u64);
        }
        
        group.bench_with_input(BenchmarkId::new("search_single", size), size, |b, _| {
            b.iter(|| {
                black_box(index.search_term("common"))
            });
        });
        
        group.bench_with_input(BenchmarkId::new("search_multi", size), size, |b, _| {
            b.iter(|| {
                black_box(index.search_terms(&["common", "text"]))
            });
        });
    }
    
    group.finish();
}

fn bench_trie_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("trie_index");
    
    for size in [1_000, 10_000, 100_000].iter() {
        let mut index = TrieIndex::new();
        for i in 0..*size {
            let key = generate_random_string(10, i as u64);
            index.insert(&key, i as u64);
        }
        
        let prefix = generate_random_string(3, (*size / 2) as u64);
        
        group.bench_with_input(BenchmarkId::new("search_prefix", size), size, |b, _| {
            b.iter(|| {
                black_box(index.search_prefix(&prefix))
            });
        });
    }
    
    group.finish();
}

fn bench_sorted_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("sorted_index");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let mut index = SortedIndex::with_capacity(*size);
        for i in 0..*size {
            index.insert(i as i64, i as u64);
        }
        
        let mid = (*size / 2) as i64;
        
        group.bench_with_input(BenchmarkId::new("search_range", size), size, |b, _| {
            b.iter(|| {
                black_box(index.search_range(mid - 100, mid + 100))
            });
        });
        
        group.bench_with_input(BenchmarkId::new("search_exact", size), size, |b, _| {
            b.iter(|| {
                black_box(index.search_exact(mid))
            });
        });
    }
    
    group.finish();
}

fn bench_bloom_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_filter");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let mut bloom = BloomFilter::new(*size, 0.01);
        for i in 0..*size {
            let key = generate_random_string(10, i as u64);
            bloom.insert(key.as_bytes());
        }
        
        let search_key = generate_random_string(10, (*size / 2) as u64);
        
        group.bench_with_input(BenchmarkId::new("may_contain", size), size, |b, _| {
            b.iter(|| {
                black_box(bloom.may_contain(search_key.as_bytes()))
            });
        });
    }
    
    group.finish();
}

fn bench_search_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_engine");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let mut engine = SearchEngine::with_capacity(3, *size);
        
        for i in 0..*size {
            let name = generate_random_string(10, i as u64);
            let desc = format!("description {} with keywords search text", i);
            engine.index_row(
                i as u64,
                &[
                    Value::String(name.into()),
                    Value::String(desc.into()),
                    Value::Int(i as i64),
                ],
            );
        }
        
        let search_str = generate_random_string(10, (*size / 2) as u64);
        let search_value = Value::String(search_str.into());
        
        group.bench_with_input(BenchmarkId::new("exact_search", size), size, |b, _| {
            b.iter(|| {
                black_box(engine.search_exact(0, &search_value))
            });
        });
        
        group.bench_with_input(BenchmarkId::new("prefix_search", size), size, |b, _| {
            b.iter(|| {
                black_box(engine.search_prefix(0, "abc"))
            });
        });
        
        group.bench_with_input(BenchmarkId::new("fulltext_search", size), size, |b, _| {
            b.iter(|| {
                black_box(engine.search_fulltext(1, "search"))
            });
        });
        
        let mid = (*size / 2) as i64;
        group.bench_function(BenchmarkId::new("range_search", size), |b| {
            b.iter(|| {
                black_box(engine.search_range(2, mid - 100, mid + 100))
            });
        });
    }
    
    group.finish();
}

fn bench_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("table");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let mut table = Table::with_capacity(
            "bench_table",
            vec![
                Column { name: "name".into(), col_type: ColumnType::String },
                Column { name: "description".into(), col_type: ColumnType::String },
                Column { name: "value".into(), col_type: ColumnType::Int },
            ],
            *size,
        );
        
        for i in 0..*size {
            let name = generate_random_string(10, i as u64);
            let desc = format!("item {} description text", i);
            table.insert(vec![
                Value::String(name.into()),
                Value::String(desc.into()),
                Value::Int(i as i64),
            ]).unwrap();
        }
        
        let search_name = generate_random_string(10, (*size / 2) as u64);
        let search_value = Value::String(search_name.into());
        
        group.bench_with_input(BenchmarkId::new("search_exact", size), size, |b, _| {
            b.iter(|| {
                black_box(table.search_exact(0, &search_value))
            });
        });
        
        group.bench_with_input(BenchmarkId::new("search_prefix", size), size, |b, _| {
            b.iter(|| {
                black_box(table.search_prefix(0, "abc"))
            });
        });
        
        group.bench_with_input(BenchmarkId::new("search_fulltext", size), size, |b, _| {
            b.iter(|| {
                black_box(table.search_fulltext(1, "description"))
            });
        });
    }
    
    group.finish();
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");
    
    group.bench_function("single_insert", |b| {
        let mut table = Table::new(
            "bench_table",
            vec![
                Column { name: "name".into(), col_type: ColumnType::String },
                Column { name: "value".into(), col_type: ColumnType::Int },
            ],
        );
        let mut i = 0u64;
        
        b.iter(|| {
            let name = generate_random_string(10, i);
            table.insert(vec![
                Value::String(name.into()),
                Value::Int(i as i64),
            ]).unwrap();
            i += 1;
        });
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_storage,
    bench_hash_index,
    bench_inverted_index,
    bench_trie_index,
    bench_sorted_index,
    bench_bloom_filter,
    bench_search_engine,
    bench_table,
    bench_insert,
);

criterion_main!(benches);
