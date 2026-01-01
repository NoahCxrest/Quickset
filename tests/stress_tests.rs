use quickset::storage::Value;
use quickset::table::{Table, Column, ColumnType, Database};
use std::time::Instant;

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
#[ignore] // run with: cargo test --release million_row -- --ignored --nocapture
fn test_million_row_performance() {
    let row_count = 1_000_000;
    
    println!("\n=== quickset million row stress test ===\n");
    
    let mut table = Table::with_capacity(
        "million_rows",
        vec![
            Column { name: "id".into(), col_type: ColumnType::Int },
            Column { name: "name".into(), col_type: ColumnType::String },
            Column { name: "description".into(), col_type: ColumnType::String },
            Column { name: "value".into(), col_type: ColumnType::Int },
        ],
        row_count,
    );
    
    // insert million rows
    println!("inserting {} rows...", row_count);
    let start = Instant::now();
    
    for i in 0..row_count {
        let name = generate_string(12, i as u64);
        let desc = format!("item {} with searchable description content text", i);
        table.insert(vec![
            Value::Int(i as i64),
            Value::String(name.into()),
            Value::String(desc.into()),
            Value::Int((i * 7) as i64),
        ]).unwrap();
        
        if i > 0 && i % 100_000 == 0 {
            println!("  inserted {} rows...", i);
        }
    }
    
    let insert_time = start.elapsed();
    println!("insert complete: {:?} ({:.0} rows/sec)\n", 
        insert_time, 
        row_count as f64 / insert_time.as_secs_f64()
    );
    
    // exact search benchmark
    println!("benchmarking exact search...");
    let iterations = 10_000;
    let start = Instant::now();
    
    for i in 0..iterations {
        let target = (i * 97) % row_count; // pseudo-random targets
        let value = Value::Int(target as i64);
        let results = table.search_exact(0, &value);
        assert_eq!(results.len(), 1);
    }
    
    let exact_time = start.elapsed();
    let exact_per_op = exact_time.as_nanos() / iterations as u128;
    println!("exact search: {} iterations in {:?} ({} ns/op)\n", 
        iterations, exact_time, exact_per_op);
    
    // prefix search benchmark
    println!("benchmarking prefix search...");
    let start = Instant::now();
    
    for i in 0..iterations {
        let seed = (i * 97) % row_count;
        let prefix = generate_string(3, seed as u64);
        let _ = table.search_prefix(1, &prefix);
    }
    
    let prefix_time = start.elapsed();
    let prefix_per_op = prefix_time.as_nanos() / iterations as u128;
    println!("prefix search: {} iterations in {:?} ({} ns/op)\n", 
        iterations, prefix_time, prefix_per_op);
    
    // fulltext search benchmark
    println!("benchmarking fulltext search...");
    let start = Instant::now();
    let terms = ["searchable", "content", "description", "text", "item"];
    
    for i in 0..iterations {
        let term = terms[i % terms.len()];
        let _ = table.search_fulltext(2, term);
    }
    
    let fulltext_time = start.elapsed();
    let fulltext_per_op = fulltext_time.as_nanos() / iterations as u128;
    println!("fulltext search: {} iterations in {:?} ({} ns/op)\n", 
        iterations, fulltext_time, fulltext_per_op);
    
    // range search benchmark  
    println!("benchmarking range search...");
    let mut table_mut = table;
    let start = Instant::now();
    
    for i in 0..iterations {
        let base = ((i * 97) % row_count) as i64;
        let _ = table_mut.search_range(3, base, base + 1000);
    }
    
    let range_time = start.elapsed();
    let range_per_op = range_time.as_nanos() / iterations as u128;
    println!("range search: {} iterations in {:?} ({} ns/op)\n", 
        iterations, range_time, range_per_op);
    
    // get by id benchmark
    println!("benchmarking get by id...");
    let start = Instant::now();
    
    for i in 0..iterations {
        let id = ((i * 97) % row_count) as u64 + 1;
        let row = table_mut.get(id);
        assert!(row.is_some());
    }
    
    let get_time = start.elapsed();
    let get_per_op = get_time.as_nanos() / iterations as u128;
    println!("get by id: {} iterations in {:?} ({} ns/op)\n", 
        iterations, get_time, get_per_op);
    
    println!("=== results summary ===");
    println!("exact search:    {} ns/op", exact_per_op);
    println!("prefix search:   {} ns/op", prefix_per_op);
    println!("fulltext search: {} ns/op", fulltext_per_op);
    println!("range search:    {} ns/op", range_per_op);
    println!("get by id:       {} ns/op", get_per_op);
    println!("total rows:      {}", table_mut.len());
}

#[test]
#[ignore]
fn test_five_million_rows() {
    let row_count = 5_000_000;
    
    println!("\n=== quickset 5 million row test ===\n");
    
    let mut table = Table::with_capacity(
        "five_million",
        vec![
            Column { name: "id".into(), col_type: ColumnType::Int },
            Column { name: "data".into(), col_type: ColumnType::String },
        ],
        row_count,
    );
    
    println!("inserting {} rows...", row_count);
    let start = Instant::now();
    
    for i in 0..row_count {
        let data = generate_string(20, i as u64);
        table.insert(vec![
            Value::Int(i as i64),
            Value::String(data.into()),
        ]).unwrap();
        
        if i > 0 && i % 500_000 == 0 {
            println!("  inserted {} rows...", i);
        }
    }
    
    let insert_time = start.elapsed();
    println!("insert complete: {:?}\n", insert_time);
    
    // search tests
    let iterations = 10_000;
    
    let start = Instant::now();
    for i in 0..iterations {
        let target = (i * 97) % row_count;
        let _ = table.search_exact(0, &Value::Int(target as i64));
    }
    let exact_time = start.elapsed();
    
    println!("exact search: {} ns/op", exact_time.as_nanos() / iterations as u128);
    println!("total rows: {}", table.len());
}

#[test]
fn test_hundred_thousand_rows() {
    let row_count = 100_000;
    
    let mut table = Table::with_capacity(
        "hundred_thousand",
        vec![
            Column { name: "id".into(), col_type: ColumnType::Int },
            Column { name: "name".into(), col_type: ColumnType::String },
            Column { name: "value".into(), col_type: ColumnType::Int },
        ],
        row_count,
    );
    
    for i in 0..row_count {
        let name = generate_string(10, i as u64);
        table.insert(vec![
            Value::Int(i as i64),
            Value::String(name.into()),
            Value::Int((i * 3) as i64),
        ]).unwrap();
    }
    
    assert_eq!(table.len(), row_count);
    
    // verify searches work
    let results = table.search_exact(0, &Value::Int(50000));
    assert_eq!(results.len(), 1);
}
