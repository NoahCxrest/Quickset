use quickset::http::HttpServer;
use quickset::storage::Value;
use quickset::table::{Column, ColumnType, Database};
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

fn main() {
    let row_count = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);

    println!("quickset - preloading {} rows...", row_count);

    let mut db = Database::new();
    db.create_table_with_capacity(
        "data",
        vec![
            Column { name: "id".into(), col_type: ColumnType::Int },
            Column { name: "name".into(), col_type: ColumnType::String },
            Column { name: "description".into(), col_type: ColumnType::String },
            Column { name: "value".into(), col_type: ColumnType::Int },
        ],
        row_count,
    ).unwrap();

    let table = db.get_table_mut("data").unwrap();
    let start = Instant::now();
    
    for i in 0..row_count {
        let name = generate_string(12, i as u64);
        let desc = format!("item {} with searchable content", i);
        table.insert(vec![
            Value::Int(i as i64),
            Value::String(name.into()),
            Value::String(desc.into()),
            Value::Int((i * 7) as i64),
        ]).unwrap();
        
        if i > 0 && i % 100_000 == 0 {
            println!("  loaded {} rows...", i);
        }
    }

    println!("loaded {} rows in {:?}", row_count, start.elapsed());
    println!();
    println!("starting http server on 0.0.0.0:8080");
    println!();
    println!("example queries:");
    println!("  curl -X POST http://localhost:8080/search -d '{{\"table\":\"data\",\"column\":\"id\",\"type\":\"exact\",\"value\":42}}'");
    println!("  curl -X POST http://localhost:8080/search -d '{{\"table\":\"data\",\"column\":\"name\",\"type\":\"prefix\",\"prefix\":\"abc\"}}'");
    println!("  curl -X POST http://localhost:8080/search -d '{{\"table\":\"data\",\"column\":\"description\",\"type\":\"fulltext\",\"query\":\"searchable\"}}'");
    println!("  curl -X POST http://localhost:8080/search -d '{{\"table\":\"data\",\"column\":\"value\",\"type\":\"range\",\"min\":100,\"max\":200}}'");
    println!();

    let server = HttpServer::with_database(db);
    server.run("0.0.0.0:8080").unwrap();
}
