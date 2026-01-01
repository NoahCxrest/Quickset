// clickhouse source implementation
// uses native http interface for simplicity (no extra deps)

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::storage::Value;
use crate::table::ColumnType;
use crate::{log_debug, log_info};

use super::source::{FetchResult, Source, SourceConfig, SourceError, SyncTable};

// url-encode a string for query parameters
fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 3);
    for c in s.chars() {
        match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

pub struct ClickHouseSource {
    config: SourceConfig,
    connected: bool,
}

impl ClickHouseSource {
    pub fn new(config: SourceConfig) -> Self {
        Self {
            config,
            connected: false,
        }
    }

    // build the select query for a table
    fn build_query(&self, table: &SyncTable) -> String {
        if let Some(ref query) = table.query_override {
            return query.clone();
        }

        let columns: Vec<&str> = table.columns.iter()
            .map(|c| c.source_name.as_str())
            .collect();

        if columns.is_empty() {
            format!("SELECT * FROM {}", table.source_table)
        } else {
            format!("SELECT {} FROM {}", columns.join(", "), table.source_table)
        }
    }

    // execute a query via clickhouse http interface
    fn execute_query(&self, query: &str) -> Result<String, SourceError> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        
        let mut stream = TcpStream::connect(&addr)
            .map_err(|e| SourceError::Connection(format!("failed to connect to {}: {}", addr, e)))?;
        
        stream.set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| SourceError::Connection(e.to_string()))?;
        stream.set_write_timeout(Some(Duration::from_secs(30)))
            .map_err(|e| SourceError::Connection(e.to_string()))?;

        // build http request
        let db = url_encode(self.config.database.as_deref().unwrap_or("default"));
        let user = url_encode(self.config.user.as_deref().unwrap_or("default"));
        let pass = url_encode(self.config.password.as_deref().unwrap_or(""));
        
        // use tsv format for easier parsing
        let full_query = format!("{} FORMAT TabSeparated", query);
        let body = full_query.as_bytes();
        
        let request = format!(
            "POST /?database={}&user={}&password={} HTTP/1.1\r\n\
             Host: {}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n",
            db, user, pass, self.config.host, body.len()
        );

        stream.write_all(request.as_bytes())
            .map_err(|e| SourceError::Query(format!("failed to send request: {}", e)))?;
        stream.write_all(body)
            .map_err(|e| SourceError::Query(format!("failed to send query: {}", e)))?;
        stream.flush()
            .map_err(|e| SourceError::Query(e.to_string()))?;

        // read response
        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        
        // read status line
        let mut status_line = String::new();
        reader.read_line(&mut status_line)
            .map_err(|e| SourceError::Query(format!("failed to read response: {}", e)))?;
        
        if !status_line.contains("200") {
            // read error body
            let mut error_body = String::new();
            let _ = reader.read_line(&mut error_body);
            return Err(SourceError::Query(format!("clickhouse error: {} {}", status_line.trim(), error_body.trim())));
        }

        // parse headers to check for chunked encoding
        let mut chunked = false;
        let mut content_length: Option<usize> = None;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line)
                .map_err(|e| SourceError::Query(e.to_string()))?;
            let line_lower = line.to_lowercase();
            if line_lower.starts_with("transfer-encoding:") && line_lower.contains("chunked") {
                chunked = true;
            }
            if line_lower.starts_with("content-length:") {
                content_length = line.split(':').nth(1).and_then(|s| s.trim().parse().ok());
            }
            if line.trim().is_empty() {
                break;
            }
        }

        // read body based on encoding
        if chunked {
            // chunked transfer encoding
            loop {
                let mut size_line = String::new();
                reader.read_line(&mut size_line)
                    .map_err(|e| SourceError::Query(e.to_string()))?;
                
                // parse hex chunk size
                let size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
                if size == 0 {
                    break; // end of chunks
                }
                
                // read chunk data
                let mut chunk = vec![0u8; size];
                reader.read_exact(&mut chunk)
                    .map_err(|e| SourceError::Query(e.to_string()))?;
                response.push_str(&String::from_utf8_lossy(&chunk));
                
                // read trailing \r\n after chunk
                let mut crlf = String::new();
                let _ = reader.read_line(&mut crlf);
            }
        } else if let Some(len) = content_length {
            // content-length based
            let mut body = vec![0u8; len];
            reader.read_exact(&mut body)
                .map_err(|e| SourceError::Query(e.to_string()))?;
            response = String::from_utf8_lossy(&body).to_string();
        } else {
            // read until connection close
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => response.push_str(&line),
                    Err(_) => break,
                }
            }
        }

        Ok(response)
    }

    // parse a tsv value into our Value type
    fn parse_value(s: &str, col_type: ColumnType) -> Value {
        let s = s.trim();
        
        if s.is_empty() || s == "\\N" || s == "NULL" {
            return Value::Null;
        }

        match col_type {
            ColumnType::Int => {
                s.parse::<i64>()
                    .map(Value::Int)
                    .unwrap_or(Value::Null)
            }
            ColumnType::Float => {
                s.parse::<f64>()
                    .map(Value::Float)
                    .unwrap_or(Value::Null)
            }
            ColumnType::String => {
                // unescape common clickhouse escapes
                let unescaped = s
                    .replace("\\t", "\t")
                    .replace("\\n", "\n")
                    .replace("\\\\", "\\");
                Value::String(unescaped.into_boxed_str())
            }
            ColumnType::Bytes => {
                Value::Bytes(s.as_bytes().to_vec().into_boxed_slice())
            }
        }
    }

    // parse tsv response into rows
    fn parse_response(&self, response: &str, table: &SyncTable) -> Result<Vec<Vec<Value>>, SourceError> {
        let mut rows = Vec::new();
        
        for line in response.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let fields: Vec<&str> = line.split('\t').collect();
            
            // handle column count mismatches gracefully
            if table.columns.is_empty() {
                // no columns specified, parse all as strings
                let row: Vec<Value> = fields.iter()
                    .map(|f| Value::String((*f).to_string().into_boxed_str()))
                    .collect();
                rows.push(row);
                continue;
            }

            // build row, padding with Null if fewer fields than expected
            let mut row: Vec<Value> = Vec::with_capacity(table.columns.len());
            for (i, col) in table.columns.iter().enumerate() {
                let value = if i < fields.len() {
                    Self::parse_value(fields[i], col.col_type)
                } else {
                    Value::Null // missing columns become Null
                };
                row.push(value);
            }
            
            rows.push(row);
        }

        Ok(rows)
    }
}

impl Source for ClickHouseSource {
    fn connect(&mut self) -> Result<(), SourceError> {
        // test connection with a simple query
        self.execute_query("SELECT 1")?;
        self.connected = true;
        Ok(())
    }

    fn disconnect(&mut self) {
        self.connected = false;
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn fetch_table(&self, table: &SyncTable) -> Result<FetchResult, SourceError> {
        let query = self.build_query(table);
        log_info!("sync", "executing query: {}", query);
        let response = self.execute_query(&query)?;
        // log first 500 chars of response for debugging
        let preview: String = response.chars().take(500).collect();
        log_debug!("sync", "response preview: {}", preview);
        let rows = self.parse_response(&response, table)?;
        let row_count = rows.len();
        
        Ok(FetchResult { rows, row_count })
    }

    fn name(&self) -> &str {
        "clickhouse"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query() {
        let config = SourceConfig::new("localhost", 8123);
        let source = ClickHouseSource::new(config);

        let table = SyncTable::new("users", "users")
            .with_column("id", "id", ColumnType::Int)
            .with_column("name", "name", ColumnType::String);

        let query = source.build_query(&table);
        assert_eq!(query, "SELECT id, name FROM users");
    }

    #[test]
    fn test_build_query_with_override() {
        let config = SourceConfig::new("localhost", 8123);
        let source = ClickHouseSource::new(config);

        let table = SyncTable::new("users", "users")
            .with_query("SELECT * FROM users WHERE active = 1");

        let query = source.build_query(&table);
        assert_eq!(query, "SELECT * FROM users WHERE active = 1");
    }

    #[test]
    fn test_parse_value() {
        assert_eq!(
            ClickHouseSource::parse_value("123", ColumnType::Int),
            Value::Int(123)
        );
        assert_eq!(
            ClickHouseSource::parse_value("45.67", ColumnType::Float),
            Value::Float(45.67)
        );
        assert_eq!(
            ClickHouseSource::parse_value("hello", ColumnType::String),
            Value::String("hello".into())
        );
        assert_eq!(
            ClickHouseSource::parse_value("\\N", ColumnType::Int),
            Value::Null
        );
    }
}
