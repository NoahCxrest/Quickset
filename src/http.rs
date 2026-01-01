use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

use crate::auth::{AuthManager, Role};
use crate::config::{AuthLevel, Config};
use crate::log::{LogLevel, Logger};
use crate::query::*;
use crate::search::SearchType;
use crate::storage::Value;
use crate::table::{Column, Database};
use crate::{log_debug, log_error, log_info, log_warn};

pub struct HttpServer {
    db: Arc<RwLock<Database>>,
    auth: Arc<AuthManager>,
    config: Config,
}

impl HttpServer {
    pub fn new() -> Self {
        let config = Config::from_env();
        Self::with_config(config)
    }

    pub fn with_config(config: Config) -> Self {
        if let Some(level) = LogLevel::from_str(&config.log_level) {
            Logger::init(level);
        }

        let auth = AuthManager::new(config.auth_enabled());
        if config.auth_enabled() && config.admin_user != "admin" {
            auth.add_user(&config.admin_user, &config.admin_pass, Role::Admin).ok();
        }

        Self {
            db: Arc::new(RwLock::new(Database::new())),
            auth: Arc::new(auth),
            config,
        }
    }

    pub fn with_database(db: Database) -> Self {
        let config = Config::from_env();
        if let Some(level) = LogLevel::from_str(&config.log_level) {
            Logger::init(level);
        }

        let auth = AuthManager::new(config.auth_enabled());

        Self {
            db: Arc::new(RwLock::new(db)),
            auth: Arc::new(auth),
            config,
        }
    }

    pub fn run(&self, addr: &str) -> std::io::Result<()> {
        let listener = TcpListener::bind(addr)?;
        log_info!("server", "quickset listening on {}", addr);
        log_info!("server", "auth level: {:?}", self.config.auth_level);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = Arc::clone(&self.db);
                    let auth = Arc::clone(&self.auth);
                    let auth_level = self.config.auth_level;
                    std::thread::spawn(move || {
                        if let Err(e) = handle_connection(stream, db, auth, auth_level) {
                            log_error!("http", "connection error: {}", e);
                        }
                    });
                }
                Err(e) => log_error!("http", "accept error: {}", e),
            }
        }
        Ok(())
    }

    pub fn database(&self) -> Arc<RwLock<Database>> {
        Arc::clone(&self.db)
    }

    pub fn auth(&self) -> Arc<AuthManager> {
        Arc::clone(&self.auth)
    }
}

impl Default for HttpServer {
    fn default() -> Self {
        Self::new()
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn parse_request(stream: &mut TcpStream) -> std::io::Result<HttpRequest> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut first_line = String::new();
    reader.read_line(&mut first_line)?;

    let parts: Vec<&str> = first_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid request"));
    }

    let method = parts[0].to_string();
    let path = parts[1].to_string();

    let mut headers = HashMap::new();
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        if let Some(pos) = line.find(':') {
            let key = line[..pos].trim().to_lowercase();
            let value = line[pos + 1..].trim().to_string();
            headers.insert(key, value);
        }
    }

    let content_length: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn send_response(stream: &mut TcpStream, status: u16, body: &[u8]) -> std::io::Result<()> {
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };

    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status, status_text, body.len()
    );

    stream.write_all(response.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn handle_connection(
    mut stream: TcpStream,
    db: Arc<RwLock<Database>>,
    auth: Arc<AuthManager>,
    auth_level: AuthLevel,
) -> std::io::Result<()> {
    let request = parse_request(&mut stream)?;
    
    log_debug!("http", "{} {}", request.method, request.path);
    
    let (status, response_body) = route_request(&request, db, auth, auth_level);
    
    if status >= 400 {
        log_warn!("http", "{} {} -> {}", request.method, request.path, status);
    }
    
    send_response(&mut stream, status, response_body.as_bytes())
}

// check auth based on configured level and operation type
fn check_auth(
    request: &HttpRequest, 
    auth: &AuthManager, 
    auth_level: AuthLevel,
    is_write: bool,
    is_health: bool,
) -> Result<Role, (u16, String)> {
    // figure out if we need auth for this request
    let needs_auth = if is_health {
        auth_level.requires_auth_for_health()
    } else if is_write {
        auth_level.requires_auth_for_write()
    } else {
        auth_level.requires_auth_for_read()
    };

    if !needs_auth {
        return Ok(Role::Admin); // no auth needed, grant full access
    }

    let auth_header = request.headers.get("authorization");
    
    match auth_header {
        None => Err((401, serde_json::to_string(&ApiResponse::<()>::err("authentication required")).unwrap())),
        Some(header) => {
            match auth.validate_basic_auth(header) {
                None => Err((401, serde_json::to_string(&ApiResponse::<()>::err("invalid credentials")).unwrap())),
                Some(role) => {
                    if is_write && !role.can_write() {
                        Err((403, serde_json::to_string(&ApiResponse::<()>::err("write access required")).unwrap()))
                    } else {
                        Ok(role)
                    }
                }
            }
        }
    }
}

fn route_request(request: &HttpRequest, db: Arc<RwLock<Database>>, auth: Arc<AuthManager>, auth_level: AuthLevel) -> (u16, String) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => {
            if let Err(e) = check_auth(request, &auth, auth_level, false, true) { return e; }
            (200, r#"{"status":"ok"}"#.to_string())
        }
        ("POST", "/table/create") => {
            if let Err(e) = check_auth(request, &auth, auth_level, true, false) { return e; }
            handle_create_table(request, db)
        }
        ("POST", "/table/drop") => {
            if let Err(e) = check_auth(request, &auth, auth_level, true, false) { return e; }
            handle_drop_table(request, db)
        }
        ("GET", "/tables") => {
            if let Err(e) = check_auth(request, &auth, auth_level, false, false) { return e; }
            handle_list_tables(db)
        }
        ("GET", "/stats") => {
            if let Err(e) = check_auth(request, &auth, auth_level, false, false) { return e; }
            handle_stats(db)
        }
        ("POST", "/insert") => {
            if let Err(e) = check_auth(request, &auth, auth_level, true, false) { return e; }
            handle_insert(request, db)
        }
        ("POST", "/search") => {
            if let Err(e) = check_auth(request, &auth, auth_level, false, false) { return e; }
            handle_search(request, db)
        }
        ("POST", "/get") => {
            if let Err(e) = check_auth(request, &auth, auth_level, false, false) { return e; }
            handle_get(request, db)
        }
        ("POST", "/delete") => {
            if let Err(e) = check_auth(request, &auth, auth_level, true, false) { return e; }
            handle_delete(request, db)
        }
        ("POST", "/update") => {
            if let Err(e) = check_auth(request, &auth, auth_level, true, false) { return e; }
            handle_update(request, db)
        }
        ("POST", "/auth/user/add") => {
            match check_auth(request, &auth, auth_level, true, false) {
                Err(e) => e,
                Ok(role) if !role.can_admin() => (403, serde_json::to_string(&ApiResponse::<()>::err("admin required")).unwrap()),
                Ok(_) => handle_add_user(request, &auth),
            }
        }
        ("POST", "/auth/user/remove") => {
            match check_auth(request, &auth, auth_level, true, false) {
                Err(e) => e,
                Ok(role) if !role.can_admin() => (403, serde_json::to_string(&ApiResponse::<()>::err("admin required")).unwrap()),
                Ok(_) => handle_remove_user(request, &auth),
            }
        }
        ("GET", "/auth/users") => {
            match check_auth(request, &auth, auth_level, false, false) {
                Err(e) => e,
                Ok(role) if !role.can_admin() => (403, serde_json::to_string(&ApiResponse::<()>::err("admin required")).unwrap()),
                Ok(_) => handle_list_users(&auth),
            }
        }
        _ => (404, serde_json::to_string(&ApiResponse::<()>::err("not found")).unwrap()),
    }
}

fn handle_create_table(request: &HttpRequest, db: Arc<RwLock<Database>>) -> (u16, String) {
    let req: CreateTableRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let columns: Vec<Column> = match req.columns.iter().map(|c| {
        c.to_column_type().map(|ct| Column {
            name: c.name.clone().into_boxed_str(),
            col_type: ct,
        })
    }).collect::<Option<Vec<_>>>() {
        Some(cols) => cols,
        None => return (400, serde_json::to_string(&ApiResponse::<()>::err("invalid column type")).unwrap()),
    };

    let mut db = db.write().unwrap();
    let result = if let Some(cap) = req.capacity {
        db.create_table_with_capacity(&req.name, columns, cap)
    } else {
        db.create_table(&req.name, columns)
    };

    match result {
        Ok(_) => (200, serde_json::to_string(&ApiResponse::ok("table created")).unwrap()),
        Err(e) => (400, serde_json::to_string(&ApiResponse::<()>::err(e)).unwrap()),
    }
}

fn handle_drop_table(request: &HttpRequest, db: Arc<RwLock<Database>>) -> (u16, String) {
    #[derive(serde::Deserialize)]
    struct DropRequest {
        name: String,
    }

    let req: DropRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let mut db = db.write().unwrap();
    if db.drop_table(&req.name) {
        (200, serde_json::to_string(&ApiResponse::ok("table dropped")).unwrap())
    } else {
        (404, serde_json::to_string(&ApiResponse::<()>::err("table not found")).unwrap())
    }
}

fn handle_list_tables(db: Arc<RwLock<Database>>) -> (u16, String) {
    let db = db.read().unwrap();
    let tables: Vec<&str> = db.table_names();
    (200, serde_json::to_string(&ApiResponse::ok(tables)).unwrap())
}

fn handle_stats(db: Arc<RwLock<Database>>) -> (u16, String) {
    let db = db.read().unwrap();
    let stats: Vec<TableInfo> = db.stats().into_iter().map(|s| TableInfo {
        name: s.name,
        row_count: s.row_count,
        column_count: s.column_count,
    }).collect();
    (200, serde_json::to_string(&ApiResponse::ok(StatsResponse { tables: stats })).unwrap())
}

fn handle_insert(request: &HttpRequest, db: Arc<RwLock<Database>>) -> (u16, String) {
    let req: InsertRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let mut db = db.write().unwrap();
    let table = match db.get_table_mut(&req.table) {
        Some(t) => t,
        None => return (404, serde_json::to_string(&ApiResponse::<()>::err("table not found")).unwrap()),
    };

    let values: Vec<Vec<Value>> = req.rows.iter()
        .map(|row| row.iter().map(|v| v.to_value()).collect())
        .collect();

    let results = table.insert_batch(values);
    let ids: Vec<u64> = results.into_iter().filter_map(|r| r.ok()).collect();
    let count = ids.len();

    (200, serde_json::to_string(&ApiResponse::ok(InsertResponse { ids, count })).unwrap())
}

fn handle_search(request: &HttpRequest, db: Arc<RwLock<Database>>) -> (u16, String) {
    let req: SearchRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let mut db = db.write().unwrap();
    let table = match db.get_table_mut(&req.table) {
        Some(t) => t,
        None => return (404, serde_json::to_string(&ApiResponse::<()>::err("table not found")).unwrap()),
    };

    let col_idx = match table.column_index(&req.column) {
        Some(idx) => idx,
        None => return (400, serde_json::to_string(&ApiResponse::<()>::err("column not found")).unwrap()),
    };

    let search_type = match req.search_type.as_str() {
        "exact" => {
            let value = match &req.value {
                Some(v) => v.to_value(),
                None => return (400, serde_json::to_string(&ApiResponse::<()>::err("value required for exact search")).unwrap()),
            };
            SearchType::Exact(value)
        }
        "prefix" => {
            let prefix = match &req.prefix {
                Some(p) => p.clone(),
                None => return (400, serde_json::to_string(&ApiResponse::<()>::err("prefix required")).unwrap()),
            };
            SearchType::Prefix(prefix)
        }
        "fulltext" => {
            let query = match &req.query {
                Some(q) => q.clone(),
                None => return (400, serde_json::to_string(&ApiResponse::<()>::err("query required")).unwrap()),
            };
            SearchType::FullText(query)
        }
        "range" => {
            let min = req.min.unwrap_or(i64::MIN);
            let max = req.max.unwrap_or(i64::MAX);
            SearchType::Range { min, max }
        }
        "contains" => {
            let query = match &req.query {
                Some(q) => q.clone(),
                None => return (400, serde_json::to_string(&ApiResponse::<()>::err("query required")).unwrap()),
            };
            SearchType::Contains(query)
        }
        _ => return (400, serde_json::to_string(&ApiResponse::<()>::err("invalid search type")).unwrap()),
    };

    let mut row_ids = table.search(col_idx, search_type);
    let total = row_ids.len();

    if let Some(offset) = req.offset {
        if offset < row_ids.len() {
            row_ids = row_ids[offset..].to_vec();
        } else {
            row_ids.clear();
        }
    }

    if let Some(limit) = req.limit {
        row_ids.truncate(limit);
    }

    let rows: Vec<RowResponse> = table.get_many(&row_ids)
        .into_iter()
        .map(|(id, values)| RowResponse {
            id,
            values: values.iter().map(JsonValue::from).collect(),
        })
        .collect();

    (200, serde_json::to_string(&ApiResponse::ok(SearchResponse { rows, total })).unwrap())
}

fn handle_get(request: &HttpRequest, db: Arc<RwLock<Database>>) -> (u16, String) {
    let req: GetRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let db = db.read().unwrap();
    let table = match db.get_table(&req.table) {
        Some(t) => t,
        None => return (404, serde_json::to_string(&ApiResponse::<()>::err("table not found")).unwrap()),
    };

    let rows: Vec<RowResponse> = table.get_many(&req.ids)
        .into_iter()
        .map(|(id, values)| RowResponse {
            id,
            values: values.iter().map(JsonValue::from).collect(),
        })
        .collect();

    (200, serde_json::to_string(&ApiResponse::ok(rows)).unwrap())
}

fn handle_delete(request: &HttpRequest, db: Arc<RwLock<Database>>) -> (u16, String) {
    let req: DeleteRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let mut db = db.write().unwrap();
    let table = match db.get_table_mut(&req.table) {
        Some(t) => t,
        None => return (404, serde_json::to_string(&ApiResponse::<()>::err("table not found")).unwrap()),
    };

    let deleted: usize = req.ids.iter().filter(|&&id| table.delete(id)).count();
    (200, serde_json::to_string(&ApiResponse::ok(deleted)).unwrap())
}

fn handle_update(request: &HttpRequest, db: Arc<RwLock<Database>>) -> (u16, String) {
    let req: UpdateRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let mut db = db.write().unwrap();
    let table = match db.get_table_mut(&req.table) {
        Some(t) => t,
        None => return (404, serde_json::to_string(&ApiResponse::<()>::err("table not found")).unwrap()),
    };

    let values: Vec<Value> = req.values.iter().map(|v| v.to_value()).collect();
    match table.update(req.id, values) {
        Ok(true) => (200, serde_json::to_string(&ApiResponse::ok("updated")).unwrap()),
        Ok(false) => (404, serde_json::to_string(&ApiResponse::<()>::err("row not found")).unwrap()),
        Err(e) => (400, serde_json::to_string(&ApiResponse::<()>::err(e)).unwrap()),
    }
}

fn handle_add_user(request: &HttpRequest, auth: &AuthManager) -> (u16, String) {
    #[derive(serde::Deserialize)]
    struct AddUserRequest {
        username: String,
        password: String,
        role: Option<String>,
    }

    let req: AddUserRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    let role = match req.role.as_deref() {
        Some("admin") => Role::Admin,
        Some("readwrite") | Some("rw") => Role::ReadWrite,
        Some("readonly") | Some("ro") | None => Role::ReadOnly,
        Some(_) => return (400, serde_json::to_string(&ApiResponse::<()>::err("invalid role")).unwrap()),
    };

    match auth.add_user(&req.username, &req.password, role) {
        Ok(_) => {
            log_info!("auth", "user added: {}", req.username);
            (200, serde_json::to_string(&ApiResponse::ok("user created")).unwrap())
        }
        Err(e) => (400, serde_json::to_string(&ApiResponse::<()>::err(e)).unwrap()),
    }
}

fn handle_remove_user(request: &HttpRequest, auth: &AuthManager) -> (u16, String) {
    #[derive(serde::Deserialize)]
    struct RemoveUserRequest {
        username: String,
    }

    let req: RemoveUserRequest = match serde_json::from_slice(&request.body) {
        Ok(r) => r,
        Err(e) => return (400, serde_json::to_string(&ApiResponse::<()>::err(&e.to_string())).unwrap()),
    };

    if auth.remove_user(&req.username) {
        log_info!("auth", "user removed: {}", req.username);
        (200, serde_json::to_string(&ApiResponse::ok("user removed")).unwrap())
    } else {
        (404, serde_json::to_string(&ApiResponse::<()>::err("user not found")).unwrap())
    }
}

fn handle_list_users(auth: &AuthManager) -> (u16, String) {
    let users: Vec<_> = auth.list_users()
        .into_iter()
        .map(|(name, role)| {
            let role_str = match role {
                Role::Admin => "admin",
                Role::ReadWrite => "readwrite",
                Role::ReadOnly => "readonly",
            };
            serde_json::json!({"username": name, "role": role_str})
        })
        .collect();
    
    (200, serde_json::to_string(&ApiResponse::ok(users)).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::ColumnType;

    #[test]
    fn test_server_creation() {
        let server = HttpServer::new();
        let db = server.database();
        assert!(db.read().unwrap().table_names().is_empty());
    }

    #[test]
    fn test_with_database() {
        let mut db = Database::new();
        db.create_table("test", vec![
            Column { name: "col".into(), col_type: ColumnType::String },
        ]).unwrap();
        
        let server = HttpServer::with_database(db);
        let db = server.database();
        assert_eq!(db.read().unwrap().table_names().len(), 1);
    }

    #[test]
    fn test_check_auth_none_level() {
        let auth = AuthManager::new(false);
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/test".to_string(),
            headers: HashMap::new(),
            body: vec![],
        };
        
        // with auth level none, everything should pass
        assert!(check_auth(&request, &auth, AuthLevel::None, false, false).is_ok());
        assert!(check_auth(&request, &auth, AuthLevel::None, true, false).is_ok());
        assert!(check_auth(&request, &auth, AuthLevel::None, false, true).is_ok());
    }

    #[test]
    fn test_check_auth_write_level() {
        let auth = AuthManager::new(true);
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/test".to_string(),
            headers: HashMap::new(),
            body: vec![],
        };
        
        // with write level, reads should pass without auth, writes should fail
        assert!(check_auth(&request, &auth, AuthLevel::Write, false, false).is_ok());
        let result = check_auth(&request, &auth, AuthLevel::Write, true, false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, 401);
    }

    #[test]
    fn test_check_auth_all_level() {
        let auth = AuthManager::new(true);
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/test".to_string(),
            headers: HashMap::new(),
            body: vec![],
        };
        
        // with all level, everything should require auth
        let result = check_auth(&request, &auth, AuthLevel::All, false, false);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, 401);
        
        let result = check_auth(&request, &auth, AuthLevel::All, false, true);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().0, 401);
    }
}
