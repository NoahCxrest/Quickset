use quickset::config::Config;
use quickset::http::HttpServer;
use quickset::log::Logger;

fn main() {
    Logger::init_from_env();
    
    let config = Config::from_env();
    let addr = config.address();
    
    let server = HttpServer::with_config(config);
    
    if let Err(e) = server.run(&addr) {
        eprintln!("server error: {}", e);
        std::process::exit(1);
    }
}
