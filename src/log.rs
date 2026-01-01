use std::io::{self, Write};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::SystemTime;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Off = 5,
}

impl LogLevel {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "trace" => Some(LogLevel::Trace),
            "debug" => Some(LogLevel::Debug),
            "info" => Some(LogLevel::Info),
            "warn" | "warning" => Some(LogLevel::Warn),
            "error" => Some(LogLevel::Error),
            "off" | "none" => Some(LogLevel::Off),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Trace => "TRACE",
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
            LogLevel::Off => "OFF",
        }
    }
}

static LOG_LEVEL: AtomicU8 = AtomicU8::new(LogLevel::Info as u8);

pub struct Logger;

impl Logger {
    pub fn init(level: LogLevel) {
        LOG_LEVEL.store(level as u8, Ordering::Relaxed);
    }

    pub fn init_from_env() {
        if let Ok(level_str) = std::env::var("QUICKSET_LOG") {
            if let Some(level) = LogLevel::from_str(&level_str) {
                Self::init(level);
            }
        }
    }

    pub fn set_level(level: LogLevel) {
        LOG_LEVEL.store(level as u8, Ordering::Relaxed);
    }

    pub fn get_level() -> LogLevel {
        match LOG_LEVEL.load(Ordering::Relaxed) {
            0 => LogLevel::Trace,
            1 => LogLevel::Debug,
            2 => LogLevel::Info,
            3 => LogLevel::Warn,
            4 => LogLevel::Error,
            _ => LogLevel::Off,
        }
    }

    #[inline(always)]
    pub fn should_log(level: LogLevel) -> bool {
        level as u8 >= LOG_LEVEL.load(Ordering::Relaxed)
    }

    pub fn log(level: LogLevel, module: &str, message: &str) {
        if !Self::should_log(level) {
            return;
        }

        let timestamp = Self::timestamp();
        let level_str = level.as_str();

        let output = format!("{} [{}] {}: {}\n", timestamp, level_str, module, message);
        
        let _ = if level >= LogLevel::Warn {
            io::stderr().write_all(output.as_bytes())
        } else {
            io::stdout().write_all(output.as_bytes())
        };
    }

    fn timestamp() -> String {
        let duration = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        
        let secs = duration.as_secs();
        let millis = duration.subsec_millis();
        
        // simple iso-ish timestamp
        let days_since_epoch = secs / 86400;
        let time_of_day = secs % 86400;
        let hours = time_of_day / 3600;
        let minutes = (time_of_day % 3600) / 60;
        let seconds = time_of_day % 60;
        
        // approximate date (good enough for logging)
        let year = 1970 + (days_since_epoch / 365);
        let day_of_year = days_since_epoch % 365;
        let month = day_of_year / 30 + 1;
        let day = day_of_year % 30 + 1;
        
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}",
            year, month, day, hours, minutes, seconds, millis
        )
    }
}

#[macro_export]
macro_rules! log_trace {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::Logger::log($crate::log::LogLevel::Trace, $module, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_debug {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::Logger::log($crate::log::LogLevel::Debug, $module, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_info {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::Logger::log($crate::log::LogLevel::Info, $module, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_warn {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::Logger::log($crate::log::LogLevel::Warn, $module, &format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::Logger::log($crate::log::LogLevel::Error, $module, &format!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_ordering() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
        assert!(LogLevel::Error < LogLevel::Off);
    }

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from_str("debug"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::from_str("DEBUG"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::from_str("Info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::from_str("warn"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("warning"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::from_str("invalid"), None);
    }

    #[test]
    fn test_should_log() {
        Logger::set_level(LogLevel::Info);
        
        assert!(!Logger::should_log(LogLevel::Trace));
        assert!(!Logger::should_log(LogLevel::Debug));
        assert!(Logger::should_log(LogLevel::Info));
        assert!(Logger::should_log(LogLevel::Warn));
        assert!(Logger::should_log(LogLevel::Error));
    }

    #[test]
    fn test_set_and_get_level() {
        Logger::set_level(LogLevel::Debug);
        assert_eq!(Logger::get_level(), LogLevel::Debug);
        
        Logger::set_level(LogLevel::Error);
        assert_eq!(Logger::get_level(), LogLevel::Error);
    }
}
