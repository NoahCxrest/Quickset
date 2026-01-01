use std::collections::HashMap;
use std::sync::RwLock;

pub struct User {
    pub username: Box<str>,
    password_hash: u64,
    pub role: Role,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Admin,
    ReadWrite,
    ReadOnly,
}

impl Role {
    pub fn can_write(&self) -> bool {
        matches!(self, Role::Admin | Role::ReadWrite)
    }

    pub fn can_admin(&self) -> bool {
        matches!(self, Role::Admin)
    }
}

pub struct AuthManager {
    users: RwLock<HashMap<Box<str>, User>>,
    enabled: bool,
}

impl AuthManager {
    pub fn new(enabled: bool) -> Self {
        let manager = Self {
            users: RwLock::new(HashMap::new()),
            enabled,
        };
        
        if enabled {
            // create default admin user
            manager.add_user("admin", "admin", Role::Admin).ok();
        }
        
        manager
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    #[inline(always)]
    fn hash_password(password: &str) -> u64 {
        // simple fnv-1a hash for passwords
        let mut h: u64 = 14695981039346656037;
        for byte in password.bytes() {
            h ^= byte as u64;
            h = h.wrapping_mul(1099511628211);
        }
        h
    }

    pub fn add_user(&self, username: &str, password: &str, role: Role) -> Result<(), &'static str> {
        let mut users = self.users.write().unwrap();
        
        if users.contains_key(username) {
            return Err("user already exists");
        }

        let user = User {
            username: username.into(),
            password_hash: Self::hash_password(password),
            role,
        };
        
        users.insert(username.into(), user);
        Ok(())
    }

    pub fn remove_user(&self, username: &str) -> bool {
        let mut users = self.users.write().unwrap();
        users.remove(username).is_some()
    }

    pub fn update_password(&self, username: &str, new_password: &str) -> bool {
        let mut users = self.users.write().unwrap();
        if let Some(user) = users.get_mut(username) {
            user.password_hash = Self::hash_password(new_password);
            true
        } else {
            false
        }
    }

    pub fn authenticate(&self, username: &str, password: &str) -> Option<Role> {
        if !self.enabled {
            return Some(Role::Admin);
        }

        let users = self.users.read().unwrap();
        users.get(username).and_then(|user| {
            if user.password_hash == Self::hash_password(password) {
                Some(user.role)
            } else {
                None
            }
        })
    }

    pub fn validate_basic_auth(&self, auth_header: &str) -> Option<Role> {
        if !self.enabled {
            return Some(Role::Admin);
        }

        // parse "Basic base64(user:pass)"
        let parts: Vec<&str> = auth_header.splitn(2, ' ').collect();
        if parts.len() != 2 || parts[0] != "Basic" {
            return None;
        }

        let decoded = Self::base64_decode(parts[1])?;
        let creds = String::from_utf8(decoded).ok()?;
        let cred_parts: Vec<&str> = creds.splitn(2, ':').collect();
        
        if cred_parts.len() != 2 {
            return None;
        }

        self.authenticate(cred_parts[0], cred_parts[1])
    }

    fn base64_decode(input: &str) -> Option<Vec<u8>> {
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        
        let mut result = Vec::with_capacity(input.len() * 3 / 4);
        let mut buffer: u32 = 0;
        let mut bits: u8 = 0;

        for byte in input.bytes() {
            if byte == b'=' {
                break;
            }
            
            let val = CHARS.iter().position(|&c| c == byte)? as u32;
            buffer = (buffer << 6) | val;
            bits += 6;

            if bits >= 8 {
                bits -= 8;
                result.push((buffer >> bits) as u8);
                buffer &= (1 << bits) - 1;
            }
        }

        Some(result)
    }

    pub fn list_users(&self) -> Vec<(String, Role)> {
        let users = self.users.read().unwrap();
        users.values()
            .map(|u| (u.username.to_string(), u.role))
            .collect()
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_disabled() {
        let auth = AuthManager::new(false);
        assert_eq!(auth.authenticate("anyone", "anything"), Some(Role::Admin));
    }

    #[test]
    fn test_add_and_authenticate() {
        let auth = AuthManager::new(true);
        auth.add_user("testuser", "testpass", Role::ReadWrite).unwrap();
        
        assert_eq!(auth.authenticate("testuser", "testpass"), Some(Role::ReadWrite));
        assert_eq!(auth.authenticate("testuser", "wrongpass"), None);
        assert_eq!(auth.authenticate("nobody", "testpass"), None);
    }

    #[test]
    fn test_default_admin() {
        let auth = AuthManager::new(true);
        assert_eq!(auth.authenticate("admin", "admin"), Some(Role::Admin));
    }

    #[test]
    fn test_remove_user() {
        let auth = AuthManager::new(true);
        auth.add_user("temp", "temp", Role::ReadOnly).unwrap();
        
        assert!(auth.authenticate("temp", "temp").is_some());
        assert!(auth.remove_user("temp"));
        assert!(auth.authenticate("temp", "temp").is_none());
    }

    #[test]
    fn test_update_password() {
        let auth = AuthManager::new(true);
        auth.add_user("user", "oldpass", Role::ReadWrite).unwrap();
        
        assert!(auth.authenticate("user", "oldpass").is_some());
        assert!(auth.update_password("user", "newpass"));
        assert!(auth.authenticate("user", "oldpass").is_none());
        assert!(auth.authenticate("user", "newpass").is_some());
    }

    #[test]
    fn test_basic_auth_parsing() {
        let auth = AuthManager::new(true);
        auth.add_user("testuser", "testpass", Role::ReadWrite).unwrap();
        
        // "testuser:testpass" base64 encoded
        let header = "Basic dGVzdHVzZXI6dGVzdHBhc3M=";
        assert_eq!(auth.validate_basic_auth(header), Some(Role::ReadWrite));
    }

    #[test]
    fn test_roles() {
        assert!(Role::Admin.can_write());
        assert!(Role::Admin.can_admin());
        assert!(Role::ReadWrite.can_write());
        assert!(!Role::ReadWrite.can_admin());
        assert!(!Role::ReadOnly.can_write());
        assert!(!Role::ReadOnly.can_admin());
    }

    #[test]
    fn test_base64_decode() {
        let decoded = AuthManager::base64_decode("aGVsbG8=").unwrap();
        assert_eq!(decoded, b"hello");
    }
}
