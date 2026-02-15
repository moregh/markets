use anyhow::{anyhow, Result};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct AppConfig {
    pub(crate) database: DatabaseConfig,
    pub(crate) jwt: JwtConfig,
    pub(crate) api: ApiConfig,
    pub(crate) fees: FeesConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct DatabaseConfig {
    pub(crate) url: String,
    pub(crate) min_pool_size: u32,
    pub(crate) max_pool_size: u32,
    pub(crate) max_lifetime_seconds: u64,
    pub(crate) acquire_timeout_seconds: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct JwtConfig {
    pub(crate) secret_key: String,
    pub(crate) algorithm: String,
    pub(crate) access_token_expire_minutes: i64,
    pub(crate) refresh_token_expire_days: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct ApiConfig {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) cors_origins: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct FeesConfig {
    pub(crate) default_seller_fee_rate: f64,
}

pub(crate) fn load_config() -> Result<AppConfig> {
    let cfg = AppConfig {
        database: DatabaseConfig {
            url: env_required("DATABASE_URL")?,
            min_pool_size: env_u32("DB_MIN_POOL_SIZE", 20),
            max_pool_size: env_u32("DB_MAX_POOL_SIZE", 120),
            max_lifetime_seconds: env_u64("DB_MAX_LIFETIME_SECONDS", 1800),
            acquire_timeout_seconds: env_u64("DB_ACQUIRE_TIMEOUT_SECONDS", 30),
        },
        jwt: JwtConfig {
            secret_key: env_required("JWT_SECRET_KEY")?,
            algorithm: env_string("JWT_ALGORITHM", "HS256"),
            access_token_expire_minutes: env_i64("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", 30),
            refresh_token_expire_days: env_i64("JWT_REFRESH_TOKEN_EXPIRE_DAYS", 30),
        },
        api: ApiConfig {
            host: env_string("API_HOST", "0.0.0.0"),
            port: env_u16("API_PORT", 8000),
            cors_origins: env_list("CORS_ORIGINS", &["*"]),
        },
        fees: FeesConfig {
            default_seller_fee_rate: env_f64("DEFAULT_SELLER_FEE_RATE", 0.005),
        },
    };
    if cfg.jwt.algorithm.to_uppercase() != "HS256" {
        return Err(anyhow!("Only HS256 is supported"));
    }
    Ok(cfg)
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("missing required env var: {key}"))
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u16(key: &str, default: u16) -> u16 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(default)
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_list(key: &str, default: &[&str]) -> Vec<String> {
    match std::env::var(key) {
        Ok(v) => parse_list_value(&v)
            .unwrap_or_else(|| default.iter().map(|s| (*s).to_string()).collect()),
        Err(_) => default.iter().map(|s| (*s).to_string()).collect(),
    }
}

fn parse_list_value(raw: &str) -> Option<Vec<String>> {
    if let Ok(v) = serde_json::from_str::<Vec<String>>(raw) {
        return Some(v.into_iter().filter(|s| !s.trim().is_empty()).collect());
    }
    let parts: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts)
    }
}
