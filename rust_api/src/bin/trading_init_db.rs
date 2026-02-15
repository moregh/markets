use anyhow::{Context, Result};
use bcrypt::{hash, DEFAULT_COST};
use sqlx::{postgres::PgPoolOptions, Row};
use std::fs;
use std::time::Duration;

fn split_sql_statements(input: &str) -> Vec<String> {
    // Simple splitter suitable for our schema.sql (no functions / dollar-quoting).
    // Skips comments/whitespace-only segments.
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_single = false;

    for line in input.lines() {
        let trimmed = line.trim_start();
        if !in_single && trimmed.starts_with("--") {
            continue;
        }
        for ch in line.chars() {
            match ch {
                '\'' => {
                    in_single = !in_single;
                    cur.push(ch);
                }
                ';' if !in_single => {
                    let s = cur.trim();
                    if !s.is_empty() {
                        out.push(s.to_string());
                    }
                    cur.clear();
                }
                _ => cur.push(ch),
            }
        }
        cur.push('\n');
    }
    let s = cur.trim();
    if !s.is_empty() {
        out.push(s.to_string());
    }
    out
}

#[tokio::main]
async fn main() -> Result<()> {
    let db_url = env_required("DATABASE_URL")?;
    let min = env_u32("DB_MIN_POOL_SIZE", 20).max(1);
    let max = env_u32("DB_MAX_POOL_SIZE", 120).max(min);
    let acquire = env_u64("DB_ACQUIRE_TIMEOUT_SECONDS", 30).max(5);
    let default_seller_fee_rate = env_f64("DEFAULT_SELLER_FEE_RATE", 0.005);
    let admin_username = env_required("ADMIN_USERNAME")?;
    let admin_email = env_required("ADMIN_EMAIL")?;
    let admin_password = env_required("ADMIN_PASSWORD")?;
    let currencies = env_list("INITIAL_CURRENCIES", &["USD", "EUR", "GBP", "JPY"]);
    let products = env_list("INITIAL_PRODUCTS", &["BTC", "ETH", "XRP", "LTC"]);

    let db = PgPoolOptions::new()
        .min_connections(min)
        .max_connections(max)
        .acquire_timeout(Duration::from_secs(acquire))
        .connect(&db_url)
        .await
        .context("connect postgres")?;

    // Hard reset (clean schema). POSTGRES_USER in compose is a superuser in dev.
    sqlx::query("DROP SCHEMA IF EXISTS public CASCADE")
        .execute(&db)
        .await
        .context("drop public schema")?;
    sqlx::query("CREATE SCHEMA public")
        .execute(&db)
        .await
        .context("create public schema")?;

    let schema_sql = fs::read_to_string("/app/schema.sql").context("read /app/schema.sql")?;
    for stmt in split_sql_statements(&schema_sql) {
        sqlx::query(&stmt)
            .execute(&db)
            .await
            .with_context(|| format!("exec schema stmt: {}", stmt.lines().next().unwrap_or("<empty>")))?;
    }

    // Seed currencies/products.
    for c in &currencies {
        sqlx::query("INSERT INTO currencies (name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
            .bind(c)
            .execute(&db)
            .await?;
    }
    for p in &products {
        sqlx::query("INSERT INTO products (name) VALUES ($1) ON CONFLICT (name) DO NOTHING")
            .bind(p)
            .execute(&db)
            .await?;
    }

    let currencies: Vec<(i16, String)> =
        sqlx::query("SELECT id, name FROM currencies ORDER BY id")
            .fetch_all(&db)
            .await?
            .into_iter()
            .map(|r| (r.get::<i16, _>("id"), r.get::<String, _>("name")))
            .collect();
    let products: Vec<(i16, String)> =
        sqlx::query("SELECT id, name FROM products ORDER BY id")
            .fetch_all(&db)
            .await?
            .into_iter()
            .map(|r| (r.get::<i16, _>("id"), r.get::<String, _>("name")))
            .collect();

    for (pid, _p) in &products {
        for (cid, _c) in &currencies {
            sqlx::query("INSERT INTO markets (product_id, currency_id) VALUES ($1,$2) ON CONFLICT DO NOTHING")
                .bind(*pid)
                .bind(*cid)
                .execute(&db)
                .await?;
        }
    }

    // Create admin user.
    let password_hash = hash(&admin_password, DEFAULT_COST).context("bcrypt hash admin password")?;
    let admin_row = sqlx::query(
        "INSERT INTO users (username, email, password_hash, is_admin) VALUES ($1,$2,$3,true) RETURNING id",
    )
    .bind(&admin_username)
    .bind(&admin_email)
    .bind(&password_hash)
    .fetch_one(&db)
    .await
    .context("insert admin user")?;
    let admin_id: i64 = admin_row.get("id");

    // Insert initial events (fee config + admin created marker).
    let fee_type: i16 = sqlx::query_scalar("SELECT id FROM event_types WHERE code='FEE_RATE_SET'")
        .fetch_one(&db)
        .await
        .context("fetch event type FEE_RATE_SET")?;
    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(fee_type)
        .bind(admin_id)
        .bind(serde_json::json!({"seller_fee_rate": default_seller_fee_rate}))
        .execute(&db)
        .await
        .context("insert FEE_RATE_SET event")?;

    let user_created_type: i16 = sqlx::query_scalar("SELECT id FROM event_types WHERE code='USER_CREATED'")
        .fetch_one(&db)
        .await
        .context("fetch event type USER_CREATED")?;
    sqlx::query("INSERT INTO events (event_type_id, user_id, payload) VALUES ($1,$2,$3)")
        .bind(user_created_type)
        .bind(admin_id)
        .bind(serde_json::json!({"username": admin_username, "email": admin_email, "is_admin": true}))
        .execute(&db)
        .await
        .context("insert USER_CREATED event")?;

    println!(
        "initialized: admin_id={}, currencies={}, products={}, markets={}",
        admin_id,
        currencies.len(),
        products.len(),
        currencies.len() * products.len()
    );

    Ok(())
}

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing required env var: {key}"))
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
