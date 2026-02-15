use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use axum::http::{HeaderMap, StatusCode};
use http::header::AUTHORIZATION;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::AppConfig;
use crate::error::ApiError;
use crate::state::AppState;

fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let h = headers.get(AUTHORIZATION)?;
    let s = h.to_str().ok()?;
    let prefix = "Bearer ";
    if !s.starts_with(prefix) {
        return None;
    }
    Some(s[prefix.len()..].to_string())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Claims {
    pub(crate) sub: String,
    pub(crate) admin: bool,
    pub(crate) usr: Option<String>,
    pub(crate) eml: Option<String>,
    pub(crate) exp: i64,
    pub(crate) jti: Option<String>,
    pub(crate) r#type: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct AuthUser {
    pub(crate) user_id: i64,
    pub(crate) is_admin: bool,
    pub(crate) username: Option<String>,
    pub(crate) email: Option<String>,
}

pub(crate) async fn auth_user(state: &AppState, headers: &HeaderMap) -> Result<AuthUser, ApiError> {
    let token = bearer_token(headers).ok_or_else(|| ApiError::new(StatusCode::UNAUTHORIZED, "Not authenticated"))?;
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    let decoded = decode::<Claims>(
        &token,
        &DecodingKey::from_secret(state.cfg.jwt.secret_key.as_bytes()),
        &validation,
    )
    .map_err(|_| ApiError::new(StatusCode::UNAUTHORIZED, "Invalid token"))?;
    if decoded.claims.r#type.as_deref().unwrap_or("access") != "access" {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid token"));
    }
    let user_id = decoded.claims.sub.parse::<i64>().unwrap_or(0);
    if user_id <= 0 {
        return Err(ApiError::new(StatusCode::UNAUTHORIZED, "Invalid token"));
    }
    Ok(AuthUser {
        user_id,
        is_admin: decoded.claims.admin,
        username: decoded.claims.usr,
        email: decoded.claims.eml,
    })
}

pub(crate) async fn admin_user(state: &AppState, headers: &HeaderMap) -> Result<AuthUser, ApiError> {
    let u = auth_user(state, headers).await?;
    if !u.is_admin {
        return Err(ApiError::new(StatusCode::FORBIDDEN, "Admin only"));
    }
    Ok(u)
}

pub(crate) fn make_access_token(cfg: &AppConfig, user_id: i64, is_admin: bool, username: &str, email: &str) -> Result<String> {
    let exp = now_epoch_secs() + cfg.jwt.access_token_expire_minutes * 60;
    let claims = Claims {
        sub: user_id.to_string(),
        admin: is_admin,
        usr: Some(username.to_string()),
        eml: Some(email.to_string()),
        exp,
        jti: Some(Uuid::new_v4().to_string()),
        r#type: Some("access".to_string()),
    };
    Ok(encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(cfg.jwt.secret_key.as_bytes()),
    )?)
}

pub(crate) fn make_refresh_token(cfg: &AppConfig, user_id: i64, is_admin: bool) -> Result<String> {
    let exp = now_epoch_secs() + cfg.jwt.refresh_token_expire_days * 86400;
    let claims = Claims {
        sub: user_id.to_string(),
        admin: is_admin,
        usr: None,
        eml: None,
        exp,
        jti: Some(Uuid::new_v4().to_string()),
        r#type: Some("refresh".to_string()),
    };
    Ok(encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(cfg.jwt.secret_key.as_bytes()),
    )?)
}

pub(crate) fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    hex::encode(h.finalize())
}
