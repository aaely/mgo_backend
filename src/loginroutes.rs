use crate::structs::*;
use crate::auth::{Claims, AuthenticatedUser};
use crate::role::Role;
use rocket::{post, serde::json::Json, State};
use rocket::http::{Cookie, CookieJar, SameSite};
use neo4rs::{query, Node};
use bcrypt::{hash, verify, DEFAULT_COST};
use chrono::{Utc, Duration};
use jsonwebtoken::{encode, Header, EncodingKey};
use rocket::http::Status;

pub fn issue_access_token(username: &str, role: &str, secret: &str) -> Result<String, jsonwebtoken::errors::Error> {
    let exp = Utc::now()
        .checked_add_signed(Duration::seconds(3600))
        .expect("valid timestamp")
        .timestamp() as usize;
    encode(
        &Header::default(),
        &Claims { username: username.to_string(), role: role.to_string(), exp },
        &EncodingKey::from_secret(secret.as_ref()),
    )
}

pub async fn store_refresh_token(
    graph: &neo4rs::Graph,
    token: &str,
    username: &str,
    role: &str,
) -> Result<(), neo4rs::Error> {
    let expires_at = Utc::now()
        .checked_add_signed(Duration::days(7))
        .expect("valid timestamp")
        .timestamp();
    graph.run(
        query("CREATE (r:RefreshToken {token: $token, username: $username, role: $role, expires_at: $expires_at})")
            .param("token",      token)
            .param("username",   username)
            .param("role",       role)
            .param("expires_at", expires_at),
    ).await
}

fn make_cookie(name: &'static str, value: String) -> Cookie<'static> {
    let mut c = Cookie::new(name, value);
    c.set_http_only(true);
    c.set_same_site(SameSite::Lax);
    c.set_path("/");
    c
}

#[post("/api/login", format = "json", data = "<login_request>")]
pub async fn login(
    jar: &CookieJar<'_>,
    login_request: Json<LoginRequest>,
    state: &State<AppState>,
) -> Result<Json<LoginResponse>, (Status, Json<String>)> {
    let graph = &state.graph;

    let mut result = match graph.execute(
        query("MATCH (u:User {name: $username}) RETURN u")
            .param("username", login_request.username.clone()),
    ).await {
        Ok(r) => r,
        Err(e) => return Err((Status::Unauthorized, Json(e.to_string()))),
    };

    let record = match result.next().await.unwrap() {
        Some(r) => r,
        None => return Err((Status::Unauthorized, Json("User not found".to_string()))),
    };

    let user_node: Node = record.get("u").unwrap();
    let stored_password: String = user_node.get("password").unwrap();
    let username: String = user_node.get("name").unwrap();
    let role: String = user_node.get("role").unwrap();

    match verify(&login_request.password, &stored_password) {
        Ok(true) => {}
        Ok(false) => return Err((Status::Unauthorized, Json("Invalid password".to_string()))),
        Err(e) => return Err((Status::Unauthorized, Json(e.to_string()))),
    }

    let access_token = issue_access_token(&username, &role, &state.jwt_secret)
        .map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    let refresh_token = uuid::Uuid::new_v4().to_string();
    store_refresh_token(graph, &refresh_token, &username, &role).await
        .map_err(|e| (Status::InternalServerError, Json(format!("Failed to store refresh token: {e}"))))?;

    jar.add(make_cookie("f126f1b7d90a5bd5", access_token));
    jar.add(make_cookie("738fadeef720a679", refresh_token));

    Ok(Json(LoginResponse {
        user: UserResponse { username, role },
    }))
}

fn validate_password(pw: &str) -> Result<(), &'static str> {
    if pw.len() < 8 {
        return Err("Password must be at least 8 characters");
    }
    if !pw.chars().any(|c| c.is_uppercase()) {
        return Err("Password must contain at least one uppercase letter");
    }
    if !pw.chars().any(|c| c.is_lowercase()) {
        return Err("Password must contain at least one lowercase letter");
    }
    if !pw.chars().any(|c| !c.is_alphanumeric()) {
        return Err("Password must contain at least one special character");
    }
    let lower: Vec<char> = pw.to_lowercase().chars().collect();
    if lower == lower.iter().rev().cloned().collect::<Vec<char>>() {
        return Err("Password must not be a palindrome");
    }
    Ok(())
}

#[post("/api/register", format = "json", data = "<user>")]
pub async fn register(
    user: Json<LoginRequest>,
    state: &State<AppState>,
) -> Result<Json<&'static str>, (Status, Json<String>)> {
    if let Err(msg) = validate_password(&user.password) {
        return Err((Status::UnprocessableEntity, Json(msg.to_string())));
    }

    let graph = &state.graph;

    let hashed_password = match hash(&user.password, DEFAULT_COST) {
        Ok(p) => p,
        Err(e) => return Err((Status::Unauthorized, Json(e.to_string()))),
    };

    println!("{} {}", user.username.clone(), hashed_password);

    let q = query("CREATE (u:User {name: $username, password: $password, role: 'read'})")
        .param("username", user.username.clone())
        .param("password", hashed_password);

    match graph.run(q).await {
        Ok(_) => Ok(Json("User registered")),
        Err(e) => Err((Status::Unauthorized, Json(format!("Failed to register user: {:?}", e)))),
    }
}

#[post("/api/refresh")]
pub async fn refresh_token(
    jar: &CookieJar<'_>,
    state: &State<AppState>,
) -> Result<Json<LoginResponse>, Json<String>> {
    let rt = jar.get("738fadeef720a679")
        .ok_or_else(|| Json("No refresh token cookie".to_string()))?
        .value()
        .to_string();

    let graph = &state.graph;
    let now = Utc::now().timestamp();

    let mut result = match graph.execute(
        query("
            MATCH (r:RefreshToken {token: $token})
            WHERE r.expires_at > $now
            WITH r, r.username AS username, r.role AS role
            DELETE r
            RETURN username, role
        ")
        .param("token", rt)
        .param("now",   now),
    ).await {
        Ok(r) => r,
        Err(e) => return Err(Json(e.to_string())),
    };

    let row = match result.next().await {
        Ok(Some(r)) => r,
        _ => return Err(Json("Invalid or expired refresh token".to_string())),
    };

    let username: String = row.get("username").map_err(|e| Json(e.to_string()))?;
    let role: String    = row.get("role").map_err(|e| Json(e.to_string()))?;

    let access_token = issue_access_token(&username, &role, &state.jwt_secret)
        .map_err(|e| Json(e.to_string()))?;

    let new_refresh_token = uuid::Uuid::new_v4().to_string();
    store_refresh_token(graph, &new_refresh_token, &username, &role).await
        .map_err(|e| Json(format!("Failed to store refresh token: {e}")))?;

    jar.add(make_cookie("f126f1b7d90a5bd5", access_token));
    jar.add(make_cookie("738fadeef720a679", new_refresh_token));

    Ok(Json(LoginResponse {
        user: UserResponse { username, role },
    }))
}

#[post("/api/logout")]
pub async fn logout(
    jar: &CookieJar<'_>,
    state: &State<AppState>,
) -> Result<Json<&'static str>, Json<&'static str>> {
    if let Some(rt_cookie) = jar.get("738fadeef720a679") {
        let rt = rt_cookie.value().to_string();

        let mut result = state.graph.execute(
            query("
                MATCH (r:RefreshToken {token: $token})
                WITH r, r.username AS username
                DELETE r
                RETURN username
            ")
            .param("token", rt),
        ).await.map_err(|_| Json("Logout failed"))?;

        if let Ok(Some(row)) = result.next().await {
            if let Ok(username) = row.get::<String>("username") {
                state.edit_refs.lock().await.remove(&username);
            }
        }
    }

    let mut ac = Cookie::new("f126f1b7d90a5bd5", "");
    ac.set_path("/");
    jar.remove(ac);

    let mut rc = Cookie::new("738fadeef720a679", "");
    rc.set_path("/");
    jar.remove(rc);

    Ok(Json("Logged out"))
}

#[post("/api/wipe_password", format = "json", data = "<req>")]
pub async fn wipe_password(
    req:   Json<WipePasswordRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role:  Role,
) -> Result<Json<String>, (Status, Json<String>)> {
    if role.0 != "admin" && role.0 != "manager" {
        return Err((Status::Forbidden, Json("Forbidden".to_string())));
    }

    let token      = uuid::Uuid::new_v4().to_string();
    let expires_at = Utc::now()
        .checked_add_signed(Duration::hours(24))
        .expect("valid timestamp")
        .timestamp();

    state.graph.run(
        query("
            MATCH (u:User {name: $username})
            SET u.password = ''
            WITH u
            MERGE (r:PasswordResetToken {username: $username})
            SET r.token = $token, r.expires_at = $expires_at
        ")
        .param("username",   req.username.clone())
        .param("token",      token.clone())
        .param("expires_at", expires_at),
    ).await.map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    // TODO: when SMTP is approved, email token to user here instead of returning it
    Ok(Json(token))
}

#[post("/api/reset_password", format = "json", data = "<req>")]
pub async fn reset_password(
    req:   Json<ResetPasswordRequest>,
    state: &State<AppState>,
) -> Result<Json<&'static str>, (Status, Json<String>)> {
    if let Err(msg) = validate_password(&req.new_password) {
        return Err((Status::UnprocessableEntity, Json(msg.to_string())));
    }

    let now = Utc::now().timestamp();

    let mut result = state.graph.execute(
        query("
            MATCH (r:PasswordResetToken {username: $username, token: $token})
            WHERE r.expires_at > $now
            RETURN r
        ")
        .param("username", req.username.clone())
        .param("token",    req.token.clone())
        .param("now",      now),
    ).await.map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    if result.next().await.ok().flatten().is_none() {
        return Err((Status::Unauthorized, Json("Invalid or expired reset token".to_string())));
    }

    let hashed = hash(&req.new_password, DEFAULT_COST)
        .map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    state.graph.run(
        query("
            MATCH (u:User {name: $username})
            SET u.password = $password
            WITH u
            MATCH (r:PasswordResetToken {username: $username})
            DELETE r
        ")
        .param("username", req.username.clone())
        .param("password", hashed),
    ).await.map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    Ok(Json("Password reset successfully"))
}

#[post("/api/change_password", format = "json", data = "<req>")]
pub async fn change_password(
    req:   Json<ChangePasswordRequest>,
    state: &State<AppState>,
) -> Result<Json<&'static str>, (Status, Json<String>)> {
    if let Err(msg) = validate_password(&req.new_password) {
        return Err((Status::UnprocessableEntity, Json(msg.to_string())));
    }

    let mut result = state.graph.execute(
        query("MATCH (u:User {name: $username}) RETURN u.password AS password")
            .param("username", req.username.clone()),
    ).await.map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    let row = result.next().await
        .map_err(|e| (Status::InternalServerError, Json(e.to_string())))?
        .ok_or_else(|| (Status::NotFound, Json("User not found".to_string())))?;

    let stored: String = row.get("password")
        .map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    match verify(&req.old_password, &stored) {
        Ok(true)  => {}
        Ok(false) => return Err((Status::Unauthorized, Json("Current password is incorrect".to_string()))),
        Err(e)    => return Err((Status::InternalServerError, Json(e.to_string()))),
    }

    let hashed = hash(&req.new_password, DEFAULT_COST)
        .map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    state.graph.run(
        query("MATCH (u:User {name: $username}) SET u.password = $password")
            .param("username", req.username.clone())
            .param("password", hashed),
    ).await.map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

    Ok(Json("Password changed successfully"))
}
