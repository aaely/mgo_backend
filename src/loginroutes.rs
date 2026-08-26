use crate::structs::*;
use crate::auth::Claims;
use rocket::{post, serde::json::Json, State};
use rocket::http::{Cookie, CookieJar, SameSite};
use neo4rs::query;
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

    let ldap_user = crate::ldap_auth::authenticate(&login_request.username, &login_request.password)
        .await
        .map_err(|e| (Status::Unauthorized, Json(e.to_string())))?;

    let username = login_request.username.clone();
    let role = ldap_user.role;

    // Auto-provision the :User node on first login; role is re-synced from
    // AD group membership on every login, other fields are admin-managed
    // (Manage Users) and only get sane defaults the first time.
    graph.run(
        query("
            MERGE (u:User {name: $username})
            ON CREATE SET u.full_name = '', u.position = '', u.shift = '', u.slack_id = ''
            SET u.role = $role
        ")
        .param("username", username.clone())
        .param("role",     role.clone()),
    ).await.map_err(|e| (Status::InternalServerError, Json(e.to_string())))?;

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
