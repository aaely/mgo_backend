//! A part's PDT and free-text comment, written on the IO page's Low DOH tab.
//!
//! (:PartComment {part, pdt, comment, updatedBy, updatedAt}) — one node per
//! part number with either set. Kept apart from PartASL because every ASL
//! upload deletes and recreates those, which would wipe these with them.
//! Once both are empty the node is removed.
//!
//! Every save is pushed to all open screens as a `part_comment` websocket
//! message carrying the saved record (both fields blank once it's removed).

use crate::auth::AuthenticatedUser;
use crate::role::Role;
use crate::structs::{AppState, IncomingMessage, MessageData};
use neo4rs::query;
use rocket::{get, http::Status, post, serde::json::Json, State};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::tungstenite::Message;

/// Websocket type carrying one saved part comment to every client. Server-sent
/// only — wsserver refuses to relay it from a client.
pub const WS_PART_COMMENT: &str = "part_comment";

type ApiError = (Status, Json<String>);

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PartComment {
    pub part:    String,
    #[serde(default)]
    pub pdt:     String,
    pub comment: String,
    #[serde(rename = "updatedBy", default)]
    pub updated_by: String,
    #[serde(rename = "updatedAt", default)]
    pub updated_at: String,
}

fn server_error(context: &str, e: impl std::fmt::Debug) -> ApiError {
    eprintln!("{context}: {e:?}");
    (Status::InternalServerError, Json(context.to_string()))
}

/// Push a saved comment to every open screen, whatever page or topic it's on.
async fn broadcast(state: &AppState, saved: &PartComment) {
    let Ok(payload) = serde_json::to_string(saved) else { return };
    let ws_msg = IncomingMessage {
        r#type: WS_PART_COMMENT.to_string(),
        data:   Some(MessageData { message: payload }),
    };
    if let Ok(message) = serde_json::to_string(&ws_msg) {
        let ws_list = state.ws_list.lock().await;
        for (_, (tx, _)) in ws_list.iter() {
            let _ = tx.send(Message::Text(message.clone()));
        }
    }
}

fn forbidden(role: &Role) -> Option<ApiError> {
    (role.0.contains("vaa") || role.0.contains("univ"))
        .then(|| (Status::Forbidden, Json("Forbidden".to_string())))
}

/// Every part's comment.
#[get("/api/get_part_comments")]
pub async fn get_part_comments(
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role:  Role,
) -> Result<Json<Vec<PartComment>>, ApiError> {
    if let Some(e) = forbidden(&role) { return Err(e); }

    let mut result = state.graph.execute(query("
        MATCH (c:PartComment)
        RETURN c.part AS part, coalesce(c.pdt, '') AS pdt, coalesce(c.comment, '') AS comment,
               c.updatedBy AS updatedBy, c.updatedAt AS updatedAt
    ")).await.map_err(|e| server_error("Failed to read part comments", e))?;

    let mut comments = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        comments.push(PartComment {
            part:       row.get("part").unwrap_or_default(),
            pdt:        row.get("pdt").unwrap_or_default(),
            comment:    row.get("comment").unwrap_or_default(),
            updated_by: row.get("updatedBy").unwrap_or_default(),
            updated_at: row.get("updatedAt").unwrap_or_default(),
        });
    }
    Ok(Json(comments))
}

#[derive(Deserialize)]
pub struct PartCommentInput {
    pub part:    String,
    /// Only the fields sent are changed, so the PDT and comment boxes can save
    /// on their own without overwriting each other.
    #[serde(default)]
    pub pdt:     Option<String>,
    #[serde(default)]
    pub comment: Option<String>,
}

/// Saves a part's PDT and/or comment, stamped with who and when. Returns both as
/// saved; once both are empty the node is removed and they come back blank.
#[post("/api/set_part_comment", format = "json", data = "<input>")]
pub async fn set_part_comment(
    input: Json<PartCommentInput>,
    state: &State<AppState>,
    user:  AuthenticatedUser,
    role:  Role,
) -> Result<Json<PartComment>, ApiError> {
    if let Some(e) = forbidden(&role) { return Err(e); }

    let part = input.part.trim().to_string();
    if part.is_empty() {
        return Err((Status::BadRequest, Json("A part number is required".into())));
    }
    if input.pdt.is_none() && input.comment.is_none() {
        return Err((Status::BadRequest, Json("Nothing to save".into())));
    }
    let updated_by = user.0.username.clone();
    let updated_at = chrono::Utc::now().format("%Y-%m-%d %H:%M").to_string();

    let mut result = state.graph.execute(query("
        MERGE (c:PartComment {part: $part})
        SET c.pdt       = CASE WHEN $setPdt     THEN $pdt     ELSE coalesce(c.pdt, '')     END,
            c.comment   = CASE WHEN $setComment THEN $comment ELSE coalesce(c.comment, '') END,
            c.updatedBy = $updatedBy,
            c.updatedAt = $updatedAt
        RETURN c.pdt AS pdt, c.comment AS comment
    ")
    .param("part",       part.clone())
    .param("setPdt",     input.pdt.is_some())
    .param("pdt",        input.pdt.as_deref().unwrap_or("").trim().to_string())
    .param("setComment", input.comment.is_some())
    .param("comment",    input.comment.as_deref().unwrap_or("").trim().to_string())
    .param("updatedBy",  updated_by.clone())
    .param("updatedAt",  updated_at.clone()))
    .await.map_err(|e| server_error("Failed to save the part comment", e))?;

    let row = result.next().await.map_err(|e| server_error("Failed to save the part comment", e))?
        .ok_or_else(|| server_error("Failed to save the part comment", "no row returned"))?;
    let pdt: String     = row.get("pdt").unwrap_or_default();
    let comment: String = row.get("comment").unwrap_or_default();

    if pdt.is_empty() && comment.is_empty() {
        state.graph.run(query("MATCH (c:PartComment {part: $part}) DELETE c").param("part", part.clone()))
            .await.map_err(|e| server_error("Failed to remove the part comment", e))?;
        let removed = PartComment { part, ..Default::default() };
        broadcast(state, &removed).await;
        return Ok(Json(removed));
    }

    let saved = PartComment { part, pdt, comment, updated_by, updated_at };
    broadcast(state, &saved).await;
    Ok(Json(saved))
}
