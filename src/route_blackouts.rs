//! Route blackouts: hours a route can't be delivered in. Set by admins on the
//! Route Blackouts page; the Exception Log and DY Log won't schedule a delivery
//! into one, and the upload routes refuse it here too so nothing gets around it.
//!
//! (:RouteBlackout {route, hours}) — one node per route with any blacked-out
//! hour; `route` is the full route ID uppercased, `hours` the delivery hours
//! (0-23) it can't be delivered in. A route with no node has no restriction.

use crate::auth::{AdminOnly, AuthenticatedUser};
use crate::structs::{AppState, IncomingMessage, MessageData};
use neo4rs::{query, Graph};
use rocket::{get, http::Status, post, serde::json::Json, State};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio_tungstenite::tungstenite::Message;

type ApiError = (Status, Json<String>);

/// Websocket type carrying the saved blackouts to every client. Server-sent
/// only — wsserver refuses to relay it from a client.
pub const WS_ROUTE_BLACKOUTS: &str = "route_blackouts";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RouteBlackout {
    pub route: String,
    pub hours: Vec<i64>,
}

fn server_error(context: &str, e: impl std::fmt::Debug) -> ApiError {
    eprintln!("{context}: {e:?}");
    (Status::InternalServerError, Json(format!("{context}: {e:?}")))
}

/// Routes are matched on the full route ID, ignoring case and stray spaces.
pub fn route_key(route: &str) -> String {
    route.trim().to_uppercase()
}

/// The delivery hour of an 'HH:MM' time; None when there's no time yet.
pub fn delivery_hour(time: &str) -> Option<i64> {
    time.trim().split(':').next()?.parse::<i64>().ok().filter(|h| (0..=23).contains(h))
}

async fn read_all(graph: &Graph) -> Result<Vec<RouteBlackout>, neo4rs::Error> {
    let mut result = graph.execute(query("
        MATCH (b:RouteBlackout) RETURN b.route AS route, b.hours AS hours ORDER BY route
    ")).await?;
    let mut out = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        out.push(RouteBlackout {
            route: row.get("route").unwrap_or_default(),
            hours: row.get("hours").unwrap_or_default(),
        });
    }
    Ok(out)
}

#[get("/api/get_route_blackouts")]
pub async fn get_route_blackouts(
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<Vec<RouteBlackout>>, ApiError> {
    read_all(&state.graph).await
        .map(Json)
        .map_err(|e| server_error("Failed to read route blackouts", e))
}

/// Replaces every route's blackouts with what the editor sends, in one
/// transaction, then pushes them to every open screen.
#[post("/api/set_route_blackouts", format = "json", data = "<data>")]
pub async fn set_route_blackouts(
    data:  Json<Vec<RouteBlackout>>,
    state: &State<AppState>,
    admin: AdminOnly,
) -> Result<Json<Vec<RouteBlackout>>, ApiError> {
    // ── Normalise and check the whole list before writing anything ──
    let mut seen = HashSet::new();
    let mut clean: Vec<RouteBlackout> = Vec::new();
    for b in data.iter() {
        let route = route_key(&b.route);
        if route.is_empty() {
            return Err((Status::BadRequest, Json("A row has no route".into())));
        }
        if !seen.insert(route.clone()) {
            return Err((Status::BadRequest, Json(format!("{route} is listed twice"))));
        }
        if let Some(h) = b.hours.iter().find(|h| !(0..=23).contains(*h)) {
            return Err((Status::BadRequest, Json(format!("{route}: hour {h} isn't 0-23"))));
        }
        let hours: Vec<i64> = b.hours.iter().copied().collect::<BTreeSet<_>>().into_iter().collect();
        // A route with every hour open has no restriction; don't keep a node for it.
        if !hours.is_empty() {
            clean.push(RouteBlackout { route, hours });
        }
    }

    let mut txn = state.graph.start_txn().await
        .map_err(|e| server_error("Failed to start transaction", e))?;

    let written: Result<(), neo4rs::Error> = async {
        txn.run(query("MATCH (b:RouteBlackout) DETACH DELETE b")).await?;
        for b in &clean {
            txn.run(query("CREATE (:RouteBlackout {route: $route, hours: $hours})")
                .param("route", b.route.clone())
                .param("hours", b.hours.clone())).await?;
        }
        Ok(())
    }.await;

    if let Err(e) = written {
        let _ = txn.rollback().await;
        return Err(server_error("Failed to save route blackouts; nothing changed", e));
    }
    txn.commit().await.map_err(|e| server_error("Failed to save route blackouts", e))?;
    println!("Route blackouts saved by {}: {} routes restricted", admin.0.username, clean.len());

    // ── Push to every open screen, whatever page or topic it's on ──
    if let Ok(payload) = serde_json::to_string(&clean) {
        let ws_msg = IncomingMessage {
            r#type: WS_ROUTE_BLACKOUTS.to_string(),
            data:   Some(MessageData { message: payload }),
        };
        if let Ok(message) = serde_json::to_string(&ws_msg) {
            let ws_list = state.ws_list.lock().await;
            for (_, (tx, _)) in ws_list.iter() {
                let _ = tx.send(Message::Text(message.clone()));
            }
        }
    }

    Ok(Json(clean))
}

/// Server-side check for the upload routes: the first (route, delivery time)
/// that lands in a blacked-out hour, as a message for the operator. Rows with no
/// route or no time yet aren't checked.
pub async fn find_blackout<'a>(
    graph: &Graph,
    rows:  impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Option<String>, ApiError> {
    let all = read_all(graph).await.map_err(|e| server_error("Failed to read route blackouts", e))?;
    if all.is_empty() {
        return Ok(None);
    }
    let blackouts: HashMap<String, HashSet<i64>> = all.into_iter()
        .map(|b| (b.route, b.hours.into_iter().collect()))
        .collect();

    for (route, time) in rows {
        let (Some(hours), Some(hour)) = (blackouts.get(&route_key(route)), delivery_hour(time)) else { continue };
        if hours.contains(&hour) {
            return Ok(Some(format!(
                "Route {} can't be delivered at {:02}:00 — that hour is blacked out for it",
                route_key(route), hour,
            )));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Same inputs as the frontend's blackedOutHour check, so the two sides agree.
    #[test]
    fn delivery_hour_reads_hh_mm() {
        assert_eq!(delivery_hour("14:30"), Some(14));
        assert_eq!(delivery_hour("00:15"), Some(0));
        assert_eq!(delivery_hour("7:05"),  Some(7));
        assert_eq!(delivery_hour(" 22:00 "), Some(22));
        assert_eq!(delivery_hour(""),      None);
        assert_eq!(delivery_hour("24:00"), None);
        assert_eq!(delivery_hour("abc"),   None);
    }

    #[test]
    fn route_key_ignores_case_and_spaces() {
        assert_eq!(route_key(" arm705aau "), "ARM705AAU");
        assert_eq!(route_key("8120340"), "8120340");
    }
}
