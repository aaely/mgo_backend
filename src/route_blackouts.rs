//! Route blackouts: 15-minute windows a route can't be delivered in. Set by admins
//! on the Route Blackouts page; the Exception Log and DY Log won't schedule a
//! delivery into one, and the upload routes refuse it here too so nothing gets
//! around it.
//!
//! (:RouteBlackout {route, slots}) — one node per route with any blacked-out
//! window; `route` is the full route ID uppercased, `slots` the start minute of
//! the day (0, 15, ... 1425) of each window it can't be delivered in. A route
//! with no node has no restriction.
//!
//! Nodes saved before windows were 15 minutes carry `hours` (0-23) instead; each
//! reads as all four of that hour's windows until the route is next saved.

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
    pub slots: Vec<i64>,
}

/// Minutes in a blackout window.
pub const SLOT_MINUTES: i64 = 15;

fn valid_slot(slot: i64) -> bool {
    (0..24 * 60).contains(&slot) && slot % SLOT_MINUTES == 0
}

/// 'HH:MM' for a slot's start minute.
pub fn slot_label(slot: i64) -> String {
    format!("{:02}:{:02}", slot / 60, slot % 60)
}

fn server_error(context: &str, e: impl std::fmt::Debug) -> ApiError {
    eprintln!("{context}: {e:?}");
    (Status::InternalServerError, Json(format!("{context}: {e:?}")))
}

/// Routes are matched on the full route ID, ignoring case and stray spaces.
pub fn route_key(route: &str) -> String {
    route.trim().to_uppercase()
}

/// The window an 'HH:MM' delivery time falls in, as its start minute of the day
/// ("14:37" -> 870, the 14:30 window); None when there's no usable time yet.
pub fn delivery_slot(time: &str) -> Option<i64> {
    let mut parts = time.trim().splitn(2, ':');
    let hour = parts.next()?.trim().parse::<i64>().ok().filter(|h| (0..=23).contains(h))?;
    let minute = match parts.next() {
        Some(m) => m.trim().get(..2).unwrap_or(m.trim()).parse::<i64>().ok().filter(|m| (0..=59).contains(m))?,
        None    => 0,
    };
    Some(hour * 60 + minute / SLOT_MINUTES * SLOT_MINUTES)
}

async fn read_all(graph: &Graph) -> Result<Vec<RouteBlackout>, neo4rs::Error> {
    let mut result = graph.execute(query("
        MATCH (b:RouteBlackout)
        RETURN b.route AS route, b.slots AS slots, b.hours AS hours ORDER BY route
    ")).await?;
    let mut out = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        // An hours-only node predates 15-minute windows: the whole hour is out
        let slots: Vec<i64> = match row.get::<Vec<i64>>("slots") {
            Ok(slots) => slots,
            Err(_) => row.get::<Vec<i64>>("hours").unwrap_or_default().into_iter()
                .flat_map(|h| (0..60 / SLOT_MINUTES).map(move |q| h * 60 + q * SLOT_MINUTES))
                .collect(),
        };
        out.push(RouteBlackout { route: row.get("route").unwrap_or_default(), slots });
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
        if let Some(m) = b.slots.iter().find(|m| !valid_slot(**m)) {
            return Err((Status::BadRequest, Json(format!(
                "{route}: {m} isn't the start of a {SLOT_MINUTES}-minute window"))));
        }
        let slots: Vec<i64> = b.slots.iter().copied().collect::<BTreeSet<_>>().into_iter().collect();
        // A route with every window open has no restriction; don't keep a node for it.
        if !slots.is_empty() {
            clean.push(RouteBlackout { route, slots });
        }
    }

    let mut txn = state.graph.start_txn().await
        .map_err(|e| server_error("Failed to start transaction", e))?;

    let written: Result<(), neo4rs::Error> = async {
        txn.run(query("MATCH (b:RouteBlackout) DETACH DELETE b")).await?;
        for b in &clean {
            txn.run(query("CREATE (:RouteBlackout {route: $route, slots: $slots})")
                .param("route", b.route.clone())
                .param("slots", b.slots.clone())).await?;
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
/// that lands in a blacked-out window, as a message for the operator. Rows with
/// no route or no time yet aren't checked.
pub async fn find_blackout<'a>(
    graph: &Graph,
    rows:  impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<Option<String>, ApiError> {
    let all = read_all(graph).await.map_err(|e| server_error("Failed to read route blackouts", e))?;
    if all.is_empty() {
        return Ok(None);
    }
    let blackouts: HashMap<String, HashSet<i64>> = all.into_iter()
        .map(|b| (b.route, b.slots.into_iter().collect()))
        .collect();

    for (route, time) in rows {
        let (Some(slots), Some(slot)) = (blackouts.get(&route_key(route)), delivery_slot(time)) else { continue };
        if slots.contains(&slot) {
            return Ok(Some(format!(
                "Route {} can't be delivered at {}–{} — that window is blacked out for it",
                route_key(route), slot_label(slot), slot_label((slot + SLOT_MINUTES) % (24 * 60)),
            )));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Same inputs as the frontend's deliverySlot check, so the two sides agree.
    #[test]
    fn delivery_slot_reads_hh_mm() {
        assert_eq!(delivery_slot("14:30"),   Some(870));
        assert_eq!(delivery_slot("14:44"),   Some(870));
        assert_eq!(delivery_slot("14:45"),   Some(885));
        assert_eq!(delivery_slot("00:15"),   Some(15));
        assert_eq!(delivery_slot("7:05"),    Some(420));
        assert_eq!(delivery_slot(" 22:00 "), Some(1320));
        assert_eq!(delivery_slot("23:59"),   Some(1425));
        assert_eq!(delivery_slot("14:30:00"), Some(870));
        assert_eq!(delivery_slot("14"),      Some(840));
        assert_eq!(delivery_slot(""),        None);
        assert_eq!(delivery_slot("24:00"),   None);
        assert_eq!(delivery_slot("14:75"),   None);
        assert_eq!(delivery_slot("abc"),     None);
    }

    #[test]
    fn slots_are_quarter_hours_of_the_day() {
        assert!(valid_slot(0) && valid_slot(870) && valid_slot(1425));
        assert!(!valid_slot(7) && !valid_slot(1440) && !valid_slot(-15));
        assert_eq!(slot_label(870), "14:30");
        assert_eq!(slot_label(0), "00:00");
    }

    #[test]
    fn route_key_ignores_case_and_spaces() {
        assert_eq!(route_key(" arm705aau "), "ARM705AAU");
        assert_eq!(route_key("8120340"), "8120340");
    }
}
