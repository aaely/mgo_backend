//! Dock capacity, edited by admins instead of living in the frontend's code.
//!
//! Two tables, one node per value:
//!   (:DockHourCapacity  {dock, hour, capacity})  — trailers a dock takes per hour
//!     (the hourly grid behind Exception/DY/IO Log availability and Dock Splits)
//!   (:ShiftDockCapacity {shift, dock, capacity}) — trailers per dock per shift
//!     (the live sheet's dock buttons, Dock Splits, the overview chart)
//!
//! An empty database means "not configured yet": the frontend keeps its built-in
//! values until the first save, so a fresh environment behaves as before.

use crate::auth::{AdminOnly, AuthenticatedUser};
use crate::structs::{AppState, IncomingMessage, MessageData};
use neo4rs::query;
use rocket::{get, http::Status, post, serde::json::Json, State};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio_tungstenite::tungstenite::Message;

/// Websocket type carrying a saved configuration to every client. Server-sent
/// only — wsserver refuses to relay it from a client.
pub const WS_DOCK_CAPACITY: &str = "dock_capacity";

type ApiError = (Status, Json<String>);

const SHIFTS: [&str; 3] = ["1st", "2nd", "3rd"];

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HourCapacity {
    pub dock:     String,
    /// 0-23, the hour a trailer is scheduled to start.
    pub hour:     i64,
    pub capacity: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShiftCapacity {
    /// '1st', '2nd' or '3rd'
    pub shift:    String,
    pub dock:     String,
    pub capacity: i64,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DockCapacity {
    pub hourly: Vec<HourCapacity>,
    pub shift:  Vec<ShiftCapacity>,
}

fn server_error(context: &str, e: impl std::fmt::Debug) -> ApiError {
    eprintln!("{context}: {e:?}");
    (Status::InternalServerError, Json(format!("{context}: {e:?}")))
}

#[get("/api/get_dock_capacity")]
pub async fn get_dock_capacity(
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<DockCapacity>, ApiError> {
    let graph = &state.graph;
    let mut out = DockCapacity::default();

    let mut result = graph.execute(query("
        MATCH (n:DockHourCapacity)
        RETURN n.dock AS dock, n.hour AS hour, n.capacity AS capacity
        ORDER BY dock, hour
    ")).await.map_err(|e| server_error("Failed to read hourly dock capacity", e))?;
    while let Ok(Some(row)) = result.next().await {
        out.hourly.push(HourCapacity {
            dock:     row.get("dock").unwrap_or_default(),
            hour:     row.get("hour").unwrap_or(0),
            capacity: row.get("capacity").unwrap_or(0),
        });
    }

    let mut result = graph.execute(query("
        MATCH (n:ShiftDockCapacity)
        RETURN n.shift AS shift, n.dock AS dock, n.capacity AS capacity
        ORDER BY shift, dock
    ")).await.map_err(|e| server_error("Failed to read shift dock capacity", e))?;
    while let Ok(Some(row)) = result.next().await {
        out.shift.push(ShiftCapacity {
            shift:    row.get("shift").unwrap_or_default(),
            dock:     row.get("dock").unwrap_or_default(),
            capacity: row.get("capacity").unwrap_or(0),
        });
    }

    Ok(Json(out))
}

/// Checked whole before anything is written. Dock codes are trimmed and
/// uppercased to match how trailers carry them.
fn validate(mut data: DockCapacity) -> Result<DockCapacity, String> {
    // An empty table reads as "not configured" and brings back the frontend's
    // built-in values, which is never what clearing it means.
    if data.hourly.is_empty() || data.shift.is_empty() {
        return Err("Both tables need at least one dock".into())
    }
    let mut seen = HashSet::new();
    for h in data.hourly.iter_mut() {
        h.dock = h.dock.trim().to_uppercase();
        if h.dock.is_empty()              { return Err("An hourly row has no dock".into()) }
        if !(0..=23).contains(&h.hour)    { return Err(format!("{}: hour {} isn't 0-23", h.dock, h.hour)) }
        if !(0..=999).contains(&h.capacity) { return Err(format!("{} hour {}: capacity {} isn't 0-999", h.dock, h.hour, h.capacity)) }
        if !seen.insert((h.dock.clone(), h.hour)) {
            return Err(format!("{} hour {} is listed twice", h.dock, h.hour))
        }
    }
    let mut seen = HashSet::new();
    for s in data.shift.iter_mut() {
        s.dock = s.dock.trim().to_uppercase();
        if s.dock.is_empty()                { return Err("A shift row has no dock".into()) }
        if !SHIFTS.contains(&s.shift.as_str()) { return Err(format!("{}: shift '{}' isn't 1st, 2nd or 3rd", s.dock, s.shift)) }
        if !(0..=999).contains(&s.capacity) { return Err(format!("{} {}: capacity {} isn't 0-999", s.dock, s.shift, s.capacity)) }
        if !seen.insert((s.shift.clone(), s.dock.clone())) {
            return Err(format!("{} {} is listed twice", s.dock, s.shift))
        }
    }
    Ok(data)
}

/// Replaces both tables with what the editor sends — the full configuration,
/// so a dock removed in the editor is removed here. One transaction: readers
/// never see a half-written table.
#[post("/api/set_dock_capacity", format = "json", data = "<data>")]
pub async fn set_dock_capacity(
    data:  Json<DockCapacity>,
    state: &State<AppState>,
    admin: AdminOnly,
) -> Result<Json<DockCapacity>, ApiError> {
    let data = validate(data.into_inner()).map_err(|msg| (Status::BadRequest, Json(msg)))?;

    // neo4rs 0.7 params take lists of primitives, not lists of maps, so each
    // table goes over as parallel lists zipped back by index.
    let h_docks: Vec<String> = data.hourly.iter().map(|h| h.dock.clone()).collect();
    let h_hours: Vec<i64>    = data.hourly.iter().map(|h| h.hour).collect();
    let h_caps:  Vec<i64>    = data.hourly.iter().map(|h| h.capacity).collect();
    let s_shifts: Vec<String> = data.shift.iter().map(|s| s.shift.clone()).collect();
    let s_docks:  Vec<String> = data.shift.iter().map(|s| s.dock.clone()).collect();
    let s_caps:   Vec<i64>    = data.shift.iter().map(|s| s.capacity).collect();

    let mut txn = state.graph.start_txn().await
        .map_err(|e| server_error("Failed to start transaction", e))?;

    let written: Result<(), neo4rs::Error> = async {
        txn.run(query("MATCH (n:DockHourCapacity) DETACH DELETE n")).await?;
        txn.run(query("MATCH (n:ShiftDockCapacity) DETACH DELETE n")).await?;
        txn.run(query("
            UNWIND range(0, size($docks) - 1) AS i
            CREATE (:DockHourCapacity {dock: $docks[i], hour: $hours[i], capacity: $caps[i]})
        ").param("docks", h_docks).param("hours", h_hours).param("caps", h_caps)).await?;
        txn.run(query("
            UNWIND range(0, size($docks) - 1) AS i
            CREATE (:ShiftDockCapacity {shift: $shifts[i], dock: $docks[i], capacity: $caps[i]})
        ").param("shifts", s_shifts).param("docks", s_docks).param("caps", s_caps)).await?;
        Ok(())
    }.await;

    match written {
        Ok(()) => {
            txn.commit().await.map_err(|e| server_error("Failed to save dock capacity", e))?;
            println!("Dock capacity saved by {}: {} hourly, {} shift values",
                admin.0.username, data.hourly.len(), data.shift.len());

            // ── Push it to every open screen, whatever page or topic it's on.
            //    Capacity isn't dock-scoped data, so VAA/Universal users get it too. ──
            if let Ok(payload) = serde_json::to_string(&data) {
                let ws_msg = IncomingMessage {
                    r#type: WS_DOCK_CAPACITY.to_string(),
                    data:   Some(MessageData { message: payload }),
                };
                if let Ok(message) = serde_json::to_string(&ws_msg) {
                    let ws_list = state.ws_list.lock().await;
                    for (_, (tx, _)) in ws_list.iter() {
                        let _ = tx.send(Message::Text(message.clone()));
                    }
                }
            }

            Ok(Json(data))
        }
        Err(e) => {
            let _ = txn.rollback().await;
            Err(server_error("Failed to save dock capacity; nothing changed", e))
        }
    }
}
