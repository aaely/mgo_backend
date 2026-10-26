//! The `io` websocket topic: IO trailer changes pushed to the IO pages as they
//! happen, so one operator's edit shows on every other open IO screen.
//!
//! Messages, all server-sent only (wsserver drops them from clients):
//! - `io_update`  — one trailer as get_io returns it (IOResponse JSON), after it
//!   was created or changed. Replaces that trailer's row, or adds it.
//! - `io_removed` — a trailer ID, after it was delivered or deleted.
//! - `io_refresh` — no payload; a bulk upload changed many trailers, so refetch
//!   get_io.
//!
//! Only subscribers get them, and never the vaa/univ roles, which get_io refuses.

use crate::getters::read_io;
use crate::structs::{AppState, IncomingMessage, MessageData};
use tokio_tungstenite::tungstenite::Message;

pub const TOPIC_IO: &str = "io";

pub const WS_IO_UPDATE:  &str = "io_update";
pub const WS_IO_REMOVED: &str = "io_removed";
pub const WS_IO_REFRESH: &str = "io_refresh";

/// Sends `message_type` to every subscriber of the io topic whose role may see IO.
async fn send(state: &AppState, message_type: &str, payload: Option<String>) {
    let ws_msg = IncomingMessage {
        r#type: message_type.to_string(),
        data:   payload.map(|message| MessageData { message }),
    };
    let Ok(text) = serde_json::to_string(&ws_msg) else { return };

    let ws_list = state.ws_list.lock().await;
    let topics  = state.ws_topics.lock().await;
    for (addr, (tx, role)) in ws_list.iter() {
        if !topics.get(addr).is_some_and(|t| t.contains(TOPIC_IO)) { continue }
        if role.contains("vaa") || role.contains("univ") { continue }
        let _ = tx.send(Message::Text(text.clone()));
    }
}

/// Pushes `trailer` as it now stands — or its removal, when it no longer shows on
/// the IO pages. A failed read is logged and skipped: the change itself is saved,
/// and the other screens catch up on their next load.
pub async fn publish_io_trailer(state: &AppState, trailer: &str) {
    match read_io(&state.graph, Some(trailer)).await {
        Ok(rows) => match rows.into_iter().next() {
            Some(row) => {
                if let Ok(json) = serde_json::to_string(&row) {
                    send(state, WS_IO_UPDATE, Some(json)).await;
                }
            }
            None => publish_io_removed(state, trailer).await,
        },
        Err(e) => eprintln!("io feed: failed to read trailer {trailer}: {e:?}"),
    }
}

pub async fn publish_io_removed(state: &AppState, trailer: &str) {
    send(state, WS_IO_REMOVED, Some(trailer.to_string())).await;
}

pub async fn publish_io_refresh(state: &AppState) {
    send(state, WS_IO_REFRESH, None).await;
}
