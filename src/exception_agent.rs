//! Sends LMS exception comments to the exception agent and writes its `reason`
//! into the trailer's Traffic Comments (ryderComments).
//!
//! Every run takes each LiveTrailer and StagedTrailer whose load (trimmed
//! lmsAccent) has LMSExceptionComments, skipping:
//!   - statusOX 'O' or 'E' (on time / early)
//!   - a 'Y' in dockStopSequence (brought over from the dropyard)
//!   - a traffic comment already there, whatever the status. Once the agent's
//!     reason is written, the trailer isn't checked again.
//!
//! Each is one request built from its comments plus the trailer's side as it
//! stands now, POSTed to EXCEPTION_AGENT_URL:
//!   gateArrivalDate  gateArrivalDate + gateArrivalTime
//!   auditStartDate   scheduleStartDate + adjustedStartTime
//!   statusOX         'L' (confirmed late) or null
//!
//! A trailer that isn't 'L' and has no gate arrival when the reason is written
//! gets "possibly late: " in front of it. The boards paint its Adjusted Start
//! Time red while that holds.
//!
//! Load on the pod: the agent calls are async HTTP, so waiting on a slow reply
//! takes no worker thread from Rocket or the websocket server. At most
//! AGENT_CONCURRENCY calls are in flight at once. Neo4j connections are held only
//! for the read at the start and each short write, never across an agent call.
//!
//! Env: EXCEPTION_AGENT_URL (unset turns the service off), and optionally
//! EXCEPTION_AGENT_AUTH, sent as the Authorization header as given
//! (e.g. "Bearer abc123").

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{self, StreamExt};
use neo4rs::{query, Graph, Node};
use serde::Serialize;
use serde_json::Value;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::tungstenite::Message;

use crate::structs::{IncomingMessage, MessageData, TrailerRecord, WebSocketList, WsClient};

const RUN_EVERY: Duration = Duration::from_secs(5 * 60);
const AGENT_TIMEOUT: Duration = Duration::from_secs(90);
/// Agent calls in flight at once
const AGENT_CONCURRENCY: usize = 3;
pub const POSSIBLY_LATE: &str = "possibly late";

/// One comment row as the agent sees it
#[derive(Serialize)]
#[allow(non_snake_case)]
struct AgentComment {
    enteredDate:       String,
    comment:           String,
    schedArrivalTime:  Option<String>,
    ActualArrivalTime: Option<String>,
}

/// The request for one trailer. Field names match the ones the response rule
/// refers to (auditStartDate, schedArrivalTime, ActualArrivalTime, statusOX,
/// gateArrival...).
#[derive(Serialize)]
#[allow(non_snake_case)]
struct AgentRequest {
    loadNo:            String,
    trailer:           String,
    routeId:           String,
    scac:              String,
    dockCode:          String,
    statusOX:          Option<String>,
    gateArrivalDate:   Option<String>,
    auditStartDate:    Option<String>,
    /// From the most recently entered comment that has one
    schedArrivalTime:  Option<String>,
    ActualArrivalTime: Option<String>,
    /// Oldest first
    comments:          Vec<AgentComment>,
}

/// A trailer due to be sent
struct Pending {
    uuid:  String,
    label: &'static str,
    hash:  String,
    req:   AgentRequest,
}

fn non_blank(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_string())
}

/// "date" + "time" as date-THH:MM; the time alone if the date is missing
/// (rows from before gateArrivalDate existed); null with no time.
fn join_date_time(date: &str, time: &str) -> Option<String> {
    let time = non_blank(time)?;
    match non_blank(date) {
        Some(date) => Some(format!("{date}T{time}")),
        None       => Some(time),
    }
}

pub async fn exception_agent_service(graph: Arc<Graph>, ws_list: WebSocketList) {
    let url = match std::env::var("EXCEPTION_AGENT_URL").ok().and_then(|u| non_blank(&u)) {
        Some(u) => u,
        None => {
            println!("EXCEPTION_AGENT_URL not set; exception agent service is off");
            return;
        }
    };
    let auth = std::env::var("EXCEPTION_AGENT_AUTH").ok().and_then(|a| non_blank(&a));
    let client = match reqwest::Client::builder().timeout(AGENT_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => { eprintln!("Exception agent: failed to build HTTP client: {e:?}"); return; }
    };

    let mut interval = tokio::time::interval(RUN_EVERY);
    // A run that overruns 5 minutes starts the next one 5 minutes after it ends,
    // rather than firing the missed ticks back to back
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let pending = match collect_pending(&graph).await {
            Ok(p) => p,
            Err(e) => { eprintln!("Exception agent service error: {e}"); continue; }
        };
        if pending.is_empty() {
            continue;
        }
        println!("Exception agent: sending {} trailer(s)", pending.len());

        stream::iter(pending)
            .for_each_concurrent(AGENT_CONCURRENCY, |p| {
                let (graph, ws_list, client, url, auth) = (&graph, &ws_list, &client, &url, auth.as_deref());
                async move { process(graph, ws_list, client, url, auth, p).await }
            })
            .await;
    }
}

/// Reads every trailer due to be sent. The result stream (and its pooled
/// connection) is done with before this returns.
async fn collect_pending(graph: &Graph) -> Result<Vec<Pending>, String> {
    let q = query("
        MATCH (t)
        WHERE (t:LiveTrailer OR t:StagedTrailer)
          AND trim(coalesce(t.lmsAccent, '')) <> ''
          AND NOT toUpper(trim(coalesce(t.statusOX, ''))) IN ['O', 'E']
          AND NOT toUpper(coalesce(t.dockStopSequence, '')) CONTAINS 'Y'
          AND trim(coalesce(t.ryderComments, '')) = ''
        MATCH (c:LMSExceptionComments)
        WHERE c.loadNo = trim(t.lmsAccent)
        WITH t, c ORDER BY c.enteredDate
        RETURN t, t:LiveTrailer AS live,
               collect(coalesce(c.enteredDate, ''))       AS entered,
               collect(coalesce(c.comment, ''))           AS texts,
               collect(coalesce(c.schedArrivalTime, ''))  AS sched,
               collect(coalesce(c.actualArrivalTime, '')) AS actual
    ");
    let mut result = graph.execute(q).await.map_err(|e| format!("reading comments: {e:?}"))?;

    let mut pending = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        let Ok(t) = row.get::<Node>("t") else { continue };
        let live = row.get::<bool>("live").unwrap_or(true);
        let entered: Vec<String> = row.get("entered").unwrap_or_default();
        let texts:   Vec<String> = row.get("texts").unwrap_or_default();
        let sched:   Vec<String> = row.get("sched").unwrap_or_default();
        let actual:  Vec<String> = row.get("actual").unwrap_or_default();

        let s = |k: &str| t.get::<String>(k).unwrap_or_default();
        let comments: Vec<AgentComment> = entered.iter().enumerate().map(|(i, e)| AgentComment {
            enteredDate:       e.clone(),
            comment:           texts.get(i).cloned().unwrap_or_default(),
            schedArrivalTime:  sched.get(i).and_then(|v| non_blank(v)),
            ActualArrivalTime: actual.get(i).and_then(|v| non_blank(v)),
        }).collect();

        let req = AgentRequest {
            loadNo:            s("lmsAccent").trim().to_string(),
            trailer:           s("trailer1"),
            routeId:           s("routeId"),
            scac:              s("scac"),
            dockCode:          s("dockCode"),
            statusOX:          (s("statusOX").trim().eq_ignore_ascii_case("L")).then(|| "L".to_string()),
            gateArrivalDate:   join_date_time(&s("gateArrivalDate"), &s("gateArrivalTime")),
            auditStartDate:    match (non_blank(&s("scheduleStartDate")), non_blank(&s("adjustedStartTime"))) {
                                   (Some(d), Some(tm)) => Some(format!("{d}T{tm}")),
                                   _ => None,
                               },
            schedArrivalTime:  comments.iter().rev().find_map(|c| c.schedArrivalTime.clone()),
            ActualArrivalTime: comments.iter().rev().find_map(|c| c.ActualArrivalTime.clone()),
            comments,
        };

        let body = serde_json::to_string(&req).map_err(|e| format!("serializing request: {e:?}"))?;
        let mut h = DefaultHasher::new();
        body.hash(&mut h);
        let hash = format!("{:016x}", h.finish());
        // The agent already answered exactly this without a usable reason
        if s("exceptionAgentHash") == hash {
            continue;
        }
        pending.push(Pending {
            uuid: s("uuid"),
            label: if live { "LiveTrailer" } else { "StagedTrailer" },
            hash,
            req,
        });
    }
    Ok(pending)
}

async fn process(
    graph:   &Graph,
    ws_list: &WebSocketList,
    client:  &reqwest::Client,
    url:     &str,
    auth:    Option<&str>,
    p:       Pending,
) {
    let Pending { uuid, label, hash, req } = p;

    let result = match ask_agent(client, url, auth, &req).await {
        Ok(r) => r,
        // Network or HTTP error: nothing recorded, so the next run tries again
        Err(e) => { eprintln!("Exception agent: load {} ({}): {e}", req.loadNo, req.trailer); return; }
    };
    let reason = result.get("reason").and_then(Value::as_str).unwrap_or("").trim().to_string();
    if reason.is_empty() {
        // The agent answered but gave nothing to write. The hash stops the same
        // request going out every run; a change to it (new comment, gate
        // arrival, going late) sends it again.
        eprintln!("Exception agent: load {} reply has no reason: {result}", req.loadNo);
        let q = query(&format!("MATCH (t:{label} {{uuid: $uuid}}) SET t.exceptionAgentHash = $hash"))
            .param("uuid", uuid).param("hash", hash);
        if let Err(e) = graph.run(q).await {
            eprintln!("Exception agent: marking load {}: {e:?}", req.loadNo);
        }
        return;
    }

    // Re-checks the traffic comment is still blank, in case someone typed one
    // while the agent was working. Possibly late is decided from the trailer as
    // it is now, not as it was when the request went out.
    let q = query(&format!("
        MATCH (t:{label} {{uuid: $uuid}})
        WHERE trim(coalesce(t.ryderComments, '')) = ''
        WITH t, (toUpper(trim(coalesce(t.statusOX, ''))) <> 'L'
                 AND trim(coalesce(t.gateArrivalTime, '')) = '') AS possiblyLate
        SET t.ryderComments = CASE WHEN possiblyLate THEN $prefix + $reason ELSE $reason END,
            t.exceptionAgentResult = $result
        REMOVE t.exceptionAgentHash
        RETURN t
    "))
        .param("uuid",   uuid)
        .param("prefix", format!("{POSSIBLY_LATE}: "))
        .param("reason", reason)
        .param("result", result.to_string());

    let mut written = match graph.execute(q).await {
        Ok(r) => r,
        Err(e) => { eprintln!("Exception agent: saving load {}: {e:?}", req.loadNo); return; }
    };
    match written.next().await {
        Ok(Some(row)) => {
            if let Ok(node) = row.get::<Node>("t") {
                broadcast_trailer(ws_list, &crate::helpers::trailer_from_node(&node)).await;
            }
        }
        // Rolled off, or a traffic comment was typed in the meantime
        _ => println!("Exception agent: load {} not written (comment added or trailer gone)", req.loadNo),
    }
}

async fn ask_agent(
    client: &reqwest::Client,
    url:    &str,
    auth:   Option<&str>,
    req:    &AgentRequest,
) -> Result<Value, String> {
    let mut call = client.post(url).json(req);
    if let Some(a) = auth {
        call = call.header(reqwest::header::AUTHORIZATION, a);
    }
    let resp = call.send().await.map_err(|e| format!("request failed: {e}"))?;
    let status = resp.status();
    let text = resp.text().await.map_err(|e| format!("reading reply: {e}"))?;
    if !status.is_success() {
        return Err(format!("agent returned {status}: {}", text.chars().take(300).collect::<String>()));
    }
    extract_result(&text).ok_or_else(|| format!("no result object in reply: {}", text.chars().take(300).collect::<String>()))
}

/// The rule asks for one bare JSON object, but agent platforms often wrap the
/// model's text ({"output": "..."}), or the model adds ``` fences anyway.
/// Takes the first object carrying `reason`: the reply itself, a JSON string,
/// or any field one level down holding either.
fn extract_result(text: &str) -> Option<Value> {
    fn parse(s: &str) -> Option<Value> {
        let s = s.trim();
        let s = s.strip_prefix("```json").or_else(|| s.strip_prefix("```")).unwrap_or(s);
        let s = s.strip_suffix("```").unwrap_or(s).trim();
        serde_json::from_str(s).ok()
    }
    fn found(v: &Value) -> Option<Value> {
        match v {
            Value::Object(m) if m.contains_key("reason") => Some(v.clone()),
            Value::String(s) => parse(s).and_then(|inner| match inner {
                Value::Object(ref m) if m.contains_key("reason") => Some(inner),
                _ => None,
            }),
            _ => None,
        }
    }

    let top = parse(text)?;
    if let Some(v) = found(&top) {
        return Some(v);
    }
    match &top {
        Value::Object(m) => m.values().find_map(found),
        _ => None,
    }
}

/// The trailer_update both boards already take (the live sheet and next shift
/// share the trailer list, matched by uuid). VAA and Universal only hear about
/// their own dock. The client keeps its own editRef for the row.
async fn broadcast_trailer(ws_list: &WebSocketList, trailer: &TrailerRecord) {
    let Ok(data) = serde_json::to_string(trailer) else { return };
    let msg = IncomingMessage { r#type: "trailer_update".to_string(), data: Some(MessageData { message: data }) };
    let Ok(text) = serde_json::to_string(&msg) else { return };
    let list = ws_list.lock().await;
    for WsClient { tx, role, .. } in list.values() {
        if !crate::permissions::dock_allowed(role, &trailer.dockCode) { continue; }
        let _ = tx.send(Message::Text(text.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_bare_fenced_and_wrapped_replies() {
        assert_eq!(extract_result(r#"{"reason":"a"}"#).unwrap()["reason"], "a");
        assert_eq!(extract_result("```json\n{\"reason\":\"b\"}\n```").unwrap()["reason"], "b");
        assert_eq!(extract_result(r#"{"output":"{\"reason\":\"c\"}"}"#).unwrap()["reason"], "c");
        assert_eq!(extract_result(r#"{"data":{"reason":"d"}}"#).unwrap()["reason"], "d");
        assert!(extract_result("not json").is_none());
        assert!(extract_result(r#"{"output":"plain text"}"#).is_none());
    }

    #[test]
    fn joins_date_and_time() {
        assert_eq!(join_date_time("2026-11-11", "08:15").as_deref(), Some("2026-11-11T08:15"));
        assert_eq!(join_date_time("", "08:15").as_deref(), Some("08:15"));
        assert_eq!(join_date_time("2026-11-11", " "), None);
    }
}
