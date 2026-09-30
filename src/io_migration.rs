//! One-time move of IO scheduling data between environments (test -> prod).
//!
//! IO trailers arrive through upload_in_transit with every schedule field blank.
//! Everything operators did afterwards — dates, times, SCAC, comments, status,
//! edited SIDs and parts, deliveries, no-shows — exists only in this graph and
//! can't be rebuilt from the in-transit report. /api/export_io reads it out and
//! /api/import_io writes it into another environment.
//!
//! Covered: active IO trailers (Trailer -> Schedule, SID, Part), DeliveredTrailer
//! and NoShowTrailer history, and the ExceptionLogEntry rows IO scheduling
//! creates (loadNum or type starting "IO") — those put a scheduled IO trailer on
//! the schedule builder.
//!
//! Node identity is carried over exactly — SID {id, ciscoID} and Part {number,
//! quantity, duns} are upload_in_transit's MERGE keys — so the next in-transit
//! upload in the target lands on the imported nodes instead of duplicating them.
//! Import only adds and updates, never deletes, and runs as one transaction.
//!
//! Delete this module (and its two routes in main.rs) once the migration is done.

use crate::auth::AdminOnly;
use crate::structs::{AppState, ExceptionLogEntry};
use neo4rs::{query, Node, Txn};
use rocket::{get, http::Status, post, serde::json::Json, State};
use serde::{Deserialize, Serialize};

type ApiError = (Status, Json<String>);

fn server_error(context: &str, e: impl std::fmt::Debug) -> ApiError {
    eprintln!("{context}: {e:?}");
    (Status::InternalServerError, Json(format!("{context}: {e:?}")))
}

/// One trailer<->part line. A trailer with no SIDs, or a SID with no parts, still
/// gets a row — with the missing columns blank — so its schedule isn't lost.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct IoLine {
    pub trailer:      String,
    pub sid:          String,
    /// Uploaded SIDs carry ciscoID as part of their MERGE key (it may be ''); SIDs
    /// added by hand in the IO screen have no ciscoID at all. The two must stay
    /// distinct or the next upload would MERGE a duplicate SID.
    pub has_cisco_id: bool,
    pub cisco_id:     String,
    pub part:         String,
    pub quantity:     i64,
    pub duns:         String,
    /// (sid)-[:HAS_PART]->(part)
    pub on_sid:       bool,
    /// (trailer)-[:CONTAINS_PART]->(part), which update_io maintains separately.
    pub on_trailer:   bool,
    // ── Schedule, repeated on every line of the trailer ──
    pub destination:   String,
    pub supplier:      String,
    pub location:      String,
    pub ship_date:     String,
    pub original_date: String,
    pub schedule_date: String,
    pub schedule_time: String,
    pub comments:      String,
    pub status:        String,
    pub scac:          String,
    pub carrier_email: String,
}

/// A DeliveredTrailer or NoShowTrailer — standalone nodes with no relationships.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct IoHistory {
    pub trailer_id:       String,
    /// delivery_date or no_show_date: with trailer_id, the node's MERGE key.
    pub event_date:       String,
    pub destination:      String,
    pub supplier:         String,
    pub location:         String,
    pub ship_date:        String,
    pub original_date:    String,
    pub schedule_date:    String,
    pub schedule_time:    String,
    pub comments:         String,
    pub status:           String,
    pub scheduled_status: String,
    pub scac:             String,
    pub carrier_email:    String,
    pub parts:            Vec<String>,
    pub sids:             Vec<String>,
    pub recorded_by:      String,
    pub recorded_at:      String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct IoData {
    pub active:     Vec<IoLine>,
    pub delivered:  Vec<IoHistory>,
    pub no_shows:   Vec<IoHistory>,
    pub exceptions: Vec<ExceptionLogEntry>,
}

#[derive(Debug, Serialize, Default)]
pub struct IoImportResult {
    pub trailers:   usize,
    pub lines:      usize,
    pub delivered:  usize,
    pub no_shows:   usize,
    pub exceptions: usize,
}

/// (label, date key) for the two history node kinds. Fixed strings, so they are
/// safe to format into Cypher.
const DELIVERED: (&str, &str) = ("DeliveredTrailer", "delivery_date");
const NO_SHOW:   (&str, &str) = ("NoShowTrailer",    "no_show_date");

fn str_prop(n: &Node, key: &str) -> String {
    n.get::<String>(key).unwrap_or_default()
}

fn with_schedule(line: IoLine, s: &Node) -> IoLine {
    IoLine {
        destination:   str_prop(s, "Destination"),
        supplier:      str_prop(s, "Supplier"),
        location:      str_prop(s, "Location"),
        ship_date:     str_prop(s, "ShipDate"),
        original_date: str_prop(s, "OriginalDate"),
        schedule_date: str_prop(s, "ScheduleDate"),
        schedule_time: str_prop(s, "ScheduleTime"),
        comments:      str_prop(s, "Comments"),
        status:        str_prop(s, "Status"),
        scac:          str_prop(s, "Scac"),
        carrier_email: str_prop(s, "CarrierEmail"),
        ..line
    }
}

async fn export_history(graph: &neo4rs::Graph, (label, date_key): (&str, &str))
    -> Result<Vec<IoHistory>, ApiError>
{
    let mut result = graph
        .execute(query(&format!("MATCH (n:{label}) RETURN n ORDER BY n.{date_key}, n.trailer_id")))
        .await
        .map_err(|e| server_error(&format!("Failed to read {label}"), e))?;

    let mut rows = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        let n: Node = row.get("n").map_err(|e| server_error(&format!("Bad {label} row"), e))?;
        rows.push(IoHistory {
            trailer_id:       str_prop(&n, "trailer_id"),
            event_date:       str_prop(&n, date_key),
            destination:      str_prop(&n, "Destination"),
            supplier:         str_prop(&n, "Supplier"),
            location:         str_prop(&n, "Location"),
            ship_date:        str_prop(&n, "ShipDate"),
            original_date:    str_prop(&n, "OriginalDate"),
            schedule_date:    str_prop(&n, "ScheduleDate"),
            schedule_time:    str_prop(&n, "ScheduleTime"),
            comments:         str_prop(&n, "Comments"),
            status:           str_prop(&n, "Status"),
            scheduled_status: str_prop(&n, "ScheduledStatus"),
            scac:             str_prop(&n, "Scac"),
            carrier_email:    str_prop(&n, "CarrierEmail"),
            parts:            n.get::<Vec<String>>("parts").unwrap_or_default(),
            sids:             n.get::<Vec<String>>("sids").unwrap_or_default(),
            recorded_by:      str_prop(&n, "recorded_by"),
            recorded_at:      str_prop(&n, "recorded_at"),
        });
    }
    Ok(rows)
}

#[get("/api/export_io")]
pub async fn export_io(
    state:  &State<AppState>,
    _guard: AdminOnly,
) -> Result<Json<IoData>, ApiError> {
    let graph = &state.graph;
    let mut active: Vec<IoLine> = Vec::new();

    // ── Every SID/part line, plus a bare row for SIDs with no parts and for
    //    trailers with no SIDs. A null-bound p makes the CONTAINS_PART match fail,
    //    so ct comes back null rather than erroring. ──
    let mut result = graph.execute(query("
        MATCH (t:Trailer)-[:HAS_SCHEDULE]->(s:Schedule)
        OPTIONAL MATCH (t)-[:HAS_SID]->(sid:SID)
        OPTIONAL MATCH (sid)-[:HAS_PART]->(p:Part)
        OPTIONAL MATCH (t)-[ct:CONTAINS_PART]->(p)
        RETURN t.id AS trailer, s,
               sid.id AS sid,
               sid.ciscoID IS NOT NULL AS has_cisco,
               sid.ciscoID AS cisco,
               p.number AS part, p.quantity AS quantity, p.duns AS duns,
               p IS NOT NULL AS on_sid,
               ct IS NOT NULL AS on_trailer
    ")).await.map_err(|e| server_error("Failed to read IO trailers", e))?;

    while let Ok(Some(row)) = result.next().await {
        let s: Node = row.get("s").map_err(|e| server_error("Bad IO schedule row", e))?;
        active.push(with_schedule(IoLine {
            trailer:      row.get("trailer").unwrap_or_default(),
            sid:          row.get("sid").unwrap_or_default(),
            has_cisco_id: row.get("has_cisco").unwrap_or(false),
            cisco_id:     row.get("cisco").unwrap_or_default(),
            part:         row.get("part").unwrap_or_default(),
            quantity:     row.get("quantity").unwrap_or(0),
            duns:         row.get("duns").unwrap_or_default(),
            on_sid:       row.get("on_sid").unwrap_or(false),
            on_trailer:   row.get("on_trailer").unwrap_or(false),
            ..Default::default()
        }, &s));
    }

    // ── Parts on the trailer that no longer hang off any of its SIDs — update_io
    //    can leave these behind, and the IO screen still lists them. ──
    let mut result = graph.execute(query("
        MATCH (t:Trailer)-[:HAS_SCHEDULE]->(s:Schedule)
        MATCH (t)-[:CONTAINS_PART]->(p:Part)
        WHERE NOT (t)-[:HAS_SID]->(:SID)-[:HAS_PART]->(p)
        RETURN t.id AS trailer, s, p.number AS part, p.quantity AS quantity, p.duns AS duns
    ")).await.map_err(|e| server_error("Failed to read loose IO parts", e))?;

    while let Ok(Some(row)) = result.next().await {
        let s: Node = row.get("s").map_err(|e| server_error("Bad IO schedule row", e))?;
        active.push(with_schedule(IoLine {
            trailer:    row.get("trailer").unwrap_or_default(),
            part:       row.get("part").unwrap_or_default(),
            quantity:   row.get("quantity").unwrap_or(0),
            duns:       row.get("duns").unwrap_or_default(),
            on_trailer: true,
            ..Default::default()
        }, &s));
    }

    active.sort_by(|a, b| (&a.trailer, &a.sid, &a.part).cmp(&(&b.trailer, &b.sid, &b.part)));

    let delivered = export_history(graph, DELIVERED).await?;
    let no_shows  = export_history(graph, NO_SHOW).await?;

    let mut exceptions = Vec::new();
    let mut result = graph.execute(query("
        MATCH (e:ExceptionLogEntry)
        WHERE e.loadNum STARTS WITH 'IO' OR e.type STARTS WITH 'IO'
        RETURN e ORDER BY e.trailer1, e.loadNum
    ")).await.map_err(|e| server_error("Failed to read IO exception entries", e))?;

    while let Ok(Some(row)) = result.next().await {
        let e: Node = row.get("e").map_err(|e| server_error("Bad exception row", e))?;
        exceptions.push(ExceptionLogEntry {
            loadNum:        str_prop(&e, "loadNum"),
            dock:           str_prop(&e, "dock"),
            r#type:         str_prop(&e, "type"),
            status:         str_prop(&e, "status"),
            route:          str_prop(&e, "route"),
            scac:           str_prop(&e, "scac"),
            trailer1:       str_prop(&e, "trailer1"),
            trailer2:       str_prop(&e, "trailer2"),
            supplier:       str_prop(&e, "supplier"),
            dockSequence:   str_prop(&e, "dockSequence"),
            originalDate:   str_prop(&e, "originalDate"),
            originalTime:   str_prop(&e, "originalTime"),
            newDate:        str_prop(&e, "newDate"),
            newTime:        str_prop(&e, "newTime"),
            newEndDate:     str_prop(&e, "newEndDate"),
            newEndTime:     str_prop(&e, "newEndTime"),
            comment:        str_prop(&e, "comment"),
            requestor:      str_prop(&e, "requestor"),
            isRepower:      e.get::<bool>("isRepower").unwrap_or(false),
            repowerLoadNum: str_prop(&e, "repowerLoadNum"),
        });
    }

    Ok(Json(IoData { active, delivered, no_shows, exceptions }))
}

/// Rejects the whole import before anything is written. Spreadsheet round-trips
/// are the likely way in for bad data: Excel turns long SIDs and part numbers
/// into scientific notation, which would import as new, wrong nodes.
fn validate(data: &IoData) -> Result<(), String> {
    let sci = |v: &str| {
        let v = v.trim();
        let digits = v.replace(['E', 'e', '+', '.'], "");
        v.contains(['E', 'e']) && v.contains('+')
            && !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
    };

    for (i, l) in data.active.iter().enumerate() {
        let at = format!("active row {}", i + 1);
        if l.trailer.trim().is_empty() { return Err(format!("{at}: trailer is blank")) }
        if !l.part.is_empty() && !l.on_sid && !l.on_trailer {
            return Err(format!("{at}: part {} is on neither the SID nor the trailer", l.part))
        }
        if l.on_sid && (l.sid.is_empty() || l.part.is_empty()) {
            return Err(format!("{at}: on_sid needs both a sid and a part"))
        }
        for (name, v) in [("trailer", &l.trailer), ("sid", &l.sid), ("cisco_id", &l.cisco_id), ("part", &l.part), ("duns", &l.duns)] {
            if sci(v) { return Err(format!("{at}: {name} '{v}' looks like Excel scientific notation")) }
        }
    }
    for (kind, rows) in [("delivered", &data.delivered), ("no-show", &data.no_shows)] {
        for (i, h) in rows.iter().enumerate() {
            if h.trailer_id.trim().is_empty() || h.event_date.trim().is_empty() {
                return Err(format!("{kind} row {}: trailer_id and event_date are required", i + 1))
            }
        }
    }
    for (i, e) in data.exceptions.iter().enumerate() {
        if e.loadNum.trim().is_empty() || e.trailer1.trim().is_empty() {
            return Err(format!("exception row {}: loadNum and trailer1 are required", i + 1))
        }
    }
    Ok(())
}

async fn import_active(txn: &mut Txn, lines: &[IoLine]) -> Result<(usize, usize), String> {
    // ── Schedules first, one per trailer. Fields come from the trailer's first
    //    row. An existing schedule is overwritten: the source is authoritative. ──
    let mut seen = std::collections::HashSet::new();
    for l in lines.iter().filter(|l| seen.insert(l.trailer.clone())) {
        txn.run(query("
            MERGE (t:Trailer {id: $trailer})
            MERGE (t)-[:HAS_SCHEDULE]->(s:Schedule)
            SET s.TrailerID    = $trailer,
                s.Destination  = $destination,
                s.Supplier     = $supplier,
                s.Location     = $location,
                s.ShipDate     = $ship_date,
                s.OriginalDate = $original_date,
                s.ScheduleDate = $schedule_date,
                s.ScheduleTime = $schedule_time,
                s.Comments     = $comments,
                s.Status       = $status,
                s.Scac         = $scac,
                s.CarrierEmail = $carrier_email
        ")
        .param("trailer",       l.trailer.clone())
        .param("destination",   l.destination.clone())
        .param("supplier",      l.supplier.clone())
        .param("location",      l.location.clone())
        .param("ship_date",     l.ship_date.clone())
        .param("original_date", l.original_date.clone())
        .param("schedule_date", l.schedule_date.clone())
        .param("schedule_time", l.schedule_time.clone())
        .param("comments",      l.comments.clone())
        .param("status",        l.status.clone())
        .param("scac",          l.scac.clone())
        .param("carrier_email", l.carrier_email.clone()))
        .await
        .map_err(|e| format!("Failed to write schedule for {}: {e:?}", l.trailer))?;
    }

    // ── SID lines. Uploaded SIDs (with a ciscoID) go first: a hand-added SID
    //    MERGEs on id alone, so it has to find the uploaded node the way it did
    //    in the source rather than create a second one. ──
    let mut written = 0;
    let with_sid: Vec<&IoLine> = lines.iter().filter(|l| !l.sid.is_empty()).collect();
    let ordered = with_sid.iter().filter(|l| l.has_cisco_id).chain(with_sid.iter().filter(|l| !l.has_cisco_id));

    for l in ordered {
        let sid_merge = if l.has_cisco_id {
            "MERGE (sid:SID {id: $sid, ciscoID: $cisco})
             MERGE (t)-[:HAS_SID]->(sid)
             MERGE (sid)-[:BELONGS_TO]->(t)"
        } else {
            "MERGE (sid:SID {id: $sid})
             MERGE (t)-[:HAS_SID]->(sid)"
        };
        let part_merge = match (l.on_sid, l.on_trailer) {
            (true, true)  => "MERGE (sid)-[:HAS_PART]->(p:Part {number: $part, quantity: $quantity, duns: $duns})
                              MERGE (t)-[:CONTAINS_PART]->(p)",
            (true, false) => "MERGE (sid)-[:HAS_PART]->(p:Part {number: $part, quantity: $quantity, duns: $duns})",
            _             => "",
        };

        txn.run(query(&format!("MATCH (t:Trailer {{id: $trailer}}) {sid_merge} {part_merge}"))
            .param("trailer",  l.trailer.clone())
            .param("sid",      l.sid.clone())
            .param("cisco",    l.cisco_id.clone())
            .param("part",     l.part.clone())
            .param("quantity", l.quantity)
            .param("duns",     l.duns.clone()))
            .await
            .map_err(|e| format!("Failed to write SID {} on {}: {e:?}", l.sid, l.trailer))?;
        written += 1;
    }

    // ── Parts on the trailer with no SID parent ──
    for l in lines.iter().filter(|l| l.sid.is_empty() && !l.part.is_empty() && l.on_trailer) {
        txn.run(query("
            MATCH (t:Trailer {id: $trailer})
            MERGE (t)-[:CONTAINS_PART]->(p:Part {number: $part, quantity: $quantity, duns: $duns})
        ")
        .param("trailer",  l.trailer.clone())
        .param("part",     l.part.clone())
        .param("quantity", l.quantity)
        .param("duns",     l.duns.clone()))
        .await
        .map_err(|e| format!("Failed to write part {} on {}: {e:?}", l.part, l.trailer))?;
        written += 1;
    }

    Ok((seen.len(), written))
}

async fn import_history(txn: &mut Txn, rows: &[IoHistory], (label, date_key): (&str, &str))
    -> Result<usize, String>
{
    // Blank optional fields are left unset rather than written as '' — the two
    // kinds carry different fields, and readers treat absent and '' alike.
    let cypher = format!("
        MERGE (n:{label} {{trailer_id: $trailer_id, {date_key}: $event_date}})
        SET n.Status          = $status,
            n.parts           = $parts,
            n.sids            = $sids,
            n.Destination     = CASE WHEN $destination      = '' THEN null ELSE $destination      END,
            n.Supplier        = CASE WHEN $supplier         = '' THEN null ELSE $supplier         END,
            n.Location        = CASE WHEN $location         = '' THEN null ELSE $location         END,
            n.ShipDate        = CASE WHEN $ship_date        = '' THEN null ELSE $ship_date        END,
            n.OriginalDate    = CASE WHEN $original_date    = '' THEN null ELSE $original_date    END,
            n.ScheduleDate    = CASE WHEN $schedule_date    = '' THEN null ELSE $schedule_date    END,
            n.ScheduleTime    = CASE WHEN $schedule_time    = '' THEN null ELSE $schedule_time    END,
            n.Comments        = CASE WHEN $comments         = '' THEN null ELSE $comments         END,
            n.ScheduledStatus = CASE WHEN $scheduled_status = '' THEN null ELSE $scheduled_status END,
            n.Scac            = CASE WHEN $scac             = '' THEN null ELSE $scac             END,
            n.CarrierEmail    = CASE WHEN $carrier_email    = '' THEN null ELSE $carrier_email    END,
            n.recorded_by     = CASE WHEN $recorded_by      = '' THEN null ELSE $recorded_by      END,
            n.recorded_at     = CASE WHEN $recorded_at      = '' THEN null ELSE $recorded_at      END
    ");

    for h in rows {
        txn.run(query(&cypher)
            .param("trailer_id",       h.trailer_id.clone())
            .param("event_date",       h.event_date.clone())
            .param("status",           h.status.clone())
            .param("parts",            h.parts.clone())
            .param("sids",             h.sids.clone())
            .param("destination",      h.destination.clone())
            .param("supplier",         h.supplier.clone())
            .param("location",         h.location.clone())
            .param("ship_date",        h.ship_date.clone())
            .param("original_date",    h.original_date.clone())
            .param("schedule_date",    h.schedule_date.clone())
            .param("schedule_time",    h.schedule_time.clone())
            .param("comments",         h.comments.clone())
            .param("scheduled_status", h.scheduled_status.clone())
            .param("scac",             h.scac.clone())
            .param("carrier_email",    h.carrier_email.clone())
            .param("recorded_by",      h.recorded_by.clone())
            .param("recorded_at",      h.recorded_at.clone()))
            .await
            .map_err(|e| format!("Failed to write {label} {} {}: {e:?}", h.trailer_id, h.event_date))?;
    }
    Ok(rows.len())
}

async fn import_exceptions(txn: &mut Txn, rows: &[ExceptionLogEntry]) -> Result<usize, String> {
    // Same MERGE key as upload_exception, but written directly: going through
    // that route would log an "exception uploaded" audit event per row, stamped
    // with the importer and today's date.
    for e in rows {
        txn.run(query("
            MERGE (e:ExceptionLogEntry {loadNum: $loadNum, dock: $dock, trailer1: $trailer1})
            SET e.type           = $type,
                e.status         = $status,
                e.route          = $route,
                e.scac           = $scac,
                e.trailer2       = $trailer2,
                e.supplier       = $supplier,
                e.dockSequence   = $dockSequence,
                e.originalDate   = $originalDate,
                e.originalTime   = $originalTime,
                e.newDate        = $newDate,
                e.newTime        = $newTime,
                e.newEndDate     = $newEndDate,
                e.newEndTime     = $newEndTime,
                e.comment        = $comment,
                e.requestor      = $requestor,
                e.isRepower      = $isRepower,
                e.repowerLoadNum = $repowerLoadNum
        ")
        .param("loadNum",        e.loadNum.clone())
        .param("dock",           e.dock.clone())
        .param("trailer1",       e.trailer1.clone())
        .param("type",           e.r#type.clone())
        .param("status",         e.status.clone())
        .param("route",          e.route.clone())
        .param("scac",           e.scac.clone())
        .param("trailer2",       e.trailer2.clone())
        .param("supplier",       e.supplier.clone())
        .param("dockSequence",   e.dockSequence.clone())
        .param("originalDate",   e.originalDate.clone())
        .param("originalTime",   e.originalTime.clone())
        .param("newDate",        e.newDate.clone())
        .param("newTime",        e.newTime.clone())
        .param("newEndDate",     e.newEndDate.clone())
        .param("newEndTime",     e.newEndTime.clone())
        .param("comment",        e.comment.clone())
        .param("requestor",      e.requestor.clone())
        .param("isRepower",      e.isRepower)
        .param("repowerLoadNum", e.repowerLoadNum.clone()))
        .await
        .map_err(|err| format!("Failed to write exception {} / {}: {err:?}", e.loadNum, e.trailer1))?;
    }
    Ok(rows.len())
}

async fn import_all(txn: &mut Txn, data: &IoData) -> Result<IoImportResult, String> {
    let (trailers, lines) = import_active(txn, &data.active).await?;
    Ok(IoImportResult {
        trailers,
        lines,
        delivered:  import_history(txn, &data.delivered, DELIVERED).await?,
        no_shows:   import_history(txn, &data.no_shows, NO_SHOW).await?,
        exceptions: import_exceptions(txn, &data.exceptions).await?,
    })
}

#[post("/api/import_io", format = "json", data = "<data>")]
pub async fn import_io(
    data:   Json<IoData>,
    state:  &State<AppState>,
    _guard: AdminOnly,
) -> Result<Json<IoImportResult>, ApiError> {
    validate(&data).map_err(|msg| (Status::BadRequest, Json(msg)))?;

    // All or nothing: a half-imported prod graph is worse than no import.
    let mut txn = state.graph.start_txn().await
        .map_err(|e| server_error("Failed to start transaction", e))?;

    match import_all(&mut txn, &data).await {
        Ok(counts) => {
            txn.commit().await.map_err(|e| server_error("Failed to commit import", e))?;
            println!("import_io committed: {counts:?}");
            Ok(Json(counts))
        }
        Err(msg) => {
            let _ = txn.rollback().await;
            eprintln!("import_io rolled back: {msg}");
            Err((Status::InternalServerError, Json(format!("Nothing was imported. {msg}"))))
        }
    }
}
