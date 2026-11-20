use std::collections::HashMap;
use chrono::{DateTime, Duration, NaiveDate, Utc, Timelike};
use neo4rs::query;
use crate::structs::TrailerRecord;

// The OpenShift cluster this runs on isn't necessarily in US Central time,
// but the business logic (shift windows, the 22:00 day boundary, weekend
// shutdown detection) all assumes wall-clock Central time. DateTime<Utc>
// converted via with_timezone handles CST/CDT DST transitions correctly,
// unlike a fixed offset.
pub fn now_central() -> DateTime<chrono_tz::Tz> {
    Utc::now().with_timezone(&chrono_tz::US::Central)
}

pub fn get_shift(hour: u32) -> &'static str {
    match hour {
        6..=13  => "1st",
        14..=21 => "2nd",
        _       => "3rd", // 22-23 and 0-5
    }
}

pub fn get_op_date(sched_arrival: &str) -> String {
    match DateTime::parse_from_rfc3339(sched_arrival) {
        Ok(dt) => {
            let dt_utc = dt.with_timezone(&Utc);
            let mut date = dt_utc.date_naive();
            if dt_utc.hour() >= 22 {
                date = date.succ_opt().unwrap_or(date);
            }
            date.format("%Y-%m-%d").to_string()
        }
        Err(_) => "Invalid Date".to_string()
    }
}

pub fn parse_asn_arrival(eda: &str, eta: &str) -> Option<chrono::NaiveDateTime> {
    if eda.is_empty() || eta.is_empty() { return None; }
    let date = chrono::NaiveDate::parse_from_str(eda, "%Y-%m-%d").ok()?;
    let time = chrono::NaiveTime::parse_from_str(eta, "%H:%M").ok()?;
    Some(date.and_time(time))
}

pub fn parse_eta(eta: &str, window_start: chrono::NaiveDateTime) -> Option<chrono::NaiveDateTime> {
    if eta.is_empty() { return None; }
    let t = chrono::NaiveTime::parse_from_str(eta, "%H:%M").ok()?;
    
    // Try anchoring to window_start date, then +1 day, pick whichever is >= window_start
    let candidate = window_start.date().and_time(t);
    if candidate >= window_start {
        Some(candidate)
    } else {
        // Time is earlier in the day than window_start time (e.g. window starts 22:00,
        // eta is 12:15 — that's the next calendar day)
        Some(candidate + chrono::Duration::days(1))
    }
}

pub struct ShiftWindow {
    pub date1:  String,
    pub hours1: Vec<String>,
    pub date2:  String,
    pub hours2: Vec<String>,
}

pub fn get_shift_window(date: &str, hour: u32) -> ShiftWindow {
    let hour_range = |start: u32, end: u32| -> Vec<String> {
        (start..=end).map(|h| format!("{:02}", h)).collect()
    };
    let offset_date = |d: &str, days: i64| -> String {
        NaiveDate::parse_from_str(d, "%Y-%m-%d")
            .map(|nd| (nd + Duration::days(days)).format("%Y-%m-%d").to_string())
            .unwrap_or_else(|_| d.to_string())
    };

    match hour {
        6..=13 => ShiftWindow {
            date1: date.to_string(), hours1: hour_range(6, 13),
            date2: date.to_string(), hours2: vec![],
        },
        14..=21 => ShiftWindow {
            date1: date.to_string(), hours1: hour_range(14, 21),
            date2: date.to_string(), hours2: vec![],
        },
        22 | 23 => ShiftWindow {
            date1: date.to_string(),   hours1: hour_range(22, 23),
            date2: offset_date(date, 1), hours2: hour_range(0, 5),
        },
        0..=5 => ShiftWindow {
            date1: offset_date(date, -1), hours1: hour_range(22, 23),
            date2: date.to_string(),      hours2: hour_range(0, 5),
        },
        _ => ShiftWindow {
            date1: date.to_string(), hours1: vec![],
            date2: date.to_string(), hours2: vec![],
        },
    }
}

pub fn get_dock(acctor_id: &str, location: &str) -> String {
    // Mirror your getDock logic here
    format!("{}-{}", acctor_id, location)
}

pub async fn send_email(to: &str, subject: &str, body: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use lettre::{
        AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor,
        message::header::ContentType,
        transport::smtp::authentication::Credentials,
    };

    let host = std::env::var("SMTP_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = std::env::var("SMTP_PORT")
        .unwrap_or_else(|_| "25".to_string())
        .parse()
        .unwrap_or(25);
    let from = std::env::var("EMAIL_FROM").unwrap_or_else(|_| "noreply@localhost".to_string());
    let user = std::env::var("SMTP_USER").ok();
    let pass = std::env::var("SMTP_PASS").ok();

    let email = Message::builder()
        .from(from.parse()?)
        .to(to.parse()?)
        .subject(subject)
        .header(ContentType::TEXT_PLAIN)
        .body(body)?;

    let mailer = match (user, pass) {
        (Some(u), Some(p)) => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host)
            .port(port)
            .credentials(Credentials::new(u, p))
            .build(),
        _ => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host)
            .port(port)
            .build(),
    };

    mailer.send(email).await?;
    Ok(())
}

pub async fn send_sms(to: &str, body: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let account_sid = std::env::var("TWILIO_ACCOUNT_SID")?;
    let auth_token  = std::env::var("TWILIO_AUTH_TOKEN")?;
    let from        = std::env::var("TWILIO_FROM_NUMBER")?;

    let client = reqwest::Client::new();
    let url = format!(
        "https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json",
        account_sid
    );

    client
        .post(&url)
        .basic_auth(&account_sid, Some(&auth_token))
        .form(&[
            ("To",   to),
            ("From", &from),
            ("Body", body),
        ])
        .send()
        .await?;

    Ok(())
}

/// The webhook URL in the env var `webhook_var`; fails when it isn't set or is empty
fn slack_webhook_url(webhook_var: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var(webhook_var).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Slack webhook is not configured: {webhook_var}"),
        )
    })?;

    if url.trim().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Slack webhook is empty: {webhook_var}"),
        )
        .into());
    }

    Ok(url)
}

/// Posts `text` to a Slack incoming webhook. `webhook_var` names the env var with
/// that channel's webhook URL; when it isn't set this fails
pub async fn post_slack_message(
    webhook_var: &str,
    text: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = slack_webhook_url(webhook_var)?;

    reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({ "text": text }))
        .send()
        .await?
        .error_for_status()?;

    Ok(())
}

// Slack's limits for one table block: 100 rows including the header, and 10,000
// characters across all cells (some margin kept). Larger tables are split over
// several messages.
const SLACK_TABLE_MAX_ROWS: usize = 99;
const SLACK_TABLE_MAX_CHARS: usize = 9_000;

/// Posts `rows` under `headers` to a Slack incoming webhook as a table block,
/// headed by `title` in bold. Split over several messages, "(1 of 2)" and so on,
/// when Slack's per-table limits require it. Columns listed in `right_aligned`
/// (numbers) are aligned right. Empty cells show as "-".
///
/// Under the table, a "Copy for Excel" code block holds the same rows separated
/// by tabs: copying Slack's table block itself pastes each row into one cell,
/// while Excel splits tab-separated text into cells.
///
/// If Slack rejects the blocks (HTTP 400), the same rows go out as a fixed-width
/// text table plus the tab-separated copy, so the message still arrives.
pub async fn post_slack_table(
    webhook_var: &str,
    title: &str,
    headers: &[&str],
    rows: &[Vec<String>],
    right_aligned: &[usize],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = slack_webhook_url(webhook_var)?;
    let client = reqwest::Client::new();
    let headers: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    let chunks = slack_table_chunks(&headers, rows);
    let parts = chunks.len();
    for (n, chunk) in chunks.into_iter().enumerate() {
        let heading = if parts > 1 { format!("{title} ({} of {parts})", n + 1) } else { title.to_string() };

        let response = client
            .post(&url)
            .json(&slack_table_payload(&heading, &headers, chunk, right_aligned))
            .send()
            .await?;

        if response.status() == reqwest::StatusCode::BAD_REQUEST {
            let reason = response.text().await.unwrap_or_default();
            eprintln!("Slack refused the table block ({reason}); sending it as a text table instead");
            let mut text = format!("*{heading}*\n```\n{}\n```", text_table(&headers, chunk, right_aligned));
            if !chunk.is_empty() {
                text.push_str(&format!("\nCopy for Excel:\n```\n{}\n```", tsv_table(&headers, chunk)));
            }
            client
                .post(&url)
                .json(&serde_json::json!({ "text": text }))
                .send()
                .await?
                .error_for_status()?;
        } else {
            response.error_for_status()?;
        }
    }

    Ok(())
}

fn cell_text(text: &str) -> &str {
    if text.trim().is_empty() { "-" } else { text }
}

/// Splits `rows` into runs that each fit one table block together with the
/// header row. Always returns at least one run (empty when there are no rows).
fn slack_table_chunks<'a>(headers: &[String], rows: &'a [Vec<String>]) -> Vec<&'a [Vec<String>]> {
    let chars = |row: &[String]| row.iter().map(|c| cell_text(c).chars().count()).sum::<usize>();
    let header_chars = chars(headers);

    let mut chunks = Vec::new();
    let mut start = 0;
    let mut used = header_chars;
    for (i, row) in rows.iter().enumerate() {
        let len = chars(row);
        if i > start && (i - start == SLACK_TABLE_MAX_ROWS || used + len > SLACK_TABLE_MAX_CHARS) {
            chunks.push(&rows[start..i]);
            start = i;
            used = header_chars;
        }
        used += len;
    }
    chunks.push(&rows[start..]);
    chunks
}

/// The webhook body for one table message. `text` is what notifications show.
fn slack_table_payload(
    heading: &str,
    headers: &[String],
    rows: &[Vec<String>],
    right_aligned: &[usize],
) -> serde_json::Value {
    let mut blocks = vec![serde_json::json!({
        "type": "section",
        "text": { "type": "mrkdwn", "text": format!("*{heading}*") },
    })];

    if !rows.is_empty() {
        let table_rows: Vec<Vec<serde_json::Value>> = std::iter::once(headers)
            .chain(rows.iter().map(Vec::as_slice))
            .map(|row| {
                row.iter()
                    .map(|c| serde_json::json!({ "type": "raw_text", "text": cell_text(c) }))
                    .collect()
            })
            .collect();

        // null leaves a column at the default (left aligned)
        let column_settings: Vec<serde_json::Value> = (0..headers.len())
            .map(|i| if right_aligned.contains(&i) { serde_json::json!({ "align": "right" }) } else { serde_json::Value::Null })
            .collect();

        blocks.push(serde_json::json!({
            "type": "table",
            "column_settings": column_settings,
            "rows": table_rows,
        }));

        blocks.push(serde_json::json!({
            "type": "context",
            "elements": [{ "type": "mrkdwn", "text": "Copy for Excel:" }],
        }));
        blocks.push(serde_json::json!({
            "type": "rich_text",
            "elements": [{
                "type": "rich_text_preformatted",
                "elements": [{ "type": "text", "text": tsv_table(headers, rows) }],
            }],
        }));
    }

    serde_json::json!({ "text": heading, "blocks": blocks })
}

/// The rows as tab-separated lines, header first, which Excel pastes into
/// separate cells. Empty cells stay empty; tabs and line breaks inside a value
/// become spaces so they can't shift the columns.
fn tsv_table(headers: &[String], rows: &[Vec<String>]) -> String {
    std::iter::once(headers)
        .chain(rows.iter().map(Vec::as_slice))
        .map(|row| {
            row.iter()
                .map(|c| c.trim().replace(['\t', '\r', '\n'], " "))
                .collect::<Vec<_>>()
                .join("\t")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The rows as fixed-width text with a rule under the header, for a code block
fn text_table(headers: &[String], rows: &[Vec<String>], right_aligned: &[usize]) -> String {
    let all: Vec<&[String]> = std::iter::once(headers).chain(rows.iter().map(Vec::as_slice)).collect();

    let mut widths = vec![0usize; headers.len()];
    for row in &all {
        for (i, c) in row.iter().enumerate().take(widths.len()) {
            widths[i] = widths[i].max(cell_text(c).chars().count());
        }
    }

    let line = |row: &[String]| {
        row.iter()
            .enumerate()
            .take(widths.len())
            .map(|(i, c)| {
                let w = widths[i];
                if right_aligned.contains(&i) { format!("{:>w$}", cell_text(c)) } else { format!("{:<w$}", cell_text(c)) }
            })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };

    let mut lines = vec![line(headers)];
    lines.push(widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  "));
    lines.extend(rows.iter().map(|row| line(row)));
    lines.join("\n")
}

pub fn get_requested_fields(trailer: &TrailerRecord) -> Vec<&'static str> {
    let mut fields = vec![];
    if !trailer.hour.is_empty()              { fields.push("hour") }
    if !trailer.dockCode.is_empty()          { fields.push("dockCode") }
    if !trailer.scac.is_empty()              { fields.push("scac") }
    if !trailer.trailer1.is_empty()          { fields.push("trailer1") }
    if !trailer.trailer2.is_empty()          { fields.push("trailer2") }
    if !trailer.adjustedStartTime.is_empty() { fields.push("adjustedStartTime") }
    if !trailer.scheduleEndDate.is_empty()   { fields.push("scheduleEndDate") }
    if !trailer.scheduleEndTime.is_empty()   { fields.push("scheduleEndTime") }
    if !trailer.gateArrivalTime.is_empty()   { fields.push("gateArrivalTime") }
    if !trailer.actualStartTime.is_empty()   { fields.push("actualStartTime") }
    if !trailer.actualEndTime.is_empty()     { fields.push("actualEndTime") }
    if !trailer.statusOX.is_empty()          { fields.push("statusOX") }
    if !trailer.ryderComments.is_empty()     { fields.push("ryderComments") }
    if trailer.gmComments.is_some()          { fields.push("gmComments") }
    fields
}

/// A TrailerRecord field by name, for the fields update_live_trailer can write
/// (permissions::editable_fields).
pub fn trailer_field(t: &TrailerRecord, field: &str) -> String {
    match field {
        "hour"              => t.hour.clone(),
        "dockCode"          => t.dockCode.clone(),
        "scac"              => t.scac.clone(),
        "trailer1"          => t.trailer1.clone(),
        "trailer2"          => t.trailer2.clone(),
        "adjustedStartTime" => t.adjustedStartTime.clone(),
        "scheduleEndDate"   => t.scheduleEndDate.clone(),
        "scheduleEndTime"   => t.scheduleEndTime.clone(),
        "gateArrivalTime"   => t.gateArrivalTime.clone(),
        "gateArrivalDate"   => t.gateArrivalDate.clone(),
        "doorArrivalTime"   => t.doorArrivalTime.clone(),
        "doorArrivalDate"   => t.doorArrivalDate.clone(),
        "door"              => t.door.clone(),
        "actualStartTime"   => t.actualStartTime.clone(),
        "actualStartDate"   => t.actualStartDate.clone(),
        "actualEndTime"     => t.actualEndTime.clone(),
        "actualEndDate"     => t.actualEndDate.clone(),
        "statusOX"          => t.statusOX.clone(),
        "stat"              => t.stat.clone(),
        "receivedDate"      => t.receivedDate.clone(),
        "receivedTime"      => t.receivedTime.clone(),
        "ryderComments"     => t.ryderComments.clone(),
        "gmComments"        => t.gmComments.clone().unwrap_or_default(),
        "dockComments"      => t.dockComments.clone(),
        "loadComments"      => t.loadComments.clone(),
        _                   => String::new(),
    }
}

/// SETs exactly `fields` — the ones this request changed and the caller may write
/// — so fields the caller didn't touch keep whatever another user saved meanwhile.
/// `label` is resolved from the node and `fields` come from
/// permissions::editable_fields; neither comes from request data.
pub fn build_update_query(trailer: &TrailerRecord, label: &str, fields: &[&str]) -> neo4rs::Query {
    let sets = fields.iter().map(|f| format!("t.{f} = ${f}")).collect::<Vec<_>>().join(", ");
    let mut q = query(&format!("MATCH (t:{label} {{uuid: $uuid}}) SET {sets} RETURN t"))
        .param("uuid", trailer.uuid.clone());
    for f in fields {
        q = q.param(f, trailer_field(trailer, f));
    }
    q
}

pub fn get_event_type(field: &str) -> &'static str {
    match field {
        "hour" | "dockCode" | "scac" | "trailer1" | "trailer2" |
        "adjustedStartTime" | "scheduleEndDate" | "scheduleEndTime" |
        "gateArrivalTime" | "actualStartTime" | "actualEndTime" |
        "gateArrivalDate" | "doorArrivalDate" | "actualStartDate" | "actualEndDate" |
        "statusOX" | "stat" | "ryderComments" | "gmComments" |
        "door" | "doorArrivalTime" | "dockComments" | "loadComments" |
        "LiveAdd" | "StagedAdd" => "Trailer Updates",
        "shift_rolled"                          => "Shift Roll",
        "hot_part_created" | "hot_part_closed"  => "Hot Parts",
        "exception_uploaded" | "dycomm_uploaded" => "Uploads",
        _                                       => "Other",
    }
}

/// Trailers with a DeliveredTrailer record in the last month, and the cutoff date.
/// The GMAP report and the ASN keep listing containers after they arrive, so the
/// IO builders skip these rather than rebuild them. delivery_date is stored as
/// YYYY-MM-DD, so it compares correctly as text.
pub async fn recently_delivered(graph: &neo4rs::Graph) -> (std::collections::HashSet<String>, String) {
    let cutoff = (Utc::now() - Duration::days(5))
        .format("%Y-%m-%d")
        .to_string();

    let delivered_q = query("
        MATCH (d:DeliveredTrailer)
        WHERE d.delivery_date >= $cutoff
        RETURN collect(DISTINCT d.trailer_id) AS trailers
    ")
    .param("cutoff", cutoff.clone());

    let delivered = match graph.execute(delivered_q).await {
        Ok(mut result) => match result.next().await {
            Ok(Some(row)) => row
                .get::<Vec<String>>("trailers")
                .unwrap_or_default()
                .into_iter()
                .collect(),
            _ => std::collections::HashSet::new(),
        },
        Err(e) => {
            eprintln!("Failed to load recently delivered trailers: {:?}", e);
            std::collections::HashSet::new()
        }
    };
    (delivered, cutoff)
}

/// Indexes behind the hot lookups: the per-row MERGEs in the IO builders and the
/// part joins between the ASN, ASL and route reports. Run at startup so every
/// environment has them. IF NOT EXISTS makes this a no-op where an equivalent
/// index already exists, including ones created by hand under another name.
pub async fn ensure_indexes(graph: &neo4rs::Graph) {
    let indexes = [
        "CREATE INDEX trailer_id      IF NOT EXISTS FOR (n:Trailer)   ON (n.id)",
        "CREATE INDEX sid_id          IF NOT EXISTS FOR (n:SID)       ON (n.id, n.ciscoID)",
        "CREATE INDEX part_asn_part   IF NOT EXISTS FOR (n:PartASN)   ON (n.part)",
        "CREATE INDEX part_asl_part   IF NOT EXISTS FOR (n:PartASL)   ON (n.part)",
        "CREATE INDEX part_route_part IF NOT EXISTS FOR (n:PartRoute) ON (n.part)",
        "CREATE INDEX part_transit_part IF NOT EXISTS FOR (n:PartTransit) ON (n.part)",
    ];
    for cypher in indexes {
        // A failed index only costs speed, so log it rather than refuse to start.
        if let Err(e) = graph.run(query(cypher)).await {
            eprintln!("Failed to ensure index ({cypher}): {e:?}");
        }
    }
}

// A user's editRefs: editRef -> trailer uuid. One ref per trailer per user, so
// every browser and tab the user has open gets the same ref and a reload in one
// can't invalidate another. Past this size the map is rebuilt from scratch —
// the refs then belong to trailers long rolled off the sheets.
pub const MAX_EDIT_REFS_PER_USER: usize = 5000;

/** The user's ref for `uuid`, reusing the one already issued if there is one. */
pub fn edit_ref_for(user_map: &mut HashMap<String, String>, uuid: &str) -> String {
    if let Some((edit_ref, _)) = user_map.iter().find(|(_, u)| u.as_str() == uuid) {
        return edit_ref.clone();
    }
    if user_map.len() >= MAX_EDIT_REFS_PER_USER {
        user_map.clear();
    }
    let edit_ref = uuid::Uuid::new_v4().to_string();
    user_map.insert(edit_ref.clone(), uuid.to_string());
    edit_ref
}

/// Deck of the route's lowest-DOH part — the part get_first_supplier names.
/// Routes match on their first 6 characters, as there; empty when nothing on
/// the route has a DOH or the route is too short to match safely.
pub async fn lowest_doh_deck(graph: &neo4rs::Graph, route: &str) -> String {
    let prefix: String = route.trim().to_lowercase().chars().take(6).collect();
    if prefix.chars().count() < 6 {
        return String::new();
    }

    let q = query("
        MATCH (r:PartRoute) WHERE toLower(r.route) STARTS WITH $prefix
        MATCH (a:PartASL {part: r.part})
        WHERE a.doh IS NOT NULL
        RETURN a.deck AS deck
        ORDER BY a.doh ASC, r.part ASC
        LIMIT 1
    ").param("prefix", prefix);

    match graph.execute(q).await {
        Ok(mut result) => match result.next().await {
            Ok(Some(row)) => row.get::<String>("deck").unwrap_or_default(),
            _ => String::new(),
        },
        Err(e) => {
            eprintln!("lowest_doh_deck error: {e}");
            String::new()
        }
    }
}

/// A LiveTrailer/StagedTrailer node as the row the boards show, with no editRef
/// (a trailer_update recipient keeps its own).
pub fn trailer_from_node(node: &neo4rs::Node) -> TrailerRecord {
    let s = |k: &str| node.get::<String>(k).unwrap_or_default();
    TrailerRecord {
        uuid:              s("uuid"),
        origin:            s("origin"),
        hour:              s("hour"),
        dateShift:         s("dateShift"),
        lmsAccent:         s("lmsAccent"),
        dockCode:          s("dockCode"),
        acaType:           s("acaType"),
        status:            s("status"),
        routeId:           s("routeId"),
        scac:              s("scac"),
        trailer1:          s("trailer1"),
        trailer2:          s("trailer2"),
        firstSupplier:     s("firstSupplier"),
        dockStopSequence:  s("dockStopSequence"),
        planStartDate:     s("planStartDate"),
        planStartTime:     s("planStartTime"),
        scheduleStartDate: s("scheduleStartDate"),
        adjustedStartTime: s("adjustedStartTime"),
        scheduleEndDate:   s("scheduleEndDate"),
        scheduleEndTime:   s("scheduleEndTime"),
        gateArrivalTime:   s("gateArrivalTime"),
        actualStartTime:   s("actualStartTime"),
        actualEndTime:     s("actualEndTime"),
        gateArrivalDate:   s("gateArrivalDate"),
        doorArrivalDate:   s("doorArrivalDate"),
        actualStartDate:   s("actualStartDate"),
        actualEndDate:     s("actualEndDate"),
        receivedDate:      s("receivedDate"),
        receivedTime:      s("receivedTime"),
        statusOX:          s("statusOX"),
        stat:              s("stat"),
        loadComments:      s("loadComments"),
        dockComments:      s("dockComments"),
        ryderComments:     s("ryderComments"),
        gmComments:        Some(s("gmComments")),
        lowestDoh:         Some(s("lowestDoh")),
        editRef:           String::new(),
        door:              s("door"),
        doorArrivalTime:   s("doorArrivalTime"),
    }
}

#[cfg(test)]
mod slack_table_tests {
    use super::*;

    fn headers() -> Vec<String> {
        ["#", "Load", "DoH"].iter().map(|h| h.to_string()).collect()
    }

    fn row(n: usize, load: &str, doh: &str) -> Vec<String> {
        vec![n.to_string(), load.to_string(), doh.to_string()]
    }

    #[test]
    fn no_rows_is_one_empty_message_with_only_the_title() {
        let rows: Vec<Vec<String>> = vec![];
        let chunks = slack_table_chunks(&headers(), &rows);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_empty());

        let payload = slack_table_payload("Rescheduled Trailers (0)", &headers(), chunks[0], &[]);
        assert_eq!(payload["blocks"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn splits_at_99_rows_under_the_header() {
        let rows: Vec<Vec<String>> = (1..=200).map(|n| row(n, "L", "1")).collect();
        let sizes: Vec<usize> = slack_table_chunks(&headers(), &rows).iter().map(|c| c.len()).collect();
        assert_eq!(sizes, vec![99, 99, 2]);
    }

    #[test]
    fn splits_before_the_character_limit() {
        let long = "x".repeat(1_000);
        let rows: Vec<Vec<String>> = (1..=20).map(|n| row(n, &long, "1")).collect();
        let chunks = slack_table_chunks(&headers(), &rows);
        assert!(chunks.len() > 1);
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), 20);
        for chunk in chunks {
            let chars: usize = std::iter::once(headers().as_slice())
                .chain(chunk.iter().map(Vec::as_slice))
                .flat_map(|r| r.iter().map(|c| cell_text(c).chars().count()))
                .sum();
            assert!(chars <= SLACK_TABLE_MAX_CHARS);
        }
    }

    #[test]
    fn payload_has_header_row_dashes_for_blanks_and_right_aligned_columns() {
        let rows = vec![row(1, "12345", ""), row(2, "678", "0.5")];
        let p = slack_table_payload("Rescheduled Trailers (2)", &headers(), &rows, &[0, 2]);

        assert_eq!(p["text"], "Rescheduled Trailers (2)");
        assert_eq!(p["blocks"][0]["text"]["text"], "*Rescheduled Trailers (2)*");

        let table = &p["blocks"][1];
        assert_eq!(table["type"], "table");
        assert_eq!(table["rows"].as_array().unwrap().len(), 3);
        assert_eq!(table["rows"][0][1]["text"], "Load");
        assert_eq!(table["rows"][1][2]["text"], "-");
        assert_eq!(table["rows"][2][2]["type"], "raw_text");
        assert_eq!(table["column_settings"][0]["align"], "right");
        assert!(table["column_settings"][1].is_null());
        assert_eq!(table["column_settings"][2]["align"], "right");

        assert_eq!(p["blocks"][2]["elements"][0]["text"], "Copy for Excel:");
        let copy = &p["blocks"][3]["elements"][0];
        assert_eq!(copy["type"], "rich_text_preformatted");
        assert_eq!(copy["elements"][0]["text"], "#\tLoad\tDoH\n1\t12345\t\n2\t678\t0.5");
    }

    #[test]
    fn tsv_keeps_blanks_empty_and_strips_tabs_inside_values() {
        let rows = vec![row(1, "a\tb", ""), row(2, " c\nd ", "1")];
        assert_eq!(tsv_table(&headers(), &rows), "#\tLoad\tDoH\n1\ta b\t\n2\tc d\t1");
    }

    #[test]
    fn text_table_lines_up_columns() {
        let rows = vec![row(1, "12345", ""), row(10, "678", "0.5")];
        let t = text_table(&headers(), &rows, &[0, 2]);
        assert_eq!(t, " #  Load   DoH\n--  -----  ---\n 1  12345    -\n10  678    0.5");
    }
}
