use std::collections::HashMap;
use chrono::{DateTime, Duration, NaiveDate, Utc, Timelike, Datelike};
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

/// Posts `text` to a Slack incoming webhook. `webhook_var` names the env var with
/// that channel's webhook URL; when it isn't set this falls back to
/// SLACK_WEBHOOK_URL, so a message doesn't go quiet just because its own channel
/// hasn't been set up yet. Slack refusing the post (e.g. a revoked webhook) is
/// an error, not a quiet success.
pub async fn post_slack_message(webhook_var: &str, text: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = std::env::var(webhook_var).or_else(|_| std::env::var("SLACK_WEBHOOK_URL"))?;
    reqwest::Client::new()
        .post(&url)
        .json(&serde_json::json!({ "text": text }))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
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
    let cutoff = (Utc::now() - Duration::days(30))
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
