use std::collections::HashMap;
use chrono::{DateTime, Duration, NaiveDate, Utc, Timelike, Datelike};
use neo4rs::query;
use crate::structs::{TrailerRecord, get_allowed_fields};

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
    if trailer.lateComments.is_some()        { fields.push("lateComments") }
    fields
}

pub fn build_update_query(role: &str, trailer: &TrailerRecord) -> neo4rs::Query {
    let allowed = get_allowed_fields(role).unwrap_or_default();
    let is_admin = allowed.contains(&"*");

    let mut sets = vec![];
    if is_admin || allowed.contains(&"hour")              { sets.push("t.hour = $hour") }
    if is_admin || allowed.contains(&"dockCode")          { sets.push("t.dockCode = $dockCode") }
    if is_admin || allowed.contains(&"scac")              { sets.push("t.scac = $scac") }
    if is_admin || allowed.contains(&"trailer1")          { sets.push("t.trailer1 = $trailer1") }
    if is_admin || allowed.contains(&"trailer2")          { sets.push("t.trailer2 = $trailer2") }
    if is_admin || allowed.contains(&"adjustedStartTime") { sets.push("t.adjustedStartTime = $adjustedStartTime") }
    if is_admin || allowed.contains(&"scheduleEndDate")   { sets.push("t.scheduleEndDate = $scheduleEndDate") }
    if is_admin || allowed.contains(&"scheduleEndTime")   { sets.push("t.scheduleEndTime = $scheduleEndTime") }
    if is_admin || allowed.contains(&"gateArrivalTime")   { sets.push("t.gateArrivalTime = $gateArrivalTime") }
    if is_admin || allowed.contains(&"actualStartTime")   { sets.push("t.actualStartTime = $actualStartTime") }
    if is_admin || allowed.contains(&"actualEndTime")     { sets.push("t.actualEndTime = $actualEndTime") }
    if is_admin || allowed.contains(&"statusOX")          { sets.push("t.statusOX = $statusOX") }
    if is_admin || allowed.contains(&"ryderComments")     { sets.push("t.ryderComments = $ryderComments") }
    if is_admin || allowed.contains(&"gmComments")        { sets.push("t.gmComments = $gmComments") }
    if is_admin || allowed.contains(&"lateComments")      { sets.push("t.lateComments = $lateComments") }
    if is_admin || allowed.contains(&"door")              { sets.push("t.door = $door") }
    if is_admin || allowed.contains(&"doorArrivalTime")   { sets.push("t.doorArrivalTime = $doorArrivalTime") }

    let cypher = format!(
        "MATCH (t:LiveTrailer {{uuid: $uuid}}) SET {} RETURN t",
        sets.join(", ")
    );

    query(&cypher)
        .param("uuid",              trailer.uuid.clone())
        .param("hour",              trailer.hour.clone())
        .param("dockCode",          trailer.dockCode.clone())
        .param("scac",              trailer.scac.clone())
        .param("trailer1",          trailer.trailer1.clone())
        .param("trailer2",          trailer.trailer2.clone())
        .param("adjustedStartTime", trailer.adjustedStartTime.clone())
        .param("scheduleEndDate",   trailer.scheduleEndDate.clone())
        .param("scheduleEndTime",   trailer.scheduleEndTime.clone())
        .param("gateArrivalTime",   trailer.gateArrivalTime.clone())
        .param("actualStartTime",   trailer.actualStartTime.clone())
        .param("actualEndTime",     trailer.actualEndTime.clone())
        .param("statusOX",          trailer.statusOX.clone())
        .param("ryderComments",     trailer.ryderComments.clone())
        .param("gmComments",        trailer.gmComments.clone().unwrap_or_default())
        .param("lateComments",      trailer.lateComments.clone().unwrap_or_default())
        .param("door",              trailer.door.clone())
        .param("doorArrivalTime",   trailer.doorArrivalTime.clone())
}

pub fn get_event_type(field: &str) -> &'static str {
    match field {
        "hour" | "dockCode" | "scac" | "trailer1" | "trailer2" |
        "adjustedStartTime" | "scheduleEndDate" | "scheduleEndTime" |
        "gateArrivalTime" | "actualStartTime" | "actualEndTime" |
        "statusOX" | "ryderComments" | "gmComments" | "lateComments" |
        "door" | "doorArrivalTime" | "LiveAdd" => "Trailer Updates",
        "shift_rolled"                          => "Shift Roll",
        "hot_part_created" | "hot_part_closed"  => "Hot Parts",
        "exception_uploaded" | "dycomm_uploaded" => "Uploads",
        _                                       => "Other",
    }
}

pub fn check_fields(role: &str, fields: &[&str]) -> Result<(), Vec<String>> {
    let allowed = get_allowed_fields(role).unwrap_or_default();
    if allowed.contains(&"*") {
        return Ok(());
    }
    let denied: Vec<String> = fields.iter()
        .filter(|f| !allowed.contains(f))
        .map(|f| f.to_string())
        .collect();
    if denied.is_empty() { Ok(()) } else { Err(denied) }
}