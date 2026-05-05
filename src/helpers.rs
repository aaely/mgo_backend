use std::collections::HashMap;
use chrono::{DateTime, Duration, NaiveDate, Utc, Timelike, Datelike};

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