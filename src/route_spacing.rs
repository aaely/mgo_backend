//! Route spacing: other deliveries on a route close to a proposed delivery time.
//! A route's deliveries should be at least 2 hours apart, so the Exception Log
//! and DY Log warn when a new time lands near another one. Advisory only —
//! nothing is refused on it.
//!
//! A route's deliveries are its scheduled LMS loads, plus loads moved by an
//! Exception Log or DY Log entry. A load with an entry in either log is counted
//! at the entry's time, not its LMS one, so it isn't counted twice.
//!
//! E dock is scheduled entirely by hand through the two logs, so for an E
//! delivery its LMS times aren't real and only the log entries count.

use crate::auth::AuthenticatedUser;
use crate::role::Role;
use crate::route_blackouts::route_key;
use crate::structs::AppState;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};
use neo4rs::query;
use rocket::{get, http::Status, serde::json::Json, State};
use serde::Serialize;

type ApiError = (Status, Json<String>);

#[derive(Debug, Serialize, PartialEq)]
pub struct NearbyDelivery {
    /// "LMS", "Exception Log" or "DY Log"
    pub source:       String,
    #[serde(rename = "loadNum")]
    pub load_num:     String,
    pub date:         String,
    pub time:         String,
    #[serde(rename = "minutesApart")]
    pub minutes_apart: i64,
}

/// 'YYYY-MM-DD' plus 'H:MM', 'HH:MM' or 'HH:MM:SS'.
fn parse_at(date: &str, time: &str) -> Option<NaiveDateTime> {
    let date = NaiveDate::parse_from_str(date.trim(), "%Y-%m-%d").ok()?;
    let mut parts = time.trim().split(':');
    let h: u32 = parts.next()?.trim().parse().ok()?;
    let m: u32 = parts.next()?.get(..2)?.parse().ok()?;
    Some(date.and_time(NaiveTime::from_hms_opt(h, m, 0)?))
}

/// Deliveries less than `window` minutes either side of `at`, closest first.
fn within(at: NaiveDateTime, window: i64, rows: Vec<(String, String, String, String)>) -> Vec<NearbyDelivery> {
    let mut near: Vec<NearbyDelivery> = rows.into_iter()
        .filter_map(|(source, load_num, date, time)| {
            let other = parse_at(&date, &time)?;
            let minutes_apart = (other - at).num_minutes().abs();
            (minutes_apart < window).then_some(NearbyDelivery { source, load_num, date, time, minutes_apart })
        })
        .collect();
    near.sort_by(|a, b| a.minutes_apart.cmp(&b.minutes_apart).then(a.load_num.cmp(&b.load_num)));
    near
}

/// Other deliveries on `route` within `hours` (default 2) of `date` `time`.
/// `load` and `repower` are the loads being scheduled, left out of the check;
/// `dock` is where it's delivering (E skips the LMS loads).
#[get("/api/route_deliveries_near?<route>&<date>&<time>&<load>&<repower>&<dock>&<hours>")]
pub async fn route_deliveries_near(
    route:   &str,
    date:    &str,
    time:    &str,
    load:    Option<&str>,
    repower: Option<&str>,
    dock:    Option<&str>,
    hours:   Option<i64>,
    state:   &State<AppState>,
    _user:   AuthenticatedUser,
    role:    Role,
) -> Result<Json<Vec<NearbyDelivery>>, ApiError> {
    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err((Status::Forbidden, Json("Forbidden".into())));
    }
    let route = route_key(route);
    let Some(at) = parse_at(date, time) else {
        return Err((Status::BadRequest, Json(format!("Can't read the delivery time '{date} {time}'"))));
    };
    if route.is_empty() {
        return Ok(Json(Vec::new()));
    }
    let window = hours.unwrap_or(2).clamp(1, 24) * 60;

    // Only the days the window can reach, so the comparison stays small
    let days: Vec<String> = [-1, 0, 1].iter()
        .map(|d| (at.date() + Duration::days(*d)).format("%Y-%m-%d").to_string())
        .collect();
    let exclude: Vec<String> = [load, repower].into_iter().flatten()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    let with_lms = !dock.is_some_and(|d| d.trim().eq_ignore_ascii_case("E"));

    let q = query(r#"
        MATCH (l:LMSRecord)
        WHERE $with_lms
          AND toUpper(trim(l.route_id)) = $route
          AND substring(l.schedule_arrival_time, 0, 10) IN $days
          AND NOT l.load_no IN $exclude
        OPTIONAL MATCH (x:ExceptionLogEntry {loadNum: l.load_no})
        OPTIONAL MATCH (y:DyCommLogEntry {loadNum: l.load_no})
        WITH l, x, y WHERE x IS NULL AND y IS NULL
        RETURN 'LMS' AS source, l.load_no AS loadNum,
               substring(l.schedule_arrival_time, 0, 10) AS date,
               substring(l.schedule_arrival_time, 11, 5) AS time
        UNION ALL
        MATCH (e:ExceptionLogEntry)
        WHERE toUpper(trim(e.route)) = $route AND e.newDate IN $days AND NOT e.loadNum IN $exclude
        RETURN 'Exception Log' AS source, e.loadNum AS loadNum, e.newDate AS date, e.newTime AS time
        UNION ALL
        MATCH (d:DyCommLogEntry)
        WHERE toUpper(trim(d.route)) = $route AND d.deliveryDate IN $days AND NOT d.loadNum IN $exclude
        RETURN 'DY Log' AS source, d.loadNum AS loadNum, d.deliveryDate AS date, d.deliveryTime AS time
    "#)
    .param("route", route.clone())
    .param("days", days)
    .param("exclude", exclude)
    .param("with_lms", with_lms);

    let mut result = state.graph.execute(q).await.map_err(|e| {
        eprintln!("route_deliveries_near error for {route}: {e:?}");
        (Status::InternalServerError, Json("Failed to read the route's deliveries".to_string()))
    })?;

    let mut rows = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        rows.push((
            row.get::<String>("source").unwrap_or_default(),
            row.get::<String>("loadNum").unwrap_or_default(),
            row.get::<String>("date").unwrap_or_default(),
            row.get::<String>("time").unwrap_or_default(),
        ));
    }
    Ok(Json(within(at, window, rows)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(load: &str, date: &str, time: &str) -> (String, String, String, String) {
        ("LMS".into(), load.into(), date.into(), time.into())
    }

    #[test]
    fn parses_the_time_formats_in_use() {
        let at = |d: &str, t: &str| parse_at(d, t).map(|x| x.format("%Y-%m-%d %H:%M").to_string());
        assert_eq!(at("2026-10-19", "07:05"),    Some("2026-10-19 07:05".into()));
        assert_eq!(at("2026-10-19", "7:05"),     Some("2026-10-19 07:05".into()));
        assert_eq!(at("2026-10-19", "07:05:00"), Some("2026-10-19 07:05".into()));
        assert_eq!(at("2026-10-19", ""),         None);
        assert_eq!(at("10/19/2026", "07:05"),    None);
    }

    #[test]
    fn keeps_only_deliveries_under_the_window_either_side() {
        let at = parse_at("2026-10-19", "10:00").unwrap();
        let near = within(at, 120, vec![
            row("A", "2026-10-19", "08:01"), // 119 min before
            row("B", "2026-10-19", "08:00"), // exactly 2 hours: allowed
            row("C", "2026-10-19", "11:30"), // 90 min after
            row("D", "2026-10-19", "12:00"), // exactly 2 hours after: allowed
            row("E", "2026-10-19", "bad"),   // unreadable: skipped
        ]);
        let loads: Vec<&str> = near.iter().map(|n| n.load_num.as_str()).collect();
        assert_eq!(loads, vec!["C", "A"]);
        assert_eq!(near[0].minutes_apart, 90);
    }

    #[test]
    fn crosses_midnight() {
        let at = parse_at("2026-10-19", "23:30").unwrap();
        let near = within(at, 120, vec![row("A", "2026-10-20", "00:45")]);
        assert_eq!(near.len(), 1);
        assert_eq!(near[0].minutes_apart, 75);
    }
}
