//! Who may do what on the Live Sheet and Next Shift — the one table both the
//! server and the screens use. Actions map to the trailer fields they write;
//! update_live_trailer refuses a change to any field the caller's role has no
//! action for, and /api/my_permissions tells the screens which buttons to enable.
//!
//! Roles not listed here (e.g. supervisor, floater, read) can view both sheets
//! but change nothing.

use crate::auth::AuthenticatedUser;
use rocket::{get, serde::json::Json};
use serde::Serialize;
use std::collections::HashSet;

/// Action -> the TrailerRecord fields it writes. addOn and rollShift aren't
/// field edits; their routes check them directly.
pub const ACTIONS: &[(&str, &str, &[&str])] = &[
    // (action, what the screens call it, fields)
    ("gate",          "Arrived (gate)",                    &["gateArrivalTime", "gateArrivalDate"]),
    ("door",          "Door arrival / Set Door",           &["door", "doorArrivalTime", "doorArrivalDate"]),
    ("unload",        "Unload / Empty",                    &["actualStartTime", "actualStartDate", "actualEndTime", "actualEndDate"]),
    ("trailer",       "Set Trailer",                       &["trailer1", "trailer2"]),
    ("status",        "Status OX / Confirm Late / Not Late", &["statusOX"]),
    ("stat",          "Stat toggle",                       &["stat"]),
    ("ryderComments", "Ryder Comments",                    &["ryderComments"]),
    ("gmComments",    "GM Comments",                       &["gmComments"]),
    ("dockComments",  "Dock Comments",                     &["dockComments"]),
    ("loadComments",  "Load Comments",                     &["loadComments"]),
    ("addOn",         "Add-on (+)",                        &[]),
    ("rollShift",     "Roll Shift",                        &[]),
];

/// Schedule fields with no action in the table: only admin may change them.
const ADMIN_ONLY_FIELDS: &[&str] = &[
    "hour", "dockCode", "scac", "adjustedStartTime", "scheduleEndDate", "scheduleEndTime",
];

/// Every field update_live_trailer can write.
pub fn editable_fields() -> Vec<&'static str> {
    ACTIONS.iter().flat_map(|(_, _, f)| f.iter().copied())
        .chain(ADMIN_ONLY_FIELDS.iter().copied())
        .collect()
}

/// Role -> the actions it may take. admin may do everything.
fn role_actions(role: &str) -> &'static [&'static str] {
    match role {
        "manager"   => &["trailer", "status", "stat", "ryderComments", "gmComments",
                         "dockComments", "loadComments", "addOn", "rollShift"],
        "dock"      => &["door", "unload", "dockComments", "addOn"],
        "mfu"       => &["trailer", "ryderComments", "loadComments"],
        "security"  => &["gate", "gmComments"],
        "receiving" => &["gate", "trailer", "stat", "ryderComments", "loadComments"],
        // Limited to their own dock by update_live_trailer and the add-on routes
        "vaa" | "univ" => &["gate", "door", "unload", "status", "stat",
                            "gmComments", "dockComments", "addOn"],
        _ => &[],
    }
}

pub fn can(role: &str, action: &str) -> bool {
    role == "admin" || role_actions(role).contains(&action)
}

/// VAA and Universal only touch their own dock's trailers.
pub fn dock_allowed(role: &str, dock_code: &str) -> bool {
    match role {
        r if r.contains("vaa")  => dock_code.trim() == "V",
        r if r.contains("univ") => dock_code.trim() == "U",
        _ => true,
    }
}

/// The fields in `changed` this role may not write, described by action for the
/// error message. Arrival and status travel together: recording an arrival also
/// sets On Time / Early / Late, and "Not Late" stamps an arrival time, so each is
/// allowed alongside the other when the role holds that one.
pub fn denied(role: &str, changed: &HashSet<&str>) -> Vec<&'static str> {
    if role == "admin" {
        return vec![];
    }
    let mut allowed: HashSet<&str> = ACTIONS.iter()
        .filter(|(action, _, _)| can(role, action))
        .flat_map(|(_, _, fields)| fields.iter().copied())
        .collect();

    let gate_changed = changed.contains("gateArrivalTime") || changed.contains("gateArrivalDate");
    if can(role, "gate") && gate_changed {
        allowed.insert("statusOX");
    }
    if can(role, "status") && changed.contains("statusOX") {
        allowed.insert("gateArrivalTime");
        allowed.insert("gateArrivalDate");
    }

    let mut labels: Vec<&'static str> = changed.iter()
        .filter(|f| !allowed.contains(**f))
        .map(|f| ACTIONS.iter()
            .find(|(_, _, fields)| fields.contains(f))
            .map(|(_, label, _)| *label)
            .unwrap_or("schedule details (admin only)"))
        .collect();
    labels.sort();
    labels.dedup();
    labels
}

#[derive(Serialize)]
pub struct MyPermissions {
    pub role:    String,
    /// What this role may do
    pub actions: Vec<&'static str>,
    /// Every action's name as the screens show it, for "can't" tooltips
    pub labels:  std::collections::HashMap<&'static str, &'static str>,
}

/// The signed-in user's actions, so the screens can disable what they can't do.
#[get("/api/my_permissions")]
pub async fn my_permissions(user: AuthenticatedUser) -> Json<MyPermissions> {
    let role = user.0.role.clone();
    let actions = ACTIONS.iter()
        .map(|(action, _, _)| *action)
        .filter(|action| can(&role, action))
        .collect();
    let labels = ACTIONS.iter().map(|(action, label, _)| (*action, *label)).collect();
    Json(MyPermissions { role, actions, labels })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set<'a>(fields: &[&'a str]) -> HashSet<&'a str> { fields.iter().copied().collect() }

    #[test]
    fn table_matches_the_matrix() {
        assert!(can("admin", "door"));
        assert!(can("manager", "rollShift") && !can("manager", "gate"));
        assert!(can("dock", "unload") && !can("dock", "gate"));
        assert!(can("security", "gate") && can("security", "gmComments") && !can("security", "status"));
        assert!(can("receiving", "trailer") && !can("receiving", "status"));
        assert!(can("vaa", "addOn") && !can("vaa", "trailer"));
        for r in ["supervisor", "floater", "read", ""] {
            assert!(ACTIONS.iter().all(|(a, _, _)| !can(r, a)), "{r} should be view-only");
        }
    }

    #[test]
    fn arrival_carries_its_status_and_not_late_its_time() {
        // Security records an arrival; the On Time/Early/Late status rides along
        assert!(denied("security", &set(&["gateArrivalTime", "gateArrivalDate", "statusOX"])).is_empty());
        // ...but can't set a status on its own
        assert_eq!(denied("security", &set(&["statusOX"])), vec!["Status OX / Confirm Late / Not Late"]);
        // Manager's "Not Late" stamps the arrival time with the status
        assert!(denied("manager", &set(&["statusOX", "gateArrivalTime"])).is_empty());
        // ...but can't record an arrival on its own
        assert_eq!(denied("manager", &set(&["gateArrivalTime"])), vec!["Arrived (gate)"]);
    }

    #[test]
    fn unlisted_fields_are_admin_only() {
        assert_eq!(denied("manager", &set(&["scac"])), vec!["schedule details (admin only)"]);
        assert!(denied("admin", &set(&["scac", "hour"])).is_empty());
        assert_eq!(denied("read", &set(&["dockComments"])), vec!["Dock Comments"]);
    }

    #[test]
    fn vaa_and_univ_stay_on_their_dock() {
        assert!(dock_allowed("vaa", "V") && !dock_allowed("vaa", "U"));
        assert!(dock_allowed("univ", " U ") && !dock_allowed("univ", "V"));
        assert!(dock_allowed("manager", "BE"));
    }
}
