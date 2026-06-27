use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tokio_tungstenite::tungstenite::protocol::Message;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use neo4rs::{Graph, Node, query};

use crate::helpers::*;


pub type WebSocketList = Arc<Mutex<HashMap<SocketAddr, UnboundedSender<Message>>>>;

#[derive(Serialize, Deserialize, PartialEq, Default, Debug)]
pub struct InTransit {
    pub trailer: String,
    pub sid: String,
    pub part: String,
    pub quantity: String,
    pub duns: String,
    pub cisco: String,
    pub destination: String,
    pub supplier: String,
    pub location: String,
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug)]
pub struct Schedule {
    pub TrailerID: String,
    pub OriginalDate: String,
    pub ScheduleDate: String,
    pub ScheduleTime: String,
    pub Comments: String,
    pub Destination: String,
    pub Status: String,
    pub Supplier: String,
    pub Scac: String,
    pub Location: String,
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug)]
pub struct LMSRecord {
    pub load_no: String,
    pub route_id: String,
    pub scac: String,
    pub trailer: String,
    pub trailer2: String,
    pub schedule_arrival_time: String,
    pub location: String,
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug)]
pub struct IOResponse {
    pub Trailer: String,
    pub Schedule: Schedule,
    pub Sids: Vec<String>,
    pub Parts: Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug)]
pub struct TrailerRecord {
    pub hour: String,
    pub origin: String,
    pub dateShift: String,
    pub lmsAccent: String,
    pub dockCode: String,
    pub acaType: String,
    pub status: String,
    pub routeId: String,
    pub scac: String,
    pub trailer1: String,
    pub trailer2: String,
    pub firstSupplier: String,
    pub dockStopSequence: String,    
    pub planStartDate: String,
    pub planStartTime: String,
    pub scheduleStartDate: String,
    pub adjustedStartTime: String,
    pub scheduleEndDate: String,
    pub scheduleEndTime: String,
    pub gateArrivalTime: String,
    pub actualStartTime: String,
    pub actualEndTime: String,
    pub statusOX: String,
    pub loadComments: String,
    pub ryderComments: String,
    pub lateComments: Option<String>,
    pub gmComments: Option<String>,
    pub lowestDoh: Option<String>,
    pub uuid: String,
    #[serde(default)]
    pub editRef: String,
    #[serde(default)]
    pub door: String,
    #[serde(default)]
    pub doorArrivalTime: String,
}

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub user: UserResponse,
}

#[derive(Serialize)]
pub struct UserResponse {
    pub username: String,
    pub role: String,
}

#[derive(Serialize, Deserialize)]
pub struct DeliveredRequest {
    pub date1: String,
    pub date2: String,
}

#[derive(Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub password: String,
    pub role: String,
    pub email: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Delivered {
    pub trailer_id:    String,
    pub delivery_date: String,
    pub Comments:      String,
    pub Destination:   String,
    pub OriginalDate:  String,
    pub ScheduleDate:  String,
    pub ScheduleTime:  String,
    pub Status:        String,
    pub Supplier:      String,
    pub Scac:          String,
    pub parts:         Vec<String>,
    pub sids:          Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug)]
pub struct ExceptionLogEntry {
    pub loadNum: String,
    pub dock: String,
    pub r#type: String,
    pub status: String,
    pub route: String,
    pub scac: String,
    pub trailer1: String,
    pub trailer2: String,
    pub supplier: String,
    pub dockSequence: String,
    pub originalDate: String,
    pub originalTime: String,
    pub newDate: String,
    pub newTime: String,
    pub newEndDate: String,
    pub newEndTime: String,
    pub comment: String,
    pub requestor: String,
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug)]
pub struct DyCommLogEntry {
    pub loadNum: String,
    pub trailer: String,
    pub scac: String,
    pub route: String,
    pub dock: String,
    pub location: String,
    pub deliveryDate: String,
    pub deliveryTime: String,
    pub supplier: String,
    pub part: String,
    pub pdt: String,
    pub createdBy: String,
}

#[derive(Serialize, Deserialize, PartialEq, Default, Debug, Clone)]
pub struct TrailerEntry {
    pub loadNo:       String,
    pub routePrefix:  String,
    pub routeId:      String,
    pub status:       String,
    pub stat2:        String,
    pub scac:         String,
    pub trailer:      String,
    pub rNote:        String,
    pub schedArrival: String,
    pub schedDepart:  String,
    pub location:     String,
    pub acctorId:     String,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct DeliveredTrailer {
    pub TrailerID:    String,
    pub DeliveryDate: String,
    pub Schedule:     Schedule,
    pub Parts:        Vec<String>,
    pub Sids:         Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct PartInfo {
    pub number:   String,
    pub duns:     String,
    pub supplier: String,
    pub desc:     String,
    pub deck:     String,
    pub dock:     String,
}

#[derive(Serialize, Deserialize)]
pub struct PartInfoRequest {
    pub part: String,
}

#[derive(Deserialize, Serialize)]
pub struct DeliveryRequest {
    pub trailer_id: String
}


#[derive(Deserialize, Serialize)]
pub struct RollNextShiftRequest {
    pub operational_date: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IncomingMessage {
    pub r#type: String,
    pub data: Option<MessageData>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MessageData {
    pub message: String,
}

pub struct AppState {
    pub graph: Arc<Graph>,
    pub jwt_secret: String,
    pub ws_list: WebSocketList,
    pub alerted_parts: Arc<Mutex<HashMap<String, chrono::DateTime<chrono::Local>>>>,
    pub edit_refs: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    pub use_https: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShiftAssignment {
    pub user_name: String,
    pub role:      String,
    pub position:  String,
    pub task:      String,
    pub task_type: String,
    pub full_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShiftSlot {
    pub shift:    String,
    pub shift_status: String,
    pub shift_reason: String,
    pub assigned: Vec<ShiftAssignment>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdatePositionRequest {
    pub user_name: String,
    pub position:  String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DaySchedule {
    pub date:   String,
    pub name:   String,
    pub shifts: Vec<ShiftSlot>,
    pub offset: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SaturdayCount {
    pub user_name:       String,
    pub full_name:       String,
    pub position:        String,
    pub shift:           String,
    pub saturdays_worked: u64,
    pub saturdays_off:   u64,
    pub mondays_worked:  u64,
    pub mondays_off:     u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ShiftStatusRequest {
    pub start_date: String,
    pub offset:     u64,
    pub shift:      String,
    pub status:     String,  
    pub reason:     String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WeekSchedule {
    pub start_date: String,
    pub days:       Vec<DaySchedule>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AssignShiftRequest {
    pub start_date: String,
    pub date:       String,
    pub shift:      String,
    pub user_name:  String,
    pub offset:     u64,
    pub task:       String,
    pub task_type:  String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnassignShiftRequest {
    pub start_date: String,
    pub offset:     u64,
    pub shift:      String,
    pub user_name:  String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeckCoverage {
    pub deck:      String,
    pub user_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShiftDetail {
    pub shift:         String,
    pub day_key:       String,
    pub assigned:      Vec<ShiftAssignment>,
    pub deck_coverage: Vec<DeckCoverage>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AssignDeckRequest {
    pub day_key:   String,
    pub shift:     String,
    pub deck:      String,
    pub user_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnassignDeckRequest {
    pub day_key: String,
    pub shift:   String,
    pub deck:    String,
}

pub fn get_allowed_fields(role: &str) -> Option<Vec<&'static str>> {
    let mut permissions: HashMap<&str, Vec<&'static str>> = HashMap::new();
    
    permissions.insert("admin", vec!["*"]);
    permissions.insert("supervisor", vec![
        "hour", "dockCode", "adjustedStartTime", "scheduleEndDate",
        "scheduleEndTime", "scac", "statusOX", "trailer1", "trailer2",
        "gateArrivalTime", "actualStartTime", "actualEndTime", "door", "doorArrivalTime"
    ]);
    permissions.insert("clerk", vec![
        "gateArrivalTime", "actualStartTime", "actualEndTime",
        "door", "doorArrivalTime", "dockComments"
    ]);
    permissions.insert("receiving", vec!["statusOX"]);
    permissions.insert("mfu", vec!["ryderComments"]);
    permissions.insert("security", vec!["gateArrivalTime", "gmComments"]);

    permissions.get(role).cloned()
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PartASN {
    pub scac:         String,
    pub trailer:      String,
    pub deck:         String,
    pub part:         String,
    pub duns:         String,
    pub quantity:     Option<f64>,
    pub status:       Option<u32>,
    pub sid:          String,
    pub countComment: Option<String>,
    pub shipComment:  String,
    pub shipDate:     String,
    pub dock:         String,
    pub eda:          String,
    pub eta:          String,
    pub mode:         String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PartASL {
    pub deck:     String,
    pub part:     String,
    pub duns:     String,
    pub supplier: String,
    pub doh:      f64,
    pub bank:     u32,
    pub desc:     String,
    pub cbal:     f64,
    pub day1:     Option<f64>,
    pub day2:     Option<f64>,
    pub day3:     Option<f64>,
    pub day4:     Option<f64>,
    pub day5:     Option<f64>,
    pub day6:     Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct PartOut {
    pub part:      String,
    pub day1_hr1:  f64,
    pub day1_hr2:  f64,
    pub day1_hr3:  f64,
    pub day1_hr4:  f64,
    pub day1_hr5:  f64,
    pub day1_hr6:  f64,
    pub day1_hr7:  f64,
    pub day1_hr8:  f64,
    pub day1_hr9:  f64,
    pub day1_hr10: f64,
    pub day1_hr11: f64,
    pub day1_hr12: f64,
    pub day1_hr13: f64,
    pub day1_hr14: f64,
    pub day1_hr15: f64,
    pub day1_hr16: f64,
    pub day1_hr17: f64,
    pub day1_hr18: f64,
    pub day1_hr19: f64,
    pub day1_hr20: f64,
    pub day1_hr21: f64,
    pub day1_hr22: f64,
    pub day1_hr23: f64,
    pub day1_hr24: f64,
    pub day2_hr1:  f64,
    pub day2_hr2:  f64,
    pub day2_hr3:  f64,
    pub day2_hr4:  f64,
    pub day2_hr5:  f64,
    pub day2_hr6:  f64,
    pub day2_hr7:  f64,
    pub day2_hr8:  f64,
    pub day2_hr9:  f64,
    pub day2_hr10: f64,
    pub day2_hr11: f64,
    pub day2_hr12: f64,
    pub day2_hr13: f64,
    pub day2_hr14: f64,
    pub day2_hr15: f64,
    pub day2_hr16: f64,
    pub day2_hr17: f64,
    pub day2_hr18: f64,
    pub day2_hr19: f64,
    pub day2_hr20: f64,
    pub day2_hr21: f64,
    pub day2_hr22: f64,
    pub day2_hr23: f64,
    pub day2_hr24: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteUserRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct HotPartAsn {
    pub trailer:  String,
    pub quantity: f64,
    pub eda:      String,
    pub eta:      String,
    pub count:    String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct HotPart {
    pub part:       String,
    pub pdt:        String,
    pub mfu:        String,
    pub comments:   String,
    #[serde(default)]
    pub updated_at: String,
    pub asn_list:   Vec<HotPartAsn>,
    pub day1:       Option<f64>,
    pub day2:       Option<f64>,
    pub day3:       Option<f64>,
    pub day4:       Option<f64>,
    pub day5:       Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct CloseHotPartRequest {
    pub part: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartAlert {
    pub part:               String,
    pub desc:               String,
    pub duns:               String,
    pub supplier:           String,
    pub deck:               String,
    pub cbal:               f64,
    pub hours_to_out:       f64,
    pub next_asn_eta:       String,
    pub next_trailer:       String,
    pub alert_level:        String, 
    pub hours_until_rescue: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DockCountResponse {
    pub hr_total:    u32,
    pub shift_total: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateUserRequest {
    pub name:      String,
    pub full_name: String,
    pub position:  String,
    pub role:      String,
    pub shift:     String,
    pub slack_id:  String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    pub trailer_uuid: String,
    pub field:        String,
    pub old_value:    String,
    pub new_value:    String,
    pub timestamp:    String,
    pub updated_by:   String,
    pub event_type:   String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WipePasswordRequest {
    pub username: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResetPasswordRequest {
    pub username:     String,
    pub token:        String,
    pub new_password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChangePasswordRequest {
    pub username:     String,
    pub old_password: String,
    pub new_password: String,
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

pub async fn late_trailer_service(graph: Arc<Graph>, ws_list: WebSocketList) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

    loop {
        interval.tick().await;
        println!("Running late trailer service...");
        let now = chrono::Local::now();
        let fifteen_mins_ago = now - chrono::Duration::minutes(15);

        let q = query("
            MATCH (t:LiveTrailer)
            WHERE t.statusOX = ''
            AND (t.actualStartTime = '' OR t.actualStartTime IS NULL)
            AND (t.actualEndTime = '' OR t.actualEndTime IS NULL)
            AND t.scheduleStartDate <> ''
            AND t.adjustedStartTime <> ''
            AND t.dockCode <> 'Y'
            AND (t.scheduleStartDate + 'T' + t.adjustedStartTime) <= $cutoff
            SET t.statusOX = 'P'
            RETURN t
        ")
        .param("cutoff", fifteen_mins_ago.format("%Y-%m-%dT%H:%M").to_string());

        match graph.execute(q).await {
            Ok(mut result) => {
                while let Ok(Some(row)) = result.next().await {
                    if let Ok(node) = row.get::<Node>("t") {
                        let updated = TrailerRecord {
                            uuid:              node.get("uuid").unwrap_or_default(),
                            origin:            node.get("origin").unwrap_or_default(),
                            hour:              node.get("hour").unwrap_or_default(),
                            dateShift:         node.get("dateShift").unwrap_or_default(),
                            lmsAccent:         node.get("lmsAccent").unwrap_or_default(),
                            dockCode:          node.get("dockCode").unwrap_or_default(),
                            acaType:           node.get("acaType").unwrap_or_default(),
                            status:            node.get("status").unwrap_or_default(),
                            routeId:           node.get("routeId").unwrap_or_default(),
                            scac:              node.get("scac").unwrap_or_default(),
                            trailer1:          node.get("trailer1").unwrap_or_default(),
                            trailer2:          node.get("trailer2").unwrap_or_default(),
                            firstSupplier:     node.get("firstSupplier").unwrap_or_default(),
                            dockStopSequence:  node.get("dockStopSequence").unwrap_or_default(),
                            planStartDate:     node.get("planStartDate").unwrap_or_default(),
                            planStartTime:     node.get("planStartTime").unwrap_or_default(),
                            scheduleStartDate: node.get("scheduleStartDate").unwrap_or_default(),
                            adjustedStartTime: node.get("adjustedStartTime").unwrap_or_default(),
                            scheduleEndDate:   node.get("scheduleEndDate").unwrap_or_default(),
                            scheduleEndTime:   node.get("scheduleEndTime").unwrap_or_default(),
                            gateArrivalTime:   node.get("gateArrivalTime").unwrap_or_default(),
                            actualStartTime:   node.get("actualStartTime").unwrap_or_default(),
                            actualEndTime:     node.get("actualEndTime").unwrap_or_default(),
                            statusOX:          node.get("statusOX").unwrap_or_default(),
                            loadComments:      node.get("loadComments").unwrap_or_default(),
                            ryderComments:     node.get("ryderComments").unwrap_or_default(),
                            lateComments:      Some(node.get("lateComments").unwrap_or_default()),
                            gmComments:        Some(node.get("gmComments").unwrap_or_default()),
                            lowestDoh:         Some(node.get("lowestDoh").unwrap_or_default()),
                            editRef:           String::new(),
                            door:              node.get("door").unwrap_or_default(),
                            doorArrivalTime:   node.get("doorArrivalTime").unwrap_or_default(),
                        };

                        // ── Broadcast to WS clients ──
                        if let Ok(data) = serde_json::to_string(&updated) {
                            let ws_msg = IncomingMessage {
                                r#type: "trailer_update".to_string(),
                                data: Some(MessageData { message: data }),
                            };
                            if let Ok(message) = serde_json::to_string(&ws_msg) {
                                let ws_list = ws_list.lock().await;
                                for (_, tx) in ws_list.iter() {
                                    let _ = tx.send(Message::Text(message.clone()));
                                }
                            }
                        }

                        // ── Email alert ──
                        if let Ok(alert_email) = std::env::var("ALERT_EMAIL") {
                            let subject = format!("Late Trailer - Route {}", updated.routeId);
                            let body = format!(
                                "Trailer {} on route {} has been flagged as late.\n\nDock: {}\nSCAC: {}\nScheduled: {} at {}\n",
                                updated.trailer1, updated.routeId, updated.dockCode,
                                updated.scac, updated.scheduleStartDate, updated.adjustedStartTime,
                            );
                            if let Err(e) = send_email(&alert_email, &subject, body).await {
                                eprintln!("Failed to send late trailer email: {:?}", e);
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Late trailer service error: {:?}", e);
            }
        }
    }
}

pub async fn part_monitoring_service(
    graph: Arc<Graph>, 
    ws_list: WebSocketList,
    alerted_parts: Arc<Mutex<HashMap<String, chrono::DateTime<chrono::Local>>>>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60 * 15));
    
    loop {
        interval.tick().await;
        println!("Running part monitoring service...");

        // ── Fetch all PartASL ──
        let asl_query = query("MATCH (n:PartASL) WHERE n.cbal > 0 RETURN n");
        let asl_map: std::collections::HashMap<String, PartASL> = match graph.execute(asl_query).await {
            Ok(mut result) => {
                let mut map = std::collections::HashMap::new();
                while let Ok(Some(row)) = result.next().await {
                    if let Ok(node) = row.get::<Node>("n") {
                        let part: String = node.get("part").unwrap_or_default();
                        map.insert(part.clone(), PartASL {
                            part,
                            deck:     node.get("deck").unwrap_or_default(),
                            duns:     node.get("duns").unwrap_or_default(),
                            supplier: node.get("supplier").unwrap_or_default(),
                            doh:      node.get("doh").unwrap_or_default(),
                            bank:     node.get("bank").unwrap_or_default(),
                            desc:     node.get("desc").unwrap_or_default(),
                            cbal:     node.get("cbal")
                                .or_else(|_| node.get::<i64>("cbal").map(|v| v as f64))
                                .unwrap_or_default(),
                            day1:     node.get("day1").unwrap_or_default(),
                            day2:     node.get("day2").unwrap_or_default(),
                            day3:     node.get("day3").unwrap_or_default(),
                            day4:     node.get("day4").unwrap_or_default(),
                            day5:     node.get("day5").unwrap_or_default(),
                            day6:     node.get("day6").unwrap_or_default(),
                        });
                    }
                }
                map
            }
            Err(e) => {
                eprintln!("Failed to fetch PartASL: {:?}", e);
                continue;
            }
        };

        // ── Fetch all PartOut ──
        let out_query = query("MATCH (n:PartOut) RETURN n");
        let out_map: std::collections::HashMap<String, PartOut> = match graph.execute(out_query).await {
            Ok(mut result) => {
                let mut map = std::collections::HashMap::new();
                while let Ok(Some(row)) = result.next().await {
                    if let Ok(node) = row.get::<Node>("n") {
                        let part: String = node.get("part").unwrap_or_default();
                        map.insert(part.clone(), PartOut {
                            part,
                            day1_hr1:  node.get("day1_hr1").unwrap_or_default(),
                            day1_hr2:  node.get("day1_hr2").unwrap_or_default(),
                            day1_hr3:  node.get("day1_hr3").unwrap_or_default(),
                            day1_hr4:  node.get("day1_hr4").unwrap_or_default(),
                            day1_hr5:  node.get("day1_hr5").unwrap_or_default(),
                            day1_hr6:  node.get("day1_hr6").unwrap_or_default(),
                            day1_hr7:  node.get("day1_hr7").unwrap_or_default(),
                            day1_hr8:  node.get("day1_hr8").unwrap_or_default(),
                            day1_hr9:  node.get("day1_hr9").unwrap_or_default(),
                            day1_hr10: node.get("day1_hr10").unwrap_or_default(),
                            day1_hr11: node.get("day1_hr11").unwrap_or_default(),
                            day1_hr12: node.get("day1_hr12").unwrap_or_default(),
                            day1_hr13: node.get("day1_hr13").unwrap_or_default(),
                            day1_hr14: node.get("day1_hr14").unwrap_or_default(),
                            day1_hr15: node.get("day1_hr15").unwrap_or_default(),
                            day1_hr16: node.get("day1_hr16").unwrap_or_default(),
                            day1_hr17: node.get("day1_hr17").unwrap_or_default(),
                            day1_hr18: node.get("day1_hr18").unwrap_or_default(),
                            day1_hr19: node.get("day1_hr19").unwrap_or_default(),
                            day1_hr20: node.get("day1_hr20").unwrap_or_default(),
                            day1_hr21: node.get("day1_hr21").unwrap_or_default(),
                            day1_hr22: node.get("day1_hr22").unwrap_or_default(),
                            day1_hr23: node.get("day1_hr23").unwrap_or_default(),
                            day1_hr24: node.get("day1_hr24").unwrap_or_default(),
                            day2_hr1:  node.get("day2_hr1").unwrap_or_default(),
                            day2_hr2:  node.get("day2_hr2").unwrap_or_default(),
                            day2_hr3:  node.get("day2_hr3").unwrap_or_default(),
                            day2_hr4:  node.get("day2_hr4").unwrap_or_default(),
                            day2_hr5:  node.get("day2_hr5").unwrap_or_default(),
                            day2_hr6:  node.get("day2_hr6").unwrap_or_default(),
                            day2_hr7:  node.get("day2_hr7").unwrap_or_default(),
                            day2_hr8:  node.get("day2_hr8").unwrap_or_default(),
                            day2_hr9:  node.get("day2_hr9").unwrap_or_default(),
                            day2_hr10: node.get("day2_hr10").unwrap_or_default(),
                            day2_hr11: node.get("day2_hr11").unwrap_or_default(),
                            day2_hr12: node.get("day2_hr12").unwrap_or_default(),
                            day2_hr13: node.get("day2_hr13").unwrap_or_default(),
                            day2_hr14: node.get("day2_hr14").unwrap_or_default(),
                            day2_hr15: node.get("day2_hr15").unwrap_or_default(),
                            day2_hr16: node.get("day2_hr16").unwrap_or_default(),
                            day2_hr17: node.get("day2_hr17").unwrap_or_default(),
                            day2_hr18: node.get("day2_hr18").unwrap_or_default(),
                            day2_hr19: node.get("day2_hr19").unwrap_or_default(),
                            day2_hr20: node.get("day2_hr20").unwrap_or_default(),
                            day2_hr21: node.get("day2_hr21").unwrap_or_default(),
                            day2_hr22: node.get("day2_hr22").unwrap_or_default(),
                            day2_hr23: node.get("day2_hr23").unwrap_or_default(),
                            day2_hr24: node.get("day2_hr24").unwrap_or_default(),
                        });
                    }
                }
                map
            }
            Err(e) => {
                eprintln!("Failed to fetch PartOut: {:?}", e);
                continue;
            }
        };

        // ── Fetch all PartASN grouped by part ──
        // Tuple: (eda, eta, trailer, quantity)
        let asn_query = query("
            MATCH (n:PartASN)
            RETURN n
            ORDER BY n.eda ASC, n.eta ASC
        ");
        let mut asn_map: std::collections::HashMap<String, Vec<(String, String, String, f64)>> =
            std::collections::HashMap::new();

        match graph.execute(asn_query).await {
            Ok(mut result) => {
                while let Ok(Some(row)) = result.next().await {
                    if let Ok(node) = row.get::<Node>("n") {
                        let part:     String = node.get("part").unwrap_or_default();
                        let eda:      String = node.get("eda").unwrap_or_default();
                        let eta:      String = node.get("eta").unwrap_or_default();
                        let trailer:  String = node.get("trailer").unwrap_or_default();
                        let quantity: f64    = node.get("quantity").unwrap_or_default();
                        asn_map.entry(part).or_default().push((eda, eta, trailer, quantity));
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch PartASN: {:?}", e);
                continue;
            }
        }
        for asns in asn_map.values_mut() {
            asns.sort_by(|a, b| {
                let a_dt = parse_asn_arrival(&a.0, &a.1);
                let b_dt = parse_asn_arrival(&b.0, &b.1);
                a_dt.cmp(&b_dt)
            });
        }

        // ── Compute current offset into the 48-hour window ──
        let now = chrono::Local::now();
        let now_naive = now.naive_local();
        let today_2200 = now_naive.date().and_hms_opt(22, 0, 0).unwrap();
        let yesterday_2200 = (now_naive.date() - chrono::Duration::days(1))
            .and_hms_opt(22, 0, 0)
            .unwrap();
        let window_start = if now_naive >= today_2200 {
            today_2200
        } else {
            yesterday_2200
        };
        let current_hour_offset = (now_naive - window_start).num_hours().clamp(0, 47) as usize;

        println!("Window start: {} Current hour offset: {}", window_start, current_hour_offset);

        // ── Run simulation for each part ──
        let mut alerts: Vec<PartAlert> = Vec::new();

        for (part, asl) in &asl_map {
            let out = match out_map.get(part) {
                Some(o) => o,
                None => continue,
            };

            let hourly = vec![
                out.day1_hr1,  out.day1_hr2,  out.day1_hr3,  out.day1_hr4,
                out.day1_hr5,  out.day1_hr6,  out.day1_hr7,  out.day1_hr8,
                out.day1_hr9,  out.day1_hr10, out.day1_hr11, out.day1_hr12,
                out.day1_hr13, out.day1_hr14, out.day1_hr15, out.day1_hr16,
                out.day1_hr17, out.day1_hr18, out.day1_hr19, out.day1_hr20,
                out.day1_hr21, out.day1_hr22, out.day1_hr23, out.day1_hr24,
                out.day2_hr1,  out.day2_hr2,  out.day2_hr3,  out.day2_hr4,
                out.day2_hr5,  out.day2_hr6,  out.day2_hr7,  out.day2_hr8,
                out.day2_hr9,  out.day2_hr10, out.day2_hr11, out.day2_hr12,
                out.day2_hr13, out.day2_hr14, out.day2_hr15, out.day2_hr16,
                out.day2_hr17, out.day2_hr18, out.day2_hr19, out.day2_hr20,
                out.day2_hr21, out.day2_hr22, out.day2_hr23, out.day2_hr24,
            ];

            let asns_for_part = asn_map.get(part).cloned().unwrap_or_default();

            // ── Phase 1: full burn from hour 0 with cbal ──
            let mut bal = asl.cbal;
            let mut outage_window_idx: Option<usize> = None;

            println!("Part: {} cbal: {:.1} asn_count: {}", part, asl.cbal, asns_for_part.len());

            for (i, &burn) in hourly.iter().enumerate() {
                if burn <= 0.0 { continue; }
                if bal <= burn {
                    outage_window_idx = Some(i);
                    break;
                }
                bal -= burn;
            }

            let out_idx = match outage_window_idx {
                Some(idx) => idx,
                None => {
                    println!("  Part: {} survives full window", part);
                    alerted_parts.lock().await.remove(part);
                    continue;
                }
            };

            let downtime_dt = window_start + chrono::Duration::hours(out_idx as i64);
            let hours_to_out = out_idx as f64 - current_hour_offset as f64;

            println!("  Part: {} out_idx: {} downtime_dt: {} hours_to_out: {:.2} bal_at_out: {:.1}",
                part, out_idx, downtime_dt, hours_to_out, bal);

            if hours_to_out > 48.0 {
                alerted_parts.lock().await.remove(part);
                continue;
            }

            // ── Phase 2: chain ASNs ──
            let mut asn_index = 0usize;
            let mut current_downtime_dt = downtime_dt;
            let mut final_hours_to_out = hours_to_out;
            let mut running_bal = asl.cbal;
            let mut last_asn_window_idx = 0usize;

            println!("  ASNs for part {} (downtime: {}):", part, current_downtime_dt);
            for (eda, eta, trailer, qty) in &asns_for_part {
                let arrival = parse_asn_arrival(eda, eta);
                println!(
                    "    trailer: {} qty: {:.1} eda: {} eta: {} arrival: {:?} arrives_before_downtime: {:?}",
                    trailer, qty, eda, eta, arrival,
                    arrival.map(|a| a < current_downtime_dt)
                );
            }

            let classify = |h: f64| -> Option<&'static str> {
                if h <= 0.0      { Some("Shut Down") }
                else if h <= 2.0 { Some("Emerging Issue") }
                else if h <= 6.0 { Some("Hot") }
                else             { None }
            };


            // ── Check initial burn result before ASN chain ──
            if let Some(level) = classify(final_hours_to_out) {
                let (next_eda, next_eta, next_trailer) = asns_for_part
                    .first()
                    .map(|(eda, eta, trailer, _)| (eda.clone(), eta.clone(), trailer.clone()))
                    .unwrap_or_default();
                let next_asn_display = if next_eda.is_empty() { "N/A".to_string() }
                    else { format!("{} {}", next_eda, next_eta) };
                let hours_until_rescue = asns_for_part
                    .get(0)
                    .and_then(|(eda, eta, _, _)| parse_asn_arrival(eda, eta))
                    .map(|arrival| (current_downtime_dt - arrival).num_minutes() as f64 / 60.0)
                    .unwrap_or(0.0);

                println!(
                    "  ALERT (initial): part: {} level: {} hours_to_out: {:.2} rescue_margin: {:.2}",
                    part, level, final_hours_to_out, hours_until_rescue
                );

                let sms_eligible = matches!(level, "Emerging Issue" | "Shut Down");
                if sms_eligible {
                    let should_sms = {
                        let alerted = alerted_parts.lock().await;
                        match alerted.get(part) {
                            Some(last_alerted) => now.signed_duration_since(*last_alerted).num_minutes() > 30,
                            None => true,
                        }
                    };
                    if should_sms {
                        let msg = format!(
                            "🚨 PART ALERT [{}] {} - {:.1} hrs to outage. Next ASN: {} on {}",
                            level.to_uppercase(), part, final_hours_to_out, next_trailer, next_asn_display,
                        );
                        if let Err(e) = send_sms("+18777804236", &msg).await {
                            eprintln!("Failed to send SMS: {:?}", e);
                        } else {
                            alerted_parts.lock().await.insert(part.clone(), now);
                        }

                        if let Ok(alert_email) = std::env::var("ALERT_EMAIL") {
                            let subject = format!("Part Alert [{}] - {}", level, part);
                            let body = format!(
                                "Part Number: {}\nDescription: {}\nSupplier: {}\nDeck: {}\n\nAlert Level: {}\nHours to Outage: {:.1}\nNext ASN ETA: {}\nNext Trailer: {}\nHours Until Rescue: {:.1}\n",
                                part, asl.desc, asl.supplier, asl.deck,
                                level, final_hours_to_out, next_asn_display, next_trailer, hours_until_rescue,
                            );
                            if let Err(e) = send_email(&alert_email, &subject, body).await {
                                eprintln!("Failed to send part alert email: {:?}", e);
                            }
                        }
                    }
                }
                alerts.push(PartAlert {
                    part:               part.clone(),
                    desc:               asl.desc.clone(),
                    duns:               asl.duns.clone(),
                    supplier:           asl.supplier.clone(),
                    deck:               asl.deck.clone(),
                    cbal:               asl.cbal,
                    hours_to_out:       final_hours_to_out,
                    next_asn_eta:       next_asn_display,
                    next_trailer,
                    alert_level:        level.to_string(),
                    hours_until_rescue,
                });
            }

            // ── ASN chain loop ──
            loop {
                let saving = asns_for_part[asn_index..].iter().enumerate().find(|(_, (eda, eta, _, _))| {
                    match parse_asn_arrival(eda, eta) {
                        Some(arrival) => arrival < current_downtime_dt,
                        None => false,
                    }
                });

                let (abs_idx, eda, eta, qty) = match saving {
                    Some((rel_idx, (eda, eta, _, qty))) => {
                        (asn_index + rel_idx, eda.clone(), eta.clone(), *qty)
                    }
                    None => {
                        println!("  No saving ASN found — outage stands at {}", current_downtime_dt);
                        break;
                    }
                };

                asn_index = abs_idx + 1;

                let arrival_dt = parse_asn_arrival(&eda, &eta).unwrap();
                let hours_from_start = (arrival_dt - window_start).num_hours();
                let asn_window_idx = hours_from_start.max(0) as usize;

                // Burn incrementally from last ASN arrival to this one
                for i in last_asn_window_idx..asn_window_idx.min(hourly.len()) {
                    let burn = hourly[i];
                    if burn > 0.0 {
                        running_bal = (running_bal - burn).max(0.0);
                    }
                }

                running_bal += qty;
                last_asn_window_idx = asn_window_idx;

                println!(
                    "  Saving ASN: eda: {} eta: {} arrival: {} asn_window_idx: {} qty: {:.1} bal_after_injection: {:.1}",
                    eda, eta, arrival_dt, asn_window_idx, qty, running_bal
                );

                // Burn forward from ASN arrival to find next outage
                let mut new_out_idx: Option<usize> = None;
                let mut temp_bal = running_bal;

                for i in asn_window_idx..hourly.len() {
                    let burn = hourly[i];
                    if burn <= 0.0 { continue; }
                    if temp_bal <= burn {
                        new_out_idx = Some(i);
                        break;
                    }
                    temp_bal -= burn;
                }

                match new_out_idx {
                    None => {
                        println!("  Part: {} survives after ASN at {}", part, arrival_dt);
                        alerted_parts.lock().await.remove(part);
                        break;
                    }
                    Some(idx) => {
                        current_downtime_dt = window_start + chrono::Duration::hours(idx as i64);
                        final_hours_to_out = idx as f64 - current_hour_offset as f64;

                        // How far before the new downtime the ASN that just arrived lands
                        let hours_until_rescue = (current_downtime_dt - arrival_dt).num_minutes() as f64 / 60.0;

                        println!(
                            "  Still runs out — new out_idx: {} new downtime: {} hours_to_out: {:.2} bal_after_burn: {:.1} rescue_margin: {:.2}",
                            idx, current_downtime_dt, final_hours_to_out, temp_bal, hours_until_rescue
                        );

                        if let Some(level) = classify(final_hours_to_out) {
                            let (next_eda, next_eta, next_trailer) = asns_for_part
                                .get(asn_index)
                                .map(|(eda, eta, trailer, _)| (eda.clone(), eta.clone(), trailer.clone()))
                                .unwrap_or_default();
                            let next_asn_display = if next_eda.is_empty() { "N/A".to_string() }
                                else { format!("{} {}", next_eda, next_eta) };

                            println!(
                                "  ALERT (chain): part: {} level: {} hours_to_out: {:.2} next_trailer: {} next_asn: {} rescue_margin: {:.2}",
                                part, level, final_hours_to_out, next_trailer, next_asn_display, hours_until_rescue
                            );

                            let sms_eligible = matches!(level, "Emerging Issue" | "Shut Down");
                            if sms_eligible {
                                let should_sms = {
                                    let alerted = alerted_parts.lock().await;
                                    match alerted.get(part) {
                                        Some(last_alerted) => now.signed_duration_since(*last_alerted).num_minutes() > 30,
                                        None => true,
                                    }
                                };
                                if should_sms {
                                    let msg = format!(
                                        "🚨 PART ALERT [{}] {} - {:.1} hrs to outage. Next ASN: {} on {}",
                                        level.to_uppercase(), part, final_hours_to_out, next_trailer, next_asn_display,
                                    );
                                    if let Err(e) = send_sms("+18777804236", &msg).await {
                                        eprintln!("Failed to send SMS: {:?}", e);
                                    } else {
                                        alerted_parts.lock().await.insert(part.clone(), now);
                                    }

                                    if let Ok(alert_email) = std::env::var("ALERT_EMAIL") {
                                        let subject = format!("Part Alert [{}] - {}", level, part);
                                        let body = format!(
                                            "Part Number: {}\nDescription: {}\nSupplier: {}\nDeck: {}\n\nAlert Level: {}\nHours to Outage: {:.1}\nNext ASN ETA: {}\nNext Trailer: {}\nHours Until Rescue: {:.1}\n",
                                            part, asl.desc, asl.supplier, asl.deck,
                                            level, final_hours_to_out, next_asn_display, next_trailer, hours_until_rescue,
                                        );
                                        if let Err(e) = send_email(&alert_email, &subject, body).await {
                                            eprintln!("Failed to send part alert email: {:?}", e);
                                        }
                                    }
                                }
                            }

                            alerts.push(PartAlert {
                                part:               part.clone(),
                                desc:               asl.desc.clone(),
                                duns:               asl.duns.clone(),
                                supplier:           asl.supplier.clone(),
                                deck:               asl.deck.clone(),
                                cbal:               asl.cbal,
                                hours_to_out:       final_hours_to_out,
                                next_asn_eta:       next_asn_display,
                                next_trailer,
                                alert_level:        level.to_string(),
                                hours_until_rescue,
                            });
                        }
                    }
                }
            }
        }

        if alerts.is_empty() {
            println!("Part monitoring: no alerts");
            continue;
        }

        println!("Part monitoring: {} alerts found", alerts.len());
        println!("Alerts: {:?}", alerts);

        // ── Broadcast alerts via WebSocket ──
        if let Ok(data) = serde_json::to_string(&alerts) {
            let ws_msg = IncomingMessage {
                r#type: "part_alert".to_string(),
                data:   Some(MessageData { message: data }),
            };
            if let Ok(message) = serde_json::to_string(&ws_msg) {
                let ws_list = ws_list.lock().await;
                for (_, tx) in ws_list.iter() {
                    let _ = tx.send(Message::Text(message.clone()));
                }
            }
        }
    }
}