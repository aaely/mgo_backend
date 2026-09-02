use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, Mutex};
use tokio_tungstenite::tungstenite::protocol::Message;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use neo4rs::{Graph, Node, query};

use crate::helpers::*;


pub type WebSocketList = Arc<Mutex<HashMap<SocketAddr, (UnboundedSender<Message>, String)>>>;

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
    pub load_no:               String,
    pub location:              String,
    pub dock:                  String,
    pub route_id:              String,
    pub route_ver:             String,
    pub scac:                  String,
    pub status:                String,
    pub trailer:               String,
    pub trailer2:              String,
    pub schedule_start_time:   String,
    pub schedule_arrival_time: String,
    pub actual_start_time:     String,
    pub actual_end_time:       String,
    pub dock_sequence:         String,
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
    #[serde(default)] pub hour: String,
    #[serde(default)] pub origin: String,
    #[serde(default)] pub dateShift: String,
    #[serde(default)] pub lmsAccent: String,
    #[serde(default)] pub dockCode: String,
    #[serde(default)] pub acaType: String,
    #[serde(default)] pub status: String,
    #[serde(default)] pub routeId: String,
    #[serde(default)] pub scac: String,
    #[serde(default)] pub trailer1: String,
    #[serde(default)] pub trailer2: String,
    #[serde(default)] pub firstSupplier: String,
    #[serde(default)] pub dockStopSequence: String,
    #[serde(default)] pub planStartDate: String,
    #[serde(default)] pub planStartTime: String,
    #[serde(default)] pub scheduleStartDate: String,
    #[serde(default)] pub adjustedStartTime: String,
    #[serde(default)] pub scheduleEndDate: String,
    #[serde(default)] pub scheduleEndTime: String,
    #[serde(default)] pub gateArrivalTime: String,
    #[serde(default)] pub actualStartTime: String,
    #[serde(default)] pub actualEndTime: String,
    #[serde(default)] pub statusOX: String,
    #[serde(default)] pub loadComments: String,
    #[serde(default)] pub ryderComments: String,
    pub lateComments: Option<String>,
    pub gmComments: Option<String>,
    pub lowestDoh: Option<String>,
    #[serde(default)] pub uuid: String,
    #[serde(default)] pub editRef: String,
    #[serde(default)] pub door: String,
    #[serde(default)] pub doorArrivalTime: String,
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
    pub isRepower: bool,
    pub repowerLoadNum: String,
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
    pub current_alerts: Arc<Mutex<Vec<PartAlert>>>,
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
    pub slack_id:  Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShiftDetail {
    pub shift:         String,
    pub day_key:       String,
    pub shift_status:  String,
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
pub struct Contact {
    pub email: String,
    pub name:  String,
    pub phone: String,
    #[serde(default)]
    pub duns:  String,
    #[serde(default)]
    pub scac:  String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DunsContacts {
    pub duns:     String,
    pub contacts: Vec<Contact>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct CarrierContacts {
    pub scac:     String,
    pub contacts: Vec<Contact>,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteContactRequest {
    pub email: String,
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
pub struct HourCount {
    pub hour:  String,
    pub count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DockCountResponse {
    pub hourly:      Vec<HourCount>,
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
pub struct PartRoute {
    pub part:  String,
    pub duns:  String,
    pub route: String,
    pub desc:  String,
    #[serde(default)]
    pub deck:  String,
    #[serde(default)]
    pub dock:  String,
    #[serde(default)]
    pub country: String,
    #[serde(default)]
    pub supplier: String,
    pub doh:   Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeckRoute {
    pub route: String,
    pub parts: Vec<String>,
}



