use crate::structs::*;
use crate::auth::{AuthenticatedUser, AdminOrManager};
use crate::role::Role;
use crate::helpers::*;
use rocket::{get, post, serde::json::Json, State};
use neo4rs::{query, Node};
use std::collections::HashMap;
use chrono::{DateTime, Utc, Timelike};

#[get("/api/get_lms")]
pub async fn get_lms(
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<LMSRecord>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }
    
    let graph = &state.graph;

    let query = query("
        MATCH (l:LMSRecord)
        return l
    ");

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut data: Vec<LMSRecord> = Vec::<LMSRecord>::new();
            while let Ok(Some(record)) = result.next().await {
                let lms_node: Node = record.get("l").unwrap();
                let rec = LMSRecord {
                    load_no:               lms_node.get("load_no").unwrap_or_default(),
                    location:              lms_node.get("location").unwrap_or_default(),
                    dock:                  lms_node.get("dock").unwrap_or_default(),
                    route_id:              lms_node.get("route_id").unwrap_or_default(),
                    route_ver:             lms_node.get("route_ver").unwrap_or_default(),
                    scac:                  lms_node.get("scac").unwrap_or_default(),
                    status:                lms_node.get("status").unwrap_or_default(),
                    trailer:               lms_node.get("trailer").unwrap_or_default(),
                    trailer2:              lms_node.get("trailer2").unwrap_or_default(),
                    schedule_start_time:   lms_node.get("schedule_start_time").unwrap_or_default(),
                    schedule_arrival_time: lms_node.get("schedule_arrival_time").unwrap_or_default(),
                    actual_start_time:     lms_node.get("actual_start_time").unwrap_or_default(),
                    actual_end_time:       lms_node.get("actual_end_time").unwrap_or_default(),
                    dock_sequence:         lms_node.get("dock_sequence").unwrap_or_default(),
                };
                data.push(rec);
            }
            Ok(Json(data))
        },
        Err(e) => {
            println!("Failed to run query: {:?}", e);
            Err(Json("Internal Server Error"))
        }
    }
}

#[get("/api/get_lms_by_route?<route>")]
pub async fn get_lms_by_route(
    route: &str,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<LMSRecord>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("
        MATCH (l:LMSRecord)
        WHERE toLower(l.route_id) CONTAINS toLower($route)
        ORDER BY l.schedule_arrival_time
        RETURN l
    ").param("route", route);

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut data: Vec<LMSRecord> = Vec::new();
            while let Ok(Some(record)) = result.next().await {
                let lms_node: Node = record.get("l").unwrap();
                data.push(LMSRecord {
                    load_no:               lms_node.get("load_no").unwrap_or_default(),
                    location:              lms_node.get("location").unwrap_or_default(),
                    dock:                  lms_node.get("dock").unwrap_or_default(),
                    route_id:              lms_node.get("route_id").unwrap_or_default(),
                    route_ver:             lms_node.get("route_ver").unwrap_or_default(),
                    scac:                  lms_node.get("scac").unwrap_or_default(),
                    status:                lms_node.get("status").unwrap_or_default(),
                    trailer:               lms_node.get("trailer").unwrap_or_default(),
                    trailer2:              lms_node.get("trailer2").unwrap_or_default(),
                    schedule_start_time:   lms_node.get("schedule_start_time").unwrap_or_default(),
                    schedule_arrival_time: lms_node.get("schedule_arrival_time").unwrap_or_default(),
                    actual_start_time:     lms_node.get("actual_start_time").unwrap_or_default(),
                    actual_end_time:       lms_node.get("actual_end_time").unwrap_or_default(),
                    dock_sequence:         lms_node.get("dock_sequence").unwrap_or_default(),
                });
            }
            Ok(Json(data))
        },
        Err(e) => {
            println!("Failed to run get_lms_by_route: {:?}", e);
            Err(Json("Internal Server Error"))
        }
    }
}

#[get("/api/get_lms_by_load?<load_no>")]
pub async fn get_lms_by_load(
    load_no: &str,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<LMSRecord>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    if load_no.trim().is_empty() {
        return Ok(Json(Vec::new()));
    }

    let graph = &state.graph;

    let q = query("
        MATCH (l:LMSRecord)
        WHERE toLower(l.load_no) STARTS WITH toLower($load_no)
        RETURN l
        ORDER BY l.load_no
        LIMIT 10
    ").param("load_no", load_no);

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut data: Vec<LMSRecord> = Vec::new();
            while let Ok(Some(record)) = result.next().await {
                let lms_node: Node = record.get("l").unwrap();
                data.push(LMSRecord {
                    load_no:               lms_node.get("load_no").unwrap_or_default(),
                    location:              lms_node.get("location").unwrap_or_default(),
                    dock:                  lms_node.get("dock").unwrap_or_default(),
                    route_id:              lms_node.get("route_id").unwrap_or_default(),
                    route_ver:             lms_node.get("route_ver").unwrap_or_default(),
                    scac:                  lms_node.get("scac").unwrap_or_default(),
                    status:                lms_node.get("status").unwrap_or_default(),
                    trailer:               lms_node.get("trailer").unwrap_or_default(),
                    trailer2:              lms_node.get("trailer2").unwrap_or_default(),
                    schedule_start_time:   lms_node.get("schedule_start_time").unwrap_or_default(),
                    schedule_arrival_time: lms_node.get("schedule_arrival_time").unwrap_or_default(),
                    actual_start_time:     lms_node.get("actual_start_time").unwrap_or_default(),
                    actual_end_time:       lms_node.get("actual_end_time").unwrap_or_default(),
                    dock_sequence:         lms_node.get("dock_sequence").unwrap_or_default(),
                });
            }
            Ok(Json(data))
        },
        Err(e) => {
            println!("Failed to run get_lms_by_load: {:?}", e);
            Err(Json("Internal Server Error"))
        }
    }
}

#[get("/api/get_dy")]
pub async fn get_dy(
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<DyCommLogEntry>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("
        MATCH (d:DyCommLogEntry)
        return d
    ");

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut data: Vec<DyCommLogEntry> = Vec::<DyCommLogEntry>::new();
            while let Ok(Some(record)) = result.next().await {
                let dy_node: Node = record.get("d").unwrap();
                let entry = DyCommLogEntry {
                    loadNum: dy_node.get("loadNum").unwrap_or_default(),
                    trailer: dy_node.get("trailer").unwrap_or_default(),
                    scac:  dy_node.get("scac").unwrap_or_default(),
                    route: dy_node.get("route").unwrap_or_default(),
                    dock:  dy_node.get("dock").unwrap_or_default(),
                    location: dy_node.get("location").unwrap_or_default(),
                    deliveryDate: dy_node.get("deliveryDate").unwrap_or_default(),
                    deliveryTime: dy_node.get("deliveryTime").unwrap_or_default(),
                    supplier: dy_node.get("supplier").unwrap_or_default(),
                    part: dy_node.get("part").unwrap_or_default(),
                    pdt: dy_node.get("pdt").unwrap_or_default(),
                    createdBy: dy_node.get("createdBy").unwrap_or_default(),
                };
                data.push(entry);
            }
            Ok(Json(data))
        }
        Err(e) => {
            println!("{:?}", e);
            return Err(Json("Failed to retrieve delivered records"));
        }
    }
}

#[get("/api/get_io")]
pub async fn get_io(
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<IOResponse>>, Json<&'static str>> {
    
    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let query = query("
        MATCH (t:Trailer)-[:HAS_SCHEDULE]->(s:Schedule)
        with t,s
        MATCH(t)-[:HAS_SID]->(sid:SID)
        with distinct t, s, sid
        MATCH(t)-[:CONTAINS_PART]->(p:Part)
        return t.id as trailer, s, COLLECT(DISTINCT sid.id) as sids, COLLECT(DISTINCT p.number) as parts
    ");

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut data: Vec<IOResponse> = Vec::<IOResponse>::new();
            while let Ok(Some(record)) = result.next().await {
                let trailer: String = record.get("trailer").unwrap();
                let lms_node: Node = record.get("s").unwrap();
                    let OriginalDate: String = lms_node.get("OriginalDate").unwrap_or_default();
                    let ScheduleDate: String = lms_node.get("ScheduleDate").unwrap_or_default();
                    let ScheduleTime: String = lms_node.get("ScheduleTime").unwrap_or_default();
                    let Comments: String = lms_node.get("Comments").unwrap_or_default();
                    let Destination: String = lms_node.get("Destination").unwrap_or_default();
                    let Status: String = lms_node.get("Status").unwrap_or_default();
                    let Location: String = lms_node.get("Location").unwrap_or_default();
                    let Scac: String = lms_node.get("Scac").unwrap_or_default();
                    let Supplier: String = lms_node.get("Supplier").unwrap_or_default();
                let s = Schedule {
                    TrailerID: trailer.clone(),
                    OriginalDate,
                    ScheduleDate,
                    ScheduleTime,
                    Comments,
                    Destination,
                    Status,
                    Location,
                    Supplier,
                    Scac,
                };
                let parts = record.get::<Vec<String>>("parts")
                    .unwrap_or_else(|_| {
                        println!("Failed to extract parts");
                        Vec::new()
                    });
                let sids = record.get::<Vec<String>>("sids")
                    .unwrap_or_else(|_| {
                        println!("Failed to extract parts");
                        Vec::new()
                    });
                let d = IOResponse {
                    Trailer: trailer,
                    Schedule: s,
                    Sids: sids,
                    Parts: parts,
                };
                data.push(d);
            }
            Ok(Json(data))
        },
        Err(e) => {
            println!("Failed to run query: {:?}", e);
            Err(Json("Internal Server Error"))
        }
    }
}

#[get("/api/get_exceptions")]
pub async fn get_exceptions(
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<ExceptionLogEntry>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let query = query("
        MATCH (e:ExceptionLogEntry)
        RETURN e
    ");

    match graph.execute(query).await {
        Ok(mut result) => {
            let mut entries = Vec::<ExceptionLogEntry>::new();

            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("e").map_err(|_| {
                    Json("Failed to get node from record")
                })?;

                let record = ExceptionLogEntry {
                    loadNum:      node.get("loadNum").unwrap_or_default(),
                    dock:         node.get("dock").unwrap_or_default(),
                    r#type:       node.get("type").unwrap_or_default(),
                    status:       node.get("status").unwrap_or_default(),
                    route:        node.get("route").unwrap_or_default(),
                    scac:         node.get("scac").unwrap_or_default(),
                    trailer1:     node.get("trailer1").unwrap_or_default(),
                    trailer2:     node.get("trailer2").unwrap_or_default(),
                    supplier:     node.get("supplier").unwrap_or_default(),
                    dockSequence: node.get("dockSequence").unwrap_or_default(),
                    originalDate: node.get("originalDate").unwrap_or_default(),
                    originalTime: node.get("originalTime").unwrap_or_default(),
                    newDate:      node.get("newDate").unwrap_or_default(),
                    newTime:      node.get("newTime").unwrap_or_default(),
                    newEndDate:   node.get("newEndDate").unwrap_or_default(),
                    newEndTime:   node.get("newEndTime").unwrap_or_default(),
                    comment:         node.get("comment").unwrap_or_default(),
                    requestor:       node.get("requestor").unwrap_or_default(),
                    isRepower:       node.get("isRepower").unwrap_or(false),
                    repowerLoadNum:  node.get("repowerLoadNum").unwrap_or_default(),
                };
                entries.push(record);
            }

            Ok(Json(entries))
        }
        Err(e) => {
            eprintln!("Failed to get exception log entries: {:?}", e);
            Err(Json("Failed to get exception log entries"))
        }
    }
}

#[post("/api/get_delivered", format = "json", data = "<dates>")]
pub async fn get_delivered(
    dates: Json<DeliveredRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<Delivered>>, Json<&'static str>> {  
    
    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let mut created_lines = Vec::<Delivered>::new();  // ← mut

    let q = query("
        MATCH (dt:DeliveredTrailer)
        WHERE dt.delivery_date >= $date1 AND dt.delivery_date <= $date2
        RETURN dt
    ")
    .param("date1", dates.date1.clone())
    .param("date2", dates.date2.clone());

    match graph.execute(q).await {
        Ok(mut result) => {
            while let Ok(Some(row)) = result.next().await {  // ← while let to get all rows
                let node: Node = row.get("dt").map_err(|_| Json("Failed to get delivered node"))?;  // ← "dt" not "d"

                let trailer = Delivered {
                    trailer_id:    node.get("trailer_id").unwrap_or_default(),
                    delivery_date: node.get("delivery_date").unwrap_or_default(),
                    Comments:      node.get("Comments").unwrap_or_default(),
                    Destination:   node.get("Destination").unwrap_or_default(),
                    OriginalDate:  node.get("OriginalDate").unwrap_or_default(),
                    ScheduleDate:  node.get("ScheduleDate").unwrap_or_default(),
                    ScheduleTime:  node.get("ScheduleTime").unwrap_or_default(),
                    Status:        node.get("Status").unwrap_or_default(),
                    Supplier:      node.get("Supplier").unwrap_or_default(),
                    Scac:          node.get("Scac").unwrap_or_default(),
                    parts:         node.get("parts").unwrap_or_default(),
                    sids:          node.get("sids").unwrap_or_default(),
                };

                created_lines.push(trailer);
            }
        }
        Err(e) => {
            eprintln!("Failed to retrieve delivered records: {:?}", e);
            return Err(Json("Failed to retrieve delivered records"));
        }
    }

    Ok(Json(created_lines))  // ← wrap in Json
}

#[get("/api/get_trailers_grouped")]
pub async fn get_trailers_grouped(
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<serde_json::Value>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let query = query("
        MATCH (t:TrailerEntry)
        RETURN t
    ");

    match graph.execute(query).await {
        Ok(mut result) => {
            // Build: opDate -> shift -> dock -> Vec<TrailerEntry>
            let mut groups: HashMap<String, HashMap<String, HashMap<String, Vec<TrailerEntry>>>> = HashMap::new();

            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("t").map_err(|_| Json("Failed to get node"))?;

                let record = TrailerEntry {
                    loadNo:       node.get("loadNo").unwrap_or_default(),
                    routePrefix:  node.get("routePrefix").unwrap_or_default(),
                    routeId:      node.get("routeId").unwrap_or_default(),
                    status:       node.get("status").unwrap_or_default(),
                    stat2:        node.get("stat2").unwrap_or_default(),
                    scac:         node.get("scac").unwrap_or_default(),
                    trailer:      node.get("trailer").unwrap_or_default(),
                    rNote:        node.get("rNote").unwrap_or_default(),
                    schedArrival: node.get("schedArrival").unwrap_or_default(),
                    schedDepart:  node.get("schedDepart").unwrap_or_default(),
                    location:     node.get("location").unwrap_or_default(),
                    acctorId:     node.get("acctorId").unwrap_or_default(),
                };

                let op_date = get_op_date(&record.schedArrival);
                let shift = match DateTime::parse_from_rfc3339(&record.schedArrival) {
                    Ok(dt) => get_shift(dt.with_timezone(&Utc).hour()).to_string(),
                    Err(_) => "Unknown".to_string(),
                };
                let dock = get_dock(&record.acctorId, &record.location);

                groups
                    .entry(op_date)
                    .or_default()
                    .entry(shift)
                    .or_default()
                    .entry(dock)
                    .or_default()
                    .push(record);
            }

            // Sort trailers within each dock by schedArrival
            for date_group in groups.values_mut() {
                for shift_group in date_group.values_mut() {
                    for dock_trailers in shift_group.values_mut() {
                        dock_trailers.sort_by(|a, b| {
                            let ta = DateTime::parse_from_rfc3339(&a.schedArrival)
                                .map(|d| d.timestamp()).unwrap_or(0);
                            let tb = DateTime::parse_from_rfc3339(&b.schedArrival)
                                .map(|d| d.timestamp()).unwrap_or(0);
                            ta.cmp(&tb)
                        });
                    }
                }
            }

            // Sort dates newest first, Unknown/Invalid last
            let shift_order = ["3rd", "1st", "2nd"];
            let mut sorted_dates: Vec<String> = groups.keys().cloned().collect();
            sorted_dates.sort_by(|a, b| {
                let a_bad = a == "Unknown" || a == "Invalid Date";
                let b_bad = b == "Unknown" || b == "Invalid Date";
                if a_bad && b_bad { return std::cmp::Ordering::Equal; }
                if a_bad { return std::cmp::Ordering::Greater; }
                if b_bad { return std::cmp::Ordering::Less; }
                let da = DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", a))
                    .map(|d| d.timestamp()).unwrap_or(0);
                let db = DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", b))
                    .map(|d| d.timestamp()).unwrap_or(0);
                db.cmp(&da) // newest first
            });

            // Build sorted JSON output
            let mut output = serde_json::Map::new();
            for date in &sorted_dates {
                let shift_map = &groups[date];
                let mut sorted_shifts: Vec<&String> = shift_map.keys().collect();
                sorted_shifts.sort_by_key(|s| {
                    shift_order.iter().position(|&x| x == s.as_str()).unwrap_or(99)
                });

                let mut shift_output = serde_json::Map::new();
                for shift in sorted_shifts {
                    let dock_map = &shift_map[shift];
                    let mut sorted_docks: Vec<&String> = dock_map.keys().collect();
                    sorted_docks.sort();

                    let mut dock_output = serde_json::Map::new();
                    for dock in sorted_docks {
                        dock_output.insert(
                            dock.clone(),
                            serde_json::to_value(&dock_map[dock]).unwrap_or_default()
                        );
                    }
                    shift_output.insert(shift.clone(), serde_json::Value::Object(dock_output));
                }
                output.insert(date.clone(), serde_json::Value::Object(shift_output));
            }

            Ok(Json(serde_json::Value::Object(output)))
        }
        Err(e) => {
            eprintln!("Failed to get grouped trailers: {:?}", e);
            Err(Json("Failed to get grouped trailers"))
        }
    }
}

#[get("/api/get_past_shift/<operational_date>")]
pub async fn get_past_shift(
    operational_date: String,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<TrailerRecord>>, Json<&'static str>> {
    let graph = &state.graph;
    let op_date = operational_date.split("-").collect::<Vec<&str>>();
    let formatted = op_date[0..3].join("-");
    let shift = op_date[3];
    println!("Fetching past shift for date: {}, shift: {}", formatted, shift);
    let q = query("
        MATCH (o:OpDate {date: $formatted})-[:HAS_TRAILER]->(r:TrailerRecord)
        WHERE r.dateShift = $operational_date
        RETURN r
    ")
    .param("formatted", formatted)
    .param("operational_date", operational_date);

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut records: Vec<TrailerRecord> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("r").map_err(|_| Json("Failed to get node"))?;
                records.push(TrailerRecord {
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
                    editRef:           node.get("editRef").unwrap_or_default(),
                    door:              node.get("door").unwrap_or_default(),
                    doorArrivalTime:   node.get("doorArrivalTime").unwrap_or_default(),
                });
            }
            if role.0.contains("univ") {
                records.retain(|r| r.dockCode == "U");
            }
            if role.0.contains("vaa") {
                records.retain(|r| r.dockCode == "V");
            }
            Ok(Json(records))
        }
        Err(e) => {
            eprintln!("Failed to fetch past shift: {:?}", e);
            Err(Json("Failed to fetch past shift"))
        }
    }
}

#[get("/api/get_live_trailers")]
pub async fn get_live_trailers(
    state: &State<AppState>,
    user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<TrailerRecord>>, Json<&'static str>> {
    let graph = &state.graph;

    let q = if role.0.contains("vaa") {
        query("MATCH (t:LiveTrailer) WHERE t.dockCode = 'V' RETURN t")
    } else if role.0.contains("univ") {
        query("MATCH (t:LiveTrailer) WHERE t.dockCode = 'U' RETURN t")
    } else {
        query("MATCH (t:LiveTrailer) RETURN t")
    };

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut records: Vec<TrailerRecord> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("t").map_err(|_| Json("Failed to get node"))?;
                records.push(TrailerRecord {
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
                });
            }

            // Generate a fresh, per-user editRef for each trailer and store the mapping.
            {
                let mut edit_refs = state.edit_refs.lock().await;
                let user_map = edit_refs
                    .entry(user.0.username.clone())
                    .or_insert_with(HashMap::new);
                user_map.clear();
                for record in &mut records {
                    let edit_ref = uuid::Uuid::new_v4().to_string();
                    user_map.insert(edit_ref.clone(), record.uuid.clone());
                    record.editRef = edit_ref;
                }
            }

            Ok(Json(records))
        }
        Err(e) => {
            eprintln!("Failed to fetch live trailers: {:?}", e);
            Err(Json("Failed to fetch live trailers"))
        }
    }
}

#[get("/api/get_staged_trailers")]
pub async fn get_staged_trailers(
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<TrailerRecord>>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (t:StagedTrailer) RETURN t
        UNION
        MATCH (t:LiveTrailer) WHERE (t.actualEndTime = '' OR t.actualEndTime IS NULL) AND t.gateArrivalTime <> '' RETURN t
    ");

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut records: Vec<TrailerRecord> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("t").map_err(|_| Json("Failed to get node"))?;
                records.push(TrailerRecord {
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
                    editRef:           node.get("editRef").unwrap_or_default(),
                    door:              node.get("door").unwrap_or_default(),
                    doorArrivalTime:   node.get("doorArrivalTime").unwrap_or_default(),
                });
            }
            if role.0.contains("univ") {
                records.retain(|r| r.dockCode == "U");
            }
            if role.0.contains("vaa") {
                records.retain(|r| r.dockCode == "V");
            }
            Ok(Json(records))
        }
        Err(e) => {
            eprintln!("Failed to fetch staged trailers: {:?}", e);
            Err(Json("Failed to fetch staged trailers"))
        }
    }
}

#[get("/api/get_users")]
pub async fn get_users(
    state: &State<AppState>,
    role:  Role,
) -> Result<Json<Vec<ShiftAssignment>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("MATCH (u:User) RETURN u");

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut users = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("u").map_err(|_| Json("Failed to get user node"))?;
                users.push(ShiftAssignment {
                    user_name: node.get("name").unwrap_or_default(),
                    role:      node.get("role").unwrap_or_default(),
                    position:  node.get("position").unwrap_or_default(),
                    full_name: node.get("full_name").unwrap_or_default(),
                    task:      String::new(),
                    task_type: String::new(),
                });
            }
            Ok(Json(users))
        }
        Err(e) => {
            eprintln!("Failed to fetch users: {:?}", e);
            Err(Json("Failed to fetch users"))
        }
    }
}

#[get("/api/get_week/<start_date>")]
pub async fn get_week(
    start_date: String,
    state: &State<AppState>,
) -> Result<Json<WeekSchedule>, Json<&'static str>> {
    let graph = &state.graph;

    // ── Query 1: Ensure week structure exists ──
    let setup_query = query("
        MERGE (w:Week {start_date: $start_date})
        WITH w
        UNWIND range(0, 6) AS day_offset
        MERGE (d:Day {date: $start_date + '_' + toString(day_offset), week_start: $start_date})
        SET d.name = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'][day_offset],
            d.offset = day_offset
        MERGE (w)-[:HAS_DAY]->(d)
        WITH d
        UNWIND ['1st','2nd','3rd'] AS shift_name
        MERGE (s:Shift {day_date: d.date, name: shift_name})
        MERGE (d)-[:HAS_SHIFT]->(s)
    ")
    .param("start_date", start_date.clone());

    graph.run(setup_query).await.map_err(|e| {
        eprintln!("Failed to setup week structure: {:?}", e);
        Json("Failed to setup week structure")
    })?;

    // ── Query 2: Fetch week with assignments ──
    let fetch_query = query("
        MATCH (w:Week {start_date: $start_date})-[:HAS_DAY]->(d:Day)-[:HAS_SHIFT]->(s:Shift)
        OPTIONAL MATCH (s)-[r:ASSIGNED]->(u:User)
        RETURN d.date AS date, d.name AS day_name, d.offset AS offset,
               s.name AS shift, s.status AS shift_status, s.reason AS shift_reason,
               collect(u.name) AS assigned_names,
               collect(u.full_name) AS full_name,
               collect(u.role) AS assigned_roles,
               collect(u.position) AS assigned_positions,
               collect(r.task) AS assigned_tasks,
               collect(r.task_type) AS assigned_task_types
        ORDER BY d.offset, s.name
    ")
    .param("start_date", start_date.clone());

    match graph.execute(fetch_query).await {
        Ok(mut result) => {
            let mut day_map: std::collections::BTreeMap<u64, DaySchedule> = std::collections::BTreeMap::new();

            while let Ok(Some(row)) = result.next().await {
                let date:                String      = row.get("date").unwrap_or_default();
                let day_name:            String      = row.get("day_name").unwrap_or_default();
                let offset:              u64         = row.get("offset").unwrap_or_default();
                let shift:               String      = row.get("shift").unwrap_or_default();
                let shift_status:        String      = row.get("shift_status").unwrap_or("active".to_string());
                let shift_reason:        String      = row.get("shift_reason").unwrap_or_default();
                let full_name:           Vec<String> = row.get("full_name").unwrap_or_default();
                let assigned_names:      Vec<String> = row.get("assigned_names").unwrap_or_default();
                let assigned_roles:      Vec<String> = row.get("assigned_roles").unwrap_or_default();
                let assigned_tasks:      Vec<String> = row.get("assigned_tasks").unwrap_or_default();
                let assigned_positions:  Vec<String> = row.get("assigned_positions").unwrap_or_default();
                let assigned_task_types: Vec<String> = row.get("assigned_task_types").unwrap_or_default();

                let assigned = assigned_names.into_iter()
                    .zip(full_name.into_iter())
                    .zip(assigned_roles.into_iter())
                    .zip(assigned_positions.into_iter())
                    .zip(assigned_tasks.into_iter())
                    .zip(assigned_task_types.into_iter())
                    .map(|(((((name, full_name), role), position), task), task_type)| ShiftAssignment {
                        user_name: name, full_name, role, position, task, task_type
                    })
                    .collect();

                let day = day_map.entry(offset).or_insert(DaySchedule {
                    date:   date.clone(),
                    name:   day_name,
                    shifts: Vec::new(),
                    offset,
                });

                day.shifts.push(ShiftSlot { shift, shift_status, shift_reason, assigned });
            }

            Ok(Json(WeekSchedule {
                start_date,
                days: day_map.into_values().collect(),
            }))
        }
        Err(e) => {
            eprintln!("Failed to fetch week: {:?}", e);
            Err(Json("Failed to fetch week"))
        }
    }
}

#[get("/api/get_shift_detail/<start_date>/<offset>")]
pub async fn get_shift_detail(
    start_date: String,
    offset:     u64,
    state:      &State<AppState>,
    role:       Role,
) -> Result<Json<Vec<ShiftDetail>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph   = &state.graph;
    let day_key = format!("{}_{}", start_date, offset);

    let q = query("
        MATCH (d:Day {date: $day_key})-[:HAS_SHIFT]->(s:Shift)
        OPTIONAL MATCH (s)-[r:ASSIGNED]->(u:User)
        OPTIONAL MATCH (s)-[c:DECK_COVERAGE]->(dk:Deck)
        OPTIONAL MATCH (cu:User {name: c.user_name})
        RETURN s.name   AS shift,
               s.status AS shift_status,
               [x IN collect(DISTINCT {
                   user_name: u.name,
                   full_name: u.full_name,
                   role:      u.role,
                   position:  u.position,
                   task:      r.task,
                   task_type: r.task_type
               }) WHERE x.user_name IS NOT NULL] AS assigned,
               [x IN collect(DISTINCT {
                   deck:      dk.name,
                   user_name: c.user_name,
                   slack_id:  cu.slack_id
               }) WHERE x.deck IS NOT NULL] AS deck_coverage
        ORDER BY s.name
    ")
    .param("day_key", day_key.clone());

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut details: Vec<ShiftDetail> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let shift:         String               = row.get("shift").unwrap_or_default();
                let shift_status:  String               = row.get("shift_status").unwrap_or("active".to_string());
                let assigned:      Vec<ShiftAssignment> = row.get("assigned").unwrap_or_default();
                let deck_coverage: Vec<DeckCoverage>    = row.get("deck_coverage").unwrap_or_default();

                details.push(ShiftDetail {
                    shift,
                    day_key: day_key.clone(),
                    shift_status,
                    assigned,
                    deck_coverage,
                });
            }
            Ok(Json(details))
        }
        Err(e) => {
            eprintln!("Failed to fetch shift detail: {:?}", e);
            Err(Json("Failed to fetch shift detail"))
        }
    }
}

#[get("/api/get_decks")]
pub async fn get_decks(
    state: &State<AppState>,
    role:  Role,
) -> Result<Json<Vec<String>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("MATCH (d:Deck) RETURN d.name AS name ORDER BY d.name");

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut decks = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let name: String = row.get("name").unwrap_or_default();
                decks.push(name);
            }
            Ok(Json(decks))
        }
        Err(e) => {
            eprintln!("Failed to fetch decks: {:?}", e);
            Err(Json("Failed to fetch decks"))
        }
    }
}

#[get("/api/saturday_counts/<start_date>")]
pub async fn saturday_counts(
    start_date: String,
    state:      &State<AppState>,
    _user:      AuthenticatedUser,
    role:       Role,
) -> Result<Json<Vec<SaturdayCount>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    // Generate the last 5 week start dates from the given start_date
    let base = chrono::NaiveDate::parse_from_str(&start_date, "%Y-%m-%d")
        .map_err(|_| Json("Invalid date format"))?;

    let week_starts: Vec<String> = (0..5)
        .map(|i| (base - chrono::Duration::weeks(i)).to_string())
        .collect();

    let q = query("
        UNWIND $week_starts AS week_start
        MATCH (w:Week {start_date: week_start})-[:HAS_DAY]->(d:Day)
            -[:HAS_SHIFT]->(s:Shift)-[:ASSIGNED]->(u:User)
        WHERE d.name IN ['Saturday', 'Monday']
        RETURN u.name AS user_name, u.full_name AS full_name,
            u.position AS position, u.shift AS shift,
            count(DISTINCT CASE WHEN d.name = 'Saturday' THEN d END) AS saturdays_worked,
            count(DISTINCT CASE WHEN d.name = 'Monday'   THEN d END) AS mondays_worked
        ORDER BY saturdays_worked DESC
    ")
    .param("week_starts", week_starts.clone());

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut counts: Vec<SaturdayCount> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let saturdays_worked: u64 = row.get("saturdays_worked").unwrap_or_default();
                let mondays_worked:   u64 = row.get("mondays_worked").unwrap_or_default();
                counts.push(SaturdayCount {
                    user_name:        row.get("user_name").unwrap_or_default(),
                    full_name:        row.get("full_name").unwrap_or_default(),
                    position:         row.get("position").unwrap_or_default(),
                    shift:            row.get("shift").unwrap_or_default(),
                    saturdays_worked,
                    saturdays_off:    5u64.saturating_sub(saturdays_worked),
                    mondays_worked,
                    mondays_off:      5u64.saturating_sub(mondays_worked),
                });
            }
            Ok(Json(counts))
        }
        Err(e) => {
            eprintln!("Failed to fetch saturday counts: {:?}", e);
            Err(Json("Failed to fetch saturday counts"))
        }
    }
}

#[get("/api/get_users_admin")]
pub async fn get_users_admin(
    state:  &State<AppState>,
    _guard: AdminOrManager,
    role: Role,
) -> Result<Json<Vec<UpdateUserRequest>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }
    if !role.0.contains("manager") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("MATCH (u:User) RETURN u ORDER BY u.full_name");

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut users = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("u").map_err(|_| Json("Failed to get user node"))?;
                users.push(UpdateUserRequest {
                    name:      node.get("name").unwrap_or_default(),
                    full_name: node.get("full_name").unwrap_or_default(),
                    position:  node.get("position").unwrap_or_default(),
                    role:      node.get("role").unwrap_or_default(),
                    shift:     node.get("shift").unwrap_or_default(),
                    slack_id:  node.get("slack_id").unwrap_or_default(),
                });
            }
            Ok(Json(users))
        }
        Err(e) => {
            eprintln!("Failed to fetch users: {:?}", e);
            Err(Json("Failed to fetch users"))
        }
    }
}

#[get("/api/get_deck_by_route?<route>")]
pub async fn get_deck_by_route(
    route: &str,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<String>, Json<&'static str>> {
    let graph  = &state.graph;
    let prefix = route.to_lowercase();
    let q = neo4rs::query("
        MATCH (r:PartRoute) WHERE toLower(r.route) STARTS WITH $prefix
        MATCH (p:PartASL {part: r.part})
        RETURN p.deck AS deck LIMIT 1
    ").param("prefix", prefix);

    match graph.execute(q).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                Ok(Json(row.get("deck").unwrap_or_default()))
            } else {
                Err(Json("Not found"))
            }
        }
        Err(e) => {
            eprintln!("get_deck_by_route error: {e}");
            Err(Json("Query failed"))
        }
    }
}

#[get("/api/get_deck_assignee_slack?<route>")]
pub async fn get_deck_assignee_slack(
    route:  &str,
    state:  &State<AppState>,
    _user:  AuthenticatedUser,
) -> Result<Json<String>, Json<&'static str>> {
    use chrono::{Local, Timelike};
    let graph  = &state.graph;
    let prefix = route.to_lowercase();
    let now    = Local::now();
    let h      = now.hour();
    let shift  = if h >= 6 && h < 14 { "1st" } else if h >= 14 && h < 22 { "2nd" } else { "3rd" };
    let date   = now.format("%Y-%m-%d").to_string();
    let day_key = format!("{}_0", date);

    let q = neo4rs::query("
        MATCH (r:PartRoute) WHERE toLower(r.route) STARTS WITH $prefix
        MATCH (p:PartASL {part: r.part})
        WITH p.deck AS deck LIMIT 1
        MATCH (d:Day {date: $day_key})-[:HAS_SHIFT]->(s:Shift {name: $shift})
        MATCH (s)-[c:DECK_COVERAGE]->(dk:Deck {name: deck})
        MATCH (u:User {name: c.user_name})
        RETURN u.slack_id AS slack_id
    ")
    .param("prefix", prefix)
    .param("day_key", day_key)
    .param("shift", shift);

    match graph.execute(q).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let slack_id: String = row.get("slack_id").unwrap_or_default();
                Ok(Json(slack_id))
            } else {
                Err(Json("Not found"))
            }
        }
        Err(e) => {
            eprintln!("get_deck_assignee_slack error: {e}");
            Err(Json("Query failed"))
        }
    }
}

#[get("/scan/decks")]
pub async fn get_scan_decks(state: &State<AppState>) -> Json<Vec<String>> {

    let graph = &state.graph;

    let query = neo4rs::query(
        "MATCH (p:PartASL) RETURN DISTINCT p.deck AS deck ORDER BY deck"
    );

    let mut result = match graph.execute(query).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_scan_decks error: {e}");
            return Json(vec![]);
        }
    };

    let mut decks: Vec<String> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(deck) = row.get::<String>("deck") {
            decks.push(deck);
        }
    }

    Json(decks)
}

#[get("/scan/parts?<deck>")]
pub async fn get_scan_parts(
    state: &State<AppState>,
    deck: String,
    role: Role,
) -> Result<Json<Vec<PartASL>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;
    let query = neo4rs::query(
        "MATCH (p:PartASL {deck: $deck}) RETURN p ORDER BY p.doh"
    )
    .param("deck", deck);

    let mut result = match graph.execute(query).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_scan_parts error: {e}");
            return Ok(Json(vec![]));
        }
    };

    let mut parts: Vec<PartASL> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<neo4rs::Node>("p") {
            parts.push(PartASL {
                deck:     node.get("deck").unwrap_or_default(),
                part:     node.get("part").unwrap_or_default(),
                duns:     node.get("duns").unwrap_or_default(),
                supplier: node.get("supplier").unwrap_or_default(),
                doh:      node.get("doh").unwrap_or_default(),
                bank:     node.get("bank").unwrap_or_default(),
                desc:     node.get("desc").unwrap_or_default(),
                cbal:     node.get("cbal").unwrap_or_default(),
                day1:     node.get("day1").ok(),
                day2:     node.get("day2").ok(),
                day3:     node.get("day3").ok(),
                day4:     node.get("day4").ok(),
                day5:     node.get("day5").ok(),
                day6:     node.get("day6").ok(),
            });
        }
    }

    Ok(Json(parts))
}

#[get("/scan/routes?<deck>")]
pub async fn get_scan_routes(
    state: &State<AppState>,
    deck:  String,
    role:  Role,
) -> Result<Json<Vec<DeckRoute>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;
    let q = neo4rs::query("
        MATCH (p:PartASL {deck: $deck})
        MATCH (pr:PartRoute {part: p.part})
        RETURN pr.route AS route, collect(DISTINCT pr.part) AS parts
        ORDER BY pr.route
    ")
    .param("deck", deck);

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut routes: Vec<DeckRoute> = vec![];
            while let Ok(Some(row)) = result.next().await {
                routes.push(DeckRoute {
                    route: row.get("route").unwrap_or_default(),
                    parts: row.get("parts").unwrap_or_default(),
                });
            }
            Ok(Json(routes))
        }
        Err(e) => {
            eprintln!("get_scan_routes error: {e}");
            Ok(Json(vec![]))
        }
    }
}

#[get("/scan/asn?<deck>&<part>")]
pub async fn get_scan_asn(
    state: &State<AppState>,
    deck: String,
    part: String,
) -> Json<Vec<PartASN>> {
    let graph = &state.graph;
    let query = neo4rs::query(
        "MATCH (a:PartASN {deck: $deck, part: $part}) RETURN a ORDER BY a.eta"
    )
    .param("deck", deck)
    .param("part", part);

    let mut result = match graph.execute(query).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_scan_asn error: {e}");
            return Json(vec![]);
        }
    };

    let mut asns: Vec<PartASN> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<neo4rs::Node>("a") {
            asns.push(PartASN {
                scac:         node.get("scac").unwrap_or_default(),
                trailer:      node.get("trailer").unwrap_or_default(),
                deck:         node.get("deck").unwrap_or_default(),
                part:         node.get("part").unwrap_or_default(),
                mode:         node.get("mode").unwrap_or_default(),
                duns:         node.get("duns").unwrap_or_default(),
                quantity:     node.get("quantity").unwrap_or_default(),
                status:       node.get("status").unwrap_or_default(),
                sid:          node.get("sid").unwrap_or_default(),
                countComment: node.get("countComment").unwrap_or_default(),
                shipComment:  node.get("shipComment").unwrap_or_default(),
                shipDate:     node.get("shipDate").unwrap_or_default(),
                dock:         node.get("dock").unwrap_or_default(),
                eda:          node.get("eda").unwrap_or_default(),
                eta:          node.get("eta").unwrap_or_default(),
            });
        }
    }

    Json(asns)
}

#[get("/scan/asn/deck?<deck>")]
pub async fn get_scan_asn_deck(
    state: &State<AppState>,
    deck:  String,
) -> Json<Vec<PartASN>> {
    let graph = &state.graph;

    let query = neo4rs::query(
        "MATCH (a:PartASN {deck: $deck}) RETURN a ORDER BY a.eta"
    )
    .param("deck", deck);

    let mut result = match graph.execute(query).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_scan_asn_deck error: {e}");
            return Json(vec![]);
        }
    };

    let mut asns: Vec<PartASN> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<neo4rs::Node>("a") {
            asns.push(PartASN {
                scac:         node.get("scac").unwrap_or_default(),
                trailer:      node.get("trailer").unwrap_or_default(),
                deck:         node.get("deck").unwrap_or_default(),
                part:         node.get("part").unwrap_or_default(),
                mode:         node.get("mode").unwrap_or_default(),
                duns:         node.get("duns").unwrap_or_default(),
                quantity:     node.get("quantity").unwrap_or_default(),
                status:       node.get("status").unwrap_or_default(),
                sid:          node.get("sid").unwrap_or_default(),
                countComment: node.get("countComment").unwrap_or_default(),
                shipComment:  node.get("shipComment").unwrap_or_default(),
                shipDate:     node.get("shipDate").unwrap_or_default(),
                dock:         node.get("dock").unwrap_or_default(),
                eda:          node.get("eda").unwrap_or_default(),
                eta:          node.get("eta").unwrap_or_default(),
            });
        }
    }

    Json(asns)
}

#[get("/api/get_edock_asl")]
pub async fn get_edock_asl(state: &State<AppState>) -> Json<Vec<PartASL>> {
    let graph = &state.graph;

    let q = neo4rs::query(
        "
         MATCH (p:PartASN) WHERE p.dock IN ['EEEDY', 'EEE', 'E01']
         WITH DISTINCT p.part AS part_num
         MATCH (l:PartASL {part: part_num})
         RETURN l
        "
    );

    let mut result = match graph.execute(q).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_edock error: {e}");
            return Json(vec![]);
        }
    };

    let mut parts: Vec<PartASL> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<neo4rs::Node>("l") {
            parts.push(PartASL {
                deck:     node.get("deck").unwrap_or_default(),
                part:     node.get("part").unwrap_or_default(),
                duns:     node.get("duns").unwrap_or_default(),
                supplier: node.get("supplier").unwrap_or_default(),
                desc:     node.get("desc").unwrap_or_default(),
                doh:      node.get::<f64>("doh").unwrap_or_default(),
                bank:     node.get::<i64>("bank").unwrap_or_default() as u32,
                cbal:     node.get::<f64>("cbal").unwrap_or_default(),
                day1:     node.get::<f64>("day1").ok(),
                day2:     node.get::<f64>("day2").ok(),
                day3:     node.get::<f64>("day3").ok(),
                day4:     node.get::<f64>("day4").ok(),
                day5:     node.get::<f64>("day5").ok(),
                day6:     node.get::<f64>("day6").ok(),
            });
        }
    }

    Json(parts)
}

#[get("/api/get_edock_asn")]
pub async fn get_edock_asn(state: &State<AppState>) -> Json<Vec<PartASN>> {
    let graph = &state.graph;

    let q = neo4rs::query(
        "MATCH (p:PartASN) WHERE p.dock = 'EEEDY' OR p.dock = 'EEE' OR p.dock = 'E01' RETURN p"
    );

    let mut result = match graph.execute(q).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_edock error: {e}");
            return Json(vec![]);
        }
    };

    let mut parts: Vec<PartASN> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<neo4rs::Node>("p") {
            parts.push(PartASN {
                scac:         node.get("scac").unwrap_or_default(),
                trailer:      node.get("trailer").unwrap_or_default(),
                deck:         node.get("deck").unwrap_or_default(),
                part:         node.get("part").unwrap_or_default(),
                mode:         node.get("mode").unwrap_or_default(),
                duns:         node.get("duns").unwrap_or_default(),
                sid:          node.get("sid").unwrap_or_default(),
                countComment: Some(node.get("countComment").unwrap_or_default()),
                shipComment:  node.get("shipComment").unwrap_or_default(),
                shipDate:     node.get("shipDate").unwrap_or_default(),
                dock:         node.get("dock").unwrap_or_default(),
                eda:          node.get("eda").unwrap_or_default(),
                eta:          node.get("eta").unwrap_or_default(),
                quantity:     Some(node.get::<f64>("quantity").unwrap_or_default()),
                status:       Some(node.get::<i64>("status").unwrap_or_default() as u32),
            });
        }
    }

    Json(parts)
}

#[get("/api/get_part_asn")]
pub async fn get_part_asn(state: &State<AppState>) -> Json<Vec<PartASN>> {
    let graph = &state.graph;

    let q = neo4rs::query(
        "MATCH (p:PartASN) RETURN p"
    );

    let mut result = match graph.execute(q).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_part_asn error: {e}");
            return Json(vec![]);
        }
    };

    let mut parts: Vec<PartASN> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<neo4rs::Node>("p") {
            parts.push(PartASN {
                scac:         node.get("scac").unwrap_or_default(),
                trailer:      node.get("trailer").unwrap_or_default(),
                deck:         node.get("deck").unwrap_or_default(),
                part:         node.get("part").unwrap_or_default(),
                mode:         node.get("mode").unwrap_or_default(),
                duns:         node.get("duns").unwrap_or_default(),
                sid:          node.get("sid").unwrap_or_default(),
                countComment: Some(node.get("countComment").unwrap_or_default()),
                shipComment:  node.get("shipComment").unwrap_or_default(),
                shipDate:     node.get("shipDate").unwrap_or_default(),
                dock:         node.get("dock").unwrap_or_default(),
                eda:          node.get("eda").unwrap_or_default(),
                eta:          node.get("eta").unwrap_or_default(),
                quantity:     Some(node.get::<f64>("quantity").unwrap_or_default()),
                status:       Some(node.get::<i64>("status").unwrap_or_default() as u32),
            });
        }
    }

    Json(parts)
}

#[get("/api/get_part_alerts")]
pub async fn get_part_alerts(
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Json<Vec<PartAlert>> {
    Json(state.current_alerts.lock().await.clone())
}

#[get("/api/get_contacts")]
pub async fn get_contacts(
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Json<Vec<Contact>> {
    let graph = &state.graph;

    let q = query("MATCH (c:Contact) RETURN c");

    let mut result = match graph.execute(q).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_contacts error: {e}");
            return Json(vec![]);
        }
    };

    let mut contacts: Vec<Contact> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<Node>("c") {
            contacts.push(Contact {
                email: node.get("email").unwrap_or_default(),
                name:  node.get("name").unwrap_or_default(),
                phone: node.get("phone").unwrap_or_default(),
                duns:  node.get("duns").unwrap_or_default(),
                scac:  node.get("scac").unwrap_or_default(),
            });
        }
    }

    Json(contacts)
}

#[get("/api/get_carriers")]
pub async fn get_carriers(
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Json<Vec<String>> {
    let graph = &state.graph;

    let q = query("MATCH (c:Carrier) RETURN c.scac AS scac ORDER BY c.scac");

    let mut result = match graph.execute(q).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_carriers error: {e}");
            return Json(vec![]);
        }
    };

    let mut carriers: Vec<String> = vec![];
    while let Ok(Some(row)) = result.next().await {
        carriers.push(row.get("scac").unwrap_or_default());
    }

    Json(carriers)
}

#[get("/api/get_route_contacts?<route>")]
pub async fn get_route_contacts(
    state: &State<AppState>,
    route: String,
    _user: AuthenticatedUser,
) -> Result<Json<Vec<DunsContacts>>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (r:Route {route: $route})-[:HAS_DUNS]->(d:Duns)
        OPTIONAL MATCH (d)-[:HAS_CONTACT]->(c:Contact)
        RETURN d.duns AS duns,
               [x IN collect(DISTINCT {
                   email: c.email,
                   name:  c.name,
                   phone: c.phone,
                   duns:  c.duns
               }) WHERE x.email IS NOT NULL] AS contacts
        ORDER BY d.duns
    ")
    .param("route", route);

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut groups: Vec<DunsContacts> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                groups.push(DunsContacts {
                    duns:     row.get("duns").unwrap_or_default(),
                    contacts: row.get("contacts").unwrap_or_default(),
                });
            }
            Ok(Json(groups))
        }
        Err(e) => {
            eprintln!("Failed to get route contacts: {:?}", e);
            Err(Json("Failed to get route contacts"))
        }
    }
}

#[get("/api/get_route_carrier_contacts?<route>")]
pub async fn get_route_carrier_contacts(
    state: &State<AppState>,
    route: String,
    _user: AuthenticatedUser,
) -> Result<Json<Vec<CarrierContacts>>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (car:Carrier)-[:HAS_ROUTE]->(r:Route {route: $route})
        OPTIONAL MATCH (car)-[:HAS_CONTACT]->(c:Contact)
        RETURN car.scac AS scac,
               [x IN collect(DISTINCT {
                   email: c.email,
                   name:  c.name,
                   phone: c.phone,
                   scac:  c.scac
               }) WHERE x.email IS NOT NULL] AS contacts
        ORDER BY car.scac
    ")
    .param("route", route);

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut groups: Vec<CarrierContacts> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                groups.push(CarrierContacts {
                    scac:     row.get("scac").unwrap_or_default(),
                    contacts: row.get("contacts").unwrap_or_default(),
                });
            }
            Ok(Json(groups))
        }
        Err(e) => {
            eprintln!("Failed to get route carrier contacts: {:?}", e);
            Err(Json("Failed to get route carrier contacts"))
        }
    }
}

#[get("/api/get_part_asl")]
pub async fn get_part_asl(state: &State<AppState>) -> Json<Vec<PartASL>> {
    let graph = &state.graph;

    let q = neo4rs::query(
        "MATCH (p:PartASL) RETURN p"
    );

    let mut result = match graph.execute(q).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("get_part_asl error: {e}");
            return Json(vec![]);
        }
    };

    let mut parts: Vec<PartASL> = vec![];
    while let Ok(Some(row)) = result.next().await {
        if let Ok(node) = row.get::<neo4rs::Node>("p") {
            parts.push(PartASL {
                deck:     node.get("deck").unwrap_or_default(),
                part:     node.get("part").unwrap_or_default(),
                duns:     node.get("duns").unwrap_or_default(),
                supplier: node.get("supplier").unwrap_or_default(),
                doh:      node.get("doh").unwrap_or_default(),
                bank:     node.get("bank").unwrap_or_default(),
                desc:     node.get("desc").unwrap_or_default(),
                cbal:     node.get("cbal").unwrap_or_default(),
                day1:     node.get("day1").ok(),
                day2:     node.get("day2").ok(),
                day3:     node.get("day3").ok(),
                day4:     node.get("day4").ok(),
                day5:     node.get("day5").ok(),
                day6:     node.get("day6").ok(),
            });
        }
    }

    Json(parts)
}

#[get("/api/dock_count?<date>&<hour>&<dock>")]
pub async fn get_dock_count(
    date:  String,
    hour:  String,
    dock:  String,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<DockCountResponse>, Json<&'static str>> {
    let graph = &state.graph;
    let mut shift_total: u32 = 0;

    let hour_u32: u32 = hour.parse().unwrap_or(0);
    let win = get_shift_window(&date, hour_u32);

    // ── Per-hour counts ──
    let hour_dates: Vec<(String, String)> = win.hours1.iter()
        .map(|h| (h.clone(), win.date1.clone()))
        .chain(win.hours2.iter().map(|h| (h.clone(), win.date2.clone())))
        .collect();

    let mut hourly: Vec<HourCount> = Vec::new();

    for (hr, date_for_hr) in &hour_dates {
        let mut count: u32 = 0;

        let q1 = query("
            MATCH (e:ExceptionLogEntry)
            WHERE e.newDate = $date AND e.dock = $dock AND substring(e.newTime, 0, 2) = $hour
            RETURN count(e) AS cnt
        ")
        .param("date", date_for_hr.clone())
        .param("dock", dock.clone())
        .param("hour", hr.clone());

        if let Ok(mut result) = graph.execute(q1).await {
            if let Ok(Some(row)) = result.next().await {
                count += row.get::<i64>("cnt").unwrap_or(0) as u32;
            }
        }

        let q2 = query("
            MATCH (d:DyCommLogEntry)
            WHERE d.deliveryDate = $date AND d.dock = $dock AND substring(d.deliveryTime, 0, 2) = $hour
            RETURN count(d) AS cnt
        ")
        .param("date", date_for_hr.clone())
        .param("dock", dock.clone())
        .param("hour", hr.clone());

        if let Ok(mut result) = graph.execute(q2).await {
            if let Ok(Some(row)) = result.next().await {
                count += row.get::<i64>("cnt").unwrap_or(0) as u32;
            }
        }

        let q3 = query("
            MATCH (l:LMSRecord)
            WHERE l.schedule_arrival_time STARTS WITH $date
              AND l.dock = $dock
              AND substring(l.schedule_arrival_time, 11, 2) = $hour
            RETURN count(l) AS cnt
        ")
        .param("date", date_for_hr.clone())
        .param("dock", dock.clone())
        .param("hour", hr.clone());

        if let Ok(mut result) = graph.execute(q3).await {
            if let Ok(Some(row)) = result.next().await {
                count += row.get::<i64>("cnt").unwrap_or(0) as u32;
            }
        }

        hourly.push(HourCount { hour: hr.clone(), count });
    }

    // ── Shift-window counts ──

    let qs1 = query("
        MATCH (e:ExceptionLogEntry)
        WHERE ((e.newDate = $date1 AND substring(e.newTime, 0, 2) IN $hours1)
            OR (e.newDate = $date2 AND substring(e.newTime, 0, 2) IN $hours2))
          AND e.dock = $dock
        RETURN count(e) AS cnt
    ")
    .param("date1", win.date1.clone())
    .param("hours1", win.hours1.clone())
    .param("date2", win.date2.clone())
    .param("hours2", win.hours2.clone())
    .param("dock", dock.clone());

    if let Ok(mut result) = graph.execute(qs1).await {
        if let Ok(Some(row)) = result.next().await {
            shift_total += row.get::<i64>("cnt").unwrap_or(0) as u32;
        }
    }

    let qs2 = query("
        MATCH (d:DyCommLogEntry)
        WHERE ((d.deliveryDate = $date1 AND substring(d.deliveryTime, 0, 2) IN $hours1)
            OR (d.deliveryDate = $date2 AND substring(d.deliveryTime, 0, 2) IN $hours2))
          AND d.dock = $dock
        RETURN count(d) AS cnt
    ")
    .param("date1", win.date1.clone())
    .param("hours1", win.hours1.clone())
    .param("date2", win.date2.clone())
    .param("hours2", win.hours2.clone())
    .param("dock", dock.clone());

    if let Ok(mut result) = graph.execute(qs2).await {
        if let Ok(Some(row)) = result.next().await {
            shift_total += row.get::<i64>("cnt").unwrap_or(0) as u32;
        }
    }

    let qs3 = query("
        MATCH (l:LMSRecord)
        WHERE ((l.schedule_arrival_time STARTS WITH $date1 AND substring(l.schedule_arrival_time, 11, 2) IN $hours1)
            OR (l.schedule_arrival_time STARTS WITH $date2 AND substring(l.schedule_arrival_time, 11, 2) IN $hours2))
          AND l.dock = $dock
        RETURN count(l) AS cnt
    ")
    .param("date1", win.date1.clone())
    .param("hours1", win.hours1.clone())
    .param("date2", win.date2.clone())
    .param("hours2", win.hours2.clone())
    .param("dock", dock.clone());

    if let Ok(mut result) = graph.execute(qs3).await {
        if let Ok(Some(row)) = result.next().await {
            shift_total += row.get::<i64>("cnt").unwrap_or(0) as u32;
        }
    }

    Ok(Json(DockCountResponse { hourly, shift_total }))
}

#[get("/api/get_hot_parts")]
pub async fn get_hot_parts(
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<Vec<HotPart>>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (h:ActiveHotPart)
        RETURN h
        ORDER BY h.updated_at DESC
    ");

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut records: Vec<HotPart> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("h").map_err(|_| Json("Failed to get node"))?;
                let asn_str: String = node.get("asn_list").unwrap_or_else(|_| "[]".to_string());
                let asn_list: Vec<HotPartAsn> = serde_json::from_str(&asn_str).unwrap_or_default();
                records.push(HotPart {
                    part:       node.get("part").unwrap_or_default(),
                    pdt:        node.get("pdt").unwrap_or_default(),
                    mfu:        node.get("mfu").unwrap_or_default(),
                    comments:   node.get("comments").unwrap_or_default(),
                    updated_at: node.get("updated_at").unwrap_or_default(),
                    asn_list,
                    day1:       node.get("day1").ok(),
                    day2:       node.get("day2").ok(),
                    day3:       node.get("day3").ok(),
                    day4:       node.get("day4").ok(),
                    day5:       node.get("day5").ok(),
                });
            }
            Ok(Json(records))
        }
        Err(e) => {
            eprintln!("Failed to get hot parts: {:?}", e);
            Err(Json("Failed to get hot parts"))
        }
    }
}

#[get("/api/get_audit_events/<op_date>")]
pub async fn get_audit_events(
    op_date: String,
    state:   &State<AppState>,
    _user:   AuthenticatedUser,
    role:    Role,
) -> Result<Json<Vec<AuditEvent>>, Json<&'static str>> {

    if role.0.contains("vaa") || role.0.contains("univ") {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("
        MATCH (o:OpDate {date: $op_date})-[:HAS_AUDIT]->(a:AuditEvent)
        OPTIONAL MATCH (t:LiveTrailer {uuid: a.trailer_uuid})
        OPTIONAL MATCH (r:TrailerRecord {uuid: a.trailer_uuid})
        RETURN a,
            CASE
                WHEN a.trailer_uuid = '' THEN ''
                WHEN t IS NOT NULL THEN t.lmsAccent + '/' + t.trailer1
                WHEN r IS NOT NULL THEN r.lmsAccent + '/' + r.trailer1
                ELSE ''
            END AS trailer_label
        ORDER BY a.timestamp ASC
    ")
    .param("op_date", op_date);

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut events: Vec<AuditEvent> = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("a").map_err(|_| Json("Failed to get audit node"))?;
                let field: String = node.get("field").unwrap_or_default();
                let event_type = get_event_type(&field).to_string();
                events.push(AuditEvent {
                    trailer_uuid: row.get("trailer_label").unwrap_or_default(),
                    field,
                    old_value:    node.get("old_value").unwrap_or_default(),
                    new_value:    node.get("new_value").unwrap_or_default(),
                    timestamp:    node.get("timestamp").unwrap_or_default(),
                    updated_by:   node.get("updated_by").unwrap_or_default(),
                    event_type,
                });
            }
            Ok(Json(events))
        }
        Err(e) => {
            eprintln!("Failed to get audit events: {:?}", e);
            Err(Json("Failed to get audit events"))
        }
    }
}

#[get("/api/get_part_routes")]
pub async fn get_part_routes(
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<Vec<PartRoute>>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("MATCH (p:PartRoute) OPTIONAL MATCH (a:PartASL {part: p.part}) RETURN p.part AS part, p.duns AS duns, p.route AS route, p.desc AS desc, p.deck AS deck, p.dock AS dock, p.country AS country, a.supplier AS supplier, a.doh AS doh");

    match graph.execute(q).await {
        Ok(mut result) => {
            let mut parts = Vec::new();
            while let Ok(Some(row)) = result.next().await {
                parts.push(PartRoute {
                    part:     row.get("part").unwrap_or_default(),
                    duns:     row.get("duns").unwrap_or_default(),
                    route:    row.get("route").unwrap_or_default(),
                    desc:     row.get("desc").unwrap_or_default(),
                    deck:     row.get("deck").unwrap_or_default(),
                    dock:     row.get("dock").unwrap_or_default(),
                    country:  row.get("country").unwrap_or_default(),
                    supplier: row.get("supplier").unwrap_or_default(),
                    doh:      row.get("doh").ok(),
                });
            }
            Ok(Json(parts))
        }
        Err(e) => {
            eprintln!("Failed to get part routes: {:?}", e);
            Err(Json("Failed to get part routes"))
        }
    }
}