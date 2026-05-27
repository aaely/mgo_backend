use crate::structs::*;
use crate::auth::AuthenticatedUser;
use crate::role::Role;
use tokio_tungstenite::tungstenite::Message;
use rocket::{State, delete, post, serde::json::Json};
use neo4rs::{query, Node};

#[post("/api/update_io", format = "json", data = "<update_io>")]
pub async fn update_io(
    update_io: Json<IOResponse>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<IOResponse>, Json<&'static str>> {
    let graph = &state.graph;

    // ── Query 1: Update Schedule and Trailer ID ──
    let schedule_query = query("
        MATCH (t:Trailer {id: $old_trailer})-[:HAS_SCHEDULE]->(s:Schedule)
        SET
            t.id           = $new_trailer,
            s.TrailerID    = $new_trailer,
            s.Comments     = $comments,
            s.Destination  = $destination,
            s.OriginalDate = $original_date,
            s.ScheduleDate = $schedule_date,
            s.ScheduleTime = $schedule_time,
            s.Status       = $status,
            s.Supplier     = $supplier,
            s.Scac         = $scac
        RETURN t, s
    ")
    .param("old_trailer",   update_io.Trailer.clone())
    .param("new_trailer",   update_io.Schedule.TrailerID.clone())
    .param("comments",      update_io.Schedule.Comments.clone())
    .param("destination",   update_io.Schedule.Destination.clone())
    .param("scac",          update_io.Schedule.Scac.clone())
    .param("original_date", update_io.Schedule.OriginalDate.clone())
    .param("supplier",      update_io.Schedule.Supplier.clone())
    .param("schedule_date", update_io.Schedule.ScheduleDate.clone())
    .param("schedule_time", update_io.Schedule.ScheduleTime.clone())
    .param("status",        update_io.Schedule.Status.clone());

    graph.run(schedule_query).await.map_err(|e| {
        eprintln!("Failed to update schedule: {:?}", e);
        Json("Failed to update schedule")
    })?;

    // ── Query 2a: Remove stale Part relationships ──
    let remove_parts_query = query("
        MATCH (t:Trailer {id: $trailer})-[r:CONTAINS_PART]->(p:Part)
        WHERE NOT p.number IN $parts
        DELETE r
    ")
    .param("trailer", update_io.Schedule.TrailerID.clone())
    .param("parts",   update_io.Parts.clone());

    graph.run(remove_parts_query).await.map_err(|e| {
        eprintln!("Failed to remove stale parts: {:?}", e);
        Json("Failed to remove stale parts")
    })?;

    // ── Query 2b: Merge new Part relationships ──
    let add_parts_query = query("
        MATCH (t:Trailer {id: $trailer})
        UNWIND $parts AS part_number
        MERGE (p:Part {number: part_number})
        MERGE (t)-[:CONTAINS_PART]->(p)
    ")
    .param("trailer", update_io.Schedule.TrailerID.clone())
    .param("parts",   update_io.Parts.clone());

    graph.run(add_parts_query).await.map_err(|e| {
        eprintln!("Failed to add parts: {:?}", e);
        Json("Failed to add parts")
    })?;

    // ── Query 3a: Remove stale SID relationships ──
    let remove_sids_query = query("
        MATCH (t:Trailer {id: $trailer})-[r:HAS_SID]->(sid:SID)
        WHERE NOT sid.id IN $sids
        DELETE r
    ")
    .param("trailer", update_io.Schedule.TrailerID.clone())
    .param("sids",    update_io.Sids.clone());

    graph.run(remove_sids_query).await.map_err(|e| {
        eprintln!("Failed to remove stale sids: {:?}", e);
        Json("Failed to remove stale sids")
    })?;

    // ── Query 3b: Merge new SID relationships ──
    let add_sids_query = query("
        MATCH (t:Trailer {id: $trailer})
        UNWIND $sids AS sid_id
        MERGE (sid:SID {id: sid_id})
        MERGE (t)-[:HAS_SID]->(sid)
    ")
    .param("trailer", update_io.Schedule.TrailerID.clone())
    .param("sids",    update_io.Sids.clone());

    graph.run(add_sids_query).await.map_err(|e| {
        eprintln!("Failed to add sids: {:?}", e);
        Json("Failed to add sids")
    })?;

    // ── Query 4: Fetch and return updated IOResponse ──
    let fetch_query = query("
        MATCH (t:Trailer {id: $trailer})-[:HAS_SCHEDULE]->(s:Schedule)
        OPTIONAL MATCH (t)-[:CONTAINS_PART]->(p:Part)
        WITH t, s, COLLECT(p.number) AS parts
        OPTIONAL MATCH (t)-[:HAS_SID]->(sid:SID)
        RETURN t.id AS trailer, s, parts, COLLECT(sid.id) AS sids
    ")
    .param("trailer", update_io.Schedule.TrailerID.clone());

    match graph.execute(fetch_query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let schedule_node: Node = row.get("s").map_err(|_| {
                    Json("Failed to get schedule node")
                })?;

                let schedule = Schedule {
                    Comments:     schedule_node.get("Comments").unwrap_or_default(),
                    Scac:         schedule_node.get("Scac").unwrap_or_default(),
                    Destination:  schedule_node.get("Destination").unwrap_or_default(),
                    OriginalDate: schedule_node.get("OriginalDate").unwrap_or_default(),
                    ScheduleDate: schedule_node.get("ScheduleDate").unwrap_or_default(),
                    ScheduleTime: schedule_node.get("ScheduleTime").unwrap_or_default(),
                    TrailerID:    schedule_node.get("TrailerID").unwrap_or_default(),
                    Status:       schedule_node.get("Status").unwrap_or_default(),
                    Location:     schedule_node.get("Location").unwrap_or_default(),
                    Supplier:     schedule_node.get("Supplier").unwrap_or_default(),
                };

                let parts: Vec<String> = row.get("parts").unwrap_or_else(|_| Vec::new());
                let sids: Vec<String>  = row.get("sids").unwrap_or_else(|_| Vec::new());
                let trailer: String    = row.get("trailer").unwrap_or_default();

                Ok(Json(IOResponse {
                    Trailer:  trailer,
                    Schedule: schedule,
                    Parts:    parts,
                    Sids:     sids,
                }))
            } else {
                Err(Json("No matching trailer found"))
            }
        }
        Err(e) => {
            eprintln!("Failed to fetch updated record: {:?}", e);
            Err(Json("Failed to fetch updated record"))
        }
    }
}

#[post("/api/upload_lms", format = "json", data = "<upload_lms>")]
pub async fn upload_lms(
    upload_lms: Json<Vec<LMSRecord>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<LMSRecord>>, Json<&'static str>> {
    let graph = &state.graph;

    let mut created_lines: Vec<LMSRecord> = Vec::new();
    
    for line in upload_lms.iter() {
        let query = query("
        CREATE(l:LMSRecord {
            load_no: $load_no,
            route_id: $route_id,
            scac: $scac,
            trailer: $trailer,
            trailer2: $trailer2,
            location: $location,
            schedule_arrival_time: $schedule_arrival_time
        })
        RETURN l
    ")
    .param("load_no", line.load_no.clone())
    .param("route_id", line.route_id.clone())
    .param("scac", line.scac.clone())
    .param("trailer", line.trailer.clone())
    .param("trailer2", line.trailer2.clone())
    .param("location", line.location.clone())
    .param("schedule_arrival_time", line.schedule_arrival_time.clone());

        match graph.execute(query).await {
            Ok(mut result) => {
                if let Ok(Some(row)) = result.next().await {
                    let lms_node: Node = row.get("l").map_err(|_| {
                        Json("Failed to get node from record")
                    })?;
                    
                    // Extract all fields
                    let load_no: String = lms_node.get("load_no").unwrap_or_default();
                    let route_id: String = lms_node.get("route_id").unwrap_or_default();
                    let scac: String = lms_node.get("scac").unwrap_or_default();
                    let trailer: String = lms_node.get("trailer").unwrap_or_default();
                    let trailer2: String = lms_node.get("trailer2").unwrap_or_default();
                    let schedule_arrival_time: String = lms_node.get("schedule_arrival_time").unwrap_or_default();
                    let location: String = lms_node.get("location").unwrap_or_default();

                    let record = LMSRecord {
                        load_no,
                        route_id,
                        scac,
                        trailer,
                        trailer2,
                        schedule_arrival_time,
                        location,
                    };
                    created_lines.push(record);
                }
            }
            Err(e) => {
                eprintln!("Failed to create trailer node: {:?}", e);
                return Err(Json("Failed to create trailer record"));
            }
        }
    }
    Ok(Json(created_lines))
}

#[post("/api/upload_in_transit", format = "json", data = "<upload_in_transit>")]
pub async fn upload_in_transit(
    upload_in_transit: Json<Vec<InTransit>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    // ── Query 1: Build Trailer → SID → Part graph ──
    for line in upload_in_transit.iter() {
        let query = query("
            MERGE (trailer:Trailer {id: $trailer})
            MERGE (sid:SID {id: $sid, ciscoID: $cisco})
            ON CREATE SET sid.id = $sid
            MERGE (trailer)-[:HAS_SID]->(sid)
            MERGE (sid)-[:BELONGS_TO]->(trailer)
            MERGE (sid)-[:HAS_PART]->(part:Part {number: $part, quantity: toInteger($quantity), duns: $duns})
        ")
        .param("trailer",  line.trailer.clone())
        .param("sid",      line.sid.clone())
        .param("cisco",    line.cisco.clone())
        .param("part",     line.part.clone())
        .param("quantity", line.quantity.clone())
        .param("duns",     line.duns.clone());

        graph.run(query).await.map_err(|e| {
            eprintln!("Failed to merge trailer/sid/part nodes: {:?}", e);
            Json("Failed to merge trailer/sid/part nodes")
        })?;
    }

    // ── Query 2: Collect unique trailer → destination pairs, then merge Schedule ──
    let mut trailer_destinations: std::collections::HashMap<String, (String, String, String)> = std::collections::HashMap::new();
    for line in upload_in_transit.iter() {
        trailer_destinations
            .entry(line.trailer.clone())
            .or_insert_with(|| (line.destination.clone(), line.supplier.clone(), line.location.clone()));
    }

    for (trailer_id, (destination, supplier, location)) in trailer_destinations.iter() {
        let schedule_query = query("
            MATCH (trailer:Trailer {id: $trailer})
            MERGE (trailer)-[:HAS_SCHEDULE]->(s:Schedule)
            ON CREATE SET
                s.TrailerID    = trailer.id,
                s.Destination  = $destination,
                s.Supplier     = $supplier,
                s.OriginalDate = '',
                s.ScheduleDate = '',
                s.ScheduleTime = '',
                s.Comments     = '',
                s.Status       = '',
                s.Scac         = '',
                s.Location     = $location
            ON MATCH SET
                s.Destination  = $destination,
                s.Supplier     = $supplier,
                s.Location     = $location
        ")
        .param("trailer",     trailer_id.clone())
        .param("destination", destination.clone())
        .param("location",    location.clone())
        .param("supplier",    supplier.clone());

        graph.run(schedule_query).await.map_err(|e| {
            eprintln!("Failed to create/update schedule node: {:?}", e);
            Json("Failed to create/update schedule node")
        })?;
    }

    // ── Query 3: Link Parts directly to Trailers ──
    let parts_query = query("
        MATCH (t:Trailer)-[:HAS_SID]->(sid:SID)-[:HAS_PART]->(p:Part)
        MERGE (t)-[:CONTAINS_PART]->(p)
    ");

    graph.run(parts_query).await.map_err(|e| {
        eprintln!("Failed to merge trailer-part relationships: {:?}", e);
        Json("Failed to merge trailer-part relationships")
    })?;

    Ok(Json("In-transit data uploaded and processed successfully"))
}

#[post("/api/upload_exception", format = "json", data = "<upload_exception>")]
pub async fn upload_exception(
    upload_exception: Json<Vec<ExceptionLogEntry>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<ExceptionLogEntry>>, Json<&'static str>> {
    let graph = &state.graph;
    let mut created_lines = Vec::<ExceptionLogEntry>::new();

    for line in upload_exception.iter() {
        let query = query("
            MERGE (e:ExceptionLogEntry {loadNum: $loadNum, dock: $dock, trailer1: $trailer1})
            ON CREATE SET
                e.type         = $type,
                e.status       = $status,
                e.route        = $route,
                e.scac         = $scac,
                e.trailer2     = $trailer2,
                e.supplier     = $supplier,
                e.dockSequence = $dockSequence,
                e.originalDate = $originalDate,
                e.originalTime = $originalTime,
                e.newDate      = $newDate,
                e.newTime      = $newTime,
                e.newEndDate   = $newEndDate,
                e.newEndTime   = $newEndTime,
                e.comment      = $comment,
                e.requestor    = $requestor
            ON MATCH SET
                e.type         = $type,
                e.status       = $status,
                e.route        = $route,
                e.scac         = $scac,
                e.trailer2     = $trailer2,
                e.supplier     = $supplier,
                e.dockSequence = $dockSequence,
                e.originalDate = $originalDate,
                e.originalTime = $originalTime,
                e.newDate      = $newDate,
                e.newTime      = $newTime,
                e.newEndDate   = $newEndDate,
                e.newEndTime   = $newEndTime,
                e.comment      = $comment,
                e.requestor    = $requestor
            RETURN e
        ")
        .param("loadNum",      line.loadNum.clone())
        .param("dock",         line.dock.clone())
        .param("trailer1",     line.trailer1.clone())
        .param("type",         line.r#type.clone())
        .param("status",       line.status.clone())
        .param("route",        line.route.clone())
        .param("scac",         line.scac.clone())
        .param("trailer2",     line.trailer2.clone())
        .param("supplier",     line.supplier.clone())
        .param("dockSequence", line.dockSequence.clone())
        .param("originalDate", line.originalDate.clone())
        .param("originalTime", line.originalTime.clone())
        .param("newDate",      line.newDate.clone())
        .param("newTime",      line.newTime.clone())
        .param("newEndDate",   line.newEndDate.clone())
        .param("newEndTime",   line.newEndTime.clone())
        .param("comment",      line.comment.clone())
        .param("requestor",    line.requestor.clone());

        match graph.execute(query).await {
            Ok(mut result) => {
                if let Ok(Some(row)) = result.next().await {
                    let e: Node = row.get("e").map_err(|_| {
                        Json("Failed to get node from record")
                    })?;

                    let record = ExceptionLogEntry {
                        loadNum:      e.get("loadNum").unwrap_or_default(),
                        dock:         e.get("dock").unwrap_or_default(),
                        r#type:       e.get("type").unwrap_or_default(),
                        status:       e.get("status").unwrap_or_default(),
                        route:        e.get("route").unwrap_or_default(),
                        scac:         e.get("scac").unwrap_or_default(),
                        trailer1:     e.get("trailer1").unwrap_or_default(),
                        trailer2:     e.get("trailer2").unwrap_or_default(),
                        supplier:     e.get("supplier").unwrap_or_default(),
                        dockSequence: e.get("dockSequence").unwrap_or_default(),
                        originalDate: e.get("originalDate").unwrap_or_default(),
                        originalTime: e.get("originalTime").unwrap_or_default(),
                        newDate:      e.get("newDate").unwrap_or_default(),
                        newTime:      e.get("newTime").unwrap_or_default(),
                        newEndDate:   e.get("newEndDate").unwrap_or_default(),
                        newEndTime:   e.get("newEndTime").unwrap_or_default(),
                        comment:      e.get("comment").unwrap_or_default(),
                        requestor:    e.get("requestor").unwrap_or_default(),
                    };
                    created_lines.push(record);
                }
            }
            Err(e) => {
                eprintln!("Failed to create exception log entry: {:?}", e);
                return Err(Json("Failed to create exception log entry"));
            }
        }
    }

    Ok(Json(created_lines))
}

#[post("/api/upload_dycomm", format = "json", data = "<upload_dycomm>")]
pub async fn upload_dycomm(
    upload_dycomm: Json<Vec<DyCommLogEntry>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<DyCommLogEntry>>, Json<&'static str>> {
    let graph = &state.graph;
    let mut created_lines = Vec::<DyCommLogEntry>::new();

    let updated_at = chrono::Utc::now().format("%Y-%m-%d %H:%M").to_string();

    for line in upload_dycomm.iter() {
        let query = query("
            MERGE (d:DyCommLogEntry {loadNum: $loadNum, dock: $dock, trailer: $trailer})
            ON CREATE SET
                d.scac         = $scac,
                d.route        = $route,
                d.location     = $location,
                d.deliveryDate = $deliveryDate,
                d.deliveryTime = $deliveryTime,
                d.supplier     = $supplier,
                d.part         = $part,
                d.pdt          = $pdt,
                d.createdBy    = $createdBy,
                d.createdAt    = $updated_at,
                d.updatedAt    = $updated_at
            ON MATCH SET
                d.scac         = $scac,
                d.route        = $route,
                d.location     = $location,
                d.deliveryDate = $deliveryDate,
                d.deliveryTime = $deliveryTime,
                d.supplier     = $supplier,
                d.part         = $part,
                d.pdt          = $pdt,
                d.updatedAt    = $updated_at
            RETURN d
        ")
        .param("loadNum",      line.loadNum.clone())
        .param("dock",         line.dock.clone())
        .param("trailer",      line.trailer.clone())
        .param("scac",         line.scac.clone())
        .param("route",        line.route.clone())
        .param("location",     line.location.clone())
        .param("deliveryDate", line.deliveryDate.clone())
        .param("deliveryTime", line.deliveryTime.clone())
        .param("supplier",     line.supplier.clone())
        .param("part",         line.part.clone())
        .param("pdt",          line.pdt.clone())
        .param("updated_at",    updated_at.clone())
        .param("createdBy",    line.createdBy.clone());

        match graph.execute(query).await {
            Ok(mut result) => {
                if let Ok(Some(row)) = result.next().await {
                    let d: Node = row.get("d").map_err(|_| {
                        Json("Failed to get node from record")
                    })?;

                    let record = DyCommLogEntry {
                        loadNum:      d.get("loadNum").unwrap_or_default(),
                        dock:         d.get("dock").unwrap_or_default(),
                        trailer:      d.get("trailer").unwrap_or_default(),
                        scac:         d.get("scac").unwrap_or_default(),
                        route:        d.get("route").unwrap_or_default(),
                        location:     d.get("location").unwrap_or_default(),
                        deliveryDate: d.get("deliveryDate").unwrap_or_default(),
                        deliveryTime: d.get("deliveryTime").unwrap_or_default(),
                        supplier:     d.get("supplier").unwrap_or_default(),
                        part:         d.get("part").unwrap_or_default(),
                        pdt:          d.get("pdt").unwrap_or_default(),
                        createdBy:    d.get("createdBy").unwrap_or_default(),
                    };
                    created_lines.push(record);
                }
            }
            Err(e) => {
                eprintln!("Failed to create dycomm log entry: {:?}", e);
                return Err(Json("Failed to create dycomm log entry"));
            }
        }
    }

    Ok(Json(created_lines))
}

#[post("/api/delivered", format = "json", data="<req>")]
pub async fn delivered(
    req: Json<DeliveryRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<DeliveredTrailer>, Json<&'static str>> {
    
    let graph = &state.graph;
    
    let delivery_date = chrono::Utc::now().format("%Y-%m-%d").to_string();

    // ── Query 1: Fetch schedule, parts, and sids for the trailer ──
    let fetch_query = query("
        MATCH (t:Trailer {id: $trailer})-[:HAS_SCHEDULE]->(s:Schedule)
        OPTIONAL MATCH (t)-[:CONTAINS_PART]->(p:Part)
        OPTIONAL MATCH (t)-[:HAS_SID]->(sid:SID)
        RETURN t.id AS trailer, s, COLLECT(DISTINCT p.number) AS parts, COLLECT(DISTINCT sid.id) AS sids
    ")
    .param("trailer", req.trailer_id.clone());

    let (schedule, parts, sids) = match graph.execute(fetch_query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let schedule_node: Node = row.get("s").map_err(|_| Json("Failed to get schedule node"))?;

                let schedule = Schedule {
                    Comments:     schedule_node.get("Comments").unwrap_or_default(),
                    Scac:         schedule_node.get("Scac").unwrap_or_default(),
                    Destination:  schedule_node.get("Destination").unwrap_or_default(),
                    OriginalDate: schedule_node.get("OriginalDate").unwrap_or_default(),
                    ScheduleDate: schedule_node.get("ScheduleDate").unwrap_or_default(),
                    ScheduleTime: schedule_node.get("ScheduleTime").unwrap_or_default(),
                    TrailerID:    schedule_node.get("TrailerID").unwrap_or_default(),
                    Status:       schedule_node.get("Status").unwrap_or_default(),
                    Supplier:     schedule_node.get("Supplier").unwrap_or_default(),
                    Location:     schedule_node.get("Location").unwrap_or_default(),
                };

                let parts: Vec<String> = row.get("parts").unwrap_or_else(|_| Vec::new());
                let sids: Vec<String>  = row.get("sids").unwrap_or_else(|_| Vec::new());

                (schedule, parts, sids)
            } else {
                return Err(Json("No matching trailer found"));
            }
        }
        Err(e) => {
            eprintln!("Failed to fetch trailer: {:?}", e);
            return Err(Json("Failed to fetch trailer"));
        }
    };

    // ── Query 2: Merge DeliveredTrailer node and relationships ──
    let deliver_query = query("
        MERGE (d:DeliveredTrailer {trailer_id: $trailer, delivery_date: $delivery_date})
        SET
            d.Comments     = $comments,
            d.Destination  = $destination,
            d.OriginalDate = $original_date,
            d.ScheduleDate = $schedule_date,
            d.ScheduleTime = $schedule_time,
            d.Status       = 'Delivered',
            d.Supplier     = $supplier,
            d.Scac         = $scac,
            d.parts        = $parts,
            d.sids         = $sids
        RETURN d
    ")
    .param("trailer",       req.trailer_id.clone())
    .param("delivery_date", delivery_date.clone())
    .param("comments",      schedule.Comments.clone())
    .param("destination",   schedule.Destination.clone())
    .param("original_date", schedule.OriginalDate.clone())
    .param("schedule_date", schedule.ScheduleDate.clone())
    .param("schedule_time", schedule.ScheduleTime.clone())
    .param("supplier",      schedule.Supplier.clone())
    .param("scac",          schedule.Scac.clone())
    .param("parts",         parts.clone())
    .param("sids",          sids.clone());

    let delivered = match graph.execute(deliver_query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("d").map_err(|_| Json("Failed to get delivered node"))?;

                DeliveredTrailer {
                    TrailerID:    node.get("trailer_id").unwrap_or_default(),
                    DeliveryDate: node.get("delivery_date").unwrap_or_default(),
                    Schedule:     schedule,
                    Parts:        parts,
                    Sids:         sids,
                }
            } else {
                return Err(Json("Failed to create delivered trailer"));
            }
        }
        Err(e) => {
            eprintln!("Failed to deliver route: {:?}", e);
            return Err(Json("Failed to deliver route"));
        }
    };

    // ── Query 3: Delete Trailer and its relationships from active pool ──
    let delete_query = query("
        MATCH (t:Trailer {id: $trailer})-[:HAS_SCHEDULE]->(s:Schedule)
        WITH t,s
        MATCH (t)-[:HAS_SID]->(sid:SID)-[:HAS_PART]->(p:Part)
        DETACH DELETE t, s, sid, p
    ")
    .param("trailer", req.trailer_id.clone());

    graph.run(delete_query).await.map_err(|e| {
        eprintln!("Failed to delete trailer: {:?}", e);
        Json("Failed to delete trailer")
    })?;

    Ok(Json(delivered))
}

#[post("/api/roll_next_shift", format = "json", data = "<req>")]
pub async fn roll_next_shift(
    req: Json<RollNextShiftRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;
    let operational_date = req.operational_date.clone();

    if role.0 != "admin" && role.0 != "manager" {
        return Err(Json("Unauthorized: Admin role required"));
    }

    // ── Query 1: Snapshot all LiveTrailers into TrailerRecords ──
    let snapshot_query = query("
        MERGE (o:OpDate {date: $operational_date})
        WITH o
        MATCH (t:LiveTrailer)
        CREATE (r:TrailerRecord {
            uuid:               t.uuid,
            hour:               t.hour,
            dateShift:          t.dateShift,
            lmsAccent:          t.lmsAccent,
            dockCode:           t.dockCode,
            acaType:            t.acaType,
            status:             t.status,
            routeId:            t.routeId,
            scac:               t.scac,
            trailer1:           t.trailer1,
            trailer2:           t.trailer2,
            firstSupplier:      t.firstSupplier,
            dockStopSequence:   t.dockStopSequence,
            planStartDate:      t.planStartDate,
            planStartTime:      t.planStartTime,
            scheduleStartDate:  t.scheduleStartDate,
            adjustedStartTime:  t.adjustedStartTime,
            scheduleEndDate:    t.scheduleEndDate,
            scheduleEndTime:    t.scheduleEndTime,
            gateArrivalTime:    t.gateArrivalTime,
            actualStartTime:    t.actualStartTime,
            actualEndTime:      t.actualEndTime,
            statusOX:           CASE
                                    WHEN t.gateArrivalTime <> '' AND (t.actualEndTime = '' OR t.actualEndTime IS NULL) THEN 'C'
                                    WHEN t.gateArrivalTime = '' AND t.actualEndTime = '' THEN 'N'
                                    ELSE t.statusOX
                                END,
            lowestDoh:          t.lowestDoh,
            loadComments:       t.loadComments,
            ryderComments:      t.ryderComments,
            lateComments:       t.lateComments,
            gmComments:         t.gmComments
        })
        CREATE (o)-[:HAS_TRAILER]->(r)
    ")
    .param("operational_date", operational_date.clone());

    graph.run(snapshot_query).await.map_err(|e| {
        eprintln!("Failed to snapshot live trailers: {:?}", e);
        Json("Failed to snapshot live trailers")
    })?;

    // ── Query 2: Delete LiveTrailers where actualEndTime is not empty ──
    let delete_query = query("
        MATCH (t:LiveTrailer)
        WHERE t.gateArrivalTime <> '' AND (t.actualEndTime <> '' OR t.actualEndTime IS NULL)
        DELETE t
    ");

    graph.run(delete_query).await.map_err(|e| {
        eprintln!("Failed to delete completed live trailers: {:?}", e);
        Json("Failed to delete completed live trailers")
    })?;

    // ── Query 3: Set statusOX = 'C' on remaining LiveTrailers ──
    let status_query = query("
        MATCH (t:LiveTrailer)
        SET t.statusOX = CASE
            WHEN t.gateArrivalTime <> '' THEN 'C'
            ELSE t.statusOX
        END
    ");

    graph.run(status_query).await.map_err(|e| {
        eprintln!("Failed to update statusOX on live trailers: {:?}", e);
        Json("Failed to update statusOX on live trailers")
    })?;

    // ── Query 4: Promote StagedTrailers to LiveTrailers and delete StagedTrailer nodes ──
    let promote_query = query("
        MATCH (s:StagedTrailer)
        CREATE (t:LiveTrailer {
            uuid:               s.uuid,
            hour:               s.hour,
            dateShift:          s.dateShift,
            lmsAccent:          s.lmsAccent,
            dockCode:           s.dockCode,
            acaType:            s.acaType,
            status:             s.status,
            routeId:            s.routeId,
            scac:               s.scac,
            trailer1:           s.trailer1,
            trailer2:           s.trailer2,
            firstSupplier:      s.firstSupplier,
            dockStopSequence:   s.dockStopSequence,
            planStartDate:      s.planStartDate,
            planStartTime:      s.planStartTime,
            scheduleStartDate:  s.scheduleStartDate,
            adjustedStartTime:  s.adjustedStartTime,
            scheduleEndDate:    s.scheduleEndDate,
            scheduleEndTime:    s.scheduleEndTime,
            gateArrivalTime:    s.gateArrivalTime,
            actualStartTime:    s.actualStartTime,
            actualEndTime:      s.actualEndTime,
            statusOX:           s.statusOX,
            lowestDoh:          s.lowestDoh,
            loadComments:       s.loadComments,
            ryderComments:      s.ryderComments,
            lateComments:       s.lateComments,
            gmComments:         s.gmComments
        })
        DELETE s
    ");

    graph.run(promote_query).await.map_err(|e| {
        eprintln!("Failed to promote staged trailers: {:?}", e);
        Json("Failed to promote staged trailers")
    })?;

    Ok(Json("Roll next shift completed successfully"))
}

#[post("/api/push_add_on", format = "json", data = "<add_on>")]
pub async fn push_add_on (
    add_on: Json<TrailerRecord>,
    state:  &State<AppState>,
    _user:  AuthenticatedUser,
    role:   Role,
) -> Result<Json<TrailerRecord>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        CREATE (t:LiveTrailer {
            uuid:              $uuid,
            hour:              $hour,
            dateShift:         $dateShift,
            lmsAccent:         $lmsAccent,
            dockCode:          $dockCode,
            acaType:           $acaType,
            status:            $status,
            routeId:           $routeId,
            scac:              $scac,
            trailer1:          $trailer1,
            trailer2:          $trailer2,
            firstSupplier:     $firstSupplier,
            dockStopSequence:  $dockStopSequence,
            planStartDate:     $planStartDate,
            planStartTime:     $planStartTime,
            scheduleStartDate: $scheduleStartDate,
            adjustedStartTime: $adjustedStartTime,
            scheduleEndDate:   $scheduleEndDate,
            scheduleEndTime:   $scheduleEndTime,
            gateArrivalTime:   $gateArrivalTime,
            actualStartTime:   $actualStartTime,
            actualEndTime:     $actualEndTime,
            statusOX:          $statusOX,
            loadComments:      $loadComments,
            ryderComments:     $ryderComments,
            lateComments:      $lateComments,
            gmComments:        $gmComments,
            lowestDoh:         $lowestDoh
        })
        RETURN t
    ")
    .param("uuid",              add_on.uuid.clone())
    .param("hour",              add_on.hour.clone())
    .param("dateShift",         add_on.dateShift.clone())
    .param("lmsAccent",         add_on.lmsAccent.clone())
    .param("dockCode",          add_on.dockCode.clone())
    .param("acaType",           add_on.acaType.clone())
    .param("status",            add_on.status.clone())
    .param("routeId",           add_on.routeId.clone())
    .param("scac",              add_on.scac.clone())
    .param("trailer1",          add_on.trailer1.clone())
    .param("trailer2",          add_on.trailer2.clone())
    .param("firstSupplier",     add_on.firstSupplier.clone())
    .param("dockStopSequence",  add_on.dockStopSequence.clone())
    .param("planStartDate",     add_on.planStartDate.clone())
    .param("planStartTime",     add_on.planStartTime.clone())
    .param("scheduleStartDate", add_on.scheduleStartDate.clone())
    .param("adjustedStartTime", add_on.adjustedStartTime.clone())
    .param("scheduleEndDate",   add_on.scheduleEndDate.clone())
    .param("scheduleEndTime",   add_on.scheduleEndTime.clone())
    .param("gateArrivalTime",   add_on.gateArrivalTime.clone())
    .param("actualStartTime",   add_on.actualStartTime.clone())
    .param("actualEndTime",     add_on.actualEndTime.clone())
    .param("statusOX",          add_on.statusOX.clone())
    .param("loadComments",      add_on.loadComments.clone())
    .param("ryderComments",     add_on.ryderComments.clone())
    .param("lateComments",      add_on.lateComments.clone().unwrap_or_default())
    .param("gmComments",        add_on.gmComments.clone().unwrap_or_default())
    .param("lowestDoh",         add_on.lowestDoh.clone().unwrap_or_default());

    match graph.execute(q).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("t").map_err(|_| Json("Failed to get node"))?;

                let created = TrailerRecord {
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
                };

                // ── Broadcast to WS clients ──
                if let Ok(data) = serde_json::to_string(&created) {
                    let ws_msg = IncomingMessage {
                        r#type: "add_on".to_string(),
                        data: Some(MessageData { message: data }),
                    };
                    if let Ok(message) = serde_json::to_string(&ws_msg) {
                        let ws_list = state.ws_list.lock().await;
                        for (_, tx) in ws_list.iter() {
                            let _ = tx.send(Message::Text(message.clone()));
                        }
                    }
                }

                Ok(Json(created))
            } else {
                Err(Json("Failed to create add on"))
            }
        }
        Err(e) => {
            eprintln!("Failed to push add on: {:?}", e);
            Err(Json("Failed to push add on"))
        }
    }
}

#[post("/api/upload_on_deck", format = "json", data = "<upload_on_deck>")]
pub async fn upload_on_deck(
    upload_on_deck: Json<Vec<TrailerRecord>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<Vec<TrailerRecord>>, Json<&'static str>> {

    if role.0 != "admin" && role.0 != "supervisor" {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let mut created_lines: Vec<TrailerRecord> = Vec::new();

    for line in upload_on_deck.iter() {
        let query = query("
        CREATE(t:StagedTrailer {
            uuid: $uuid,
            origin: $origin,
            hour: $hour,
            origin: $origin,
            dateShift: $dateShift,
            lmsAccent: $lmsAccent,
            dockCode: $dockCode,
            acaType: $acaType,
            status: $status,
            routeId: $routeId,
            scac: $scac,
            trailer1: $trailer1,
            trailer2: $trailer2,
            firstSupplier: $firstSupplier,
            dockStopSequence: $dockStopSequence,
            planStartDate: $planStartDate,
            planStartTime: $planStartTime,
            scheduleStartDate: $scheduleStartDate,
            adjustedStartTime: $adjustedStartTime,
            scheduleEndDate: $scheduleEndDate,
            scheduleEndTime: $scheduleEndTime,
            gateArrivalTime: $gateArrivalTime,
            actualStartTime: $actualStartTime,
            actualEndTime: $actualEndTime,
            statusOX: $statusOX,
            lowestDoh: $lowestDoh,
            loadComments: $loadComments,
            ryderComments: $ryderComments,
            gmComments: $gmComments
        })
        RETURN t
    ")
    .param("hour", line.hour.clone())
    .param("origin", line.origin.clone())
    .param("dateShift", line.dateShift.clone())
    .param("lmsAccent", line.lmsAccent.clone())
    .param("dockCode", line.dockCode.clone())
    .param("acaType", line.acaType.clone())
    .param("status", line.status.clone())
    .param("routeId", line.routeId.clone())
    .param("scac", line.scac.clone())
    .param("trailer1", line.trailer1.clone())
    .param("trailer2", line.trailer2.clone())
    .param("firstSupplier", line.firstSupplier.clone())
    .param("dockStopSequence", line.dockStopSequence.clone())
    .param("planStartDate", line.planStartDate.clone())
    .param("planStartTime", line.planStartTime.clone())
    .param("scheduleStartDate", line.scheduleStartDate.clone())
    .param("lowestDoh", line.lowestDoh.clone())
    .param("adjustedStartTime", line.adjustedStartTime.clone())
    .param("scheduleEndDate", line.scheduleEndDate.clone())
    .param("scheduleEndTime", line.scheduleEndTime.clone())
    .param("gateArrivalTime", line.gateArrivalTime.clone())
    .param("actualStartTime", line.actualStartTime.clone())
    .param("actualEndTime", line.actualEndTime.clone())
    .param("statusOX", line.statusOX.clone())
    .param("loadComments", line.loadComments.clone())
    .param("ryderComments", line.ryderComments.clone())
    .param("gmComments", line.gmComments.clone())
    .param("uuid", line.uuid.clone());

        match graph.execute(query).await {
            Ok(mut result) => {
                match result.next().await {
                Ok(Some(row)) => {
                    let trailer_node: Node = row.get("t").map_err(|_| {
                        Json("Failed to get node from record")
                    })?;
                    
                    // Extract all fields
                    let uuid:              String = trailer_node.get("uuid").unwrap_or_default();
                    let origin:            String = trailer_node.get("origin").unwrap_or_default();
                    let hour:              String = trailer_node.get("hour").unwrap_or_default();
                    let dateShift:         String = trailer_node.get("dateShift").unwrap_or_default();
                    let lmsAccent:         String = trailer_node.get("lmsAccent").unwrap_or_default();
                    let dockCode:          String = trailer_node.get("dockCode").unwrap_or_default();
                    let acaType:           String = trailer_node.get("acaType").unwrap_or_default();
                    let status:            String = trailer_node.get("status").unwrap_or_default();
                    let routeId:           String = trailer_node.get("routeId").unwrap_or_default();
                    let scac:              String = trailer_node.get("scac").unwrap_or_default();
                    let trailer1:          String = trailer_node.get("trailer1").unwrap_or_default();
                    let trailer2:          String = trailer_node.get("trailer2").unwrap_or_default();
                    let firstSupplier:     String = trailer_node.get("firstSupplier").unwrap_or_default();
                    let dockStopSequence:  String = trailer_node.get("dockStopSequence").unwrap_or_default();
                    let planStartDate:     String = trailer_node.get("planStartDate").unwrap_or_default();
                    let planStartTime:     String = trailer_node.get("planStartTime").unwrap_or_default();
                    let scheduleStartDate: String = trailer_node.get("scheduleStartDate").unwrap_or_default();
                    let adjustedStartTime: String = trailer_node.get("adjustedStartTime").unwrap_or_default();
                    let scheduleEndDate:   String = trailer_node.get("scheduleEndDate").unwrap_or_default();
                    let scheduleEndTime:   String = trailer_node.get("scheduleEndTime").unwrap_or_default();
                    let lowestDoh:         String = trailer_node.get("lowestDoh").unwrap_or("".to_string());
                    let gateArrivalTime:   String = trailer_node.get("gateArrivalTime").unwrap_or_default();
                    let actualStartTime:   String = trailer_node.get("actualStartTime").unwrap_or_default();
                    let actualEndTime:     String = trailer_node.get("actualEndTime").unwrap_or_default();
                    let statusOX:          String = trailer_node.get("statusOX").unwrap_or_default();
                    let loadComments:      String = trailer_node.get("loadComments").unwrap_or_default();
                    let ryderComments:     String = trailer_node.get("ryderComments").unwrap_or_default();
                    let lateComments:      String = trailer_node.get("lateComments").unwrap_or_default();
                    let gmComments:        String = trailer_node.get("gmComments").unwrap_or_default();

                    let trailer = TrailerRecord {
                        uuid,
                        origin,
                        hour,
                        dateShift,
                        lmsAccent,
                        dockCode,
                        acaType,
                        status,
                        routeId,
                        scac,
                        trailer1,
                        trailer2,
                        firstSupplier,
                        dockStopSequence,
                        planStartDate,
                        planStartTime,
                        scheduleStartDate,
                        adjustedStartTime,
                        scheduleEndDate,
                        scheduleEndTime,
                        gateArrivalTime,
                        actualStartTime,
                        actualEndTime,
                        statusOX,
                        loadComments,
                        ryderComments,
                        lateComments: Some(lateComments),
                        gmComments: Some(gmComments),
                        lowestDoh: Some(lowestDoh)
                    };
                    created_lines.push(trailer);
                }
                Ok(None) => {}
                Err(_) => {}
                }
            }
            Err(e) => {
                eprintln!("Failed to create trailer node: {:?}", e);
                return Err(Json("Failed to create trailer record"));
            }
        }
    }
    for line in upload_on_deck.iter() {
        if line.origin == "DropYard" {
            let archive_q = query("
                MATCH (d:DyCommLogEntry {loadNum: $loadNum, dock: $dock, trailer: $trailer})
                CREATE (a:ArchivedDyEntries {
                    loadNum:      d.loadNum,
                    trailer:      d.trailer,
                    scac:         d.scac,
                    route:        d.route,
                    dock:         d.dock,
                    location:     d.location,
                    deliveryDate: d.deliveryDate,
                    deliveryTime: d.deliveryTime,
                    supplier:     d.supplier,
                    part:         d.part,
                    pdt:          d.pdt,
                    createdBy:    d.createdBy
                })
                DELETE d
            ")
            .param("loadNum", line.lmsAccent.clone())
            .param("dock",    line.dockCode.clone())
            .param("trailer", line.trailer1.clone());

            if let Err(e) = graph.run(archive_q).await {
                eprintln!("Failed to archive DyComm entry ({}/{}/{}): {:?}", line.lmsAccent, line.dockCode, line.trailer1, e);
            }
        } else if line.origin == "Exception" {
            let archive_q = query("
                MATCH (e:ExceptionLogEntry {loadNum: $loadNum, dock: $dock, trailer1: $trailer1})
                CREATE (a:ArchivedExceptions {
                    loadNum:      e.loadNum,
                    dock:         e.dock,
                    type:         e.type,
                    status:       e.status,
                    route:        e.route,
                    scac:         e.scac,
                    trailer1:     e.trailer1,
                    trailer2:     e.trailer2,
                    supplier:     e.supplier,
                    dockSequence: e.dockSequence,
                    originalDate: e.originalDate,
                    originalTime: e.originalTime,
                    newDate:      e.newDate,
                    newTime:      e.newTime,
                    newEndDate:   e.newEndDate,
                    newEndTime:   e.newEndTime,
                    comment:      e.comment,
                    requestor:    e.requestor
                })
                DELETE e
            ")
            .param("loadNum",  line.lmsAccent.clone())
            .param("dock",     line.dockCode.clone())
            .param("trailer1", line.trailer1.clone());

            if let Err(e) = graph.run(archive_q).await {
                eprintln!("Failed to archive Exception entry ({}/{}/{}): {:?}", line.lmsAccent, line.dockCode, line.trailer1, e);
            }
        }
    }

    Ok(Json(created_lines))
}

#[post("/api/update_live_trailer", format = "json", data = "<trailer_info>")]
pub async fn update_live_trailer(
    trailer_info: Json<TrailerRecord>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<TrailerRecord>, Json<&'static str>> {
    let graph = &state.graph;

    let update_query = build_update_query(&role.0, &trailer_info);

    match graph.execute(update_query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let node: Node = row.get("t").map_err(|_| Json("Failed to get updated node"))?;
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
                };
                if let Ok(data) = serde_json::to_value(&updated) {
                    let ws_msg = IncomingMessage {
                        r#type: "trailer_update".to_string(),
                        data: Some(MessageData { message: data.to_string() }),
                    };
                    if let Ok(message) = serde_json::to_string(&ws_msg) {
                            let ws_list = state.ws_list.lock().await;
                            for (_, tx) in ws_list.iter() {
                                let _ = tx.send(Message::Text(message.clone()));
                            }
                        }
                }
                Ok(Json(updated))
            } else {
                Err(Json("Trailer not found"))
            }
        }
        Err(e) => {
            eprintln!("Failed to update trailer: {:?}", e);
            Err(Json("Failed to update trailer"))
        }
    }
}

#[post("/api/assign_shift", format = "json", data = "<req>")]
pub async fn assign_shift(
    req: Json<AssignShiftRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<DaySchedule>, Json<&'static str>> {
    let graph = &state.graph;
    let day_key = format!("{}_{}", req.start_date, req.offset);

    // ── Check shift is not holiday/closed ──
    let status_query = query("
        MATCH (s:Shift {day_date: $day_key, name: $shift})
        RETURN s.status AS status
    ")
    .param("day_key", day_key.clone())
    .param("shift",   req.shift.clone());

    match graph.execute(status_query).await {
        Ok(mut result) => {
            if let Ok(Some(row)) = result.next().await {
                let status: String = row.get("status").unwrap_or("active".to_string());
                if status == "holiday" || status == "closed" {
                    return Err(Json("Cannot assign to a holiday or closed shift"));
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to check shift status: {:?}", e);
            return Err(Json("Failed to check shift status"));
        }
    }

    // ── Assign the user ──
    let assign_query = query("
        MATCH (s:Shift {day_date: $day_key, name: $shift})
        MATCH (u:User {name: $user_name})
        MERGE (s)-[r:ASSIGNED]->(u)
        SET r.task = $task, r.task_type = $task_type
    ")
    .param("day_key",   day_key.clone())
    .param("shift",     req.shift.clone())
    .param("task",      req.task.clone())
    .param("user_name", req.user_name.clone())
    .param("task_type", req.task_type.clone());

    graph.run(assign_query).await.map_err(|e| {
        eprintln!("Failed to assign shift: {:?}", e);
        Json("Failed to assign shift")
    })?;

    // ── Fetch updated day ──
    let fetch_query = query("
        MATCH (d:Day {date: $day_key})-[:HAS_SHIFT]->(s:Shift)
        OPTIONAL MATCH (s)-[r:ASSIGNED]->(u:User)
        RETURN d.date AS date, d.name AS day_name, d.offset AS offset,
            s.name AS shift,
            s.status AS shift_status,
            s.reason AS shift_reason,
            collect(u.name) AS assigned_names,
            collect(u.full_name) AS full_name,
            collect(u.role) AS assigned_roles,
            collect(u.position) AS assigned_positions,
            collect(r.task) AS assigned_tasks,
            collect(r.task_type) AS assigned_task_types
        ORDER BY s.name
    ")
    .param("day_key", day_key);

    match graph.execute(fetch_query).await {
        Ok(mut result) => {
            let mut shifts: Vec<ShiftSlot> = Vec::new();
            let mut day_name = String::new();
            let mut offset: u64 = 0;

            while let Ok(Some(row)) = result.next().await {
                day_name = row.get("day_name").unwrap_or_default();
                offset   = row.get("offset").unwrap_or_default();
                let shift:               String      = row.get("shift").unwrap_or_default();
                let shift_status:        String      = row.get("shift_status").unwrap_or("active".to_string());
                let shift_reason:        String      = row.get("shift_reason").unwrap_or_default();
                let full_name:           Vec<String> = row.get("full_name").unwrap_or_default();
                let assigned_names:      Vec<String> = row.get("assigned_names").unwrap_or_default();
                let assigned_roles:      Vec<String> = row.get("assigned_roles").unwrap_or_default();
                let assigned_tasks:      Vec<String> = row.get("assigned_tasks").unwrap_or_default();
                let assigned_task_types: Vec<String> = row.get("assigned_task_types").unwrap_or_default();
                let assigned_positions:  Vec<String> = row.get("assigned_positions").unwrap_or_default();

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

                shifts.push(ShiftSlot { shift, shift_status, shift_reason, assigned });
            }

            Ok(Json(DaySchedule {
                date:   req.date.clone(),
                name:   day_name,
                offset,
                shifts,
            }))
        }
        Err(e) => {
            eprintln!("Failed to fetch updated day: {:?}", e);
            Err(Json("Failed to fetch updated day"))
        }
    }
}

#[delete("/api/unassign_shift", format = "json", data = "<req>")]
pub async fn unassign_shift(
    req: Json<UnassignShiftRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    let day_key = format!("{}_{}", req.start_date, req.offset);

    let q = query("
        MATCH (s:Shift {day_date: $day_key, name: $shift})-[r:ASSIGNED]->(u:User {name: $user_name})
        DELETE r
        WITH s
        MATCH (s)-[c:DECK_COVERAGE {user_name: $user_name}]->(d:Deck)
        DELETE c
    ")
    .param("day_key",   day_key)
    .param("shift",     req.shift.clone())
    .param("user_name", req.user_name.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to unassign shift: {:?}", e);
        Json("Failed to unassign shift")
    })?;

    Ok(Json("Unassigned successfully"))
}

#[post("/api/update_user_position", format = "json", data = "<req>")]
pub async fn update_user_position(
    req: Json<UpdatePositionRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (u:User {name: $user_name})
        SET u.position = $position
    ")
    .param("user_name", req.user_name.clone())
    .param("position",  req.position.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to update position: {:?}", e);
        Json("Failed to update position")
    })?;

    Ok(Json("Position updated"))
}

#[post("/api/assign_deck", format = "json", data = "<req>")]
pub async fn assign_deck(
    req:   Json<AssignDeckRequest>,
    state: &State<AppState>,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (s:Shift {day_date: $day_key, name: $shift})
        MATCH (d:Deck {name: $deck})
        MERGE (s)-[c:DECK_COVERAGE]->(d)
        SET c.user_name = $user_name
    ")
    .param("day_key",   req.day_key.clone())
    .param("shift",     req.shift.clone())
    .param("deck",      req.deck.clone())
    .param("user_name", req.user_name.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to assign deck: {:?}", e);
        Json("Failed to assign deck")
    })?;

    Ok(Json("Deck assigned"))
}

#[delete("/api/unassign_deck", format = "json", data = "<req>")]
pub async fn unassign_deck(
    req:   Json<UnassignDeckRequest>,
    state: &State<AppState>,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (s:Shift {day_date: $day_key, name: $shift})-[c:DECK_COVERAGE]->(d:Deck {name: $deck})
        DELETE c
    ")
    .param("day_key", req.day_key.clone())
    .param("shift",   req.shift.clone())
    .param("deck",    req.deck.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to unassign deck: {:?}", e);
        Json("Failed to unassign deck")
    })?;

    Ok(Json("Deck unassigned"))
}

#[post("/api/generate_week/<start_date>")]
pub async fn generate_week(
    start_date: String,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
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

    // ── Query 2: Assign 1st shift users to Monday-Saturday (offsets 1-6) ──
    let first_shift_query = query("
        MATCH (u:User {shift: '1st'})
        WITH collect(u) AS users
        UNWIND range(1, 6) AS day_offset
        MATCH (s:Shift {day_date: $start_date + '_' + toString(day_offset), name: '1st'})
        WHERE s.status IS NULL OR s.status = 'active'
        WITH s, users
        UNWIND users AS u
        MERGE (s)-[r:ASSIGNED]->(u)
        SET r.task = '', r.task_type = 'work'
    ")
    .param("start_date", start_date.clone());

    graph.run(first_shift_query).await.map_err(|e| {
        eprintln!("Failed to assign 1st shift: {:?}", e);
        Json("Failed to assign 1st shift")
    })?;

    // ── Query 3: Assign 2nd shift users to Monday-Saturday (offsets 1-6) ──
    let second_shift_query = query("
        MATCH (u:User {shift: '2nd'})
        WITH collect(u) AS users
        UNWIND range(1, 6) AS day_offset
        MATCH (s:Shift {day_date: $start_date + '_' + toString(day_offset), name: '2nd'})
        WITH s, users
        WHERE s.status IS NULL OR s.status = 'active'
        UNWIND users AS u
        MERGE (s)-[r:ASSIGNED]->(u)
        SET r.task = '', r.task_type = 'work'
    ")
    .param("start_date", start_date.clone());

    graph.run(second_shift_query).await.map_err(|e| {
        eprintln!("Failed to assign 2nd shift: {:?}", e);
        Json("Failed to assign 2nd shift")
    })?;

    // ── Query 4: Assign 3rd shift users to Sunday-Friday (offsets 0-5) ──
    let third_shift_query = query("
        MATCH (u:User {shift: '3rd'})
        WITH collect(u) AS users
        UNWIND range(0, 5) AS day_offset
        MATCH (s:Shift {day_date: $start_date + '_' + toString(day_offset), name: '3rd'})
        WITH s, users
        WHERE s.status IS NULL OR s.status = 'active'
        UNWIND users AS u
        MERGE (s)-[r:ASSIGNED]->(u)
        SET r.task = '', r.task_type = 'work'
    ")
    .param("start_date", start_date.clone());

    graph.run(third_shift_query).await.map_err(|e| {
        eprintln!("Failed to assign 3rd shift: {:?}", e);
        Json("Failed to assign 3rd shift")
    })?;

    Ok(Json("Week generated successfully"))
}

#[delete("/api/restart_week/<start_date>")]
pub async fn restart_week(
    start_date:   String,
    state: &State<AppState>,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    let q = query("
        MATCH (w:Week {start_date: $start_date})-[:HAS_DAY]->(d:Day)-[:HAS_SHIFT]->(s:Shift)
        DETACH DELETE w, d, s
    ")
    .param("start_date",    start_date.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to unassign week: {:?}", e);
        Json("Failed to unassign week")
    })?;

    Ok(Json("Deck unassigned"))
}

#[post("/api/set_shift_status", format = "json", data = "<req>")]
pub async fn set_shift_status(
    req:   Json<ShiftStatusRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;
    let day_key = format!("{}_{}", req.start_date, req.offset);

    let q = query("
        MATCH (s:Shift {day_date: $day_key, name: $shift})
        SET s.status = $status, s.reason = $reason
    ")
    .param("day_key", day_key)
    .param("shift",   req.shift.clone())
    .param("status",  req.status.clone())
    .param("reason",  req.reason.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to set shift status: {:?}", e);
        Json("Failed to set shift status")
    })?;

    Ok(Json("Shift status updated"))
}

#[post("/api/upload_part_asn", format = "json", data = "<data>")]
pub async fn upload_part_asn(
    data:  Json<Vec<PartASN>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    // ── Clear existing ASN data ──
    let clear_query = query("MATCH (n:PartASN) DETACH DELETE n");
    graph.run(clear_query).await.map_err(|e| {
        eprintln!("Failed to clear PartASN: {:?}", e);
        Json("Failed to clear PartASN")
    })?;

    for asn in data.iter() {
        let q = query("
            CREATE (n:PartASN {
                scac:          $scac,
                trailer:       $trailer,
                deck:          $deck,
                part:          $part,
                duns:          $duns,
                quantity:      $quantity,
                status:        $status,
                sid:           $sid,
                countComment:  $countComment,
                shipComment:   $shipComment,
                shipDate:      $shipDate,
                dock:          $dock,
                eda:           $eda,
                eta:           $eta,
                mode:          $mode
            })
        ")
        .param("scac",          asn.scac.clone())
        .param("trailer",       asn.trailer.clone())
        .param("deck",          asn.deck.clone())
        .param("part",          asn.part.clone())
        .param("duns",          asn.duns.clone())
        .param("quantity",      asn.quantity)
        .param("status",        asn.status)
        .param("sid",           asn.sid.clone())
        .param("countComment",  asn.countComment.clone())
        .param("shipComment",   asn.shipComment.clone())
        .param("shipDate",      asn.shipDate.clone())
        .param("dock",          asn.dock.clone())
        .param("mode",          asn.mode.clone())
        .param("eda",           asn.eda.clone())
        .param("eta",           asn.eta.clone())
        .param("mode",          asn.mode.clone());

        graph.run(q).await.map_err(|e| {
            eprintln!("Failed to upload PartASN: {:?}", e);
            Json("Failed to upload PartASN")
        })?;
    }

    Ok(Json("PartASN uploaded successfully"))
}

#[post("/api/upload_part_asl", format = "json", data = "<data>")]
pub async fn upload_part_asl(
    data:  Json<Vec<PartASL>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    // ── Clear existing ASL data ──
    let clear_query = query("MATCH (n:PartASL) DETACH DELETE n");
    graph.run(clear_query).await.map_err(|e| {
        eprintln!("Failed to clear PartASL: {:?}", e);
        Json("Failed to clear PartASL")
    })?;

    for asl in data.iter() {
        let q = query("
            CREATE (n:PartASL {
                deck:     $deck,
                part:     $part,
                duns:     $duns,
                supplier: $supplier,
                doh:      $doh,
                bank:     $bank,
                desc:     $desc,
                cbal:     $cbal,
                day1:     $day1,
                day2:     $day2,
                day3:     $day3,
                day4:     $day4,
                day5:     $day5,
                day6:     $day6
            })
        ")
        .param("deck",     asl.deck.clone())
        .param("part",     asl.part.clone())
        .param("duns",     asl.duns.clone())
        .param("supplier", asl.supplier.clone())
        .param("doh",      asl.doh)
        .param("bank",     asl.bank.clone())
        .param("desc",     asl.desc.clone())
        .param("cbal",     asl.cbal)
        .param("day1",     asl.day1)
        .param("day2",     asl.day2)
        .param("day3",     asl.day3)
        .param("day4",     asl.day4)
        .param("day5",     asl.day5)
        .param("day6",     asl.day6);

        graph.run(q).await.map_err(|e| {
            eprintln!("Failed to upload PartASL: {:?}", e);
            Json("Failed to upload PartASL")
        })?;
    }

    Ok(Json("PartASL uploaded successfully"))
}

#[post("/api/upload_part_out", format = "json", data = "<data>")]
pub async fn upload_part_out(
    data:  Json<Vec<PartOut>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    // ── Clear existing PartOut data ──
    let clear_query = query("MATCH (n:PartOut) DETACH DELETE n");
    graph.run(clear_query).await.map_err(|e| {
        eprintln!("Failed to clear PartOut: {:?}", e);
        Json("Failed to clear PartOut")
    })?;

    for out in data.iter() {
        let q = query("
            CREATE (n:PartOut {
                part:      $part,
                day1_hr1:  $day1_hr1,  day1_hr2:  $day1_hr2,  day1_hr3:  $day1_hr3,
                day1_hr4:  $day1_hr4,  day1_hr5:  $day1_hr5,  day1_hr6:  $day1_hr6,
                day1_hr7:  $day1_hr7,  day1_hr8:  $day1_hr8,  day1_hr9:  $day1_hr9,
                day1_hr10: $day1_hr10, day1_hr11: $day1_hr11, day1_hr12: $day1_hr12,
                day1_hr13: $day1_hr13, day1_hr14: $day1_hr14, day1_hr15: $day1_hr15,
                day1_hr16: $day1_hr16, day1_hr17: $day1_hr17, day1_hr18: $day1_hr18,
                day1_hr19: $day1_hr19, day1_hr20: $day1_hr20, day1_hr21: $day1_hr21,
                day1_hr22: $day1_hr22, day1_hr23: $day1_hr23, day1_hr24: $day1_hr24,
                day2_hr1:  $day2_hr1,  day2_hr2:  $day2_hr2,  day2_hr3:  $day2_hr3,
                day2_hr4:  $day2_hr4,  day2_hr5:  $day2_hr5,  day2_hr6:  $day2_hr6,
                day2_hr7:  $day2_hr7,  day2_hr8:  $day2_hr8,  day2_hr9:  $day2_hr9,
                day2_hr10: $day2_hr10, day2_hr11: $day2_hr11, day2_hr12: $day2_hr12,
                day2_hr13: $day2_hr13, day2_hr14: $day2_hr14, day2_hr15: $day2_hr15,
                day2_hr16: $day2_hr16, day2_hr17: $day2_hr17, day2_hr18: $day2_hr18,
                day2_hr19: $day2_hr19, day2_hr20: $day2_hr20, day2_hr21: $day2_hr21,
                day2_hr22: $day2_hr22, day2_hr23: $day2_hr23, day2_hr24: $day2_hr24
            })
        ")
        .param("part",      out.part.clone())
        .param("day1_hr1",  out.day1_hr1)  .param("day1_hr2",  out.day1_hr2)  .param("day1_hr3",  out.day1_hr3)
        .param("day1_hr4",  out.day1_hr4)  .param("day1_hr5",  out.day1_hr5)  .param("day1_hr6",  out.day1_hr6)
        .param("day1_hr7",  out.day1_hr7)  .param("day1_hr8",  out.day1_hr8)  .param("day1_hr9",  out.day1_hr9)
        .param("day1_hr10", out.day1_hr10) .param("day1_hr11", out.day1_hr11) .param("day1_hr12", out.day1_hr12)
        .param("day1_hr13", out.day1_hr13) .param("day1_hr14", out.day1_hr14) .param("day1_hr15", out.day1_hr15)
        .param("day1_hr16", out.day1_hr16) .param("day1_hr17", out.day1_hr17) .param("day1_hr18", out.day1_hr18)
        .param("day1_hr19", out.day1_hr19) .param("day1_hr20", out.day1_hr20) .param("day1_hr21", out.day1_hr21)
        .param("day1_hr22", out.day1_hr22) .param("day1_hr23", out.day1_hr23) .param("day1_hr24", out.day1_hr24)
        .param("day2_hr1",  out.day2_hr1)  .param("day2_hr2",  out.day2_hr2)  .param("day2_hr3",  out.day2_hr3)
        .param("day2_hr4",  out.day2_hr4)  .param("day2_hr5",  out.day2_hr5)  .param("day2_hr6",  out.day2_hr6)
        .param("day2_hr7",  out.day2_hr7)  .param("day2_hr8",  out.day2_hr8)  .param("day2_hr9",  out.day2_hr9)
        .param("day2_hr10", out.day2_hr10) .param("day2_hr11", out.day2_hr11) .param("day2_hr12", out.day2_hr12)
        .param("day2_hr13", out.day2_hr13) .param("day2_hr14", out.day2_hr14) .param("day2_hr15", out.day2_hr15)
        .param("day2_hr16", out.day2_hr16) .param("day2_hr17", out.day2_hr17) .param("day2_hr18", out.day2_hr18)
        .param("day2_hr19", out.day2_hr19) .param("day2_hr20", out.day2_hr20) .param("day2_hr21", out.day2_hr21)
        .param("day2_hr22", out.day2_hr22) .param("day2_hr23", out.day2_hr23) .param("day2_hr24", out.day2_hr24);

        graph.run(q).await.map_err(|e| {
            eprintln!("Failed to upload PartOut: {:?}", e);
            Json("Failed to upload PartOut")
        })?;
    }

    Ok(Json("PartOut uploaded successfully"))
}

#[post("/api/update_user", format = "json", data = "<req>")]
pub async fn update_user(
    req:   Json<UpdateUserRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role:  Role,
) -> Result<Json<&'static str>, Json<&'static str>> {
    if role.0 != "admin" && role.0 != "manager" {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("
        MATCH (u:User {name: $name})
        SET u.full_name = $full_name,
            u.position  = $position,
            u.role      = $role,
            u.shift     = $shift,
            u.slack_id  = $slack_id
    ")
    .param("name",      req.name.clone())
    .param("full_name", req.full_name.clone())
    .param("position",  req.position.clone())
    .param("role",      req.role.clone())
    .param("shift",     req.shift.clone())
    .param("slack_id",  req.slack_id.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to update user: {:?}", e);
        Json("Failed to update user")
    })?;

    Ok(Json("User updated"))
}

#[delete("/api/delete_user", format = "json", data = "<req>")]
pub async fn delete_user(
    req:   Json<DeleteUserRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role:  Role,
) -> Result<Json<&'static str>, Json<&'static str>> {
    if role.0 != "admin" {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    let q = query("
        MATCH (u:User {name: $name})
        DETACH DELETE u
    ")
    .param("name", req.name.clone());

    graph.run(q).await.map_err(|e| {
        eprintln!("Failed to delete user: {:?}", e);
        Json("Failed to delete user")
    })?;

    Ok(Json("User deleted"))
}

#[post("/api/push_reschedules", format = "json", data = "<reschedules>")]
pub async fn push_reschedules(
    reschedules: Json<Vec<TrailerRecord>>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<&'static str>, Json<&'static str>> {
    if role.0 != "admin" && role.0 != "supervisor" {
        return Err(Json("Forbidden"));
    }

    let graph = &state.graph;

    for line in reschedules.iter() {
        let q = query("
            CREATE (r:RescheduledTrailer {
                uuid:              $uuid,
                hour:              $hour,
                dateShift:         $dateShift,
                lmsAccent:         $lmsAccent,
                dockCode:          $dockCode,
                acaType:           $acaType,
                status:            $status,
                routeId:           $routeId,
                scac:              $scac,
                trailer1:          $trailer1,
                trailer2:          $trailer2,
                firstSupplier:     $firstSupplier,
                dockStopSequence:  $dockStopSequence,
                planStartDate:     $planStartDate,
                planStartTime:     $planStartTime,
                scheduleStartDate: $scheduleStartDate,
                adjustedStartTime: $adjustedStartTime,
                scheduleEndDate:   $scheduleEndDate,
                scheduleEndTime:   $scheduleEndTime,
                gateArrivalTime:   $gateArrivalTime,
                actualStartTime:   $actualStartTime,
                actualEndTime:     $actualEndTime,
                statusOX:          $statusOX,
                lowestDoh:         $lowestDoh,
                loadComments:      $loadComments,
                ryderComments:     $ryderComments,
                lateComments:      $lateComments,
                gmComments:        $gmComments
            })
        ")
        .param("uuid",              line.uuid.clone())
        .param("hour",              line.hour.clone())
        .param("dateShift",         line.dateShift.clone())
        .param("lmsAccent",         line.lmsAccent.clone())
        .param("dockCode",          line.dockCode.clone())
        .param("acaType",           line.acaType.clone())
        .param("status",            line.status.clone())
        .param("routeId",           line.routeId.clone())
        .param("scac",              line.scac.clone())
        .param("trailer1",          line.trailer1.clone())
        .param("trailer2",          line.trailer2.clone())
        .param("firstSupplier",     line.firstSupplier.clone())
        .param("dockStopSequence",  line.dockStopSequence.clone())
        .param("planStartDate",     line.planStartDate.clone())
        .param("planStartTime",     line.planStartTime.clone())
        .param("scheduleStartDate", line.scheduleStartDate.clone())
        .param("adjustedStartTime", line.adjustedStartTime.clone())
        .param("scheduleEndDate",   line.scheduleEndDate.clone())
        .param("scheduleEndTime",   line.scheduleEndTime.clone())
        .param("gateArrivalTime",   line.gateArrivalTime.clone())
        .param("actualStartTime",   line.actualStartTime.clone())
        .param("actualEndTime",     line.actualEndTime.clone())
        .param("statusOX",          line.statusOX.clone())
        .param("lowestDoh",         line.lowestDoh.clone())
        .param("loadComments",      line.loadComments.clone())
        .param("ryderComments",     line.ryderComments.clone())
        .param("lateComments",      line.lateComments.clone().unwrap_or_default())
        .param("gmComments",        line.gmComments.clone().unwrap_or_default());

        graph.run(q).await.map_err(|e| {
            eprintln!("Failed to create rescheduled trailer: {:?}", e);
            Json("Failed to create rescheduled trailer")
        })?;
    }

    Ok(Json("Reschedules pushed"))
}