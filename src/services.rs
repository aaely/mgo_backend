use crate::structs::*;
use crate::helpers::{send_email, send_sms, parse_asn_arrival};
use neo4rs::{query, Node, Graph};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use std::collections::HashMap;
use std::sync::Arc;

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

                        if let Ok(data) = serde_json::to_string(&updated) {
                            let ws_msg = IncomingMessage {
                                r#type: "trailer_update".to_string(),
                                data: Some(MessageData { message: data }),
                            };
                            if let Ok(message) = serde_json::to_string(&ws_msg) {
                                let ws_list = ws_list.lock().await;
                                for (_, (tx, _)) in ws_list.iter() {
                                    let _ = tx.send(Message::Text(message.clone()));
                                }
                            }
                        }

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

fn level_rank(level: Option<&'static str>) -> u8 {
    match level {
        Some("Shut Down")      => 0,
        Some("Emerging Issue") => 1,
        Some("Hot")            => 2,
        _                      => 3,
    }
}

// Picks the more urgent of two classifications, so a thin rescue margin on the
// ASN that just saved a part can raise the alert even when the *next* projected
// outage (final_hours_to_out) is comfortably far away.
fn worse_level(a: Option<&'static str>, b: Option<&'static str>) -> Option<&'static str> {
    if level_rank(a) <= level_rank(b) { a } else { b }
}

struct AlertParams<'a> {
    part: &'a str,
    asl: &'a PartASL,
    level: &'static str,
    hours_to_out: f64,
    next_asn_display: String,
    next_trailer: String,
    hours_until_rescue: f64,
}

async fn dispatch_alert(
    p: AlertParams<'_>,
    now: chrono::DateTime<chrono::Local>,
    alerted_parts: &Arc<Mutex<HashMap<String, chrono::DateTime<chrono::Local>>>>,
    alerts: &mut Vec<PartAlert>,
) {
    let sms_eligible = matches!(p.level, "Emerging Issue" | "Shut Down");

    if sms_eligible {
        println!(
            "ALERT [{}] part: {} deck: {} hours_to_out: {:.2} next_trailer: {} next_asn: {} rescue_margin: {:.2}",
            p.level, p.part, p.asl.deck, p.hours_to_out, p.next_trailer, p.next_asn_display, p.hours_until_rescue
        );

        let should_sms = {
            let alerted = alerted_parts.lock().await;
            match alerted.get(p.part) {
                Some(last) => now.signed_duration_since(*last).num_minutes() > 30,
                None => true,
            }
        };

        if should_sms {
            let sms_to = std::env::var("ALERT_SMS").unwrap_or_else(|_| "+18777804236".to_string());
            let msg = format!(
                "🚨 PART ALERT [{}] {} - {} - {:.1} hrs to outage. Next ASN: {} on {}",
                p.level.to_uppercase(), p.part, p.asl.deck, p.hours_to_out, p.next_trailer, p.next_asn_display,
            );
            if let Err(e) = send_sms(&sms_to, &msg).await {
                eprintln!("Failed to send SMS: {:?}", e);
            } else {
                alerted_parts.lock().await.insert(p.part.to_string(), now);
            }

            if let Ok(alert_email) = std::env::var("ALERT_EMAIL") {
                let subject = format!("Part Alert [{}] - {}", p.level, p.part);
                let body = format!(
                    "Part Number: {}\nDescription: {}\nSupplier: {}\nDeck: {}\n\nAlert Level: {}\nHours to Outage: {:.1}\nNext ASN ETA: {}\nNext Trailer: {}\nHours Until Rescue: {:.1}\n",
                    p.part, p.asl.desc, p.asl.supplier, p.asl.deck,
                    p.level, p.hours_to_out, p.next_asn_display, p.next_trailer, p.hours_until_rescue,
                );
                if let Err(e) = send_email(&alert_email, &subject, body).await {
                    eprintln!("Failed to send part alert email: {:?}", e);
                }
            }
        }
    }

    alerts.push(PartAlert {
        part:               p.part.to_string(),
        desc:               p.asl.desc.clone(),
        duns:               p.asl.duns.clone(),
        supplier:           p.asl.supplier.clone(),
        deck:               p.asl.deck.clone(),
        cbal:               p.asl.cbal,
        hours_to_out:       p.hours_to_out,
        next_asn_eta:       p.next_asn_display,
        next_trailer:       p.next_trailer,
        alert_level:        p.level.to_string(),
        hours_until_rescue: p.hours_until_rescue,
    });
}

pub async fn part_monitoring_service(
    graph: Arc<Graph>,
    ws_list: WebSocketList,
    alerted_parts: Arc<Mutex<HashMap<String, chrono::DateTime<chrono::Local>>>>,
    current_alerts: Arc<Mutex<Vec<PartAlert>>>,
) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60 * 15));

    loop {
        interval.tick().await;

        let asl_query = query("MATCH (n:PartASL) WHERE n.cbal > 0
                                      AND n.deck <> 'TT'
                                      AND n.deck <> 'ST'
                                      AND n.deck <> 'OB'
                                      AND NOT n.deck CONTAINS 'O'
                                      AND NOT n.deck CONTAINS 'X'
                                      RETURN n");
        let asl_map: HashMap<String, PartASL> = match graph.execute(asl_query).await {
            Ok(mut result) => {
                let mut map = HashMap::new();
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

        let out_query = query("MATCH (n:PartOut) RETURN n");
        let out_map: HashMap<String, PartOut> = match graph.execute(out_query).await {
            Ok(mut result) => {
                let mut map = HashMap::new();
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

        let asn_query = query("
            MATCH (n:PartASN)
            WHERE n.eta <> '11:11'
            RETURN n
            ORDER BY n.eda ASC, n.eta ASC
        ");
        let mut asn_map: HashMap<String, Vec<(String, String, String, f64)>> = HashMap::new();

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

        let now = chrono::Local::now();
        let now_naive = now.naive_local();
        let today_2200 = now_naive.date().and_hms_opt(22, 0, 0).unwrap();
        let yesterday_2200 = (now_naive.date() - chrono::Duration::days(1))
            .and_hms_opt(22, 0, 0)
            .unwrap();
        let window_start = if now_naive >= today_2200 { today_2200 } else { yesterday_2200 };
        let current_hour_offset = (now_naive - window_start).num_hours().clamp(0, 47) as usize;

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

            let mut bal = asl.cbal;
            let mut outage_window_idx: Option<usize> = None;

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
                    alerted_parts.lock().await.remove(part);
                    continue;
                }
            };

            let downtime_dt = window_start + chrono::Duration::hours(out_idx as i64);
            let hours_to_out = out_idx as f64 - current_hour_offset as f64;

            if hours_to_out > 48.0 {
                alerted_parts.lock().await.remove(part);
                continue;
            }

            let mut asn_index = 0usize;
            let mut current_downtime_dt = downtime_dt;
            let mut final_hours_to_out = hours_to_out;
            let mut running_bal = asl.cbal;
            let mut last_asn_window_idx = 0usize;

            let classify = |h: f64| -> Option<&'static str> {
                if h <= 0.0      { Some("Shut Down") }
                else if h <= 2.0 { Some("Emerging Issue") }
                else if h <= 6.0 { Some("Hot") }
                else             { None }
            };

            {
                let next_asn = asns_for_part.first();
                let (next_eda, next_eta, next_trailer) = next_asn
                    .map(|(eda, eta, trailer, _)| (eda.clone(), eta.clone(), trailer.clone()))
                    .unwrap_or_default();
                let next_asn_display = if next_eda.is_empty() { "N/A".to_string() }
                    else { format!("{} {}", next_eda, next_eta) };
                let rescue_margin: Option<f64> = next_asn
                    .and_then(|(eda, eta, _, _)| parse_asn_arrival(eda, eta))
                    .map(|arrival| (current_downtime_dt - arrival).num_minutes() as f64 / 60.0);
                let hours_until_rescue = rescue_margin.unwrap_or(0.0);

                // Alert if the eventual outage is close OR the ASN saving from
                // the *current* outage only cleared it by a thin margin.
                let level = worse_level(classify(final_hours_to_out), rescue_margin.and_then(classify));

                if let Some(level) = level {
                    dispatch_alert(
                        AlertParams { part, asl, level, hours_to_out: final_hours_to_out,
                            next_asn_display, next_trailer, hours_until_rescue },
                        now, &alerted_parts, &mut alerts,
                    ).await;
                }
            }

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
                    None => break,
                };

                asn_index = abs_idx + 1;

                let arrival_dt = parse_asn_arrival(&eda, &eta).unwrap();
                let hours_from_start = (arrival_dt - window_start).num_hours();
                let asn_window_idx = hours_from_start.max(0) as usize;

                for i in last_asn_window_idx..asn_window_idx.min(hourly.len()) {
                    let burn = hourly[i];
                    if burn > 0.0 {
                        running_bal = (running_bal - burn).max(0.0);
                    }
                }

                running_bal += qty;
                last_asn_window_idx = asn_window_idx;

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
                        alerted_parts.lock().await.remove(part);
                        break;
                    }
                    Some(idx) => {
                        // Margin this ASN gave before the outage it just saved the part from —
                        // must be measured against the *previous* downtime, not the next one.
                        let prev_downtime_dt = current_downtime_dt;
                        let hours_until_rescue =
                            (prev_downtime_dt - arrival_dt).num_minutes() as f64 / 60.0;

                        current_downtime_dt = window_start + chrono::Duration::hours(idx as i64);
                        final_hours_to_out = idx as f64 - current_hour_offset as f64;

                        // Alert if the next projected outage is close OR this save cleared
                        // the previous outage by only a thin margin.
                        let level = worse_level(classify(final_hours_to_out), classify(hours_until_rescue));

                        if let Some(level) = level {
                            let (next_eda, next_eta, next_trailer) = asns_for_part
                                .get(asn_index)
                                .map(|(eda, eta, trailer, _)| (eda.clone(), eta.clone(), trailer.clone()))
                                .unwrap_or_default();
                            let next_asn_display = if next_eda.is_empty() { "N/A".to_string() }
                                else { format!("{} {}", next_eda, next_eta) };

                            dispatch_alert(
                                AlertParams { part, asl, level,
                                    hours_to_out: final_hours_to_out,
                                    next_asn_display, next_trailer, hours_until_rescue },
                                now, &alerted_parts, &mut alerts,
                            ).await;
                        }
                    }
                }
            }
        }

        *current_alerts.lock().await = alerts.clone();

        if alerts.is_empty() {
            continue;
        }

        if let Ok(data) = serde_json::to_string(&alerts) {
            let ws_msg = IncomingMessage {
                r#type: "part_alert".to_string(),
                data:   Some(MessageData { message: data }),
            };
            if let Ok(message) = serde_json::to_string(&ws_msg) {
                let ws_list = ws_list.lock().await;
                for (_, (tx, _)) in ws_list.iter() {
                    let _ = tx.send(Message::Text(message.clone()));
                }
            }
        }
    }
}
