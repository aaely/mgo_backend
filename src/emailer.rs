use rocket::{post, State, serde::json::Json};
use serde::{Deserialize, Serialize};
use neo4rs::query;
use crate::auth::{AdminOrSupervisor, AuthenticatedUser};
use crate::helpers::send_email;
use crate::structs::AppState;

#[derive(Debug, Serialize, Deserialize)]
pub struct EmailRequest {
    pub to: String,
    pub subject: String,
    pub body: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LateNotifyRequest {
    pub scac:           String,
    pub load_no:        String,
    pub route_id:       String,
    pub scheduled_date: String,
    pub scheduled_time: String,
    pub dock:           String,
}

#[post("/api/notify_carrier_late", format = "json", data = "<req>")]
pub async fn notify_carrier_late(
    req:   Json<LateNotifyRequest>,
    state: &State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<&'static str>, Json<&'static str>> {
    let graph = &state.graph;

    let mut result = graph.execute(
        query("MATCH (c:Contact {scac: $scac}) WHERE c.email <> '' RETURN c.email AS email")
            .param("scac", req.scac.clone()),
    ).await.map_err(|e| {
        eprintln!("notify_carrier_late: contact lookup failed for {}: {:?}", req.scac, e);
        Json("Contact lookup failed")
    })?;

    let subject = format!(
        "Late Trailer Notification — Load {} | Route {} | Dock {}",
        req.load_no, req.route_id, req.dock
    );
    let body = format!(
        "This is an automated notification from GM MGO.\n\n\
        Load {} (Route: {}, Dock: {}) was scheduled for {} on {} and has been confirmed late.\n\n\
        Please contact the facility if you have questions.",
        req.load_no, req.route_id, req.dock, req.scheduled_time, req.scheduled_date
    );

    let mut sent = 0u32;
    while let Ok(Some(row)) = result.next().await {
        let email: String = row.get("email").unwrap_or_default();
        if email.is_empty() { continue; }
        if let Err(e) = send_email(&email, &subject, body.clone()).await {
            eprintln!("notify_carrier_late: failed to email {}: {:?}", email, e);
        } else {
            sent += 1;
        }
    }

    eprintln!("notify_carrier_late: sent {} email(s) for SCAC {}", sent, req.scac);
    Ok(Json("Notifications sent"))
}

#[post("/api/send_email", format = "json", data = "<req>")]
pub async fn send_email_route(
    req: Json<EmailRequest>,
    _guard: AdminOrSupervisor,
) -> Result<Json<&'static str>, Json<&'static str>> {
    match send_email(&req.to, &req.subject, req.body.clone()).await {
        Ok(_) => Ok(Json("Email sent")),
        Err(e) => {
            eprintln!("Failed to send email: {:?}", e);
            Err(Json("Failed to send email"))
        }
    }
}
