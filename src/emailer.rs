use rocket::{post, serde::json::Json};
use serde::{Deserialize, Serialize};
use crate::auth::AdminOrSupervisor;
use crate::helpers::send_email;

#[derive(Debug, Serialize, Deserialize)]
pub struct EmailRequest {
    pub to: String,
    pub subject: String,
    pub body: String,
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
