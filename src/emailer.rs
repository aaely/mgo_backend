use rocket::{post, serde::json::Json};
use serde::{Deserialize, Serialize};
use crate::auth::AuthenticatedUser;
use crate::role::Role;
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
    _user: AuthenticatedUser,
    role: Role,
) -> Result<Json<&'static str>, Json<&'static str>> {
    if role.0 != "admin" && role.0 != "supervisor" {
        return Err(Json("Forbidden"));
    }

    match send_email(&req.to, &req.subject, req.body.clone()).await {
        Ok(_) => Ok(Json("Email sent")),
        Err(e) => {
            eprintln!("Failed to send email: {:?}", e);
            Err(Json("Failed to send email"))
        }
    }
}
