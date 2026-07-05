use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};
use rocket::request::{FromRequest, Outcome, Request};
use rocket::http::Status;
use serde::{Deserialize, Serialize};

// Username injected by nginx after Kerberos/SPNEGO authentication.
// Strip domain prefix (DOMAIN\user) or suffix (user@DOMAIN.COM) so the
// bare username matches what's stored in Neo4j.
pub struct LDAPUser(pub String);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for LDAPUser {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, ()> {
        match request.headers().get_one("X-Remote-User") {
            Some(raw) => {
                let username = raw
                    .split('\\').last().unwrap_or(raw)
                    .split('@').next().unwrap_or(raw)
                    .trim()
                    .to_lowercase();
                if username.is_empty() {
                    Outcome::Error((Status::Unauthorized, ()))
                } else {
                    Outcome::Success(LDAPUser(username))
                }
            }
            None => Outcome::Error((Status::Unauthorized, ())),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Claims {
    pub username: String,
    pub role: String,
    pub exp: usize,
}

pub fn decode_token(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let key = DecodingKey::from_secret(secret.as_ref());
    let validation = Validation::new(Algorithm::HS256);
    decode::<Claims>(token, &key, &validation).map(|data| data.claims)
}

pub struct AuthenticatedUser(pub Claims);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AuthenticatedUser {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        let secret = std::env::var("JWT_SECRET").unwrap_or_default();
        let secret = secret.as_str();
        if let Some(cookie) = request.cookies().get("f126f1b7d90a5bd5") {
            match decode_token(cookie.value(), secret) {
                Ok(claims) => return Outcome::Success(AuthenticatedUser(claims)),
                Err(e) => {
                    println!("{:?}", e);
                    return Outcome::Error((Status::Unauthorized, ()));
                }
            }
        }
        Outcome::Error((Status::Unauthorized, ()))
    }
}

pub struct AdminOrManager(pub Claims);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AdminOrManager {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        match AuthenticatedUser::from_request(request).await {
            Outcome::Success(u) if matches!(u.0.role.as_str(), "admin" | "manager") =>
                Outcome::Success(AdminOrManager(u.0)),
            Outcome::Success(_) => Outcome::Error((Status::Forbidden, ())),
            _ => Outcome::Error((Status::Unauthorized, ())),
        }
    }
}

pub struct AdminOnly(pub Claims);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AdminOnly {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        match AuthenticatedUser::from_request(request).await {
            Outcome::Success(u) if u.0.role == "admin" =>
                Outcome::Success(AdminOnly(u.0)),
            Outcome::Success(_) => Outcome::Error((Status::Forbidden, ())),
            _ => Outcome::Error((Status::Unauthorized, ())),
        }
    }
}

pub struct AdminOrSupervisor(pub Claims);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AdminOrSupervisor {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
        match AuthenticatedUser::from_request(request).await {
            Outcome::Success(u) if matches!(u.0.role.as_str(), "admin" | "supervisor") =>
                Outcome::Success(AdminOrSupervisor(u.0)),
            Outcome::Success(_) => Outcome::Error((Status::Forbidden, ())),
            _ => Outcome::Error((Status::Unauthorized, ())),
        }
    }
}