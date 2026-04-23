use serde::{Deserialize, Serialize};
use rocket::figment::{Figment, providers::{Env, Toml}};
use rocket::fairing::AdHoc;
use std::env;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EmailConfig {
    /// Email address that will appear in the 'From' field
    pub sending_email: String,
    
    /// Email address where emails should be sent
    pub destination_email: String,
    
    /// Optional SMTP relay host
    pub mail_relay_host: Option<String>,
    
    /// SMTP relay port (defaults to 25 if not specified)
    pub mail_relay_port: Option<u16>,
    
    /// Optional SMTP username for authentication
    pub smtp_username: Option<String>,
    
    /// Optional SMTP password for authentication
    pub smtp_password: Option<String>,
    
    /// Whether to use TLS for SMTP
    pub smtp_use_tls: bool,
    
    /// Default email subject if none provided
    pub default_subject: String,
}

impl EmailConfig {
    pub fn from_env() -> Result<Self, config::ConfigError> {
        let config = config::Config::builder()
            .set_default("smtp_port", 25)?
            .set_default("smtp_use_tls", false)?
            .set_default("default_subject", "ASL/ASN Analysis Report")?
            .add_source(config::Environment::with_prefix("EMAIL"))
            .build()?;

        config.try_deserialize()
    }
    
    /// Get the SMTP relay port, defaulting to 25 if not set
    pub fn get_mail_relay_port(&self) -> u16 {
        self.mail_relay_port.unwrap_or(25)
    }
    
    /// Determine if we should use SMTP relay or local sendmail
    pub fn use_smtp_relay(&self) -> bool {
        self.mail_relay_host.is_some()
    }
}

/// Rocket fairing to manage email configuration
pub fn stage() -> AdHoc {
    AdHoc::config::<EmailConfig>()
}