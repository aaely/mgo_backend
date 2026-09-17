use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use url::Url;

#[derive(Debug)]
pub enum AzureAuthError {
    Config(String),
    Http(String),
    InvalidState,
    InvalidToken(String),
    NoRoleAssigned,
}

impl std::fmt::Display for AzureAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AzureAuthError::Config(e)       => write!(f, "Azure AD misconfigured: {e}"),
            AzureAuthError::Http(e)         => write!(f, "Azure AD request failed: {e}"),
            AzureAuthError::InvalidState    => write!(f, "Invalid or expired sign-in attempt"),
            AzureAuthError::InvalidToken(e) => write!(f, "Invalid identity token: {e}"),
            AzureAuthError::NoRoleAssigned  => write!(f, "User is not a member of any authorized group"),
        }
    }
}

pub struct AzureUser {
    pub username: String,
    pub role: String,
}

// Azure AD group Object ID (GUID) -> app role. Same four roles and same
// priority order as ldap_auth.rs: first match wins, so this is ordered
// most-privileged first.
const GROUP_ROLE_ENV_VARS: [(&str, &str); 4] = [
    ("AZURE_GROUP_ADMIN",   "admin"),
    ("AZURE_GROUP_MANAGER", "manager"),
    ("AZURE_GROUP_FLOATER", "floater"),
    ("AZURE_GROUP_MFU",     "mfu"),
];

fn role_from_groups(groups: &[String]) -> Option<String> {
    for (env_key, role) in GROUP_ROLE_ENV_VARS {
        if let Ok(group_id) = std::env::var(env_key) {
            if !group_id.is_empty() && groups.iter().any(|g| g.eq_ignore_ascii_case(&group_id)) {
                return Some(role.to_string());
            }
        }
    }
    // Temporary, mirroring the same shortcut in ldap_auth.rs so the Azure
    // round-trip can be exercised before the Entra groups exist. Revert to
    // None alongside that one at onboarding.
    Some("admin".to_string())
    //None
}

fn tenant_id() -> Result<String, AzureAuthError> {
    std::env::var("AZURE_TENANT_ID")
        .map_err(|_| AzureAuthError::Config("AZURE_TENANT_ID not set".into()))
}

fn client_id() -> Result<String, AzureAuthError> {
    std::env::var("AZURE_CLIENT_ID")
        .map_err(|_| AzureAuthError::Config("AZURE_CLIENT_ID not set".into()))
}

fn redirect_uri() -> Result<String, AzureAuthError> {
    std::env::var("AZURE_REDIRECT_URI")
        .map_err(|_| AzureAuthError::Config("AZURE_REDIRECT_URI not set".into()))
}

/// Step 1 — where to bounce the browser. `state` is a caller-generated random
/// value that must round-trip through Microsoft unchanged; the caller stashes
/// it in a short-lived cookie and re-checks it on the callback as CSRF defense.
pub fn authorize_url(state: &str) -> Result<String, AzureAuthError> {
    let tenant   = tenant_id()?;
    let client   = client_id()?;
    let redirect = redirect_uri()?;

    let mut url = Url::parse(&format!(
        "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize"
    ))
    .map_err(|e| AzureAuthError::Config(e.to_string()))?;

    url.query_pairs_mut()
        .append_pair("client_id",     &client)
        .append_pair("response_type", "code")
        .append_pair("redirect_uri",  &redirect)
        .append_pair("response_mode", "query")
        .append_pair("scope",         "openid profile email")
        .append_pair("state",         state);

    Ok(url.into())
}

#[derive(Deserialize)]
struct TokenResponse {
    id_token: String,
}

/// Step 2 — trade the one-time `code` for an ID token. This leg needs
/// AZURE_CLIENT_SECRET, so it only ever happens server-side; the secret must
/// never reach the browser.
async fn exchange_code(code: &str) -> Result<String, AzureAuthError> {
    let tenant   = tenant_id()?;
    let client   = client_id()?;
    let redirect = redirect_uri()?;
    let secret   = std::env::var("AZURE_CLIENT_SECRET")
        .map_err(|_| AzureAuthError::Config("AZURE_CLIENT_SECRET not set".into()))?;

    let token_url = format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token");

    let params = [
        ("client_id",     client.as_str()),
        ("client_secret", secret.as_str()),
        ("grant_type",    "authorization_code"),
        ("code",          code),
        ("redirect_uri",  redirect.as_str()),
        ("scope",         "openid profile email"),
    ];

    let resp = reqwest::Client::new()
        .post(&token_url)
        .form(&params)
        .send()
        .await
        .map_err(|e| AzureAuthError::Http(e.to_string()))?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(AzureAuthError::Http(format!("token exchange failed: {body}")));
    }

    let token: TokenResponse = resp
        .json()
        .await
        .map_err(|e| AzureAuthError::Http(e.to_string()))?;

    Ok(token.id_token)
}

#[derive(Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Deserialize)]
struct Jwk {
    kid: String,
    n:   String,
    e:   String,
}

#[derive(Debug, Deserialize)]
struct IdTokenClaims {
    oid: String,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    email: Option<String>,
    // Azure drops this claim entirely once a user is in more than ~200 groups
    // ("group overage") and substitutes a _claim_names pointer at the Graph API
    // instead — not handled here. If someone reports "not a member of any
    // authorized group" despite real membership, check that first.
    #[serde(default)]
    groups: Vec<String>,
}

/// Step 3 — prove the ID token came from our tenant, for our app. The
/// signature is checked against Microsoft's published keys, and `aud`/`iss`
/// are pinned so a token minted for a different app can't be replayed here.
async fn validate_id_token(id_token: &str) -> Result<IdTokenClaims, AzureAuthError> {
    let tenant = tenant_id()?;
    let client = client_id()?;

    let header = decode_header(id_token)
        .map_err(|e| AzureAuthError::InvalidToken(e.to_string()))?;
    let kid = header
        .kid
        .ok_or_else(|| AzureAuthError::InvalidToken("token header has no kid".into()))?;

    let jwks_url = format!("https://login.microsoftonline.com/{tenant}/discovery/v2.0/keys");
    let jwks: Jwks = reqwest::get(&jwks_url)
        .await
        .map_err(|e| AzureAuthError::Http(e.to_string()))?
        .json()
        .await
        .map_err(|e| AzureAuthError::Http(e.to_string()))?;

    let jwk = jwks
        .keys
        .iter()
        .find(|k| k.kid == kid)
        .ok_or_else(|| AzureAuthError::InvalidToken("signing key not in Azure AD's key set".into()))?;

    let decoding_key = DecodingKey::from_rsa_components(&jwk.n, &jwk.e)
        .map_err(|e| AzureAuthError::InvalidToken(e.to_string()))?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[client]);
    validation.set_issuer(&[format!("https://login.microsoftonline.com/{tenant}/v2.0")]);

    let data = decode::<IdTokenClaims>(id_token, &decoding_key, &validation)
        .map_err(|e| AzureAuthError::InvalidToken(e.to_string()))?;

    Ok(data.claims)
}

/// Verifies the CSRF state, then runs steps 2 and 3 and maps group membership
/// to an app role.
pub async fn complete_login(
    code: &str,
    state: &str,
    expected_state: Option<&str>,
) -> Result<AzureUser, AzureAuthError> {
    match expected_state {
        Some(expected) if expected == state => {}
        _ => return Err(AzureAuthError::InvalidState),
    }

    let id_token = exchange_code(code).await?;
    let claims   = validate_id_token(&id_token).await?;

    let username = claims
        .preferred_username
        .or(claims.email)
        .unwrap_or(claims.oid);

    let role = role_from_groups(&claims.groups).ok_or(AzureAuthError::NoRoleAssigned)?;

    Ok(AzureUser { username, role })
}
