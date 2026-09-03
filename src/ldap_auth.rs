use ldap3::{LdapConnAsync, Scope, SearchEntry};

#[derive(Debug)]
pub enum LdapAuthError {
    Config(String),
    Connection(String),
    Bind(String),
    UserNotFound,
    InvalidCredentials,
    NoRoleAssigned,
}

impl std::fmt::Display for LdapAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LdapAuthError::Config(e)          => write!(f, "LDAP misconfigured: {e}"),
            LdapAuthError::Connection(e)      => write!(f, "LDAP connection error: {e}"),
            LdapAuthError::Bind(e)            => write!(f, "LDAP error: {e}"),
            LdapAuthError::UserNotFound                   => write!(f, "User not found"),
            LdapAuthError::InvalidCredentials             => write!(f, "Invalid credentials"),
            LdapAuthError::NoRoleAssigned                 => write!(f, "User is not a member of any authorized group"),
        }
    }
}

pub struct LdapUser {
    pub role: String,
}

// AD group DN -> app role. Priority order matters: if a user is a member of
// multiple mapped groups, the highest-privilege one wins.
const GROUP_ROLE_ENV_VARS: [(&str, &str); 4] = [
    ("LDAP_GROUP_ADMIN",   "admin"),
    ("LDAP_GROUP_MANAGER", "manager"),
    ("LDAP_GROUP_FLOATER", "floater"),
    ("LDAP_GROUP_MFU",     "mfu"),
];

fn role_from_groups(member_of: &[String]) -> Option<String> {
    for (env_key, role) in GROUP_ROLE_ENV_VARS {
        if let Ok(group_dn) = std::env::var(env_key) {
            if !group_dn.is_empty() && member_of.iter().any(|dn| dn.eq_ignore_ascii_case(&group_dn)) {
                return Some(role.to_string());
            }
        }
    }
    None
}

// Escapes an LDAP filter value per RFC 4515 so a submitted username can't
// break out of the filter (e.g. via `*`, `(`, `)`, `\`).
fn escape_filter_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\5c"),
            '*'  => out.push_str("\\2a"),
            '('  => out.push_str("\\28"),
            ')'  => out.push_str("\\29"),
            '\0' => out.push_str("\\00"),
            _    => out.push(c),
        }
    }
    out
}

pub async fn authenticate(username: &str, password: &str) -> Result<LdapUser, LdapAuthError> {
    // A simple bind with an empty password is an *anonymous* bind and many
    // directory servers report that as a success — never let that through.
    if password.is_empty() {
        return Err(LdapAuthError::InvalidCredentials);
    }

    let ldap_url = std::env::var("LDAP_URL").map_err(|_| LdapAuthError::Config("LDAP_URL not set".into()))?;
    // Every bind on this connection carries a real password — the service
    // account's, then every user's during the rebind step. Refuse to send
    // any of that in plaintext.
    if !ldap_url.to_ascii_lowercase().starts_with("ldaps://") {
        return Err(LdapAuthError::Config(
            "LDAP_URL must use ldaps:// — plain ldap:// would send bind passwords in plaintext".into()
        ));
    }
    let bind_dn     = std::env::var("LDAP_BIND_DN").map_err(|_| LdapAuthError::Config("LDAP_BIND_DN not set".into()))?;
    let bind_pw     = std::env::var("LDAP_BIND_PASSWORD").map_err(|_| LdapAuthError::Config("LDAP_BIND_PASSWORD not set".into()))?;
    let search_base = std::env::var("LDAP_USER_SEARCH_BASE").map_err(|_| LdapAuthError::Config("LDAP_USER_SEARCH_BASE not set".into()))?;
    let domain      = std::env::var("LDAP_DOMAIN").unwrap_or_default();

    // ── Step 1: bind as the service account and look up the user's DN + groups ──
    let (conn, mut ldap) = LdapConnAsync::new(&ldap_url)
        .await
        .map_err(|e| LdapAuthError::Connection(e.to_string()))?;
    ldap3::drive!(conn);

    ldap.simple_bind(&bind_dn, &bind_pw)
        .await
        .map_err(|e| LdapAuthError::Bind(e.to_string()))?
        .success()
        .map_err(|e| LdapAuthError::Bind(format!("service account bind failed: {e}")))?;

    let safe_username = escape_filter_value(username);
    let filter = if domain.is_empty() {
        format!("(sAMAccountName={safe_username})")
    } else {
        format!("(|(sAMAccountName={safe_username})(userPrincipalName={safe_username}@{domain}))")
    };

    let search_result = ldap
        .search(&search_base, Scope::Subtree, &filter, vec!["memberOf"])
        .await
        .map_err(|e| LdapAuthError::Bind(e.to_string()))?;
    let (entries, _res) = search_result
        .success()
        .map_err(|e| LdapAuthError::Bind(e.to_string()))?;

    let _ = ldap.unbind().await;

    let raw_entry = entries.into_iter().next().ok_or(LdapAuthError::UserNotFound)?;
    let entry = SearchEntry::construct(raw_entry);
    let user_dn = entry.dn;
    let member_of = entry.attrs.get("memberOf").cloned().unwrap_or_default();

    // ── Step 2: rebind as the found user DN with the password they supplied ──
    let (conn2, mut ldap2) = LdapConnAsync::new(&ldap_url)
        .await
        .map_err(|e| LdapAuthError::Connection(e.to_string()))?;
    ldap3::drive!(conn2);

    ldap2
        .simple_bind(&user_dn, password)
        .await
        .map_err(|_| LdapAuthError::InvalidCredentials)?
        .success()
        .map_err(|_| LdapAuthError::InvalidCredentials)?;

    let _ = ldap2.unbind().await;

    let role = role_from_groups(&member_of).ok_or(LdapAuthError::NoRoleAssigned)?;

    Ok(LdapUser { role })
}
