#!/usr/bin/env bash
# Builds nginx with the SPNEGO/Kerberos auth module and installs a config
# that proxies to Rocket and injects X-Remote-User after Kerberos validation.
#
# Run as root on the server. Set NGINX_VERSION if you want a specific release.
# After running, fill in /etc/nginx/conf.d/app.conf with your domain details
# and place your keytab at /etc/nginx/krb5.keytab.

set -euo pipefail

NGINX_VERSION="${NGINX_VERSION:-1.26.3}"
BUILD_DIR="/tmp/nginx-spnego-build"

echo "==> Installing build dependencies..."
apt-get update -qq
apt-get install -y --no-install-recommends \
    build-essential libpcre3-dev zlib1g-dev libssl-dev \
    libkrb5-dev krb5-user git wget ca-certificates

echo "==> Downloading nginx $NGINX_VERSION source..."
mkdir -p "$BUILD_DIR" && cd "$BUILD_DIR"
wget -q "http://nginx.org/download/nginx-${NGINX_VERSION}.tar.gz"
tar xzf "nginx-${NGINX_VERSION}.tar.gz"

echo "==> Cloning spnego-http-auth-nginx-module..."
git clone --depth=1 https://github.com/stnoonan/spnego-http-auth-nginx-module.git

echo "==> Configuring and building nginx..."
cd "nginx-${NGINX_VERSION}"
./configure \
    --prefix=/etc/nginx \
    --sbin-path=/usr/sbin/nginx \
    --modules-path=/etc/nginx/modules \
    --conf-path=/etc/nginx/nginx.conf \
    --error-log-path=/var/log/nginx/error.log \
    --http-log-path=/var/log/nginx/access.log \
    --pid-path=/var/run/nginx.pid \
    --with-http_ssl_module \
    --with-http_v2_module \
    --with-http_realip_module \
    --add-module=../spnego-http-auth-nginx-module

make -j"$(nproc)"
make install

echo "==> Creating systemd service..."
cat > /etc/systemd/system/nginx.service <<'UNIT'
[Unit]
Description=nginx SPNEGO proxy
After=network.target

[Service]
Type=forking
PIDFile=/var/run/nginx.pid
ExecStartPre=/usr/sbin/nginx -t
ExecStart=/usr/sbin/nginx
ExecReload=/bin/kill -s HUP $MAINPID
ExecStop=/bin/kill -s QUIT $MAINPID
PrivateTmp=true

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable nginx

echo "==> Writing nginx config..."
mkdir -p /etc/nginx/conf.d
cat > /etc/nginx/conf.d/app.conf <<'NGINX'
# ── Fill in the values marked CHANGE_ME ──────────────────────────────────────
server {
    listen 443 ssl http2;
    server_name CHANGE_ME_HOSTNAME;           # e.g. modms.corp.com

    ssl_certificate     /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;
    ssl_ciphers         HIGH:!aNULL:!MD5;

    # Kerberos/SPNEGO — Edge negotiates this automatically on intranet
    auth_gss            on;
    auth_gss_realm      CHANGE_ME_REALM;      # e.g. CORP.COM  (uppercase)
    auth_gss_keytab     /etc/nginx/krb5.keytab;
    auth_gss_service_name HTTP;
    auth_gss_allow_basic_fallback off;

    location / {
        proxy_pass         http://127.0.0.1:8000;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto https;
        # Strip any client-supplied header so it cannot be spoofed,
        # then set it from nginx's validated $remote_user.
        proxy_set_header   X-Remote-User     "";
        proxy_set_header   X-Remote-User     $remote_user;
    }
}

# Redirect HTTP → HTTPS
server {
    listen 80;
    server_name CHANGE_ME_HOSTNAME;
    return 301 https://$host$request_uri;
}
NGINX

echo ""
echo "Done. Next steps:"
echo "  1. Place your TLS cert/key at /etc/nginx/ssl/cert.pem and key.pem"
echo "  2. Place your Kerberos keytab at /etc/nginx/krb5.keytab"
echo "     (Generate with: ktpass -princ HTTP/hostname@REALM -mapuser svc_http -crypto AES256-SHA1 -ptype KRB5_NT_PRINCIPAL -pass * -out krb5.keytab)"
echo "  3. Edit /etc/nginx/conf.d/app.conf — replace CHANGE_ME_HOSTNAME and CHANGE_ME_REALM"
echo "  4. Configure /etc/krb5.conf with your domain (see below)"
echo "  5. Set env vars in your Rocket .env: LDAP_BIND_DN, LDAP_BIND_PASSWORD"
echo "  6. systemctl start nginx"
echo ""
echo "Minimal /etc/krb5.conf:"
echo "  [libdefaults]"
echo "      default_realm = CHANGE_ME_REALM"
echo "  [realms]"
echo "      CHANGE_ME_REALM = { kdc = your-dc.corp.com }"
echo "  [domain_realm]"
echo "      .corp.com = CHANGE_ME_REALM"
