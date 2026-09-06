#!/bin/sh
# Trust any internal CA cert(s) mounted from the OpenShift Secret (see
# k8s/deployment.yaml's ca-certs volume) before the app makes its first
# ldaps:// connection. No-op locally / anywhere that Secret isn't mounted.
if [ -d /usr/local/share/ca-certificates/custom ] && [ -n "$(ls -A /usr/local/share/ca-certificates/custom 2>/dev/null)" ]; then
    update-ca-certificates
fi

if [ "$USE_HTTPS" = "true" ]; then
    exec ./rocket_http --https
else
    exec ./rocket_http
fi
