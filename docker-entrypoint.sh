#!/bin/sh
if [ "$USE_HTTPS" = "true" ]; then
    exec ./rocket_http --https
else
    exec ./rocket_http
fi
