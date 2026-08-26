# Internal CA certificates

If GM's domain controller (`LDAP_URL=ldaps://...`) presents a certificate signed
by an internal/private CA rather than a public one, put that CA's `.crt` file
in this directory, then uncomment the corresponding `COPY`/`RUN update-ca-certificates`
lines in `../Dockerfile`.

Get the file from GM IT — this repo doesn't ship one.
