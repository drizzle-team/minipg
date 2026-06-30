#!/usr/bin/env bash
set -euo pipefail
PGDATA="$(cd "$(dirname "$0")" && pwd)/.pgdata"
PGPORT=54329
SOCK=/tmp/minipg_sock
export PGPASSWORD=postgres

pg_ctl -D "$PGDATA" stop -m immediate >/dev/null 2>&1 || true
rm -rf "$PGDATA"; mkdir -p "$SOCK"
printf 'postgres\n' > /tmp/minipg_pw
initdb -D "$PGDATA" -U postgres --auth-host=scram-sha-256 --auth-local=trust --pwfile=/tmp/minipg_pw >/dev/null

cat > "$PGDATA/pg_hba.conf" <<EOF
local   all all                trust
host    all md5user 127.0.0.1/32 md5
host    all md5user ::1/128      md5
host    all all     127.0.0.1/32 scram-sha-256
host    all all     ::1/128      scram-sha-256
EOF

# self-signed server cert so the TLS suite can do a real handshake (CN=localhost)
openssl req -new -x509 -days 3650 -nodes -text -subj "/CN=localhost" \
  -out "$PGDATA/server.crt" -keyout "$PGDATA/server.key" >/dev/null 2>&1
chmod 600 "$PGDATA/server.key"
cat >> "$PGDATA/postgresql.conf" <<EOF
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
EOF

pg_ctl -D "$PGDATA" -o "-p $PGPORT -k $SOCK -c listen_addresses=127.0.0.1" -l "$PGDATA/server.log" -w start

psql "host=127.0.0.1 port=$PGPORT user=postgres dbname=postgres" -v ON_ERROR_STOP=1 <<SQL
SET password_encryption='md5';
CREATE ROLE md5user LOGIN PASSWORD 'md5pw';
SET password_encryption='scram-sha-256';
CREATE ROLE scramuser LOGIN PASSWORD 'scrampw';
CREATE DATABASE testdb OWNER postgres;
SQL

psql "host=127.0.0.1 port=$PGPORT user=postgres dbname=testdb" -v ON_ERROR_STOP=1 <<SQL
CREATE TABLE t(id serial primary key, name text, n8 bigint, amount numeric, ok bool, data jsonb, blob bytea);
INSERT INTO t(name,n8,amount,ok,data,blob) VALUES
 ('alice', 9007199254740993, 1234.56, true, '{"a":1}', '\xdeadbeef'),
 ('bob',   42,               0.1,     false,'[1,2,3]', '\x00ff'),
 (NULL,    NULL,             NULL,    NULL, NULL,      NULL);
SQL

echo "PG READY on 127.0.0.1:$PGPORT (db=testdb user=postgres pw=postgres; md5user/md5pw; scramuser/scrampw)"
