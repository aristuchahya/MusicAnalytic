#!/bin/bash
# Generate .env file untuk Grafana datasources dari service_account.json + config.ini
# Usage: ./setup-grafana-env.sh

if [ ! -f "service_account.json" ]; then
    echo "❌ service_account.json not found"
    echo "   Download it from GCP Console → IAM → Service Accounts → Create Key → JSON"
    exit 1
fi

# --- BigQuery ---
BQ_PROJECT=$(python3 -c "import json; print(json.load(open('service_account.json'))['project_id'])")
BQ_CLIENT_EMAIL=$(python3 -c "import json; print(json.load(open('service_account.json'))['client_email'])")
BQ_PRIVATE_KEY=$(python3 -c "
import json
key = json.load(open('service_account.json'))['private_key']
print(key.replace(chr(10), '\\\\n'))
")

# --- PostgreSQL (from config.ini) ---
PG_HOST=$(python3 -c "
import configparser
c = configparser.ConfigParser()
c.read('config.ini')
print(c['postgresql'].get('host', 'localhost'))
")
PG_PORT=$(python3 -c "
import configparser
c = configparser.ConfigParser()
c.read('config.ini')
print(c['postgresql'].get('port', '5432'))
")
PG_USER=$(python3 -c "
import configparser
c = configparser.ConfigParser()
c.read('config.ini')
print(c['postgresql']['username'])
")
PG_PASSWORD=$(python3 -c "
import configparser
c = configparser.ConfigParser()
c.read('config.ini')
print(c['postgresql']['password'])
")
PG_DBNAME=$(python3 -c "
import configparser
c = configparser.ConfigParser()
c.read('config.ini')
print(c['postgresql']['dbname'])
")

# --- Kafka ---
KAFKA_BROKER=$(python3 -c "
import configparser
c = configparser.ConfigParser()
c.read('config.ini')
print(c['kafka'].get('bootstrap_servers', 'kafka:29092'))
")

cat > .env <<EOF
# Auto-generated — $(date)
# BigQuery
BQ_PROJECT=${BQ_PROJECT}
BQ_CLIENT_EMAIL=${BQ_CLIENT_EMAIL}
BQ_PRIVATE_KEY=${BQ_PRIVATE_KEY}

# PostgreSQL
PG_HOST=${PG_HOST}
PG_PORT=${PG_PORT}
PG_USER=${PG_USER}
PG_PASSWORD=${PG_PASSWORD}
PG_DBNAME=${PG_DBNAME}

# Kafka
KAFKA_BROKER=${KAFKA_BROKER}

# Grafana admin
GRAFANA_USER=admin
GRAFANA_PASSWORD=admin
EOF

echo "✅ .env generated"
echo ""
echo "BigQuery:"
echo "   project = ${BQ_PROJECT}"
echo "   email   = ${BQ_CLIENT_EMAIL}"
echo ""
echo "PostgreSQL:"
echo "   host = ${PG_HOST}:${PG_PORT}"
echo "   db   = ${PG_DBNAME}"
echo "   user = ${PG_USER}"
echo ""
echo "Kafka:"
echo "   broker = ${KAFKA_BROKER}"
echo ""
echo "Run: docker compose up -d grafana"
echo "Open: http://localhost:3000"
