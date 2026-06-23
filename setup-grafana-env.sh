#!/bin/bash
# Generate .env file untuk Grafana BigQuery datasource dari service_account.json
# Usage: ./setup-grafana-env.sh

if [ ! -f "service_account.json" ]; then
    echo "❌ service_account.json not found"
    echo "   Download it from GCP Console → IAM → Service Accounts → Create Key → JSON"
    exit 1
fi

# Extract values from service_account.json
BQ_PROJECT=$(python3 -c "import json; print(json.load(open('service_account.json'))['project_id'])")
BQ_CLIENT_EMAIL=$(python3 -c "import json; print(json.load(open('service_account.json'))['client_email'])")
BQ_PRIVATE_KEY=$(python3 -c "
import json
key = json.load(open('service_account.json'))['private_key']
# Escape newlines for .env (replace literal \n with \\n)
print(key.replace(chr(10), '\\\\n'))
")

cat > .env <<EOF
# Auto-generated from service_account.json — $(date)
# BigQuery
BQ_PROJECT=${BQ_PROJECT}
BQ_CLIENT_EMAIL=${BQ_CLIENT_EMAIL}
BQ_PRIVATE_KEY=${BQ_PRIVATE_KEY}

# Grafana admin (ubah password setelah login pertama)
GRAFANA_USER=admin
GRAFANA_PASSWORD=admin
EOF

echo "✅ .env generated"
echo "   BQ_PROJECT      = ${BQ_PROJECT}"
echo "   BQ_CLIENT_EMAIL = ${BQ_CLIENT_EMAIL}"
echo ""
echo "Run: docker compose up -d grafana"
echo "Open: http://localhost:3000"
