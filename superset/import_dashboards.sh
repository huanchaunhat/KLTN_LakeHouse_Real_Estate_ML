#!/usr/bin/env bash
# Build the dashboard bundle from build_dashboards.py and load it into the running Superset.
#
#   docker compose up -d superset
#   bash superset/import_dashboards.sh
#
# Superset's dashboard import only overwrites dashboards, never existing charts or
# datasets, so the charts and datasets declared in build_dashboards.py are deleted
# first and re-created (same UUIDs). Hand-made charts on other datasets are untouched.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
URL=${SUPERSET_URL:-http://localhost:8089}
USER=${SUPERSET_USER:-admin}
PASS=${SUPERSET_PASSWORD:-admin}

"$PY" superset/build_dashboards.py

"$PY" - "$URL" "$USER" "$PASS" <<'PYEOF'
import json, sys, urllib.request
sys.path.insert(0, "superset")
import build_dashboards as b
url, user, password = sys.argv[1:4]

def api(method, path, body=None, token=None):
    req = urllib.request.Request(url + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json", **({"Authorization": "Bearer " + token} if token else {})})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

token = api("POST", "/api/v1/security/login", {"username": user, "password": password, "provider": "db"})["access_token"]
mine = {d["id"] for d in api("GET", "/api/v1/dataset/?q=(page_size:500)", token=token)["result"] if d["table_name"] in b.DATASETS}
charts = [c["id"] for c in api("GET", "/api/v1/chart/?q=(page_size:500)", token=token)["result"] if c["datasource_id"] in mine]
if charts:
    print(api("DELETE", f"/api/v1/chart/?q=!({','.join(map(str, charts))})", token=token)["message"])
if mine:
    print(api("DELETE", f"/api/v1/dataset/?q=!({','.join(map(str, mine))})", token=token)["message"])
PYEOF

docker compose cp superset/dashboards.zip superset:/tmp/reip_dashboards.zip
docker compose exec -T superset superset import-dashboards -p /tmp/reip_dashboards.zip -u "$USER" \
  2>&1 | grep -E "Traceback|CommandInvalidError|DashboardImportError" && { echo "import failed"; exit 1; } || true

# The import format has no "published" flag; publish through the REST API.
"$PY" - "$URL" "$USER" "$PASS" <<'PYEOF'
import json, sys, urllib.request
sys.path.insert(0, "superset")
import build_dashboards as b
url, user, password = sys.argv[1:4]

def api(method, path, body=None, token=None):
    req = urllib.request.Request(url + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json", **({"Authorization": "Bearer " + token} if token else {})})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

token = api("POST", "/api/v1/security/login", {"username": user, "password": password, "provider": "db"})["access_token"]
for d in b.DASHBOARDS.values():
    did = api("GET", f"/api/v1/dashboard/{d['slug']}", token=token)["result"]["id"]
    api("PUT", f"/api/v1/dashboard/{did}", {"published": True}, token=token)
    print(f"published {url}/superset/dashboard/{d['slug']}/")
PYEOF
