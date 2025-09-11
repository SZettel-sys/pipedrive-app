import os
import re
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List
from rapidfuzz import fuzz
from pyphonetics import Soundex

app = FastAPI()
soundex = Soundex()

# ================== Konfiguration ==================
CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise ValueError("❌ BASE_URL ist nicht gesetzt (z. B. https://app-dublicheck.onrender.com)")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"

OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

user_tokens = {}

# ================== Static Files ==================
app.mount("/static", StaticFiles(directory="static"), name="static")

# ================== Root Redirect ==================
@app.get("/")
def root():
    return RedirectResponse("/overview")

# ================== Login ==================
@app.get("/login")
def login():
    return RedirectResponse(
        f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    )

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        return HTMLResponse(f"<h3>❌ Fehler beim Login: {token_data}</h3>")

    user_tokens["default"] = access_token
    print("✅ Login erfolgreich, Token gespeichert.")
    return RedirectResponse("/overview")

# ================== Auth Helper ==================
def get_auth():
    api_token = os.getenv("PD_API_TOKEN")
    if api_token:
        return {}, {"api_token": api_token}
    token = user_tokens.get("default")
    if token:
        return {"Authorization": f"Bearer {token}"}, {}
    return {}, {}

# ================== Normalizer ==================
def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r"\b(gmbh|ag|ug|ltd|inc|co|kg|ohg)\b", "", n)
    n = re.sub(r"[^a-z0-9 ]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

# ================== Blocking Key ==================
def make_block_key(name: str) -> str:
    norm = normalize_name(name)
    if not norm:
        return ""
    parts = norm.split()
    main = parts[0] if parts else ""
    sound = soundex.phonetics(main) if main else ""
    length_class = str(len(norm) // 5)
    return f"{sound}_{length_class}"

# ================== Prepare Merge ==================
async def prepare_merge(keep_id: int, merge_id: int, headers, params):
    async with httpx.AsyncClient() as client:
        keep_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{keep_id}", headers=headers, params=params)
        merge_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{merge_id}", headers=headers, params=params)

        keep_org = keep_resp.json().get("data", {})
        merge_org = merge_resp.json().get("data", {})

        updates = {}
        for field in ["address", "website", "label_id"]:
            if not keep_org.get(field) and merge_org.get(field):
                updates[field] = merge_org.get(field)

        if updates:
            print(f"📝 Ergänze Felder in Org {keep_id}: {updates}")
            await client.put(
                f"{PIPEDRIVE_API_URL}/organizations/{keep_id}",
                headers=headers,
                params=params,
                json=updates,
            )

# ================== Scan Organisations ==================
@app.get("/scan_orgs")
async def scan_orgs(threshold: int = 80):
    headers, params = get_auth()
    if not headers and not params:
        return {"ok": False, "error": "Nicht eingeloggt"}

    orgs = []
    start = 0
    limit = 500
    more_items = True

    async with httpx.AsyncClient() as client:
        # Labels laden
        label_map = {}
        label_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizationLabels", headers=headers, params=params)
        labels = label_resp.json().get("data", [])
        for l in labels:
            label_map[l["id"]] = {"name": l["name"], "color": l.get("color", "#666")}

        while more_items:
            resp = await client.get(
                f"{PIPEDRIVE_API_URL}/organizations",
                headers=headers,
                params={**params, "start": start, "limit": limit},
            )
            data = resp.json()
            items = data.get("data") or []

            for org in items:
                org["deal_count"] = org.get("open_deals_count", 0)
                org["contact_count"] = org.get("people_count", 0)

                # Label-Fix
                label_id = org.get("label_id") or org.get("label")
                if isinstance(label_id, dict):
                    label_id = label_id.get("id")
                if label_id and label_id in label_map:
                    org["label_name"] = label_map[label_id]["name"]
                    org["label_color"] = label_map[label_id]["color"]
                else:
                    org["label_name"] = "-"
                    org["label_color"] = "#999"

                org["address"] = org.get("address") or "-"
                org["website"] = org.get("website") or "-"
                if "owner_id" in org and isinstance(org["owner_id"], dict):
                    org["owner_name"] = org["owner_id"].get("name", "-")
                else:
                    org["owner_name"] = "-"

            orgs.extend(items)
            more_items = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            start += limit

    print(f"🔎 Insgesamt {len(orgs)} Organisationen geladen.")
    return {"ok": True, "pairs": [], "meta": {"orgs_total": len(orgs)}}

# ================== Preview Merge ==================
@app.get("/preview_merge/{org_id}")
async def preview_merge(org_id: int):
    headers, params = get_auth()
    if not headers and not params:
        return {"ok": False, "error": "Nicht eingeloggt"}

    async with httpx.AsyncClient() as client:
        org_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{org_id}", headers=headers, params=params)
        org = org_resp.json().get("data", {})

        label_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizationLabels", headers=headers, params=params)
        labels = label_resp.json().get("data", [])
        label_map = {l["id"]: l["name"] for l in labels}

        label_id = org.get("label_id") or org.get("label")
        if isinstance(label_id, dict):
            label_id = label_id.get("id")
        label_name = label_map.get(label_id, "-")

    return {
        "ok": True,
        "org": {
            "id": org.get("id"),
            "name": org.get("name"),
            "owner": org.get("owner_id", {}).get("name", "-"),
            "website": org.get("website") or "-",
            "address": org.get("address") or "-",
            "label": label_name,
            "deals": org.get("open_deals_count", 0),
            "contacts": org.get("people_count", 0),
        },
    }

# ================== HTML Overview ==================
@app.get("/overview")
async def overview(request: Request):
    if not get_auth():
        return RedirectResponse("/login")

    html = """
    <html>
    <head>
        <title></title>
        <style>
          body { font-family: 'Source Sans Pro', Arial, sans-serif; background:#f4f6f8; margin:0; padding:0; }
          header { display:flex; justify-content:center; background:#3f51b5; padding:15px; }
          header img { height: 120px; }
          .container { padding:20px; }
          button { padding:10px 18px; border:none; border-radius:6px; font-size:15px; cursor:pointer; font-family: 'Source Sans Pro', Arial, sans-serif; }
          .btn-scan { background:#009fe3; color:white; }
          .btn-bulk { background:#5bc0eb; color:white; }
          .btn-merge { background:#1565c0; color:white; }
          .pair { background:white; border:1px solid #ddd; border-radius:8px; margin-bottom:20px; }
          .pair-table { width:100%; border-collapse:collapse; }
          .pair-table th { width:50%; padding:20px; background:#f0f0f0; text-align:center; vertical-align:top; }
          .org-table { width:100%; border-collapse:collapse; margin:12px 20px; }
          .org-table td { padding:4px 8px; vertical-align:top; }
          .org-table td.label { font-weight:600; width:90px; }
          .org-table td.value { font-weight:400; }
          .org-table td.value b { font-weight:600; }
          .badge { padding:2px 6px; border-radius:4px; font-size:12px; color:white; }
        </style>
    </head>
    <body>
        <header>
            <img src="/static/expert-biz-logo.png" alt="Logo">
        </header>
        <div class="container">
            <button class="btn-scan" onclick="loadData()">🔎 Scan starten</button>
            <button class="btn-bulk" onclick="bulkMerge()">🚀 Bulk Merge ausführen</button>
            <div id="results"></div>
        </div>
        <script>
        async function loadData(){
            let res = await fetch('/scan_orgs?threshold=80');
            let data = await res.json();
            let div = document.getElementById("results");
            div.innerHTML = "<pre>"+JSON.stringify(data,null,2)+"</pre>"; // Debug-Ausgabe
        }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ================== Lokaler Start ==================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, loop="uvloop", http="httptools")
