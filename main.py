import os
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from rapidfuzz import fuzz

# ================== App Setup ==================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# ================== Konfiguration ==================
CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")
DB_URL = os.getenv("DATABASE_URL")

if not BASE_URL:
    raise ValueError("❌ BASE_URL ist nicht gesetzt (z. B. https://app-dublicheck.onrender.com)")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

# Tokens im Speicher (für Produktion: DB/Redis)
user_tokens = {}

# DB Engine (für Ignore-Tabelle)
engine = create_engine(DB_URL, pool_pre_ping=True)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ignored_pairs (
            id SERIAL PRIMARY KEY,
            org1_id BIGINT NOT NULL,
            org2_id BIGINT NOT NULL,
            UNIQUE(org1_id, org2_id)
        )
    """))

# ================== Root ==================
@app.get("/")
def root():
    return RedirectResponse("/overview")

# ================== Login starten ==================
@app.get("/login")
def login():
    return RedirectResponse(
        f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    )

# ================== Callback ==================
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
    return RedirectResponse("/overview")

# ================== Helpers ==================
def get_headers():
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}

async def fetch_org_details(client, org):
    """ Zusätzliche Infos laden: Deals, Kontakte, Label """
    org_id = org.get("id")
    details = {"deals": 0, "contacts": 0, "label": "-"}

    # Deals zählen
    deals = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{org_id}/deals", headers=get_headers())
    if deals.status_code == 200:
        djson = deals.json()
        details["deals"] = len(djson.get("data") or [])

    # Kontakte zählen
    persons = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{org_id}/persons", headers=get_headers())
    if persons.status_code == 200:
        pjson = persons.json()
        details["contacts"] = len(pjson.get("data") or [])

    # Label (sichtbares Feld statt ID)
    details["label"] = org.get("label") or "-"

    org["extra"] = details
    return org

def normalize_name(name: str) -> str:
    """ Vereinfachte Normalisierung für bessere Duplikat-Erkennung """
    if not name:
        return ""
    return (
        name.lower()
        .replace("gmbh", "")
        .replace("ag", "")
        .replace("&", "und")
        .replace("-", " ")
        .strip()
    )

# ================== Scan Orgs ==================
@app.get("/scan_orgs")
async def scan_orgs(threshold: int = 80):
    if "default" not in user_tokens:
        return {"ok": False, "error": "Nicht eingeloggt"}

    orgs = []
    start = 0
    limit = 500
    more_items = True

    async with httpx.AsyncClient() as client:
        while more_items:
            resp = await client.get(
                f"{PIPEDRIVE_API_URL}/organizations?start={start}&limit={limit}",
                headers=get_headers(),
            )
            data = resp.json()
            items = data.get("data") or []
            orgs.extend(items)

            more_items = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            start += limit

    # Extra-Infos laden
    async with httpx.AsyncClient() as client:
        orgs = [await fetch_org_details(client, org) for org in orgs]

    # Ignored Pairs aus DB laden
    with engine.begin() as conn:
        ignored = conn.execute(text("SELECT org1_id, org2_id FROM ignored_pairs")).fetchall()
        ignored_pairs = {(min(r[0], r[1]), max(r[0], r[1])) for r in ignored}

    # Duplikate suchen
    results = []
    for i, org1 in enumerate(orgs):
        for j, org2 in enumerate(orgs):
            if i >= j:
                continue
            pair = (min(org1["id"], org2["id"]), max(org1["id"], org2["id"]))
            if pair in ignored_pairs:
                continue

            score = fuzz.token_set_ratio(normalize_name(org1.get("name", "")), normalize_name(org2.get("name", "")))
            if score >= threshold:
                results.append({"org1": org1, "org2": org2, "score": round(score, 2)})

    return {"ok": True, "pairs": results, "total_orgs": len(orgs), "duplicates": len(results)}

# ================== Ignore Pair ==================
@app.post("/ignore_pair")
async def ignore_pair(org1_id: int, org2_id: int):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO ignored_pairs (org1_id, org2_id) VALUES (:o1, :o2) ON CONFLICT DO NOTHING"),
            {"o1": min(org1_id, org2_id), "o2": max(org1_id, org2_id)},
        )
    return {"ok": True}

# ================== Merge Orgs ==================
@app.post("/merge_orgs")
async def merge_orgs(org1_id: int, org2_id: int, keep_id: int):
    async with httpx.AsyncClient() as client:
        resp = await client.put(
            f"{PIPEDRIVE_API_URL}/organizations/{keep_id}/merge",
            headers=get_headers(),
            json={"merge_with_id": org2_id if keep_id == org1_id else org1_id},
        )

    if resp.status_code != 200:
        return {"ok": False, "error": resp.text}

    return {"ok": True, "result": resp.json()}

# ================== Overview HTML ==================
@app.get("/overview")
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    return HTMLResponse("""
    <html>
    <head>
      <title>Organisationen Übersicht</title>
      <style>
        body { font-family: Arial, sans-serif; background:#f4f6f8; margin:0; padding:0; }
        header { display:flex; justify-content:center; align-items:center; background:white; padding:15px; }
        header img { height:60px; }

        .container { padding:20px; }
        .btn { padding:8px 16px; border:none; border-radius:5px; cursor:pointer; margin:5px; }
        .btn-blue { background:#0096d6; color:white; }

        .pair { background:white; border:1px solid #ddd; border-radius:8px; margin-bottom:20px; }
        .pair-table { width:95%; margin:0 auto; border-collapse:collapse; }
        .pair-table td { padding:6px; vertical-align:top; }

        .conflict-row { background:#e3f2fd; padding:12px; display:flex; justify-content:space-between; align-items:center; }
        .similarity { font-size:13px; padding:8px; color:#333; }
      </style>
    </head>
    <body>
      <header>
        <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
      </header>

      <div class="container">
        <button class="btn btn-blue" onclick="loadData()">🔎 Scan starten</button>
        <div id="stats"></div>
        <div id="results"></div>
      </div>

      <script>
      async function loadData(){
        let res = await fetch('/scan_orgs?threshold=80');
        let data = await res.json();
        let stats = document.getElementById("stats");
        let results = document.getElementById("results");

        if(!data.ok){ stats.innerHTML = "<p>⚠️ Fehler beim Scan</p>"; return; }
        stats.innerHTML = `<p>Geladene Organisationen: <b>${data.total_orgs}</b><br>Duplikate insgesamt: <b>${data.duplicates}</b></p>`;

        results.innerHTML = data.pairs.map(p => `
          <div class="pair">
            <table class="pair-table">
              <tr>
                <td><b>Name:</b> ${p.org1.name}<br><b>ID:</b> ${p.org1.id}<br><b>Besitzer:</b> ${p.org1.owner_id?.name || "-"}<br><b>Label:</b> ${p.org1.extra?.label || "-"}<br><b>Website:</b> ${p.org1.website || "-"}<br><b>Adresse:</b> ${p.org1.address || "-"}<br><b>Deals:</b> ${p.org1.extra?.deals}<br><b>Kontakte:</b> ${p.org1.extra?.contacts}</td>
                <td><b>Name:</b> ${p.org2.name}<br><b>ID:</b> ${p.org2.id}<br><b>Besitzer:</b> ${p.org2.owner_id?.name || "-"}<br><b>Label:</b> ${p.org2.extra?.label || "-"}<br><b>Website:</b> ${p.org2.website || "-"}<br><b>Adresse:</b> ${p.org2.address || "-"}<br><b>Deals:</b> ${p.org2.extra?.deals}<br><b>Kontakte:</b> ${p.org2.extra?.contacts}</td>
              </tr>
              <tr>
                <td colspan="2" class="conflict-row">
                  <div>
                    <b>Primär Datensatz:</b>
                    <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                    <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
                  </div>
                  <div>
                    <button class="btn btn-blue" onclick="mergeOrgs(${p.org1.id},${p.org2.id},'${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                    <button class="btn btn-blue" onclick="ignorePair(${p.org1.id},${p.org2.id})">🚫 Ignorieren</button>
                  </div>
                </td>
              </tr>
              <tr><td colspan="2" class="similarity">Ähnlichkeit: ${p.score}%</td></tr>
            </table>
          </div>
        `).join("");
      }

      async function mergeOrgs(org1, org2, group){
        let keep_id = document.querySelector(`input[name='keep_${group}']:checked`).value;
        let res = await fetch(`/merge_orgs?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`,{method:"POST"});
        let data = await res.json();
        if(data.ok){ alert("✅ Merge erfolgreich!"); loadData(); }
        else{ alert("❌ Fehler: "+data.error); }
      }

      async function ignorePair(org1, org2){
        if(!confirm("Dieses Paar wirklich ignorieren?")) return;
        let res = await fetch(`/ignore_pair?org1_id=${org1}&org2_id=${org2}`,{method:"POST"});
        let data = await res.json();
        if(data.ok){ alert("✅ Ignoriert!"); loadData(); }
      }
      </script>
    </body>
    </html>
    """)

    return HTMLResponse(html)

# ================== Lokaler Start ==================
if __name__=="__main__":
    import uvicorn
    port=int(os.environ.get("PORT",8000))
    uvicorn.run("main:app",host="0.0.0.0",port=port,reload=False)

