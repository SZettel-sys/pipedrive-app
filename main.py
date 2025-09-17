import os
import logging
import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from rapidfuzz import fuzz

# =====================================
# Setup
# =====================================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
app = FastAPI()

# Static (Logo, CSS usw.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# DB-Verbindung
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None

# Pipedrive API Konfiguration
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"
API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")

CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")

OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
REDIRECT_URI = f"{BASE_URL}/oauth/callback" if BASE_URL else None

user_tokens = {}  # in-memory (für echte Nutzung besser DB speichern)

# =====================================
# Helper
# =====================================

def get_auth_params(user_id: str = None):
    """Liefert Auth-Parameter für API-Requests"""
    if API_TOKEN:
        return {"api_token": API_TOKEN}
    elif user_id and user_id in user_tokens:
        return {"access_token": user_tokens[user_id]["access_token"]}
    else:
        raise RuntimeError("⚠️ Kein gültiger Token gefunden. Bitte API_TOKEN oder OAuth nutzen!")

async def fetch_all_orgs(user_id: str = None):
    """Alle Organisationen laden (paging)"""
    all_orgs = []
    start = 0
    limit = 500
    async with httpx.AsyncClient() as client:
        while True:
            params = {"start": start, "limit": limit}
            params.update(get_auth_params(user_id))
            url = f"{PIPEDRIVE_API_URL}/organizations"
            logger.info(f"📥 Lade Orgs: {url} params={params}")
            r = await client.get(url, params=params)
            data = r.json()
            if not data.get("success"):
                logger.error(f"Fehler bei API: {data}")
                break
            items = data.get("data") or []
            if not items:
                break
            all_orgs.extend(items)
            if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
                break
            start += limit
    return all_orgs

def normalize(s: str) -> str:
    return s.lower().replace(" ", "").replace("-", "").replace(".", "")

def is_duplicate(a: str, b: str, threshold: int = 85):
    score = fuzz.ratio(normalize(a), normalize(b))
    return score >= threshold, score

def store_ignore(id1, id2):
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO ignored_pairs (org_id_1, org_id_2) VALUES (:a, :b) ON CONFLICT DO NOTHING"),
            {"a": min(id1, id2), "b": max(id1, id2)},
        )

def is_ignored(id1, id2):
    if not engine:
        return False
    with engine.begin() as conn:
        res = conn.execute(
            text("SELECT 1 FROM ignored_pairs WHERE org_id_1=:a AND org_id_2=:b"),
            {"a": min(id1, id2), "b": max(id1, id2)},
        )
        return res.first() is not None

async def enrich_org(org):
    return {
        "id": org.get("id"),
        "name": org.get("name"),
        "owner_name": org.get("owner_name") or "-",
        "label": org.get("label") or "-",
        "address": org.get("address") or "-",
        "website": org.get("website") or "-",
        "open_deals_count": org.get("open_deals_count", 0),
        "people_count": org.get("people_count", 0),
    }

# =====================================
# OAuth Routes
# =====================================
@app.get("/login")
async def login():
    if not CLIENT_ID or not CLIENT_SECRET:
        return JSONResponse({"error": "OAuth nicht konfiguriert, bitte API_TOKEN nutzen"})
    url = f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    return RedirectResponse(url)

@app.get("/oauth/callback")
async def oauth_callback(code: str):
    async with httpx.AsyncClient() as client:
        r = await client.post(OAUTH_TOKEN_URL, data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        })
        data = r.json()
    if "access_token" not in data:
        return JSONResponse({"error": "OAuth-Fehler", "details": data})
    user_tokens["default"] = data
    return RedirectResponse("/")

# =====================================
# HTML Overview
# =====================================
@app.get("/", response_class=HTMLResponse)
async def overview(request: Request):
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"><title>OrgDupliCheck</title></head>
    <body>
        <h2>OrgDupliCheck</h2>
        <button onclick="scan()">🔍 Scan starten</button>
        <p id="stats"></p>
        <div id="results"></div>
        <script>
        async function scan(){
            let r = await fetch("/scan_orgs");
            let data = await r.json();
            document.getElementById("stats").innerHTML =
              "Geladene Organisationen: <b>"+data.count_orgs+"</b><br>Duplikate: <b>"+data.count+"</b>";
        }
        </script>
    </body>
    </html>
    """)

# =====================================
# API Endpoints
# =====================================
@app.get("/scan_orgs")
async def scan_orgs():
    orgs = await fetch_all_orgs("default" if not API_TOKEN else None)
    results = []
    checked = set()
    for i, o1 in enumerate(orgs):
        for o2 in orgs[i+1:]:
            if (o1["id"], o2["id"]) in checked or (o2["id"], o1["id"]) in checked:
                continue
            checked.add((o1["id"], o2["id"]))
            dup, score = is_duplicate(o1["name"], o2["name"])
            if dup and not is_ignored(o1["id"], o2["id"]):
                results.append({"org1": await enrich_org(o1), "org2": await enrich_org(o2), "score": score})
    return {"count_orgs": len(orgs), "count": len(results), "pairs": results}

@app.post("/ignore")
async def ignore(org1_id: int = Form(...), org2_id: int = Form(...)):
    store_ignore(org1_id, org2_id)
    return {"status": "ignored"}

# ================== Lokaler Start ==================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
