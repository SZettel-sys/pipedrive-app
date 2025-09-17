import os
import logging
import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
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

# Static (für Logo, CSS usw.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# DB-Verbindung
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None

# Pipedrive API
COMPANY_DOMAIN = os.getenv("PIPEDRIVE_COMPANY_DOMAIN", "bizforwardgmbh-sandbox")
PIPEDRIVE_API = f"https://{COMPANY_DOMAIN}.pipedrive.com/api/v1"
API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")

# =====================================
# Hilfsfunktionen
# =====================================

async def fetch_all_orgs():
    """Alle Organisationen seitenweise laden"""
    all_orgs = []
    start = 0
    limit = 100
    async with httpx.AsyncClient() as client:
        while True:
            url = f"{PIPEDRIVE_API}/organizations?api_token={API_TOKEN}&start={start}&limit={limit}"
            logger.info(f"👉 Request an Pipedrive: {url}")
            r = await client.get(url)
            try:
                data = r.json()
            except Exception:
                logger.error(f"❌ Fehler beim JSON-Parse: {r.text}")
                break

            logger.info(f"🔎 Response: {data}")

            if not data.get("success"):
                logger.error("❌ API Response success=false")
                break

            items = data.get("data") or []
            if not items:
                logger.warning("⚠️ Keine Items in dieser Page.")
                break

            all_orgs.extend(items)
            if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
                break
            start += limit
    logger.info(f"✅ Insgesamt {len(all_orgs)} Organisationen geladen.")
    return all_orgs


def normalize(s: str) -> str:
    return s.lower().replace(" ", "").replace("-", "").replace(".", "")


def is_duplicate(a: str, b: str, threshold: int = 85) -> tuple[bool, float]:
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
    """Zusatzinfos für Org laden"""
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
# HTML Overview
# =====================================

@app.get("/", response_class=HTMLResponse)
async def overview(request: Request):
    return HTMLResponse("""
    <html>
    <head><title>OrgDupliCheck</title></head>
    <body>
        <button onclick="scan()">🔍 Scan starten</button>
        <div id="results"></div>
        <script>
        async function scan(){
            let r = await fetch("/scan_orgs");
            let data = await r.json();
            console.log(data);
            document.getElementById("results").innerHTML =
                "Geladene Organisationen: " + data.count_orgs + "<br>" +
                "Duplikate: " + data.count;
        }
        </script>
    </body>
    </html>
    """)

@app.get("/overview", response_class=HTMLResponse)
async def overview_iframe(request: Request):
    return await overview(request)

@app.get("/scan_orgs")
async def scan_orgs():
    orgs = await fetch_all_orgs()
    results = []
    checked = set()

    for i, o1 in enumerate(orgs):
        for o2 in orgs[i + 1:]:
            if (o1["id"], o2["id"]) in checked or (o2["id"], o1["id"]) in checked:
                continue
            checked.add((o1["id"], o2["id"]))
            dup, score = is_duplicate(o1["name"], o2["name"])
            if dup and not is_ignored(o1["id"], o2["id"]):
                results.append({
                    "org1": await enrich_org(o1),
                    "org2": await enrich_org(o2),
                    "score": score
                })

    return {"count_orgs": len(orgs), "count": len(results), "pairs": results}


# ================== Lokaler Start ==================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
