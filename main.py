import os
import logging
import httpx
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
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

# Pipedrive API (Domain dynamisch)
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
            logger.info(f"👉 Lade Orgs: {url}")
            r = await client.get(url)
            data = r.json()
            if not data.get("success"):
                logger.error(f"Fehler bei API-Call: {data}")
                break
            items = data.get("data") or []
            if not items:
                break
            all_orgs.extend(items)
            if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
                break
            start += limit
    logger.info(f"✅ Insgesamt {len(all_orgs)} Organisationen geladen.")
    return all_orgs


def normalize(s: str) -> str:
    return s.lower().replace(" ", "").replace("-", "").replace(".", "")


def is_duplicate(a: str, b: str, threshold: int = 85) -> bool:
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
        "owner_name": org.get("owner_id", {}).get("name", "-"),
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
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>OrgDupliCheck</title>
    <style>
        body { font-family: Arial, sans-serif; background:#f6f8fa; margin:0; }
        header { background:white; padding:10px; text-align:center; }
        header img { height:60px; }
        .container { padding:20px; }
        button { padding:8px 14px; border:none; border-radius:6px; cursor:pointer; }
        .btn-scan { background:#0096db; color:white; }
        .btn-merge, .btn-ignore { background:#0096db; color:white; margin:5px; }
        .pair { border:1px solid #ddd; margin-bottom:20px; padding:15px; background:white; border-radius:8px; }
        .org-box { width:45%; display:inline-block; vertical-align:top; padding:10px; }
        .footer-bar { background:#e6f2fb; padding:10px; display:flex; justify-content:space-between; align-items:center; }
    </style>
</head>
<body>
    <header>
        <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
    </header>
    <div class="container">
        <button class="btn-scan" onclick="scan()">🔍 Scan starten</button>
        <button class="btn-scan" onclick="bulkMerge()">🔗 Bulk Merge ausführen</button>
        <p id="stats"></p>
        <div id="results"></div>
    </div>

<script>
async function scan() {
    document.getElementById("results").innerHTML = "⏳ Lade Organisationen...";
    let r = await fetch("/scan_orgs");
    let data = await r.json();
    document.getElementById("stats").innerHTML =
      "Geladene Organisationen: <b>"+data.count_orgs+"</b><br>Duplikate insgesamt: <b>"+data.count+"</b>";
    let html = "";
    data.pairs.forEach(p => {
        html += `
        <div class="pair">
            <div class="org-box">
                <b>${p.org1.name}</b><br>
                ID: ${p.org1.id}<br>
                Besitzer: ${p.org1.owner_name}<br>
                Label: ${p.org1.label}<br>
                Website: ${p.org1.website}<br>
                Adresse: ${p.org1.address}<br>
                Deals: ${p.org1.open_deals_count}<br>
                Kontakte: ${p.org1.people_count}<br>
            </div>
            <div class="org-box">
                <b>${p.org2.name}</b><br>
                ID: ${p.org2.id}<br>
                Besitzer: ${p.org2.owner_name}<br>
                Label: ${p.org2.label}<br>
                Website: ${p.org2.website}<br>
                Adresse: ${p.org2.address}<br>
                Deals: ${p.org2.open_deals_count}<br>
                Kontakte: ${p.org2.people_count}<br>
            </div>
            <div class="footer-bar">
                <div>
                    Primär Datensatz:
                    <input type="radio" name="primary-${p.org1.id}-${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}
                    <input type="radio" name="primary-${p.org1.id}-${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}
                </div>
                <div>
                    <button class="btn-merge" onclick="previewMerge(${p.org1.id}, ${p.org2.id})">+ Zusammenführen</button>
                    <button class="btn-ignore" onclick="ignore(${p.org1.id}, ${p.org2.id})">🚫 Ignorieren</button>
                </div>
            </div>
            <div>Ähnlichkeit: ${p.score.toFixed(2)}%</div>
        </div>`;
    });
    document.getElementById("results").innerHTML = html;
}

async function previewMerge(org1, org2) {
    let selected = document.querySelector('input[name="primary-'+org1+'-'+org2+'"]:checked').value;
    let r = await fetch("/preview_merge", {
        method:"POST",
        body:new URLSearchParams({primary_id:selected, secondary_id:(selected==org1?org2:org1)})
    });
    let data = await r.json();
    if(data.error){alert("Fehler: "+data.error);return;}
    let msg = `Vorschau Primär-Datensatz:\\nName: ${data.primary.name}\\nAdresse: ${data.primary.address}\\nLabel: ${data.primary.label}\\nDeals: ${data.primary.open_deals_count}\\nKontakte: ${data.primary.people_count}\\n\\nMerge wirklich ausführen?`;
    if(confirm(msg)){
        mergeOrgs(selected, (selected==org1?org2:org1));
    }
}

async function mergeOrgs(primary, secondary) {
    let r = await fetch("/merge_orgs", {
        method:"POST",
        body:new URLSearchParams({primary_id:primary, secondary_id:secondary})
    });
    let data = await r.json();
    if(data.success){alert("Merge erfolgreich!"); scan();}
    else{alert("Fehler: "+JSON.stringify(data));}
}

async function ignore(org1, org2){
    await fetch("/ignore", {
        method:"POST",
        body:new URLSearchParams({org1_id:org1, org2_id:org2})
    });
    scan();
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


@app.post("/ignore")
async def ignore(org1_id: int = Form(...), org2_id: int = Form(...)):
    store_ignore(org1_id, org2_id)
    return {"status": "ignored"}


@app.post("/preview_merge")
async def preview_merge(primary_id: int = Form(...), secondary_id: int = Form(...)):
    async with httpx.AsyncClient() as client:
        url = f"{PIPEDRIVE_API}/organizations/{primary_id}?api_token={API_TOKEN}"
        r = await client.get(url)
        primary = r.json().get("data")
    if not primary:
        return {"error": "Primärdatensatz nicht gefunden"}

    enriched = await enrich_org(primary)
    return {"primary": enriched, "secondary_id": secondary_id}


@app.post("/merge_orgs")
async def merge_orgs(primary_id: int = Form(...), secondary_id: int = Form(...)):
    async with httpx.AsyncClient() as client:
        url = f"{PIPEDRIVE_API}/organizations/{secondary_id}/merge?api_token={API_TOKEN}"
        r = await client.put(url, data={"merge_with_id": primary_id})
    data = r.json()
    if not data.get("success"):
        return {"error": data}
    return {"success": True, "merged": data.get("data")}


# ================== Lokaler Start ==================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
