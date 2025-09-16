import os
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, Table, Column, Integer, MetaData
from sqlalchemy.sql import insert, select
from rapidfuzz import fuzz, process

app = FastAPI()

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

# Tokens im Speicher
user_tokens = {}

# ================== Static Files ==================
app.mount("/static", StaticFiles(directory="static"), name="static")

# ================== DB Setup ==================
engine = None
ignore_table = None
if DB_URL:
    engine = create_engine(DB_URL)
    metadata = MetaData()
    ignore_table = Table(
        "ignored_pairs", metadata,
        Column("id", Integer, primary_key=True),
        Column("org1_id", Integer),
        Column("org2_id", Integer),
    )
    metadata.create_all(engine)

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

# ================== OAuth Callback ==================
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

# ================== Helper ==================
def get_headers():
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}

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
                f"{PIPEDRIVE_API_URL}/organizations?start={start}&limit={limit}&include_fields=owner_id,label,website,address",
                headers=get_headers(),
            )
            data = resp.json()
            items = data.get("data") or []
            orgs.extend(items)
            more_items = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            start += limit

    # Filter ignorierte Paare
    ignored = set()
    if engine:
        with engine.connect() as conn:
            rows = conn.execute(select(ignore_table)).fetchall()
            for r in rows:
                ignored.add(tuple(sorted([r.org1_id, r.org2_id])))

    pairs = []
    for i, org1 in enumerate(orgs):
        for j, org2 in enumerate(orgs):
            if i >= j:
                continue
            combo = tuple(sorted([org1["id"], org2["id"]]))
            if combo in ignored:
                continue

            score = fuzz.token_set_ratio(org1.get("name", ""), org2.get("name", ""))
            if score >= threshold:
                pairs.append({
                    "org1": extract_org_data(org1),
                    "org2": extract_org_data(org2),
                    "score": round(score, 2),
                })

    return {"ok": True, "count_orgs": len(orgs), "count_pairs": len(pairs), "pairs": pairs}

def extract_org_data(org):
    return {
        "id": org.get("id"),
        "name": org.get("name"),
        "owner": org.get("owner_id", {}).get("name", "-"),
        "label": org.get("label", {}).get("name") if org.get("label") else "-",
        "label_color": org.get("label", {}).get("color") if org.get("label") else None,
        "website": org.get("website") or "-",
        "address": org.get("address") or "-",
        "deals_count": org.get("open_deals_count", 0),
        "contacts_count": org.get("people_count", 0),
    }

# ================== Ignore ==================
@app.post("/ignore")
async def ignore_pair(org1_id: int, org2_id: int):
    if not engine:
        return {"ok": False, "error": "DB nicht konfiguriert"}
    with engine.connect() as conn:
        conn.execute(insert(ignore_table).values(org1_id=org1_id, org2_id=org2_id))
        conn.commit()
    return {"ok": True}

# ================== Merge ==================
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

# ================== Overview ==================
@app.get("/overview")
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    html = """
    <html>
    <head>
      <title>OrgDupliCheck</title>
      <style>
        body { font-family: Arial, sans-serif; margin:0; background:#f5f7f9; }
        header { display:flex; align-items:center; justify-content:center; background:#fff; padding:15px; }
        header img { height: 60px; }
        .container { padding:20px; max-width:1200px; margin:auto; }
        .btn { padding:8px 16px; border:none; border-radius:6px; cursor:pointer; color:white; margin:4px; }
        .btn-scan { background:#039be5; }
        .btn-bulk { background:#0277bd; }
        .btn-merge { background:#0288d1; }
        .btn-ignore { background:#0288d1; }
        .pair { background:white; border-radius:8px; margin:20px 0; box-shadow:0 2px 5px rgba(0,0,0,0.1); padding:15px; }
        .pair-table { width:100%; border-collapse:collapse; }
        .pair-table td { vertical-align:top; padding:10px; }
        .conflict-row { background:#e3f2fd; padding:12px; display:flex; justify-content:space-between; align-items:center; border-radius:6px; }
      </style>
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg"></header>
      <div class="container">
        <button class="btn btn-scan" onclick="loadData()">🔍 Scan starten</button>
        <button class="btn btn-bulk" onclick="bulkMerge()">🚀 Bulk Merge ausführen</button>
        <div id="stats"></div>
        <div id="results"></div>
      </div>

      <script>
      async function loadData(){
        let res = await fetch('/scan_orgs?threshold=80');
        let data = await res.json();
        let stats = document.getElementById("stats");
        let div = document.getElementById("results");
        if(!data.ok){ stats.innerHTML="⚠️ Fehler: "+data.error; return; }

        stats.innerHTML = `<p>Geladene Organisationen: <b>${data.count_orgs}</b><br>Duplikate insgesamt: <b>${data.count_pairs}</b></p>`;

        div.innerHTML = data.pairs.map(p=>`
          <div class="pair">
            <table class="pair-table">
              <tr>
                <td>
                  <b>Name:</b> ${p.org1.name}<br>
                  <b>ID:</b> ${p.org1.id}<br>
                  <b>Besitzer:</b> ${p.org1.owner}<br>
                  <b>Label:</b> ${p.org1.label}<br>
                  <b>Website:</b> ${p.org1.website}<br>
                  <b>Adresse:</b> ${p.org1.address}<br>
                  <b>Deals:</b> ${p.org1.deals_count}<br>
                  <b>Kontakte:</b> ${p.org1.contacts_count}
                </td>
                <td>
                  <b>Name:</b> ${p.org2.name}<br>
                  <b>ID:</b> ${p.org2.id}<br>
                  <b>Besitzer:</b> ${p.org2.owner}<br>
                  <b>Label:</b> ${p.org2.label}<br>
                  <b>Website:</b> ${p.org2.website}<br>
                  <b>Adresse:</b> ${p.org2.address}<br>
                  <b>Deals:</b> ${p.org2.deals_count}<br>
                  <b>Kontakte:</b> ${p.org2.contacts_count}
                </td>
              </tr>
            </table>
            <div class="conflict-row">
              <div>
                Primär Datensatz:
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
              </div>
              <div>
                <button class="btn btn-merge" onclick="mergeOrgs(${p.org1.id},${p.org2.id},'${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                <button class="btn btn-ignore" onclick="ignore(${p.org1.id},${p.org2.id})">🚫 Ignorieren</button>
              </div>
            </div>
            <div>Ähnlichkeit: ${p.score}%</div>
          </div>`).join("");
      }

      async function mergeOrgs(org1,org2,group){
        let keep_id=document.querySelector(\`input[name='keep_\${group}']:checked\`).value;
        let preview=confirm("⚠️ Vorschau: Primär wird ID "+keep_id+". Merge ausführen?");
        if(!preview) return;
        let res=await fetch(\`/merge_orgs?org1_id=\${org1}&org2_id=\${org2}&keep_id=\${keep_id}\`,{method:"POST"});
        let data=await res.json();
        alert(data.ok?"✅ Merge erfolgreich!":"❌ Fehler: "+data.error);
        if(data.ok) loadData();
      }

      async function ignore(org1,org2){
        let res=await fetch(\`/ignore?org1_id=\${org1}&org2_id=\${org2}\`,{method:"POST"});
        let data=await res.json();
        if(data.ok) loadData();
      }

      async function bulkMerge(){
        alert("⚠️ Bulk Merge Vorschau – implementiert analog zu Merge.");
      }
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ================== Lokaler Start ==================
if __name__=="__main__":
    import uvicorn
    port=int(os.environ.get("PORT",8000))
    uvicorn.run("main:app",host="0.0.0.0",port=port,reload=False)
