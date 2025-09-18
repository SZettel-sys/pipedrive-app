import os
import re
import httpx
import asyncpg
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from rapidfuzz import fuzz

app = FastAPI()

# ================== Konfiguration ==================
CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise ValueError("❌ BASE_URL fehlt")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

user_tokens = {}

# ================== DB für Ignore ==================
DB_URL = os.getenv("DATABASE_URL")

async def get_conn():
    return await asyncpg.connect(DB_URL)

async def load_ignored():
    conn = await get_conn()
    rows = await conn.fetch("SELECT org1_id, org2_id FROM ignored_pairs")
    await conn.close()
    return {tuple(sorted([r["org1_id"], r["org2_id"]])) for r in rows}

@app.post("/ignore_pair")
async def ignore_pair(org1_id: int, org2_id: int):
    org1, org2 = sorted([org1_id, org2_id])
    conn = await get_conn()
    await conn.execute(
        "INSERT INTO ignored_pairs (org1_id, org2_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        org1, org2
    )
    await conn.close()
    return {"ok": True, "ignored": (org1, org2)}

# ================== Static ==================
app.mount("/static", StaticFiles(directory="static"), name="static")

# ================== Root ==================
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
    return RedirectResponse("/overview")

def get_headers():
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}

# ================== Normalizer ==================
def normalize_name(name: str) -> str:
    if not name: return ""
    n = name.lower()
    n = re.sub(r"\b(gmbh|ug|ag|kg|ohg|inc|ltd)\b", "", n)
    n = re.sub(r"[^a-z0-9 ]", "", n)
    return re.sub(r"\s+", " ", n).strip()

# ================== Scan Orgs ==================
@app.get("/scan_orgs")
async def scan_orgs(threshold: int = 80):
    if "default" not in user_tokens:
        return {"ok": False, "error": "Nicht eingeloggt", "total": 0, "duplicates": 0, "pairs": []}

    headers = get_headers()
    limit = 1000
    start = 0
    orgs = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        # Labels laden
        label_map = {}
        label_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizationLabels", headers=headers)
        if label_resp.status_code == 200:
            for l in label_resp.json().get("data", []) or []:
                label_map[l["id"]] = {"name": l["name"], "color": l.get("color", "#666")}

        # Organisationen seitenweise laden
        while True:
            resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations?start={start}&limit={limit}", headers=headers)
            if resp.status_code != 200:
                return {"ok": False, "error": resp.text, "total": len(orgs), "duplicates": 0, "pairs": []}

            data = resp.json()
            items = data.get("data") or []
            if not items:
                break

            for org in items:
                label_id = org.get("label") or org.get("label_id")
                if isinstance(label_id, dict):
                    label_id = label_id.get("id")
                if label_id and label_id in label_map:
                    label_name = label_map[label_id]["name"]
                    label_color = label_map[label_id]["color"]
                else:
                    label_name = "-"
                    label_color = "#ccc"

                orgs.append({
                    "id": org.get("id"),
                    "name": org.get("name"),
                    "owner": org.get("owner_id", {}).get("name", "-"),
                    "website": org.get("website") or "-",
                    "address": org.get("address") or "-",
                    "deals_count": org.get("open_deals_count", 0),
                    "contacts_count": org.get("people_count", 0),
                    "label_name": label_name,
                    "label_color": label_color,
                })

            more = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            if not more:
                break
            start += limit

    ignored = await load_ignored()

    # Buckets nach Präfix
    buckets = {}
    for org in orgs:
        key = normalize_name(org["name"])[:2]
        buckets.setdefault(key, []).append(org)

    results = []
    for key, bucket in buckets.items():
        for i, org1 in enumerate(bucket):
            for j in range(i + 1, len(bucket)):
                org2 = bucket[j]
                pair_key = tuple(sorted([org1["id"], org2["id"]]))
                if pair_key in ignored:
                    continue
                score = fuzz.token_sort_ratio(normalize_name(org1["name"]), normalize_name(org2["name"]))
                if score >= threshold:
                    results.append({"org1": org1, "org2": org2, "score": round(score, 2)})

    return {"ok": True, "pairs": results, "total": len(orgs), "duplicates": len(results)}

# ================== HTML Overview ==================
@app.get("/overview")
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    html = """
    <html>
    <head>
      <title>Organisationen Übersicht</title>
      <style>
        body { font-family:'Source Sans Pro',Arial,sans-serif; background:#f4f6f8; margin:0; }
        header { display:flex; justify-content:center; align-items:center; background:#ffffff; padding:10px; }
        header img { height:80px; }
        .container { max-width:1400px; margin:20px auto; padding:10px; }
        .pair { background:white; border:1px solid #ddd; border-radius:8px; margin-bottom:20px; }
        .pair-table { width:100%; border-collapse:collapse; }
        .pair-table td { padding:10px; vertical-align:top; }
        .label-badge { padding:2px 6px; border-radius:6px; color:white; font-size:12px; }
        .conflict-bar { background:#e6f3fb; padding:10px; display:flex; justify-content:space-between; align-items:center; }
        .conflict-left { display:flex; gap:15px; align-items:center; font-size:14px; }
        .conflict-right { display:flex; flex-direction:column; gap:6px; align-items:flex-end; }
        .btn-action { background:#009fe3; color:white; border:none; padding:8px 16px; border-radius:6px; cursor:pointer; }
        .btn-action:hover { opacity:0.9; }
        .similarity { padding:8px; font-size:13px; color:#333; }
      </style>
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <button class="btn-action" onclick="loadData()">🔎 Scan starten</button>
        <button class="btn-action" onclick="bulkMerge()">🚀 Bulk Merge ausführen</button>
        <div id="stats"></div>
        <div id="results"></div>
      </div>

      <script>
      async function loadData(){
        let res = await fetch('/scan_orgs?threshold=80');
        let data = await res.json();
        document.getElementById("stats").innerHTML =
          "Geladene Organisationen: <b>" + data.total + "</b> | Duplikate: <b>" + data.duplicates + "</b>";
        if(!data.ok){ document.getElementById("results").innerHTML = "Fehler"; return; }
        if(data.pairs.length===0){ document.getElementById("results").innerHTML = "✅ Keine Duplikate"; return; }

        document.getElementById("results").innerHTML = data.pairs.map(p => `
          <div class="pair">
            <table class="pair-table">
              <tr>
                <td>
                  <b>${p.org1.name}</b><br>
                  ID: ${p.org1.id}<br>
                  Besitzer: ${p.org1.owner}<br>
                  Label: <span class="label-badge" style="background:${p.org1.label_color}">${p.org1.label_name}</span><br>
                  Website: ${p.org1.website}<br>
                  Adresse: ${p.org1.address}<br>
                  Deals: ${p.org1.deals_count}<br>
                  Kontakte: ${p.org1.contacts_count}
                </td>
                <td>
                  <b>${p.org2.name}</b><br>
                  ID: ${p.org2.id}<br>
                  Besitzer: ${p.org2.owner}<br>
                  Label: <span class="label-badge" style="background:${p.org2.label_color}">${p.org2.label_name}</span><br>
                  Website: ${p.org2.website}<br>
                  Adresse: ${p.org2.address}<br>
                  Deals: ${p.org2.deals_count}<br>
                  Kontakte: ${p.org2.contacts_count}
                </td>
              </tr>
            </table>
            <div class="conflict-bar">
              <div class="conflict-left">
                Primär Datensatz:
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
              </div>
              <div class="conflict-right">
                <div>
                  <button class="btn-action" onclick="previewMerge(${p.org1.id},${p.org2.id},'${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                  <button class="btn-action" onclick="ignorePair(${p.org1.id},${p.org2.id})">🚫 Ignorieren</button>
                </div>
                <label><input type="checkbox" class="bulkCheck" value="${p.org1.id}_${p.org2.id}"> Für Bulk auswählen</label>
              </div>
            </div>
            <div class="similarity">Ähnlichkeit: ${p.score}%</div>
          </div>
        `).join("");
      }

      async function previewMerge(org1,org2,group){
        let keep_id=document.querySelector(`input[name='keep_${group}']:checked`).value;
        let res=await fetch(`/merge_orgs?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`,{method:"POST"});
        let data=await res.json();
        if(data.ok){
          alert("✅ Merge durchgeführt.");
          loadData();
        } else {
          alert("❌ Fehler beim Merge: " + data.error);
        }
      }

      async function bulkMerge(){
        let selected=document.querySelectorAll(".bulkCheck:checked");
        if(selected.length===0){alert("⚠️ Keine Paare ausgewählt");return;}
        if(!confirm(selected.length+" Paare wirklich zusammenführen?")) return;
        alert("🚀 Bulk Merge Dummy");
      }

      async function ignorePair(org1,org2){
        if(!confirm("Paar ignorieren?")) return;
        await fetch(`/ignore_pair?org1_id=${org1}&org2_id=${org2}`,{method:"POST"});
        alert("✅ Paar ignoriert");
        loadData();
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
