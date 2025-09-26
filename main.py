import os
import re
import httpx
import asyncpg
import asyncio
import logging
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
logger = logging.getLogger("main")
logging.basicConfig(level=logging.INFO)
# ================== Scan Orgs ==================
@app.get("/scan_orgs")
async def scan_orgs(threshold: int = 80):
    if "default" not in user_tokens:
        return {
            "ok": False,
            "error": "Nicht eingeloggt",
            "total": 0,
            "duplicates": 0,
            "pairs": []
        }

    headers = get_headers()
    limit = 500
    start = 0
    orgs = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            resp = await client.get(
                f"{PIPEDRIVE_API_URL}/organizations?start={start}&limit={limit}&include_fields=label",
                headers=headers
            )
            if resp.status_code != 200:
                logging.error(f"❌ Fehler beim Laden von Orgs (start={start}): {resp.text}")
                break

            data = resp.json()
            items = data.get("data") or []
            if not items:
                break

            for org in items:
                label_name, label_color = None, None
                label = org.get("label")

                # Direkt Label-Objekt prüfen
                if isinstance(label, dict):
                    label_name = label.get("name")
                    label_color = label.get("color", "#ccc")

                orgs.append({
                    "id": org.get("id"),
                    "name": org.get("name"),
                    "owner": org.get("owner_id", {}).get("name", "-"),
                    "website": org.get("website") or "-",
                    "address": org.get("address") or "-",
                    "deals_count": org.get("open_deals_count", 0),
                    "contacts_count": org.get("people_count", 0),
                    "label_name": label_name or "-",
                    "label_color": label_color or "#ccc",
                })

            # Pagination prüfen
            more = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            if not more:
                break
            start += limit

    ignored = await load_ignored()

    # Buckets nach Präfix (3 Zeichen)
    buckets = {}
    for org in orgs:
        key = normalize_name(org["name"])[:3]
        buckets.setdefault(key, []).append(org)

    results = []
    for key, bucket in buckets.items():
        for i, org1 in enumerate(bucket):
            for j in range(i + 1, len(bucket)):
                org2 = bucket[j]

                if abs(len(org1["name"]) - len(org2["name"])) > 10:
                    continue

                pair_key = tuple(sorted([org1["id"], org2["id"]]))
                if pair_key in ignored:
                    continue

                score = fuzz.token_sort_ratio(
                    normalize_name(org1["name"]),
                    normalize_name(org2["name"])
                )
                if score >= threshold:
                    results.append({
                        "org1": org1,
                        "org2": org2,
                        "score": round(score, 2)
                    })

    logging.info(f"✅ Gesamt Orgs: {len(orgs)} | Duplikate: {len(results)}")

    return {
        "ok": True,
        "pairs": results,
        "total": len(orgs),
        "duplicates": len(results)
    }



# ================== HTML Overview ==================
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
        body { font-family:'Source Sans Pro',Arial,sans-serif; background:#f4f6f8; margin:0; color:#333; }
        header { display:flex; justify-content:center; align-items:center; background:#ffffff; padding:15px; border-bottom:1px solid #ddd; }
        header img { height:70px; }
        .container { max-width:1400px; margin:20px auto; padding:10px; }
        .pair { background:white; border:1px solid #ddd; border-radius:10px; margin-bottom:25px; box-shadow:0 2px 4px rgba(0,0,0,0.05); overflow:hidden; }
        .pair-table { width:100%; border-collapse:collapse; }
        .pair-table td { padding:10px 14px; vertical-align:top; width:50%; }
        .pair-table tr:first-child td { font-weight:bold; font-size:15px; background:#f0f6fb; }
        .label-badge { padding:4px 10px; border-radius:12px; color:#fff; font-size:12px; font-weight:600; display:inline-block; min-width:60px; text-align:center; }
        .conflict-bar { background:#e6f3fb; padding:12px 16px; display:flex; justify-content:space-between; align-items:center; border-top:1px solid #d5e5f0; }
        .conflict-left { display:flex; gap:20px; align-items:center; font-size:14px; }
        .conflict-right { display:flex; flex-direction:column; gap:6px; align-items:flex-end; }
        .btn-action { background:#009fe3; color:white; border:none; padding:8px 18px; border-radius:6px; cursor:pointer; font-size:14px; transition:all .2s; }
        .btn-action:hover { background:#007bb8; }
        .similarity { padding:10px 16px; font-size:13px; color:#555; background:#f9f9f9; border-top:1px solid #eee; }
      </style>
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <button class="btn-action" onclick="loadData()">🔎 Scan starten</button>
        <button class="btn-action" onclick="bulkMerge()">🚀 Bulk Merge ausführen</button>
        <div id="stats" style="margin:15px 0; font-size:15px;"></div>
        <div id="results"></div>
      </div>

      <script>
      async function loadData(){
        let res = await fetch('/scan_orgs?threshold=80');
        let data = await res.json();
        document.getElementById("stats").innerHTML =
          "Geladene Organisationen: <b>" + data.total + "</b> | Duplikate: <b>" + data.duplicates + "</b>";
        if(!data.ok){ document.getElementById("results").innerHTML = "❌ Fehler beim Laden"; return; }
        if(data.pairs.length===0){ document.getElementById("results").innerHTML = "✅ Keine Duplikate gefunden"; return; }

        document.getElementById("results").innerHTML = data.pairs.map(p => {
          function renderLabel(name, color){
            if(!name || name === "-") return "–";
            return `<span class="label-badge" style="background:${color}">${name}</span>`;
          }

          return `
          <div class="pair">
            <table class="pair-table">
              <tr><td>${p.org1.name}</td><td>${p.org2.name}</td></tr>
              <tr><td>ID: ${p.org1.id}</td><td>ID: ${p.org2.id}</td></tr>
              <tr><td>Besitzer: ${p.org1.owner}</td><td>Besitzer: ${p.org2.owner}</td></tr>
              <tr>
                <td>Label: ${renderLabel(p.org1.label_name, p.org1.label_color)}</td>
                <td>Label: ${renderLabel(p.org2.label_name, p.org2.label_color)}</td>
              </tr>
              <tr><td>Website: ${p.org1.website}</td><td>Website: ${p.org2.website}</td></tr>
              <tr><td>Adresse: ${p.org1.address}</td><td>Adresse: ${p.org2.address}</td></tr>
              <tr><td>Deals: ${p.org1.deals_count}</td><td>Deals: ${p.org2.deals_count}</td></tr>
              <tr><td>Kontakte: ${p.org1.contacts_count}</td><td>Kontakte: ${p.org2.contacts_count}</td></tr>
            </table>
            <div class="conflict-bar">
              <div class="conflict-left">
                Primär Datensatz:
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
              </div>
              <div class="conflict-right">
                <div>
                  <button class="btn-action" onclick="doPreviewMerge(${p.org1.id},${p.org2.id},'${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                  <button class="btn-action" onclick="ignorePair(${p.org1.id},${p.org2.id})">🚫 Ignorieren</button>
                </div>
                <label><input type="checkbox" class="bulkCheck" value="${p.org1.id}_${p.org2.id}"> Für Bulk auswählen</label>
              </div>
            </div>
            <div class="similarity">Ähnlichkeit: ${p.score}%</div>
          </div>
        `;
        }).join("");
      }

      async function doPreviewMerge(org1,org2,group){
        let keep_id=document.querySelector(`input[name='keep_${group}']:checked`).value;
        let res=await fetch(`/preview_merge?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`,{method:"POST"});
        let data=await res.json();
        if(data.ok){
          let org=data.preview;
          let msg="⚠️ Vorschau Primär-Datensatz:\\n"+
                  "ID: "+(org.id||"-")+"\\n"+
                  "Name: "+(org.name||"-")+"\\n"+
                  "Label: "+(org.label?.name||"-")+"\\n"+
                  "Adresse: "+(org.address||"-")+"\\n"+
                  "Website: "+(org.website||"-")+"\\n"+
                  "Deals: "+(org.open_deals_count||"-")+"\\n"+
                  "Kontakte: "+(org.people_count||"-")+"\\n\\n"+
                  "Diesen Datensatz behalten?";
          if(confirm(msg)){ doMerge(org1,org2,keep_id); }
        } else {
          alert("❌ Fehler Vorschau: "+data.error);
        }
      }

      async function doMerge(org1,org2,keep_id){
        let res=await fetch(`/merge_orgs?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`,{method:"POST"});
        let data=await res.json();
        if(data.ok){ alert("✅ Merge erfolgreich"); loadData(); }
        else{ alert("❌ Fehler beim Merge: "+data.error); }
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


