import os
import httpx
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from rapidfuzz import fuzz, process

app = FastAPI()

# ================== Konfiguration ==================
CLIENT_ID = os.getenv("PD_CLIENT_ID")
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET")
BASE_URL = os.getenv("BASE_URL")
if not BASE_URL:
    raise ValueError("❌ BASE_URL ist nicht gesetzt")

REDIRECT_URI = f"{BASE_URL}/oauth/callback"
OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

user_tokens = {}

# ================== Static Files ==================
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
                f"{PIPEDRIVE_API_URL}/organizations?start={start}&limit={limit}&api_token={user_tokens['default']}",
                headers=get_headers(),
            )
            data = resp.json()
            items = data.get("data") or []
            for org in items:
                orgs.append({
                    "id": org.get("id"),
                    "name": org.get("name"),
                    "owner": org.get("owner_id", {}).get("name", "-"),
                    "website": org.get("website") or "-",
                    "address": org.get("address") or "-",
                    "label_name": org.get("label", {}).get("name") if isinstance(org.get("label"), dict) else "-",
                    "label_color": org.get("label", {}).get("color") if isinstance(org.get("label"), dict) else "#ccc",
                    "phone": (org.get("phone")[0]["value"] if org.get("phone") else "-"),
                })
            more_items = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            start += limit

    pairs = []
    for i, org1 in enumerate(orgs):
        for j, org2 in enumerate(orgs):
            if i >= j:
                continue
            score = fuzz.token_set_ratio(org1["name"], org2["name"])
            if score >= threshold:
                pairs.append({"org1": org1, "org2": org2, "score": round(score, 2)})

    return {"ok": True, "pairs": pairs, "count": len(orgs)}

# ================== Merge Org ==================
@app.post("/merge_orgs")
async def merge_orgs(org1_id: int, org2_id: int, keep_id: int):
    if "default" not in user_tokens:
        return {"ok": False, "error": "Nicht eingeloggt"}
    async with httpx.AsyncClient() as client:
        resp = await client.put(
            f"{PIPEDRIVE_API_URL}/organizations/{org1_id}/merge",
            headers=get_headers(),
            json={"merge_with_id": org2_id},
        )
    return resp.json()

# ================== Overview ==================
@app.get("/overview")
async def overview(request: Request):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    html = """
    <html>
    <head>
      <title>Organisationen Übersicht</title>
      <style>
        body { font-family: 'Source Sans Pro', Arial, sans-serif; background:#f4f6f8; margin:0; }
        header { display:flex; justify-content:center; align-items:center; background:#f4f4f4; padding:10px; }
        header img { height:80px; }
        .container { max-width:1400px; margin:20px auto; padding:10px; }
        .stats { margin:10px 0; }
        .pair { background:white; border:1px solid #ddd; border-radius:8px; margin-bottom:20px; }
        .pair-table { width:100%; border-collapse:collapse; }
        .pair-table td { padding:10px; vertical-align:top; }
        .label-badge { padding:2px 6px; border-radius:6px; color:white; font-size:12px; }
        .conflict-row { background:#e6f3fb; padding:10px; display:flex; align-items:center; justify-content:space-between; }
        .conflict-left { display:flex; align-items:center; gap:15px; font-size:14px; }
        .buttons { display:flex; gap:10px; }
        .btn { background:#009fe3; color:white; border:none; padding:8px 16px; border-radius:6px; cursor:pointer; }
        .btn:hover { opacity:0.9; }
        .similarity { padding:8px; font-size:13px; color:#333; }
      </style>
    </head>
    <body>
      <header>
        <img src="/static/bizforward_R_gesamt_10pt_weiss.png" alt="Logo">
      </header>
      <div class="container">
        <button class="btn" onclick="loadData()">🔎 Scan starten</button>
        <button class="btn" onclick="bulkMerge()">🚀 Bulk Merge ausführen</button>
        <div class="stats" id="stats"></div>
        <div id="results"></div>
      </div>

      <script>
      async function loadData(){
        let res = await fetch('/scan_orgs?threshold=80');
        let data = await res.json();
        document.getElementById("stats").innerHTML = 
          "Geladene Organisationen: <b>" + data.count + "</b> | Duplikate: <b>" + data.pairs.length + "</b>";
        if(!data.ok){ document.getElementById("results").innerHTML = "Fehler"; return; }
        document.getElementById("results").innerHTML = data.pairs.map(p => `
          <div class="pair">
            <table class="pair-table">
              <tr>
                <td>
                  <b>Name:</b> ${p.org1.name}<br>
                  <b>ID:</b> ${p.org1.id}<br>
                  <b>Besitzer:</b> ${p.org1.owner}<br>
                  <b>Label:</b> <span class="label-badge" style="background:${p.org1.label_color}">${p.org1.label_name}</span><br>
                  <b>Website:</b> ${p.org1.website}<br>
                  <b>Adresse:</b> ${p.org1.address}<br>
                </td>
                <td>
                  <b>Name:</b> ${p.org2.name}<br>
                  <b>ID:</b> ${p.org2.id}<br>
                  <b>Besitzer:</b> ${p.org2.owner}<br>
                  <b>Label:</b> <span class="label-badge" style="background:${p.org2.label_color}">${p.org2.label_name}</span><br>
                  <b>Website:</b> ${p.org2.website}<br>
                  <b>Adresse:</b> ${p.org2.address}<br>
                </td>
              </tr>
              <tr>
                <td colspan="2" class="conflict-row">
                  <div class="conflict-left">
                    Primär Datensatz:
                    <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                    <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
                  </div>
                  <div class="buttons">
                    <button class="btn" onclick="mergeOrgs(${p.org1.id}, ${p.org2.id}, '${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                    <button class="btn" style="background:#ff4d4d" onclick="ignorePair(${p.org1.id}, ${p.org2.id})">🚫 Ignorieren</button>
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
        if(!confirm("Zusammenführen durchführen?")) return;
        let res = await fetch(`/merge_orgs?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`, { method:"POST" });
        let data = await res.json();
        alert(data.ok ? "✅ Merge erfolgreich" : "❌ Fehler");
        loadData();
      }

      async function bulkMerge(){
        if(!confirm("Alle ausgewählten Paare wirklich zusammenführen?")) return;
        alert("⚠️ Bulk Merge noch nicht implementiert in dieser Version.");
      }

      async function ignorePair(org1, org2){
        alert("⚠️ Paar wird ignoriert (DB-Anbindung erforderlich)");
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




