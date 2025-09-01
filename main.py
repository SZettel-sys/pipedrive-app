import os
import difflib
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

app = FastAPI()

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

# Tokens im Speicher (für Produktion: DB/Redis o.ä.)
user_tokens = {}

# ================== Root Redirect ==================
@app.get("/")
def root():
    return RedirectResponse("/overview")

# ================== Login starten ==================
@app.get("/login")
def login():
    return RedirectResponse(
        f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    )

# ================== Callback von Pipedrive ==================
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
        return HTMLResponse("<h3>❌ Fehler beim Login</h3>")

    user_tokens["default"] = access_token
    return RedirectResponse("/overview")

# ================== Hilfsfunktion ==================
def get_headers():
    token = user_tokens.get("default")
    return {"Authorization": f"Bearer {token}"} if token else {}

# ================== Scan Organisations ==================
@app.get("/scan_orgs")
async def scan_orgs(threshold: int = 80):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{PIPEDRIVE_API_URL}/organizations",
            headers=get_headers(),
        )
        orgs = resp.json().get("data", [])

    results = []
    for i, org1 in enumerate(orgs):
        for j, org2 in enumerate(orgs):
            if i >= j:
                continue
            score = (
                difflib.SequenceMatcher(None, org1["name"], org2["name"]).ratio() * 100
            )
            if score >= threshold:
                results.append(
                    {
                        "org1": org1,
                        "org2": org2,
                        "score": round(score, 2),
                    }
                )

    return {"ok": True, "pairs": results}

# ================== Merge Organisations ==================
@app.post("/merge_orgs")
async def merge_orgs(org1_id: int, org2_id: int, keep_id: int):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{PIPEDRIVE_API_URL}/organizations/{keep_id}/merge",
            headers=get_headers(),
            json={"merge_with_id": org2_id if keep_id == org1_id else org1_id},
        )

    if resp.status_code != 200:
        return {"ok": False, "error": resp.text}

    return {"ok": True, "result": resp.json()}

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
          body { font-family: Arial; margin: 20px; }
          .pair { border:1px solid #ddd; margin-bottom:20px; padding:10px; }
          .org { width:45%; display:inline-block; vertical-align:top; }
          .header { font-weight:bold; margin-bottom:10px; font-size:16px; }
          .conflict { background:#fffae6; padding:10px; margin-top:10px; }
          button { margin-top:10px; padding:5px 10px; }
        </style>
    </head>
    <body>
        <h2>🔎 Duplikatsprüfung Organisationen</h2>
        <button onclick="loadData()">Scan starten</button>
        <div id="results"></div>

        <script>
        async function loadData() {
            let res = await fetch('/scan_orgs?threshold=80');
            let data = await res.json();
            let div = document.getElementById("results");
            if(!data.ok) { div.innerHTML = "<p>⚠️ Keine Daten</p>"; return; }

            div.innerHTML = data.pairs.map(p => `
              <div class="pair">
                <div class="org">
                  <div class="header">${p.org1.name}</div>
                  <p>ID: ${p.org1.id}</p>
                  <p>Besitzer: ${p.org1.owner_id?.name || "-"}</p>
                  <p>Website: ${p.org1.website || "-"}</p>
                  <p>Telefon: ${(p.org1.phone && p.org1.phone[0]?.value) || "-"}</p>
                </div>
                <div class="org">
                  <div class="header">${p.org2.name}</div>
                  <p>ID: ${p.org2.id}</p>
                  <p>Besitzer: ${p.org2.owner_id?.name || "-"}</p>
                  <p>Website: ${p.org2.website || "-"}</p>
                  <p>Telefon: ${(p.org2.phone && p.org2.phone[0]?.value) || "-"}</p>
                </div>
                <div class="conflict">
                  Im Konfliktfall:
                  <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                  <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
                  <button onclick="mergeOrgs(${p.org1.id}, ${p.org2.id}, '${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                </div>
                <p>Ähnlichkeit: ${p.score}%</p>
              </div>
            `).join("");
        }

        async function mergeOrgs(org1, org2, group) {
            let keep_id = document.querySelector(`input[name='keep_${group}']:checked`).value;
            let merge_with = (keep_id == org1 ? org2 : org1);

            if(!confirm(`Organisation ${keep_id} als Master behalten und mit ${merge_with} zusammenführen?`)) return;

            let res = await fetch(`/merge_orgs?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`, {
                method: "POST"
            });
            let data = await res.json();

            if(data.ok){
                alert("✅ Merge erfolgreich!");
                location.reload();
            } else {
                alert("❌ Fehler beim Merge: " + data.error);
            }
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
    uvicorn.run(app, host="0.0.0.0", port=port)
