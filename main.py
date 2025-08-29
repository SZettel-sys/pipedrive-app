import os
import httpx
import uvicorn
from fastapi import FastAPI, Request, Depends, Query
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ===================== Konfiguration =====================
CLIENT_ID = os.getenv("PD_CLIENT_ID", "8d460c8795c8de13")   # deine Client ID
CLIENT_SECRET = os.getenv("PD_CLIENT_SECRET", "b7ac0302846ca51871a9133ddfa48200cf956ab4") # in Developer Hub eintragen!
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:5000")  # aktuelle ngrok-URL!
REDIRECT_URI = f"{BASE_URL}/oauth/callback"

OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

# ===================== App Setup =====================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Tokens im Speicher (für Sandbox reicht das)
TOKENS = {}

# ===================== OAuth-Flow =====================
@app.get("/login")
async def login():
    """Startet OAuth Login"""
    url = (
        f"{OAUTH_AUTHORIZE_URL}"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
    )
    return RedirectResponse(url)

@app.get("/oauth/callback")
async def oauth_callback(code: str = Query(...)):
    """Callback von Pipedrive"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            OAUTH_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
        )
        token_data = resp.json()

    if "access_token" not in token_data:
        return JSONResponse({"ok": False, "error": token_data})

    TOKENS["default"] = token_data
    return RedirectResponse("/overview")

@app.get("/debug/token")
async def debug_token():
    """Zeigt gespeicherte Tokens"""
    return {
        "have_default_token": "default" in TOKENS,
        "context_tokens": list(TOKENS.keys()),
    }

def get_token():
    if "default" not in TOKENS:
        return None
    return TOKENS["default"]["access_token"]

# ===================== Business-Logik =====================
@app.get("/scan")
async def scan(threshold: int = 80, companyId: int = Query(None), userId: int = Query(None)):
    """
    Beispiel: Scan nach Duplikaten (nur Demo)
    """
    token = get_token()
    if not token:
        return {"ok": False, "needsAuth": True,
                "authorizeUrl": f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"}

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{PIPEDRIVE_API_URL}/organizations",
            headers={"Authorization": f"Bearer {token}"}
        )
        orgs = resp.json().get("data", [])

    # Demo-Duplikatslogik (Name >= threshold Prozent identisch -> fake)
    results = []
    for i, org in enumerate(orgs or []):
        if i % 2 == 0:  # Dummy
            results.append({
                "id": org["id"],
                "name": org.get("name"),
                "score": 95
            })

    return {"ok": True, "duplicates": results}

@app.get("/overview")
async def overview(request: Request,
                   resource: str = Query(None),
                   view: str = Query(None),
                   companyId: int = Query(None),
                   userId: int = Query(None)):
    """
    Übersicht im Panel (HTML-UI)
    """
    token = get_token()
    if not token:
        return RedirectResponse("/login")

    html = """
    <html>
    <head>
        <title>Duplicates Overview</title>
        <link rel="stylesheet" href="/static/style.css" />
    </head>
    <body>
        <h2>Duplicate Check</h2>
        <button onclick="runScan()">Scan Organizations</button>
        <div id="results"></div>

        <script>
        async function runScan() {
            let res = await fetch('/scan?threshold=85');
            let data = await res.json();
            let div = document.getElementById("results");
            if(!data.ok) {
                div.innerHTML = "<p>⚠️ Auth required. <a href='/login'>Login</a></p>";
                return;
            }
            div.innerHTML = "<ul>" + data.duplicates.map(d => 
                `<li>${d.name} (Score: ${d.score})</li>`).join("") + "</ul>";
        }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(html)

# ===================== Lokaler Start =====================
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)
