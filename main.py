import os
import httpx
import uvicorn
from fastapi import FastAPI, Request, Query
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ===================== Konfiguration =====================
CLIENT_ID = os.getenv("8d460c8795c8de13")
CLIENT_SECRET = os.getenv("b7ac0302846ca51871a9133ddfa48200cf956ab4")
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:5000")  # lokal default
REDIRECT_URI = f"{BASE_URL}/oauth/callback"

OAUTH_AUTHORIZE_URL = "https://oauth.pipedrive.com/oauth/authorize"
OAUTH_TOKEN_URL = "https://oauth.pipedrive.com/oauth/token"
PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1"

# ===================== App Setup =====================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

TOKENS = {}

# ===================== OAuth-Flow =====================
@app.get("/login")
async def login():
    """Startet OAuth Login"""
    url = f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
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

def get_token():
    if "default" not in TOKENS:
        return None
    return TOKENS["default"]["access_token"]

# ===================== Business-Logik =====================
@app.get("/scan")
async def scan():
    """
    Beispiel: Scan nach Duplikaten (Dummy)
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

    results = []
    for i, org in enumerate(orgs or []):
        if i % 2 == 0:  # Dummy-Duplikatslogik
            results.append({
                "id": org["id"],
                "name": org.get("name"),
                "score": 95
            })

    return {"ok": True, "duplicates": results}

@app.get("/overview")
async def overview(request: Request):
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

# ===================== Root Endpoint =====================
@app.get("/")
async def root():
    return {"message": "✅ App läuft – gehe zu /login, um dich mit Pipedrive zu verbinden."}

# ===================== Start (lokal oder Render) =====================
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))  # Render: $PORT, lokal: 5000
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
