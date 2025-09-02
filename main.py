import os
import difflib
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from rapidfuzz import fuzz

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

# Tokens im Speicher (für Produktion: DB/Redis)
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
        return HTMLResponse(f"<h3>❌ Fehler beim Login: {token_data}</h3>")

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
            score = fuzz.token_sort_ratio(org1["name"], org2["name"])
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


# ================== Bulk Merge Organisations ==================
@app.post("/bulk_merge")
async def bulk_merge(pairs: list[dict]):
    if "default" not in user_tokens:
        return RedirectResponse("/login")

    headers = get_headers()
    results = []

    async with httpx.AsyncClient() as client:
        for pair in pairs:
            org1 = pair["org1_id"]
            org2 = pair["org2_id"]
            keep = pair["keep_id"]

            resp = await client.post(
                f"{PIPEDRIVE_API_URL}/organizations/{keep}/merge",
                headers=headers,
                json={"merge_with_id": org2 if keep == org1 else org1},
            )

            if resp.status_code == 200:
                results.append({"pair": pair, "status": "ok"})
            else:
                results.append({"pair": pair, "status": "error", "msg": resp.text})

    return {"ok": True, "results": results}


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
          body { font-family: Arial, sans-serif; margin: 0; padding: 0; background:#f4f6f8; }
          header { display:flex; align-items:center; background:#2b3a67; color:white; padding:15px; }
          header img { height:40px; margin-right:15px; }
          header h1 { font-size:22px; margin:0; }

          .container { padding:20px; }
          button { margin:10px 0; padding:8px 16px; border:none; border-radius:5px; cursor:pointer; }
          button:hover { opacity:0.9; }
          .btn-scan { background:#2b3a67; color:white; }
          .btn-merge { background:#2e7d32; color:white; }
          .btn-bulk { background:#0277bd; color:white; }

          .pair { background:white; border:1px solid #ddd; border-radius:8px; margin-bottom:25px; box-shadow:0 2px 4px rgba(0,0,0,0.1); }
          .pair-table { width:100%; border-collapse:collapse; }
          .pair-table th, .pair-table td { padding:10px; text-align:left; }
          .pair-table th { background:#f0f0f0; }

          .conflict-row { background:#e8f5e9; text-align:center; padding:12px; font-weight:bold; color:#2e7d32; }
          .similarity { padding:10px; font-size:14px; color:#555; text-align:right; }
        </style>
    </head>
    <body>
        <header>
            <img src="/static/logo.png" alt="Logo">
            <h1>Duplikatsprüfung Organisationen</h1>
        </header>

        <div class="container">
            <button class="btn-scan" onclick="loadData()">🔎 Scan starten</button>
            <button class="btn-bulk" onclick="bulkMerge()">🚀 Bulk Merge ausführen</button>
            <div id="results"></div>
            <div id="bulkResult"></div>
        </div>

        <script>
        async function loadData() {
            let res = await fetch('/scan_orgs?threshold=80');
            let data = await res.json();
            let div = document.getElementById("results");
            if(!data.ok) { div.innerHTML = "<p>⚠️ Keine Daten</p>"; return; }

            div.innerHTML = data.pairs.map(p => `
              <div class="pair">
                <table class="pair-table">
                  <tr>
                    <th>${p.org1.name}</th>
                    <th>${p.org2.name}</th>
                  </tr>
                  <tr>
                    <td>ID: ${p.org1.id}<br>Besitzer: ${p.org1.owner_id?.name || "-"}<br>Website: ${p.org1.website || "-"}<br>Telefon: ${(p.org1.phone && p.org1.phone[0]?.value) || "-"}</td>
                    <td>ID: ${p.org2.id}<br>Besitzer: ${p.org2.owner_id?.name || "-"}<br>Website: ${p.org2.website || "-"}<br>Telefon: ${(p.org2.phone && p.org2.phone[0]?.value) || "-"}</td>
                  </tr>
                  <tr>
                    <td colspan="2" class="conflict-row">
                      Im Konfliktfall:
                      <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                      <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
                      <input type="checkbox" class="bulkCheck" value="${p.org1.id}_${p.org2.id}"> Für Bulk auswählen
                      <button class="btn-merge" onclick="mergeOrgs(${p.org1.id}, ${p.org2.id}, '${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                    </td>
                  </tr>
                  <tr>
                    <td colspan="2" class="similarity">Ähnlichkeit: ${p.score}%</td>
                  </tr>
                </table>
              </div>
            `).join("");
        }

        async function mergeOrgs(org1, org2, group) {
            let keep_id = document.querySelector(`input[name='keep_${group}']:checked`).value;
            let merge_with = (keep_id == org1 ? org2 : org1);

            if(!confirm(\`Organisation \${keep_id} als Master behalten und mit \${merge_with} zusammenführen?\`)) return;

            let res = await fetch(\`/merge_orgs?org1_id=\${org1}&org2_id=\${org2}&keep_id=\${keep_id}\`, { method: "POST" });
            let data = await res.json();

            if(data.ok){
                alert("✅ Merge erfolgreich!");
                location.reload();
            } else {
                alert("❌ Fehler beim Merge: " + data.error);
            }
        }

        async function bulkMerge(){
            let selected = document.querySelectorAll(".bulkCheck:checked");
            let pairs = [];
            selected.forEach(cb => {
                let [org1, org2] = cb.value.split("_");
                let keep_id = document.querySelector(\`input[name='keep_\${org1}_\${org2}']:checked\`).value;
                pairs.push({org1_id: parseInt(org1), org2_id: parseInt(org2), keep_id: parseInt(keep_id)});
            });

            if(pairs.length === 0){
                alert("⚠️ Keine Paare ausgewählt!");
                return;
            }

            if(!confirm(\`\${pairs.length} Paare wirklich zusammenführen?\`)) return;

            let res = await fetch("/bulk_merge", {
                method: "POST",
                headers: {"Content-Type": "application/json"},
                body: JSON.stringify(pairs)
            });
            let data = await res.json();

            let resultDiv = document.getElementById("bulkResult");
            if(data.ok){
                let html = "<h3>Bulk Merge Ergebnis</h3><ul>";
                data.results.forEach(r => {
                    if(r.status === "ok"){
                        html += \`<li>✅ Merge erfolgreich: Org \${r.pair.org1_id} & \${r.pair.org2_id}</li>\`;
                    } else {
                        html += \`<li>❌ Fehler: Org \${r.pair.org1_id} & \${r.pair.org2_id} → \${r.msg}</li>\`;
                    }
                });
                html += "</ul>";
                resultDiv.innerHTML = html;
            } else {
                resultDiv.innerHTML = "<p>❌ Fehler beim Bulk Merge</p>";
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




