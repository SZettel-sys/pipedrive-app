import os
import httpx
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine
from dotenv import load_dotenv
from rapidfuzz import fuzz

# ================== Konfiguration ==================
load_dotenv()

API_TOKEN = os.getenv("PIPEDRIVE_API_TOKEN")
COMPANY_DOMAIN = os.getenv("PIPEDRIVE_COMPANY_DOMAIN", "bizforwardgmbh-sandbox")
DATABASE_URL = os.getenv("DATABASE_URL")  # Neon/Postgres
engine = create_engine(DATABASE_URL)

# ================== FastAPI App ==================
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# ================== Hilfsfunktionen ==================
async def fetch_all_organizations():
    """Alle Organisationen mit Paging laden"""
    all_orgs = []
    start = 0
    limit = 500
    async with httpx.AsyncClient() as client:
        while True:
            url = f"https://{COMPANY_DOMAIN}.pipedrive.com/api/v1/organizations?start={start}&limit={limit}&api_token={API_TOKEN}"
            r = await client.get(url)
            data = r.json()
            if not data.get("success"):
                break
            orgs = data["data"] or []
            all_orgs.extend(orgs)
            if not data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection"):
                break
            start += limit
    return all_orgs


def normalize_name(name: str) -> str:
    """Namen für Vergleich normalisieren"""
    return name.lower().replace(" ", "").replace("-", "")


def detect_duplicates(orgs, threshold=85):
    """Finde Duplikate mit RapidFuzz"""
    duplicates = []
    seen = set()
    for i, org1 in enumerate(orgs):
        for j, org2 in enumerate(orgs):
            if i >= j:
                continue
            key = tuple(sorted([org1["id"], org2["id"]]))
            if key in seen:
                continue
            score = fuzz.token_sort_ratio(normalize_name(org1["name"]), normalize_name(org2["name"]))
            if score >= threshold:
                duplicates.append((org1, org2, score))
                seen.add(key)
    return duplicates


def get_label_info(org):
    """Label Name + Farbe zurückgeben"""
    label = org.get("label")
    if isinstance(label, dict):
        return label.get("name", "-"), label.get("color", "#ccc")
    return "-", "#ccc"


async def merge_organizations(primary_id: int, secondary_id: int):
    """API-Call zum Zusammenführen"""
    url = f"https://{COMPANY_DOMAIN}.pipedrive.com/api/v1/organizations/{secondary_id}/merge?api_token={API_TOKEN}"
    async with httpx.AsyncClient() as client:
        r = await client.put(url, json={"merge_with_id": primary_id})
        return r.json()

# ================== Routen ==================
@app.get("/", response_class=HTMLResponse)
async def overview():
    html = """
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <title>OrgDupliCheck</title>
        <style>
            body { font-family: Arial, sans-serif; margin:0; padding:0; background:#f4f6f8; }
            header { background:#fff; text-align:center; padding:20px; }
            header img { height:80px; }
            .controls { margin:20px; }
            .btn { padding:8px 16px; border:none; border-radius:5px; cursor:pointer; }
            .btn-primary { background:#2b6cb0; color:white; }
            .btn-secondary { background:#999; color:white; }
            .dup-card { background:white; margin:20px auto; padding:20px; border-radius:8px; width:80%; box-shadow:0 2px 4px rgba(0,0,0,0.1); }
            .org { width:45%; display:inline-block; vertical-align:top; margin:0 2%; }
            .actions { margin-top:15px; }
            .actions label { margin-right:15px; font-size:14px; }
        </style>
    </head>
    <body>
        <header>
            <img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo">
        </header>

        <main>
            <div class="controls">
                <button id="scanBtn" class="btn btn-primary">🔍 Scan starten</button>
                <button id="bulkMergeBtn" class="btn btn-primary">📦 Bulk Merge ausführen</button>
            </div>

            <div id="stats"></div>
            <div id="duplicates"></div>
        </main>

        <script>
            document.getElementById("scanBtn").addEventListener("click", async () => {
                const res = await fetch("/scan_orgs");
                const data = await res.json();
                document.getElementById("stats").innerHTML =
                    `<p>Geladene Organisationen: <b>${data.count}</b></p>
                     <p>Duplikate insgesamt: <b>${data.duplicates.length}</b></p>`;
                renderDuplicates(data.duplicates);
            });

            async function renderDuplicates(dupes) {
                const container = document.getElementById("duplicates");
                container.innerHTML = "";
                dupes.forEach((d, idx) => {
                    const html = `
                    <div class="dup-card">
                        <div class="org">
                            <b>${d.org1.name}</b><br>
                            ID: ${d.org1.id}<br>
                            Besitzer: <b>${d.org1.owner}</b><br>
                            Label: <span style="color:${d.org1.label_color}">${d.org1.label}</span><br>
                            Website: ${d.org1.website}<br>
                            Adresse: ${d.org1.address}<br>
                            Deals: ${d.org1.deals_count}<br>
                            Kontakte: ${d.org1.contacts_count}
                        </div>
                        <div class="org">
                            <b>${d.org2.name}</b><br>
                            ID: ${d.org2.id}<br>
                            Besitzer: <b>${d.org2.owner}</b><br>
                            Label: <span style="color:${d.org2.label_color}">${d.org2.label}</span><br>
                            Website: ${d.org2.website}<br>
                            Adresse: ${d.org2.address}<br>
                            Deals: ${d.org2.deals_count}<br>
                            Kontakte: ${d.org2.contacts_count}
                        </div>
                        <div class="actions">
                            <label>Primär Datensatz:
                                <input type="radio" name="primary-${idx}" value="${d.org1.id}" checked> ${d.org1.name}
                                <input type="radio" name="primary-${idx}" value="${d.org2.id}"> ${d.org2.name}
                            </label>
                            <button onclick="previewMerge(${idx}, ${d.org1.id}, ${d.org2.id})" class="btn btn-primary">➕ Zusammenführen</button>
                            <button class="btn btn-secondary">🚫 Ignorieren</button>
                            <label><input type="checkbox" class="bulkChk" value="${d.org1.id},${d.org2.id}"> Für Bulk auswählen</label>
                        </div>
                        <p>Ähnlichkeit: ${d.similarity}%</p>
                    </div>`;
                    container.innerHTML += html;
                });
            }

            async function previewMerge(idx, id1, id2) {
                const primary = document.querySelector(`input[name="primary-${idx}"]:checked`).value;
                const secondary = primary == id1 ? id2 : id1;
                const res = await fetch("/preview_merge", {
                    method: "POST",
                    body: new URLSearchParams({ primary_id: primary, secondary_id: secondary })
                });
                const data = await res.json();
                if (data.success) {
                    if (confirm(
                        `Primär: ${data.preview.name}\\nAdresse: ${data.preview.address}\\nLabel: ${data.preview.label}\\nMerge wirklich ausführen?`
                    )) {
                        await fetch("/merge_orgs", {
                            method: "POST",
                            body: new URLSearchParams({ primary_id: primary, secondary_id: secondary })
                        });
                        alert("Merge erfolgreich!");
                    }
                } else {
                    alert("Fehler: " + data.error);
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/scan_orgs")
async def scan_orgs():
    orgs = await fetch_all_organizations()
    duplicates = detect_duplicates(orgs)

    results = []
    for org1, org2, score in duplicates:
        label1, color1 = get_label_info(org1)
        label2, color2 = get_label_info(org2)

        results.append({
            "org1": {
                "id": org1["id"],
                "name": org1["name"],
                "owner": org1.get("owner_id", {}).get("name", "-"),
                "label": label1,
                "label_color": color1,
                "website": org1.get("website", "-"),
                "address": org1.get("address", "-"),
                "deals_count": org1.get("open_deals_count", 0),
                "contacts_count": org1.get("people_count", 0),
            },
            "org2": {
                "id": org2["id"],
                "name": org2["name"],
                "owner": org2.get("owner_id", {}).get("name", "-"),
                "label": label2,
                "label_color": color2,
                "website": org2.get("website", "-"),
                "address": org2.get("address", "-"),
                "deals_count": org2.get("open_deals_count", 0),
                "contacts_count": org2.get("people_count", 0),
            },
            "similarity": score
        })

    return JSONResponse({"count": len(orgs), "duplicates": results})


@app.post("/preview_merge")
async def preview_merge(primary_id: int = Form(...), secondary_id: int = Form(...)):
    orgs = await fetch_all_organizations()
    org_map = {o["id"]: o for o in orgs}
    primary = org_map.get(primary_id)

    if not primary:
        return JSONResponse({"success": False, "error": "Organisation nicht gefunden"})

    label, color = get_label_info(primary)

    return JSONResponse({
        "success": True,
        "preview": {
            "id": primary["id"],
            "name": primary["name"],
            "owner": primary.get("owner_id", {}).get("name", "-"),
            "label": label,
            "label_color": color,
            "website": primary.get("website", "-"),
            "address": primary.get("address", "-"),
            "deals_count": primary.get("open_deals_count", 0),
            "contacts_count": primary.get("people_count", 0),
        }
    })


@app.post("/merge_orgs")
async def merge_orgs(primary_id: int = Form(...), secondary_id: int = Form(...)):
    result = await merge_organizations(primary_id, secondary_id)
    return JSONResponse(result)

# ================== Lokaler Start ==================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
