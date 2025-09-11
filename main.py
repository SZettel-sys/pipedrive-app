import os
import re
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from rapidfuzz import fuzz
from pyphonetics import Soundex

app = FastAPI()
soundex = Soundex()

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

# ================== Root Redirect ==================
@app.get("/")
def root():
    return RedirectResponse("/overview")

# ================== Login ==================
@app.get("/login")
def login():
    return RedirectResponse(f"{OAUTH_AUTHORIZE_URL}?client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}")

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

# ================== Auth Helper ==================
def get_auth():
    api_token = os.getenv("PD_API_TOKEN")
    if api_token:
        return {}, {"api_token": api_token}
    token = user_tokens.get("default")
    if token:
        return {"Authorization": f"Bearer {token}"}, {}
    return {}, {}

# ================== Normalizer ==================
def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    n = n.replace("-", " ").replace(".", " ").replace("/", " ")
    n = re.sub(r"\b(gmbh|ag|ug|ltd|inc|co|kg|ohg)\b", "", n)
    n = re.sub(r"[^a-z0-9 ]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n

# ================== Blocking Key ==================
def make_block_key(name: str) -> str:
    norm = normalize_name(name)
    if not norm:
        return ""
    parts = norm.split()
    main = parts[0] if parts else ""
    sound = soundex.phonetics(main) if main else ""
    length_class = str(len(norm) // 5)
    return f"{sound}_{length_class}"

# ================== Scan Organisations ==================
@app.get("/scan_orgs")
async def scan_orgs(threshold: int = 80):
    headers, params = get_auth()
    if not headers and not params:
        return {"ok": False, "error": "Nicht eingeloggt"}
    orgs = []
    start = 0
    limit = 500
    more_items = True
    async with httpx.AsyncClient() as client:
        # Labels laden
        label_map = {}
        label_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizationLabels", headers=headers, params=params)
        labels = label_resp.json().get("data", [])
        for l in labels:
            label_map[l["id"]] = {"name": l["name"], "color": l.get("color", "#666")}
        while more_items:
            resp = await client.get(
                f"{PIPEDRIVE_API_URL}/organizations",
                headers=headers,
                params={**params, "start": start, "limit": limit},
            )
            data = resp.json()
            items = data.get("data") or []
            for org in items:
                org["deal_count"] = org.get("open_deals_count", 0)
                org["contact_count"] = org.get("people_count", 0)
                # Label-Fix
                label_id = org.get("label_id") or org.get("label")
                if not label_id and "label_ids" in org and org["label_ids"]:
                    label_id = org["label_ids"][0]
                if isinstance(label_id, dict):
                    label_id = label_id.get("id")
                if label_id and label_id in label_map:
                    org["label_name"] = label_map[label_id]["name"]
                    org["label_color"] = label_map[label_id]["color"]
                else:
                    org["label_name"] = "-"
                    org["label_color"] = "#999"
                org["address"] = org.get("address") or "-"
                org["website"] = org.get("website") or "-"
                if "owner_id" in org and isinstance(org["owner_id"], dict):
                    org["owner_name"] = org["owner_id"].get("name", "-")
                else:
                    org["owner_name"] = "-"
            orgs.extend(items)
            more_items = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            start += limit
    buckets = {}
    for org in orgs:
        key = make_block_key(org.get("name", ""))
        if not key:
            continue
        buckets.setdefault(key, []).append((org, normalize_name(org.get("name", ""))))
    results = []
    for key, items in buckets.items():
        if len(items) < 2:
            continue
        for i in range(len(items)):
            for j in range(i+1, len(items)):
                org1, norm1 = items[i]
                org2, norm2 = items[j]
                score = fuzz.token_sort_ratio(norm1, norm2)
                if score >= threshold:
                    results.append({"org1": org1, "org2": org2, "score": round(score, 2)})
    return {"ok": True, "pairs": results, "meta": {"orgs_total": len(orgs), "pairs_found": len(results)}}

# ================== Preview Merge ==================
@app.get("/preview_merge/{org_id}")
async def preview_merge(org_id: int):
    headers, params = get_auth()
    if not headers and not params:
        return {"ok": False, "error": "Nicht eingeloggt"}
    async with httpx.AsyncClient() as client:
        org_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{org_id}", headers=headers, params=params)
        org = org_resp.json().get("data", {})
        label_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizationLabels", headers=headers, params=params)
        labels = label_resp.json().get("data", [])
        label_map = {l["id"]: l["name"] for l in labels}
        label_id = org.get("label_id") or org.get("label")
        if not label_id and "label_ids" in org and org["label_ids"]:
            label_id = org["label_ids"][0]
        if isinstance(label_id, dict):
            label_id = label_id.get("id")
        label_name = label_map.get(label_id, "-")
    return {"ok": True, "org": {
        "id": org.get("id"),
        "name": org.get("name"),
        "owner": org.get("owner_id", {}).get("name", "-"),
        "website": org.get("website") or "-",
        "address": org.get("address") or "-",
        "label": label_name,
        "deals": org.get("open_deals_count", 0),
        "contacts": org.get("people_count", 0),
    }}

# ================== Merge Organisations ==================
class MergeRequest(BaseModel):
    org1_id: int
    org2_id: int
    keep_id: int

@app.put("/merge_orgs")
async def merge_orgs(req: MergeRequest):
    headers, params = get_auth()
    if not headers and not params:
        return {"ok": False, "error": "Nicht eingeloggt"}
    org1_id, org2_id, keep_id = req.org1_id, req.org2_id, req.keep_id
    merge_id = org2_id if keep_id == org1_id else org1_id
    async with httpx.AsyncClient() as client:
        resp = await client.request("PUT", f"{PIPEDRIVE_API_URL}/organizations/{keep_id}/merge",
            headers=headers, params=params, json={"merge_with_id": merge_id})
    return {"ok": resp.status_code == 200, "result": resp.json()}

# ================== HTML Overview ==================
@app.get("/overview")
async def overview(request: Request):
    if not get_auth():
        return RedirectResponse("/login")
    html = """
    <html>
    <head>
        <title>Organisationen Übersicht</title>
        <style>
          body { font-family:'Source Sans Pro',Arial,sans-serif; background:#f4f6f8; margin:0; padding:0; }
          header { display:flex; justify-content:center; background:#4a90e2; padding:15px; }
          header img { height:120px; }
          .container { padding:20px; max-width:1200px; margin:0 auto; }
          button { padding:10px 18px; border:none; border-radius:6px; font-size:15px; cursor:pointer; font-family:'Source Sans Pro',Arial,sans-serif; }
          .btn-scan{background:#009fe3;color:white;} .btn-bulk{background:#5bc0eb;color:white;} .btn-merge{background:#1565c0;color:white;}
          .pair{background:white;border:1px solid #ddd;border-radius:8px;margin-bottom:20px;}
          .pair-table{width:100%;border-collapse:collapse;}
          .pair-table th{width:50%;padding:20px 40px;background:#f9f9f9;text-align:left;vertical-align:top;}
          .org-table{width:100%;border-collapse:collapse;margin:12px 20px;}
          .org-table td{padding:4px 8px;vertical-align:top;}
          .org-table td.label{font-weight:600;width:90px;}
          .org-table td.value{font-weight:400;}
          .org-table td.value b{font-weight:600;}
          .badge{padding:2px 6px;border-radius:4px;font-size:12px;color:white;}
          .conflict-row{background:#e3f2fd;padding:10px;font-weight:bold;}
          .conflict-actions{text-align:right;padding:10px;}
        </style>
    </head>
    <body>
        <header><img src="/static/expert-biz-logo.png" alt="Logo"></header>
        <div class="container">
            <button class="btn-scan" onclick="loadData()">🔎 Scan starten</button>
            <button class="btn-bulk" onclick="bulkMerge()">🚀 Bulk Merge ausführen</button>
            <div id="scanMeta"></div><div id="results"></div>
        </div>
        <script>
        async function loadData(){
            let res=await fetch('/scan_orgs?threshold=80'); let data=await res.json();
            let div=document.getElementById("results");
            if(!data.ok){div.innerHTML="<p>⚠️ Fehler: "+(data.error||"Keine Daten")+"</p>";return;}
            document.getElementById("scanMeta").innerHTML=`<p>Geladene Organisationen:<b>${data.meta.orgs_total}</b> | Duplikate:<b>${data.meta.pairs_found}</b></p>`;
            if(data.pairs.length===0){div.innerHTML="<p>✅ Keine Duplikate gefunden</p>";return;}
            div.innerHTML=data.pairs.map(p=>`
              <div class="pair"><table class="pair-table"><tr>
                <th><table class="org-table">
                  <tr><td class="label">Name:</td><td class="value"><b>${p.org1.name}</b></td></tr>
                  <tr><td class="label">ID:</td><td class="value">${p.org1.id}</td></tr>
                  <tr><td class="label">Besitzer:</td><td class="value">${p.org1.owner_name}</td></tr>
                  <tr><td class="label">Label:</td><td class="value"><span class="badge" style="background:${p.org1.label_color};">${p.org1.label_name}</span></td></tr>
                  <tr><td class="label">Website:</td><td class="value">${p.org1.website}</td></tr>
                  <tr><td class="label">Adresse:</td><td class="value">${p.org1.address}</td></tr>
                  <tr><td class="label">Deals:</td><td class="value">${p.org1.deal_count}</td></tr>
                  <tr><td class="label">Kontakte:</td><td class="value">${p.org1.contact_count}</td></tr>
                </table></th>
                <th><table class="org-table">
                  <tr><td class="label">Name:</td><td class="value"><b>${p.org2.name}</b></td></tr>
                  <tr><td class="label">ID:</td><td class="value">${p.org2.id}</td></tr>
                  <tr><td class="label">Besitzer:</td><td class="value">${p.org2.owner_name}</td></tr>
                  <tr><td class="label">Label:</td><td class="value"><span class="badge" style="background:${p.org2.label_color};">${p.org2.label_name}</span></td></tr>
                  <tr><td class="label">Website:</td><td class="value">${p.org2.website}</td></tr>
                  <tr><td class="label">Adresse:</td><td class="value">${p.org2.address}</td></tr>
                  <tr><td class="label">Deals:</td><td class="value">${p.org2.deal_count}</td></tr>
                  <tr><td class="label">Kontakte:</td><td class="value">${p.org2.contact_count}</td></tr>
                </table></th></tr>
                <tr><td colspan="2" class="conflict-row">
                  Primär Datensatz:
                  <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                  <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
                </td></tr>
                <tr><td>Ähnlichkeit: ${p.score}%</td>
                  <td class="conflict-actions">
                    <button class="btn-merge" onclick="mergeOrgs(${p.org1.id},${p.org2.id},'${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                    <input type="checkbox" class="bulkCheck" value="${p.org1.id}_${p.org2.id}"> Bulk
                  </td></tr></table></div>`).join("");
        }
        async function mergeOrgs(org1,org2,group){
            let keep_id=document.querySelector(`input[name='keep_${group}']:checked`).value;
            let preview=await fetch(`/preview_merge/${keep_id}`); let pdata=await preview.json();
            if(!pdata.ok){alert("❌ Fehler bei Vorschau");return;}
            let o=pdata.org;
            let msg=`⚠️ Vorschau Primär-Datensatz:\\nID:${o.id}\\nName:${o.name}\\nBesitzer:${o.owner}\\nLabel:${o.label}\\nWebsite:${o.website}\\nAdresse:${o.address}\\nDeals:${o.deals}\\nKontakte:${o.contacts}\\n\\nMerge ausführen?`;
            if(!confirm(msg)) return;
            let res=await fetch("/merge_orgs",{method:"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify({org1_id:org1,org2_id:org2,keep_id:parseInt(keep_id)})});
            let data=await res.json();
            if(data.ok){alert("✅ Merge erfolgreich!");location.reload();}else{alert("❌ Fehler: "+JSON.stringify(data.error));}
        }
        async function bulkMerge(){
            let selected=document.querySelectorAll(".bulkCheck:checked");
            let pairs=[]; selected.forEach(cb=>{let[o1,o2]=cb.value.split("_");let keep=document.querySelector(`input[name='keep_${o1}_${o2}']:checked`).value;pairs.push({org1_id:parseInt(o1),org2_id:parseInt(o2),keep_id:parseInt(keep)});});
            if(pairs.length===0){alert("⚠️ Keine Paare ausgewählt!");return;}
            let msg="⚠️ Folgende Paare werden gemerged:\\n"; pairs.forEach(p=>{msg+=`Org ${p.org1_id} & Org ${p.org2_id} → Keep ${p.keep_id}\\n`;});
            if(!confirm(msg)) return;
            let res=await fetch("/merge_orgs",{method:"PUT",headers:{"Content-Type":"application/json"},body:JSON.stringify(pairs[0])});
            let data=await res.json();
            if(data.ok){alert("✅ Bulk Merge erfolgreich!");location.reload();}else{alert("❌ Fehler: "+JSON.stringify(data.error));}
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
    uvicorn.run("main:app",host="0.0.0.0",port=port,reload=False,loop="uvloop",http="httptools")
