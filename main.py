import os
import re
import httpx
import jellyfish  # für Soundex
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List
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
    print("✅ Login erfolgreich, Token gespeichert.")
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
    sound = jellyfish.soundex(main) if main else ""
    length_class = str(len(norm) // 5)
    return f"{sound}_{length_class}"

# ================== Prepare Merge ==================
async def prepare_merge(keep_id: int, merge_id: int, headers, params):
    async with httpx.AsyncClient() as client:
        keep_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{keep_id}", headers=headers, params=params)
        merge_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{merge_id}", headers=headers, params=params)

        keep_org = keep_resp.json().get("data", {})
        merge_org = merge_resp.json().get("data", {})

        updates = {}
        for field in ["address", "website", "label"]:
            if not keep_org.get(field) and merge_org.get(field):
                updates[field] = merge_org.get(field)

        if updates:
            print(f"📝 Ergänze Felder in Org {keep_id}: {updates}")
            await client.put(
                f"{PIPEDRIVE_API_URL}/organizations/{keep_id}",
                headers=headers,
                params=params,
                json=updates,
            )

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
                org["label"] = org.get("label") or "-"
                org["address"] = org.get("address") or "-"
                org["website"] = org.get("website") or "-"
                if "owner_id" in org and isinstance(org["owner_id"], dict):
                    org["owner_name"] = org["owner_id"].get("name", "-")
                else:
                    org["owner_name"] = "-"

            orgs.extend(items)
            more_items = data.get("additional_data", {}).get("pagination", {}).get("more_items_in_collection", False)
            start += limit

    print(f"🔎 Insgesamt {len(orgs)} Organisationen geladen.")

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
                    results.append({
                        "org1": org1,
                        "org2": org2,
                        "score": round(score, 2),
                    })

    print(f"✅ {len(results)} mögliche Duplikate gefunden.")
    return {
        "ok": True,
        "pairs": results,
        "meta": {"orgs_total": len(orgs), "pairs_found": len(results), "buckets": len(buckets)}
    }

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

    await prepare_merge(keep_id, merge_id, headers, params)

    async with httpx.AsyncClient() as client:
        resp = await client.request(
            "PUT",
            f"{PIPEDRIVE_API_URL}/organizations/{keep_id}/merge",
            headers=headers,
            params=params,
            json={"merge_with_id": merge_id},
        )

    data = resp.json()
    return {"ok": resp.status_code == 200, "result": data}

# ================== Bulk Merge Organisations ==================
class BulkPair(BaseModel):
    org1_id: int
    org2_id: int
    keep_id: int

@app.put("/bulk_merge")
async def bulk_merge(pairs: List[BulkPair]):
    headers, params = get_auth()
    if not headers and not params:
        return {"ok": False, "error": "Nicht eingeloggt"}

    results = []
    async with httpx.AsyncClient() as client:
        for pair in pairs:
            org1, org2, keep = pair.org1_id, pair.org2_id, pair.keep_id
            merge_id = org2 if keep == org1 else org1

            await prepare_merge(keep, merge_id, headers, params)

            resp = await client.request(
                "PUT",
                f"{PIPEDRIVE_API_URL}/organizations/{keep}/merge",
                headers=headers,
                params=params,
                json={"merge_with_id": merge_id},
            )

            if resp.status_code == 200:
                results.append({"pair": pair.dict(), "status": "ok"})
            else:
                results.append({"pair": pair.dict(), "status": "error", "msg": resp.text})

    return {"ok": True, "results": results}

# ================== Preview Endpoint ==================
@app.get("/preview_merge/{org_id}")
async def preview_merge(org_id: int):
    headers, params = get_auth()
    if not headers and not params:
        return {"ok": False, "error": "Nicht eingeloggt"}

    async with httpx.AsyncClient() as client:
        org_resp = await client.get(f"{PIPEDRIVE_API_URL}/organizations/{org_id}", headers=headers, params=params)
        org = org_resp.json().get("data", {})

    result = {
        "id": org.get("id"),
        "name": org.get("name"),
        "owner": org.get("owner_id", {}).get("name", "-"),
        "website": org.get("website") or "-",
        "address": org.get("address") or "-",
        "label": org.get("label") or "-",
        "deals": org.get("open_deals_count", 0),
        "contacts": org.get("people_count", 0),
    }
    return {"ok": True, "org": result}

# ================== Lokaler Start ==================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        loop="uvloop",
        http="httptools"
    )
