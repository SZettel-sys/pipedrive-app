import os
import re
import asyncio
import json
import time
import httpx
import asyncpg
from fastapi import FastAPI, Request, Body
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
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
PIPEDRIVE_API_V2_URL = "https://api.pipedrive.com/api/v2"
# Einige Endpunkte (z.B. Merge von Organisationen) sind Stand heute noch nur als API v1 verfügbar.
PIPEDRIVE_API_V1_URL = "https://api.pipedrive.com/v1"
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

def extract_address(address_value):
    """API v2 liefert 'address' als Objekt; wir wollen für die UI einen String."""
    if isinstance(address_value, dict):
        return address_value.get("value") or "-"
    return address_value or "-"


async def fetch_user_map(headers: dict) -> dict[int, str]:
    """Owner-Namen nachladen (Users API ist Stand heute noch API v1)."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{PIPEDRIVE_API_V1_URL}/users", headers=headers)
    if resp.status_code != 200:
        return {}
    data = resp.json().get("data") or []
    out: dict[int, str] = {}
    for u in data:
        try:
            out[int(u.get("id"))] = u.get("name") or str(u.get("id"))
        except Exception:
            continue
    return out


async def fetch_org_label_option_map(headers: dict) -> dict[int, dict]:
    """Mappt label_ids -> (Name, Farbe) über die OrganizationFields API v2."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(f"{PIPEDRIVE_API_V2_URL}/organizationFields", headers=headers)
    if resp.status_code != 200:
        return {}

    fields = resp.json().get("data") or []
    label_field = None
    for f in fields:
        code = (f.get("field_code") or "").lower()
        fname = (f.get("field_name") or "").lower()
        if code == "label_ids" or fname in {"label", "labels"}:
            label_field = f
            break

    options = (label_field or {}).get("options") or []
    out: dict[int, dict] = {}
    for opt in options:
        oid = opt.get("id")
        if oid is None:
            continue
        try:
            oid_int = int(oid)
        except Exception:
            continue
        out[oid_int] = {
            "id": oid_int,
            "name": opt.get("label") or f"Label {oid_int}",
            "color": opt.get("color") or "#999",
        }
    return out

# ================== Normalizer ==================
def normalize_name(name: str) -> str:
    if not name: return ""
    n = name.lower()
    n = re.sub(r"\b(gmbh|ug|ag|kg|ohg|inc|ltd)\b", "", n)
    n = re.sub(r"[^a-z0-9 ]", "", n)
    return re.sub(r"\s+", " ", n).strip()

# ================== Scan Orgs ==================
@app.get("/scan_orgs")
async def scan_orgs(threshold: int = 85):
    if "default" not in user_tokens:
        return {
            "ok": False,
            "error": "Nicht eingeloggt",
            "total": 0,
            "duplicates": 0,
            "pairs": [],
        }

    headers = get_headers()

    # v2: Cursor-basierte Pagination (cursor + limit)
    limit = 500
    cursor = None
    orgs = []

    # Label-Definitionen (label_ids -> Name/Farbe) und Owner-Namen laden (Users ist noch v1)
    label_map, user_map = await asyncio.gather(
        fetch_org_label_option_map(headers),
        fetch_user_map(headers),
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            params = {
                "limit": limit,
                # open_deals_count und people_count sind in v2 optional und müssen explizit angefordert werden
                "include_fields": "open_deals_count,people_count",
            }
            if cursor:
                params["cursor"] = cursor

            resp = await client.get(f"{PIPEDRIVE_API_V2_URL}/organizations", headers=headers, params=params)

            if resp.status_code != 200:
                return {
                    "ok": False,
                    "error": f"Pipedrive API Fehler ({resp.status_code}): {resp.text}",
                    "pairs": [],
                    "total": 0,
                    "duplicates": 0,
                }

            data = resp.json()
            items = data.get("data") or []
            if not items:
                break

            for org in items:
                owner_id = org.get("owner_id")
                owner_name = user_map.get(int(owner_id), str(owner_id)) if owner_id is not None else "-"

                # v2: label_ids ist ein Array (kann leer sein)
                labels = []
                for lid in (org.get("label_ids") or []):
                    try:
                        lid_int = int(lid)
                    except Exception:
                        continue
                    labels.append(label_map.get(lid_int) or {"id": lid_int, "name": f"Label {lid_int}", "color": "#999"})

                orgs.append(
                    {
                        "id": org.get("id"),
                        "name": org.get("name"),
                        "owner": owner_name,
                        "website": org.get("website") or "-",
                        "address": extract_address(org.get("address")),
                        "deals_count": org.get("open_deals_count", 0) or 0,
                        "contacts_count": org.get("people_count", 0) or 0,
                        "labels": labels,  # Liste von Badges
                    }
                )

            # v2: next_cursor steht in additional_data.next_cursor (null => Ende)
            cursor = (data.get("additional_data") or {}).get("next_cursor")
            if not cursor:
                break

    ignored = await load_ignored()

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
                    normalize_name(org1["name"]), normalize_name(org2["name"])
                )
                if score >= threshold:
                    results.append(
                        {"org1": org1, "org2": org2, "score": round(score, 2)}
                    )

    return {
        "ok": True,
        "pairs": results,
        "total": len(orgs),
        "duplicates": len(results),
    }


# ================== Preview Merge ==================
# ================== Preview Merge ==================


# ================== SSE Scan (Progress) ==================
def _sse(data: dict) -> str:
    """Format a dict as an SSE message (JSON in data: ...)."""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


async def _scan_orgs_with_progress(threshold: int, progress):
    """
    Internal scan function that reports progress via:
      await progress({"type": "...", ...})
    Returns the same payload shape as /scan_orgs.
    """
    if "default" not in user_tokens:
        return {"ok": False, "error": "Nicht eingeloggt", "total": 0, "duplicates": 0, "pairs": []}

    headers = get_headers()

    await progress({"type": "status", "stage": "init", "mode": "indeterminate", "message": "Starte Scan…"})
    await progress({"type": "status", "stage": "meta", "mode": "indeterminate", "message": "Lade Label-Definitionen & User…"})

    label_map, user_map = await asyncio.gather(
        fetch_org_label_option_map(headers),
        fetch_user_map(headers),
    )

    await progress({"type": "status", "stage": "fetch", "mode": "indeterminate", "message": "Lade Organisationen aus Pipedrive…"})

    # v2 pagination (cursor + limit)
    limit = 500
    cursor = None
    orgs = []
    page = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            page += 1
            params = {
                "limit": limit,
                "include_fields": "open_deals_count,people_count",
            }
            if cursor:
                params["cursor"] = cursor

            resp = await client.get(f"{PIPEDRIVE_API_V2_URL}/organizations", headers=headers, params=params)
            if resp.status_code != 200:
                return {
                    "ok": False,
                    "error": f"Pipedrive API Fehler ({resp.status_code}): {resp.text}",
                    "pairs": [],
                    "total": 0,
                    "duplicates": 0,
                }

            data = resp.json()
            items = data.get("data") or []
            if not items:
                break

            for org in items:
                owner_id = org.get("owner_id")
                owner_name = user_map.get(int(owner_id), str(owner_id)) if owner_id is not None else "-"

                labels = []
                for lid in (org.get("label_ids") or []):
                    try:
                        lid_int = int(lid)
                    except Exception:
                        continue
                    labels.append(label_map.get(lid_int) or {"id": lid_int, "name": f"Label {lid_int}", "color": "#999"})

                address_obj = org.get("address") or {}
                address_value = address_obj.get("value") if isinstance(address_obj, dict) else str(address_obj)

                orgs.append(
                    {
                        "id": org.get("id"),
                        "name": org.get("name"),
                        "owner": owner_name,
                        "website": org.get("website") or "-",
                        "address": extract_address(org.get("address")),
                        "deals_count": org.get("open_deals_count", 0) or 0,
                        "contacts_count": org.get("people_count", 0) or 0,
                        "labels": labels,
                    }
                )
            await progress(
                {
                    "type": "status",
                    "stage": "fetch",
                    "mode": "indeterminate",
                    "message": f"Lade Organisationen… Seite {page} (bisher {len(orgs)})",
                    "loaded": len(orgs),
                    "page": page,
                }
            )

            cursor = (data.get("additional_data") or {}).get("next_cursor")
            if not cursor:
                break

    await progress({"type": "status", "stage": "prepare", "mode": "indeterminate", "message": f"Vorbereitung: {len(orgs)} Organisationen geladen. Lade Ignore-Liste…"})
    ignored = await load_ignored()

    # Buckets bilden
    await progress({"type": "status", "stage": "bucket", "mode": "indeterminate", "message": "Gruppiere Organisationen (Buckets)…"})
    buckets = {}
    for org in orgs:
        key = normalize_name(org["name"])[:3]
        buckets.setdefault(key, []).append(org)

    # Total comparisons (for a determinate progress bar)
    total_comparisons = 0
    for group in buckets.values():
        n = len(group)
        if n > 1:
            total_comparisons += (n * (n - 1)) // 2

    await progress(
        {
            "type": "status",
            "stage": "match",
            "mode": "determinate",
            "message": "Fuzzy-Matching…",
            "percent": 50,
            "processed": 0,
            "total": total_comparisons,
        }
    )

    pairs = []
    processed = 0
    last_emit = time.time()

    # Matching
    for group in buckets.values():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a = group[i]
                b = group[j]
                processed += 1

                # Emit at most ~5x per second to keep SSE lightweight
                now = time.time()
                if now - last_emit > 0.2:
                    pct = 50
                    if total_comparisons > 0:
                        pct = 50 + int((processed / total_comparisons) * 50)
                        pct = max(50, min(99, pct))
                    await progress(
                        {
                            "type": "status",
                            "stage": "match",
                            "mode": "determinate",
                            "message": f"Fuzzy-Matching… {processed}/{total_comparisons}",
                            "percent": pct,
                            "processed": processed,
                            "total": total_comparisons,
                        }
                    )
                    last_emit = now

                score = fuzz.token_sort_ratio(a["name"], b["name"])
                if score >= threshold:
                    pair_key = tuple(sorted([int(a["id"]), int(b["id"])]))
                    if pair_key in ignored:
                        continue
                    pairs.append(
                        {
                            "score": round(score, 2),
                            "org1": a,
                            "org2": b,
                        }
                    )

    await progress({"type": "status", "stage": "final", "mode": "determinate", "message": "Finalisiere Ergebnis…", "percent": 100})

    pairs.sort(key=lambda x: x["score"], reverse=True)
    return {
        "ok": True,
        "total": len(orgs),
        "duplicates": len(pairs),
        "pairs": pairs,
    }


@app.get("/scan_orgs_stream")
async def scan_orgs_stream(threshold: int = 85):
    """
    Server-Sent Events endpoint for live scan progress.
    Client opens EventSource('/scan_orgs_stream?threshold=85') and receives JSON messages.
    """
    q: asyncio.Queue = asyncio.Queue()
    done = asyncio.Event()

    async def progress(msg: dict):
        # push status messages
        await q.put(msg)

    async def runner():
        try:
            result = await _scan_orgs_with_progress(threshold, progress)
            await q.put({"type": "done", "payload": result})
        except Exception as e:
            await q.put({"type": "error", "message": str(e)})
        finally:
            done.set()

    asyncio.create_task(runner())

    async def gen():
        # initial hello so the client can show UI instantly
        yield _sse({"type": "status", "stage": "init", "mode": "indeterminate", "message": "Verbunden. Starte…"})
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=15.0)
                yield _sse(msg)
                if msg.get("type") in ("done", "error"):
                    break
            except asyncio.TimeoutError:
                # keepalive ping
                yield _sse({"type": "ping"})
                if done.is_set() and q.empty():
                    break

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/preview_merge")
async def preview_merge(org1_id: int, org2_id: int, keep_id: int):
    headers = get_headers()
    if not headers:
        return {"ok": False, "error": "Nicht eingeloggt"}

    other_id = org2_id if keep_id == org1_id else org1_id

    # Label-Mapping für lesbare Vorschau
    label_map = await fetch_org_label_option_map(headers)

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp_keep = await client.get(
            f"{PIPEDRIVE_API_V2_URL}/organizations/{keep_id}",
            headers=headers,
            params={"include_fields": "open_deals_count,people_count"},
        )
        resp_other = await client.get(
            f"{PIPEDRIVE_API_V2_URL}/organizations/{other_id}",
            headers=headers,
            params={"include_fields": "open_deals_count,people_count"},
        )

    if resp_keep.status_code != 200 or resp_other.status_code != 200:
        return {"ok": False, "error": "Fehler beim Laden"}

    keep_org = resp_keep.json().get("data", {}) or {}
    other_org = resp_other.json().get("data", {}) or {}

    def labels_from(o: dict) -> list[dict]:
        out = []
        for lid in (o.get("label_ids") or []):
            try:
                lid_int = int(lid)
            except Exception:
                continue
            out.append(label_map.get(lid_int) or {"id": lid_int, "name": f"Label {lid_int}", "color": "#999"})
        return out

    keep_labels = labels_from(keep_org)
    other_labels = labels_from(other_org)

    enriched = {
        "id": keep_org.get("id"),
        "name": keep_org.get("name"),
        "labels": keep_labels or other_labels,
        "address": extract_address(keep_org.get("address")) or extract_address(other_org.get("address")),
        "website": keep_org.get("website") or other_org.get("website"),
        "open_deals_count": keep_org.get("open_deals_count") or other_org.get("open_deals_count"),
        "people_count": keep_org.get("people_count") or other_org.get("people_count"),
    }

    return {"ok": True, "preview": enriched}
@app.post("/merge_orgs")
async def merge_orgs(org1_id: int, org2_id: int, keep_id: int):
    headers = get_headers()
    if not headers:
        return {"ok": False, "error": "Nicht eingeloggt"}

    # Sekundär = der andere → dieser wird in der URL verwendet (= gelöscht)
    secondary_id = org2_id if keep_id == org1_id else org1_id
    primary_id = keep_id  # soll bleiben

    async with httpx.AsyncClient() as client:
        resp = await client.put(
            f"{PIPEDRIVE_API_V1_URL}/organizations/{secondary_id}/merge",
            headers=headers,
            json={"merge_with_id": primary_id},  # jetzt bleibt primary_id erhalten
        )

    if resp.status_code != 200:
        return {"ok": False, "error": resp.text}

    return {"ok": True, "merged": resp.json().get("data", {})}
# ================== Bulk Merge (neu) ==================
@app.post("/bulk_merge")
async def bulk_merge(pairs: list = Body(...)):
    if "default" not in user_tokens:
        return {"ok": False, "error": "Nicht eingeloggt"}

    headers = get_headers()
    results = []

    async with httpx.AsyncClient(timeout=60.0) as client:
        for pair in pairs:
            org1_id = pair.get("org1_id")
            org2_id = pair.get("org2_id")
            keep_id = pair.get("keep_id")

            if not all([org1_id, org2_id, keep_id]):
                results.append({"ok": False, "error": f"Ungültiges Paar: {pair}"})
                continue

            secondary_id = org2_id if keep_id == org1_id else org1_id
            primary_id = keep_id

            resp = await client.put(
                f"{PIPEDRIVE_API_V1_URL}/organizations/{secondary_id}/merge",
                headers=headers,
                json={"merge_with_id": primary_id},  # primary bleibt erhalten
            )

            if resp.status_code == 200:
                results.append({
                    "ok": True,
                    "pair": {"primary_id": primary_id, "secondary_id": secondary_id},
                    "merged": resp.json().get("data", {})
                })
            else:
                results.append({
                    "ok": False,
                    "pair": {"primary_id": primary_id, "secondary_id": secondary_id},
                    "error": resp.text
                })

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
        :root{
          --bg:#f6f7fb;
          --card:#ffffff;
          --text:#0f172a;
          --muted:#64748b;
          --border:#e2e8f0;
          --brand:#0ea5e9;
          --brand-hover:#0284c7;
          --danger:#ef4444;
          --danger-hover:#dc2626;
          --shadow:0 10px 25px rgba(15,23,42,.08);
        }

        *{ box-sizing:border-box; }
        body{
          font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
          background:var(--bg);
          margin:0;
          color:var(--text);
        }

        header{
          background:linear-gradient(90deg,#ffffff 0%, #f8fbff 60%, #ffffff 100%);
          border-bottom:1px solid var(--border);
          padding:14px 16px;
          display:flex;
          justify-content:center;
          align-items:center;
          position:sticky;
          top:0;
          z-index:10;
        }
        header img{ height:48px; }

        .container{
          max-width:1400px;
          margin:18px auto 90px;
          padding:0 14px;
        }

        .top-actions{
          display:flex;
          gap:12px;
          align-items:center;
          flex-wrap:wrap;
          margin:10px 0 14px;
        }

        #stats{
          color:var(--muted);
          background:var(--card);
          border:1px solid var(--border);
          border-radius:14px;
          padding:10px 12px;
          box-shadow:0 2px 10px rgba(15,23,42,.04);
        }

        .card{
          background:var(--card);
          border:1px solid var(--border);
          border-radius:16px;
          box-shadow:var(--shadow);
        }

        /* Buttons */
        .btn{
          appearance:none;
          border:1px solid transparent;
          border-radius:12px;
          padding:10px 14px;
          font-weight:700;
          cursor:pointer;
          display:inline-flex;
          align-items:center;
          gap:8px;
          transition:background .15s ease, box-shadow .15s ease, transform .05s ease;
          box-shadow:0 2px 10px rgba(15,23,42,.06);
        }
        .btn:active{ transform:translateY(1px); }
        .btn-primary{
          background:var(--brand);
          color:white;
        }
        .btn-primary:hover{ background:var(--brand-hover); }
        .btn-danger{
          background:var(--danger);
          color:white;
        }
        .btn-danger:hover{ background:var(--danger-hover); }
        .btn-outline{
          background:white;
          color:var(--text);
          border-color:var(--border);
          box-shadow:0 2px 10px rgba(15,23,42,.04);
        }
        .btn-outline:hover{ background:#f8fafc; }
        .btn-small{
          padding:8px 10px;
          border-radius:10px;
          font-size:13px;
          font-weight:700;
        }

        /* Pair cards */
        .pair{
          margin:14px 0;
          overflow:hidden;
        }
        .pair-table{
          width:100%;
          border-collapse:separate;
          border-spacing:0;
        }
        .pair-table td{
          padding:10px 14px;
          border-bottom:1px solid var(--border);
          vertical-align:top;
          width:50%;
        }
        .pair-table tr td:first-child{
          border-right:1px solid var(--border);
        }
        .pair-table tr:first-child td{
          font-weight:800;
          background:#f1f7ff;
          font-size:15px;
        }
        .pair-table tr:nth-child(even) td{
          background:#fcfdff;
        }
        .pair-table tr:last-child td{
          border-bottom:none;
        }

        .label-badge{
          padding:4px 10px;
          border-radius:999px;
          color:white;
          font-size:12px;
          font-weight:800;
          display:inline-flex;
          align-items:center;
          line-height:18px;
          box-shadow:0 1px 6px rgba(15,23,42,.10);
          margin-right:6px;
        }

        .conflict-bar{
          background:#f8fafc;
          padding:12px 14px;
          display:flex;
          justify-content:space-between;
          align-items:flex-start;
          gap:14px;
          border-top:1px solid var(--border);
        }
        .conflict-left{
          display:flex;
          flex-wrap:wrap;
          gap:18px;
          align-items:center;
          font-size:14px;
          color:var(--muted);
        }
        .conflict-left b{ color:var(--text); }
        .conflict-right{
          display:flex;
          flex-direction:column;
          gap:8px;
          align-items:flex-end;
        }

        .similarity{
          padding:10px 14px;
          font-size:13px;
          color:var(--muted);
          background:#ffffff;
          border-top:1px solid var(--border);
        }
        .similarity b{ color:var(--text); }

        /* Progress panel */
        #progress-panel{
          margin-top:12px;
          padding:12px 14px;
        }
        #progress-title{
          font-weight:900;
          margin-bottom:8px;
        }
        .progress-outer{
          width:100%;
          height:12px;
          background:#eaf2ff;
          border-radius:999px;
          overflow:hidden;
          border:1px solid var(--border);
        }
        .progress-inner{
          height:100%;
          width:0%;
          background:linear-gradient(90deg,var(--brand), #22c55e);
          transition:width .2s ease;
        }
        #progress-text{ margin-top:8px; color:var(--muted); font-size:13px; }
        #progress-log{
          margin-top:10px;
          font-family: ui-monospace, Menlo, Consolas, monospace;
          font-size:12px;
          max-height:180px;
          overflow:auto;
          background:#0b1220;
          color:#dbeafe;
          border:1px solid rgba(226,232,240,.25);
          border-radius:14px;
          padding:10px;
          white-space:pre-wrap;
        }

        /* Sticky Toolbar */
        .bulk-toolbar{
          position:fixed;
          bottom:18px;
          right:18px;
          display:flex;
          gap:10px;
          padding:10px;
          background:rgba(255,255,255,.75);
          backdrop-filter: blur(10px);
          border:1px solid var(--border);
          border-radius:16px;
          box-shadow:var(--shadow);
        }

        /* Small helpers */
        input[type="radio"], input[type="checkbox"]{ transform: translateY(1px); }
        small{ color:var(--muted); }

        /* Ghost / subtle danger button */
        .btn-ghost{
          background:transparent;
          border-color:var(--border);
          color:var(--text);
        }
        .btn-ghost:hover{ background:#f8fafc; }
        .btn-ghost.danger{
          border-color:rgba(239,68,68,.35);
          color:#b91c1c;
          background:rgba(239,68,68,.06);
        }
        .btn-ghost.danger:hover{ background:rgba(239,68,68,.10); }

        /* Modal */
        .modal-backdrop{
          position:fixed;
          inset:0;
          background:rgba(15,23,42,.55);
          display:flex;
          align-items:center;
          justify-content:center;
          padding:18px;
          z-index:9999;
        }
        .modal{
          width:min(720px, 100%);
          background:white;
          border-radius:18px;
          box-shadow:0 20px 60px rgba(15,23,42,.35);
          border:1px solid rgba(255,255,255,.2);
          overflow:hidden;
          transform:translateY(6px);
          animation:modalIn .14s ease-out forwards;
        }
        @keyframes modalIn{
          to{ transform:translateY(0); opacity:1; }
        }
        .modal-header{
          display:flex;
          align-items:center;
          justify-content:space-between;
          padding:14px 16px;
          background:linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
          border-bottom:1px solid var(--border);
        }
        .modal-title{
          font-size:15px;
          font-weight:900;
        }
        .modal-close{
          appearance:none;
          border:1px solid var(--border);
          background:white;
          width:34px;
          height:34px;
          border-radius:10px;
          cursor:pointer;
          font-size:18px;
          line-height:1;
          display:flex;
          align-items:center;
          justify-content:center;
          color:var(--muted);
        }
        .modal-close:hover{ background:#f8fafc; color:var(--text); }
        .modal-body{
          padding:16px;
          color:var(--text);
        }
        .modal-footer{
          display:flex;
          justify-content:flex-end;
          gap:10px;
          padding:14px 16px;
          border-top:1px solid var(--border);
          background:#ffffff;
        }
        .kv{
          display:grid;
          grid-template-columns: 150px 1fr;
          gap:8px 12px;
          margin:10px 0 0;
          padding:12px;
          border:1px solid var(--border);
          border-radius:14px;
          background:#fbfdff;
        }
        .kv .k{ color:var(--muted); font-weight:800; }
        .kv .v{ font-weight:700; }
        .pill{
          display:inline-flex;
          align-items:center;
          gap:8px;
          padding:6px 10px;
          border-radius:999px;
          background:rgba(2,132,199,.10);
          color:#075985;
          font-weight:900;
          border:1px solid rgba(2,132,199,.18);
        }
        .toast{
          position:fixed;
          right:16px;
          bottom:16px;
          max-width:min(420px, calc(100% - 32px));
          background:#0f172a;
          color:white;
          padding:12px 14px;
          border-radius:14px;
          box-shadow:0 16px 40px rgba(15,23,42,.35);
          z-index:10000;
          font-weight:800;
          opacity:0;
          transform:translateY(8px);
          transition:opacity .16s ease, transform .16s ease;
        }
        .toast.show{ opacity:1; transform:translateY(0); }
        .toast.error{ background:#7f1d1d; }
        .toast.success{ background:#064e3b; }

      </style>
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <div class="top-actions">
          <button id="scanBtn" class="btn btn-primary" onclick="loadData()">🔎 Scan starten</button>
          <div id="stats">Noch keine Daten.</div>
        </div>
        <div id="progress-panel">
          <div class="progress-outer"><div id="progress-bar" class="progress-inner"></div></div>
          <div id="progress-text"></div>
          <div id="progress-log"></div>
        </div>


        <!-- Zusammenfassung ausgewählter Paare -->
        <div id="bulk-summary">
          <b>Ausgewählte Paare:</b>
          <ul id="bulk-list" style="margin:8px 0; padding-left:18px;"></ul>
          <small>Insgesamt: <span id="bulk-count">0</span> Paare</small>
        </div>

        <div id="results"></div>
      </div>

      <!-- Sticky Toolbar -->
      <div class="bulk-toolbar">
        <button class="btn btn-primary" onclick="bulkMerge()">🚀 Bulk Merge</button>
        <button class="btn btn-outline" onclick="clearSelection()">❌ Auswahl löschen</button>
      </div>


      <!-- Modal / Toast -->
      <div id="modal-backdrop" class="modal-backdrop" style="display:none;">
        <div class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
          <div class="modal-header">
            <div class="modal-title" id="modal-title"></div>
            <button class="modal-close" id="modal-close" aria-label="Schließen">×</button>
          </div>
          <div class="modal-body" id="modal-body"></div>
          <div class="modal-footer" id="modal-footer"></div>
        </div>
      </div>
      <div id="toast" class="toast" style="display:none;"></div>

      <script>

      // ---- UI helpers (Modal/Toast) ----
      const modalEl = () => document.getElementById("modal-backdrop");
      let _modalResolve = null;

      function showToast(text, kind=""){
        const el = document.getElementById("toast");
        if(!el) return;
        el.className = "toast" + (kind ? (" " + kind) : "");
        el.textContent = text;
        el.style.display = "block";
        // trigger transition
        requestAnimationFrame(()=> el.classList.add("show"));
        clearTimeout(el._t);
        el._t = setTimeout(()=>{
          el.classList.remove("show");
          setTimeout(()=>{ el.style.display="none"; }, 180);
        }, 2600);
      }

      function openModal({title="Hinweis", bodyHtml="", actions=[]}){
        const backdrop = modalEl();
        const titleEl = document.getElementById("modal-title");
        const bodyEl = document.getElementById("modal-body");
        const footerEl = document.getElementById("modal-footer");
        const closeBtn = document.getElementById("modal-close");

        titleEl.textContent = title;
        bodyEl.innerHTML = bodyHtml;
        footerEl.innerHTML = "";

        if(!actions.length){
          actions = [{id:"ok", text:"OK", cls:"btn btn-primary"}];
        }

        actions.forEach(a=>{
          const b = document.createElement("button");
          b.className = a.cls || "btn btn-outline";
          b.textContent = a.text || a.id;
          b.onclick = () => closeModal(a.id);
          footerEl.appendChild(b);
        });

        function onBackdrop(e){
          if(e.target === backdrop) closeModal("cancel");
        }
        backdrop.onclick = onBackdrop;
        closeBtn.onclick = () => closeModal("cancel");

        backdrop.style.display = "flex";
        document.body.style.overflow = "hidden";

        return new Promise(resolve=>{
          _modalResolve = resolve;
        });
      }

      function closeModal(result){
        const backdrop = modalEl();
        if(backdrop) backdrop.style.display = "none";
        document.body.style.overflow = "";
        const r = _modalResolve;
        _modalResolve = null;
        if(r) r(result);
      }

      function safe(v, fallback="–"){
        return (v === undefined || v === null || v === "" || v === "undefined") ? fallback : v;
      }

      function removePairCard(a, b){
        const id1 = `pair_${a}_${b}`;
        const id2 = `pair_${b}_${a}`;
        const el = document.getElementById(id1) || document.getElementById(id2);
        if(el) el.remove();

        // Update duplicates count from DOM
        const dup = document.querySelectorAll(".pair.card").length;
        const dupEl = document.getElementById("dupCount");
        if(dupEl) dupEl.textContent = String(dup);

        updateBulkSummary();
      }

      window.onerror = function(message, source, lineno, colno, error) {
        console.error("JS-Fehler:", message, source, lineno, colno, error);
        showToast("JS-Fehler: " + message + " @ " + lineno, "error");
      };

      async function loadData(){
        const btn = document.getElementById("scanBtn");
        if(btn) btn.disabled = true;

        // Reset UI
        document.getElementById("results").innerHTML = "";
        document.getElementById("stats").innerHTML = "";
        const panel = document.getElementById("progress-panel");
        const logEl = document.getElementById("progress-log");
        const textEl = document.getElementById("progress-text");
        const barEl = document.getElementById("progress-bar");
        if(panel) panel.style.display = "block";
        if(logEl) logEl.textContent = "";
        if(textEl) textEl.textContent = "Starte Scan…";
        if(barEl) {
          barEl.classList.add("indeterminate");
          barEl.style.width = "0%";
        }

        function logLine(line){
          if(!logEl) return;
          const ts = new Date().toLocaleTimeString();
          logEl.textContent += `[${ts}] ${line}\n`;
          logEl.scrollTop = logEl.scrollHeight;
        }

        function setProgress(mode, percent, message){
          if(textEl && message) textEl.textContent = message;
          if(!barEl) return;
          if(mode === "indeterminate"){
            barEl.classList.add("indeterminate");
            barEl.style.width = "0%";
          } else {
            barEl.classList.remove("indeterminate");
            const p = Math.max(0, Math.min(100, percent||0));
            barEl.style.width = p + "%";
          }
        }

        // Start SSE stream
        let es = null;
        try {
          es = new EventSource(`/scan_orgs_stream?threshold=85`);
        } catch (e) {
          logLine("SSE konnte nicht gestartet werden – Fallback auf normalen Scan.");
          try {
            const res = await fetch('/scan_orgs?threshold=85');
            const data = await res.json();
            setProgress("determinate", 100, "Fertig.");
            renderScanResult(data);
          } catch (err) {
            document.getElementById("results").innerHTML = "❌ Fehler: " + err;
          } finally {
            if(btn) btn.disabled = false;
          }
          return;
        }

        es.onmessage = (ev) => {
          if(!ev.data) return;
          let msg = {};
          try { msg = JSON.parse(ev.data); } catch (e) { return; }
          if(!msg || !msg.type) return;

          if(msg.type === "status"){
            const mode = msg.mode || "indeterminate";
            const percent = msg.percent || 0;
            const message = msg.message || "";
            setProgress(mode, percent, message);
            if(message) logLine(message);
          } else if(msg.type === "done"){
            setProgress("determinate", 100, "Fertig.");
            logLine("Scan abgeschlossen.");
            es.close();
            renderScanResult(msg.payload);
            if(btn) btn.disabled = false;
          } else if(msg.type === "error"){
            setProgress("determinate", 100, "Fehler.");
            logLine("Fehler: " + (msg.message || "Unbekannt"));
            es.close();
            document.getElementById("results").innerHTML = "❌ Fehler: " + (msg.message || "Unbekannt");
            if(btn) btn.disabled = false;
          }
        };

        es.onerror = () => {
          // Most browsers call this for transient disconnects. We keep it user-visible.
          logLine("⚠️ Verbindung unterbrochen (SSE).");
        };
      }

      function renderScanResult(data){
document.getElementById("stats").innerHTML =
          `Geladene Organisationen: <b><span id="totalCount">${data.total}</span></b> | Duplikate: <b><span id="dupCount">${data.duplicates}</span></b>`;
        if(!data.ok){ document.getElementById("results").innerHTML = "❌ Fehler: " + (data.error||"Unbekannt"); return; }
        if(data.pairs.length===0){ document.getElementById("results").innerHTML = "✅ Keine Duplikate gefunden"; return; }

        document.getElementById("results").innerHTML = data.pairs.map(p => {
          function renderLabels(labels){
            if(!labels || !labels.length) return "–";
            return labels.map(l => {
              const name = l.name || (l.id ? ("Label " + l.id) : "Label");
              const color = l.color || "#ccc";
              return `<span class="label-badge" style="background:${color}">${name}</span>`;
            }).join(" ");
          }

          const safe = (v, fallback="–") => (v === undefined || v === null || v === "" ? fallback : v);
          const fmtScore = (v) => {
            const n = Number(v);
            return Number.isFinite(n) ? n.toFixed(2) : "–";
          };

          return `
          <div class="pair card" id="pair_${p.org1.id}_${p.org2.id}" data-pair="${p.org1.id}_${p.org2.id}">
            <table class="pair-table">
              <tr><td>${p.org1.name}</td><td>${p.org2.name}</td></tr>
              <tr><td>ID: ${p.org1.id}</td><td>ID: ${p.org2.id}</td></tr>
              <tr><td>Besitzer: ${p.org1.owner}</td><td>Besitzer: ${p.org2.owner}</td></tr>
              <tr>
                <td>Labels: ${renderLabels(p.org1.labels)}</td>
                <td>Labels: ${renderLabels(p.org2.labels)}</td>
              </tr>
              <tr><td>Website: ${safe(p.org1.website)}</td><td>Website: ${safe(p.org2.website)}</td></tr>
              <tr><td>Adresse: ${safe(p.org1.address)}</td><td>Adresse: ${safe(p.org2.address)}</td></tr>
              <tr><td>Deals: ${safe(p.org1.deals_count)}</td><td>Deals: ${safe(p.org2.deals_count)}</td></tr>
              <tr><td>Kontakte: ${safe(p.org1.contacts_count)}</td><td>Kontakte: ${safe(p.org2.contacts_count)}</td></tr>
            </table>
            <div class="conflict-bar">
              <div class="conflict-left">
                Primär Datensatz:
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org1.id}" checked> ${p.org1.name}</label>
                <label><input type="radio" name="keep_${p.org1.id}_${p.org2.id}" value="${p.org2.id}"> ${p.org2.name}</label>
              </div>
              <div class="conflict-right">
                <div>
                  <button class="btn btn-primary btn-small" onclick="doPreviewMerge(${p.org1.id},${p.org2.id},'${p.org1.id}_${p.org2.id}')">➕ Zusammenführen</button>
                  <button class="btn btn-ghost btn-small danger" onclick="ignorePair(${p.org1.id},${p.org2.id})">🚫 Ignorieren</button>
                </div>
                <label><input type="checkbox" class="bulkCheck" value="${p.org1.id}_${p.org2.id}"> Für Bulk auswählen</label>
              </div>
            </div>
            <div class="similarity">Ähnlichkeit: <b>${fmtScore(p.score)}%</b></div>
          </div>
        `;
        }).join("");

        updateBulkSummary();
      }

      
async function doPreviewMerge(org1,org2,group){
  const keep_id = document.querySelector(`input[name='keep_${group}']:checked`).value;
  let res = await fetch(`/preview_merge?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`,{method:"POST"});
  let data = await res.json();

  if(!data.ok){
    await openModal({
      title:"Vorschau fehlgeschlagen",
      bodyHtml:`<div class="pill">⚠️ Fehler</div><div style="margin-top:10px;color:var(--muted);font-weight:700">${safe(data.error,"Unbekannter Fehler")}</div>`,
      actions:[{id:"ok", text:"OK", cls:"btn btn-outline"}]
    });
    return;
  }

  const org = data.preview || {};
  const labelText = (org.labels && org.labels.length) ? org.labels.map(l => l.name).join(", ") : "–";
  const keepName = org && org.id ? `${org.name || "–"} (ID ${org.id})` : "–";

  const body = `
    <div class="pill">🔎 Vorschau (nach Anreicherung)</div>
    <div style="margin-top:10px; font-weight:800;">Diesen Datensatz als <b>Primär</b> behalten und zusammenführen?</div>

    <div class="kv">
      <div class="k">Primär</div><div class="v">${safe(keepName)}</div>
      <div class="k">Labels</div><div class="v">${safe(labelText)}</div>
      <div class="k">Adresse</div><div class="v">${safe(org.address)}</div>
      <div class="k">Website</div><div class="v">${safe(org.website)}</div>
      <div class="k">Deals</div><div class="v">${safe(org.open_deals_count)}</div>
      <div class="k">Kontakte</div><div class="v">${safe(org.people_count)}</div>
    </div>

    <div style="margin-top:10px;color:var(--muted);font-weight:700;">
      Hinweis: Der andere Datensatz wird in den Primär-Datensatz gemerged.
    </div>
  `;

  const choice = await openModal({
    title:"Zusammenführen bestätigen",
    bodyHtml: body,
    actions:[
      {id:"cancel", text:"Abbrechen", cls:"btn btn-outline"},
      {id:"merge", text:"Zusammenführen", cls:"btn btn-primary"}
    ]
  });

  if(choice === "merge"){
    await doMerge(org1, org2, keep_id);
  }
}

async function doMerge(org1,org2,keep_id){
  let res;
  try{
    res = await fetch(`/merge_orgs?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`,{method:"POST"});
  }catch(e){
    await openModal({
      title:"Netzwerkfehler",
      bodyHtml:`<div class="pill">⚠️ Fehler</div><div style="margin-top:10px;color:var(--muted);font-weight:700">${safe(String(e))}</div>`,
      actions:[{id:"ok", text:"OK", cls:"btn btn-outline"}]
    });
    return;
  }

  let data = null;
  try{
    data = await res.json();
  }catch(e){
    let t = "";
    try { t = await res.text(); } catch(_) {}
    data = { ok:false, error: t || String(e) };
  }

  if(data.ok){
    showToast("Zusammengeführt", "success");
    await openModal({
      title:"Zusammenführen",
      bodyHtml:`<div class="pill">✅ Erfolgreich</div>
                <div style="margin-top:10px;font-weight:800">Die Datensätze wurden zusammengeführt.</div>`,
      actions:[{id:"ok", text:"OK", cls:"btn btn-primary"}]
    });
    removePairCard(org1, org2);
  } else {
    await openModal({
      title:"Merge fehlgeschlagen",
      bodyHtml:`<div class="pill">⚠️ Fehler</div><div style="margin-top:10px;color:var(--muted);font-weight:700">${safe(data.error,"Unbekannt")}</div>`,
      actions:[{id:"ok", text:"OK", cls:"btn btn-outline"}]
    });
  }
}

      async function bulkMerge(){
  const selected = document.querySelectorAll(".bulkCheck:checked");
  if(selected.length === 0){
    showToast("Keine Paare ausgewählt", "error");
    return;
  }

  const choice = await openModal({
    title:"Bulk Merge",
    bodyHtml:`<div class="pill">🚀 Bulk Merge</div>
              <div style="margin-top:10px;font-weight:800">${selected.length} Paare zusammenführen?</div>
              <div style="margin-top:8px;color:var(--muted);font-weight:700">Es wird jeweils der ausgewählte Primär-Datensatz behalten.</div>`,
    actions:[
      {id:"cancel", text:"Abbrechen", cls:"btn btn-outline"},
      {id:"merge", text:"Zusammenführen", cls:"btn btn-primary"}
    ]
  });
  if(choice !== "merge") return;

  const pairs = [];
  selected.forEach(cb=>{
    const [id1,id2] = cb.value.split("_");
    const keep_id = document.querySelector(`input[name='keep_${id1}_${id2}']:checked`).value;
    pairs.push({ org1_id: parseInt(id1), org2_id: parseInt(id2), keep_id: parseInt(keep_id) });
  });

  let res;
  try{
    res = await fetch("/bulk_merge",{
      method:"POST",
      headers:{ "Content-Type":"application/json" },
      body: JSON.stringify(pairs)
    });
  }catch(e){
    await openModal({title:"Netzwerkfehler", bodyHtml:`<div class="pill">⚠️ Fehler</div><div style="margin-top:10px;color:var(--muted);font-weight:700">${safe(String(e))}</div>`});
    return;
  }

  let data = null;
  try{ data = await res.json(); }
  catch(e){
    let t=""; try{ t = await res.text(); } catch(_){}
    data = { ok:false, error: t || String(e) };
  }

  if(data.ok){
    const results = data.results || [];
    const okCount = results.filter(r => r.ok).length;
    const errCount = results.length - okCount;

    // remove merged pairs from UI
    results.filter(r => r.ok && r.pair).forEach(r=>{
      removePairCard(r.pair.primary_id, r.pair.secondary_id);
    });

    const lines = results.slice(0, 40).map(r=>{
      if(r.ok) return `✅ ${r.pair.primary_id} ⇐ ${r.pair.secondary_id}`;
      const p = r.pair ? `${r.pair.primary_id} ⇐ ${r.pair.secondary_id}` : "";
      return `❌ ${p} ${safe(r.error,"Fehler")}`;
    }).join("<br>");

    showToast(`Bulk Merge: ${okCount} ok, ${errCount} Fehler`, errCount ? "error" : "success");

    await openModal({
      title:"Bulk Merge abgeschlossen",
      bodyHtml:`<div class="pill">✅ Fertig</div>
                <div style="margin-top:10px;font-weight:800">${okCount} erfolgreich, ${errCount} fehlgeschlagen</div>
                <div style="margin-top:10px;color:var(--muted);font-weight:700;max-height:280px;overflow:auto;border:1px solid var(--border);padding:10px;border-radius:12px;background:#fbfdff;">
                  ${lines || "–"}
                </div>`,
      actions:[{id:"ok", text:"OK", cls:"btn btn-primary"}]
    });
  } else {
    await openModal({
      title:"Bulk Merge fehlgeschlagen",
      bodyHtml:`<div class="pill">⚠️ Fehler</div><div style="margin-top:10px;color:var(--muted);font-weight:700">${safe(data.error,"Unbekannt")}</div>`,
      actions:[{id:"ok", text:"OK", cls:"btn btn-outline"}]
    });
  }
}

      async function ignorePair(org1,org2){
  const choice = await openModal({
    title:"Paar ignorieren",
    bodyHtml:`<div class="pill">🚫 Ignorieren</div>
              <div style="margin-top:10px;font-weight:800">Soll dieses Paar dauerhaft ignoriert werden?</div>
              <div style="margin-top:8px;color:var(--muted);font-weight:700">Es wird künftig nicht mehr als Duplikat vorgeschlagen.</div>`,
    actions:[
      {id:"cancel", text:"Abbrechen", cls:"btn btn-outline"},
      {id:"ignore", text:"Ignorieren", cls:"btn btn-ghost danger"}
    ]
  });
  if(choice !== "ignore") return;

  try{
    await fetch(`/ignore_pair?org1_id=${org1}&org2_id=${org2}`,{method:"POST"});
    showToast("Paar ignoriert", "success");
    removePairCard(org1, org2);
  }catch(e){
    await openModal({title:"Fehler", bodyHtml:`<div class="pill">⚠️ Fehler</div><div style="margin-top:10px;color:var(--muted);font-weight:700">${safe(String(e))}</div>`});
  }
}
function updateBulkSummary(){
        const selected=document.querySelectorAll(".bulkCheck:checked");
        const summary=document.getElementById("bulk-summary");
        const list=document.getElementById("bulk-list");
        const count=document.getElementById("bulk-count");

        if(selected.length===0){
          summary.style.display="none";
          list.innerHTML="";
          count.textContent="0";
          return;
        }

        summary.style.display="block";
        list.innerHTML="";
        selected.forEach(cb=>{
          let [id1,id2]=cb.value.split("_");
          let li=document.createElement("li");
          li.textContent=`Paar: ${id1} ↔ ${id2}`;
          list.appendChild(li);
        });
        count.textContent=selected.length;
      }

      function clearSelection(){
        document.querySelectorAll(".bulkCheck:checked").forEach(cb => cb.checked=false);
        updateBulkSummary();
      }

      document.addEventListener("change", e=>{
        if(e.target.classList.contains("bulkCheck")){
          updateBulkSummary();
        }
      });
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









