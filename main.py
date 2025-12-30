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
                        "address": address_value or "-",
                        "open_deals": org.get("open_deals_count", 0),
                        "people_count": org.get("people_count", 0),
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
                            "score": score,
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
        body { font-family:'Source Sans Pro',Arial,sans-serif; background:#f4f6f8; margin:0; color:#333; }
        header { display:flex; justify-content:center; align-items:center; background:#ffffff; padding:15px; border-bottom:1px solid #ddd; }
        header img { height:70px; }
        .container { max-width:1400px; margin:20px auto; padding:10px; }
        .pair { background:white; border:1px solid #ddd; border-radius:10px; margin-bottom:25px; box-shadow:0 2px 4px rgba(0,0,0,0.05); overflow:hidden; }
        .pair-table { width:100%; border-collapse:collapse; }
        .pair-table td { padding:8px 12px; border:1px solid #eee; vertical-align:top; width:50%; }
        .pair-table tr:first-child td { font-weight:bold; background:#f0f6fb; font-size:15px; }
        .label-badge { padding:4px 10px; border-radius:12px; color:#fff; font-size:12px; font-weight:600; display:inline-block; min-width:60px; text-align:center; }
        .conflict-bar { background:#e6f3fb; padding:12px 16px; display:flex; justify-content:space-between; align-items:center; border-top:1px solid #d5e5f0; }
        .conflict-left { display:flex; gap:20px; align-items:center; font-size:14px; }
        .conflict-right { display:flex; flex-direction:column; gap:6px; align-items:flex-end; }
        .btn-action { background:#009fe3; color:white; border:none; padding:8px 18px; border-radius:6px; cursor:pointer; font-size:14px; transition:all .2s; }
        .btn-action:hover { background:#007bb8; }
        .similarity { padding:10px 16px; font-size:13px; color:#555; background:#f9f9f9; border-top:1px solid #eee; }

        /* Neue Styles für Bulk */
        .bulk-toolbar {
          position: fixed;
          bottom: 20px;
          right: 20px;
          background: white;
          border: 1px solid #ccc;
          border-radius: 10px;
          padding: 10px 15px;
          box-shadow: 0 2px 8px rgba(0,0,0,0.25);
          display: flex;
          flex-direction: column;
          gap: 6px;
          z-index: 1000;
        }
        #bulk-summary {
  display: none;
  position: sticky;
  top: 0;
  z-index: 500;

  background: linear-gradient(180deg, #f8fbfe 0%, #eef5fb 100%);
  border: 1px solid #b7d4ec;
  border-radius: 10px;
  padding: 14px 18px;
  margin-bottom: 20px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08);

  font-size: 15px;
  color: #1a3c5a;
  transition: all 0.3s ease;
}

#bulk-summary b {
  color: #007bb8;
}

#bulk-summary ul {
  margin: 8px 0;
  padding-left: 22px;
  list-style-type: "• ";
}

#bulk-summary li {
  margin: 2px 0;
}

#bulk-summary small {
  font-size: 13px;
  color: #555;
}

      

/* Progress UI */
#progress-panel{
  display:none;
  background:#fff;
  border:1px solid #ddd;
  border-radius:10px;
  padding:14px 18px;
  margin:15px 0 20px;
  box-shadow:0 2px 4px rgba(0,0,0,0.05);
}
.progress-outer{ width:100%; height:14px; background:#eee; border-radius:999px; overflow:hidden; }
.progress-inner{ height:100%; width:0%; background:#2d8cff; transition: width .2s ease; }
.progress-inner.indeterminate{ width:40%; animation: indet 1.2s infinite; }
@keyframes indet { 0%{ transform:translateX(-120%);} 100%{ transform:translateX(280%);} }
#progress-text{ margin-top:10px; font-size:14px; }
#progress-log{
  margin-top:8px;
  font-family: ui-monospace, Menlo, Consolas, monospace;
  font-size:12px;
  max-height:160px;
  overflow:auto;
  background:#f7f7f7;
  border:1px solid #eee;
  border-radius:8px;
  padding:10px;
  white-space:pre-wrap;
}

</style>
    </head>
    <body>
      <header><img src="/static/bizforward-Logo-Clean-2024.svg" alt="Logo"></header>
      <div class="container">
        <button id="scanBtn" class="btn-action" onclick="loadData()">🔎 Scan starten</button>
        <div id="stats" style="margin:15px 0; font-size:15px;"></div>
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
        <button class="btn-action" onclick="bulkMerge()">🚀 Bulk Merge</button>
        <button class="btn-action" onclick="clearSelection()">❌ Auswahl löschen</button>
      </div>

      <script>
      window.onerror = function(message, source, lineno, colno, error) {
        alert("❌ JS-Fehler: " + message + " @ " + lineno);
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
          "Geladene Organisationen: <b>" + data.total + "</b> | Duplikate: <b>" + data.duplicates + "</b>";
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

          return `
          <div class="pair">
            <table class="pair-table">
              <tr><td>${p.org1.name}</td><td>${p.org2.name}</td></tr>
              <tr><td>ID: ${p.org1.id}</td><td>ID: ${p.org2.id}</td></tr>
              <tr><td>Besitzer: ${p.org1.owner}</td><td>Besitzer: ${p.org2.owner}</td></tr>
              <tr>
                <td>Labels: ${renderLabels(p.org1.labels)}</td>
                <td>Labels: ${renderLabels(p.org2.labels)}</td>
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

        updateBulkSummary();
      }

      async function doPreviewMerge(org1,org2,group){
        let keep_id=document.querySelector(`input[name='keep_${group}']:checked`).value;
        let res=await fetch(`/preview_merge?org1_id=${org1}&org2_id=${org2}&keep_id=${keep_id}`,{method:"POST"});
        let data=await res.json();
        if(data.ok){
          let org=data.preview;
          let labelText = (org.labels && org.labels.length) ? org.labels.map(l => l.name).join(", ") : "-";
          let msg = `⚠️ Vorschau Primär-Datensatz (nach Anreicherung):
ID: ${org.id||"-"}
Name: ${org.name||"-"}
Labels: ${labelText}
Adresse: ${org.address||"-"}
Website: ${org.website||"-"}
Deals: ${org.open_deals_count||"-"}
Kontakte: ${org.people_count||"-"}

Diesen Datensatz behalten?`;
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
        const selected=document.querySelectorAll(".bulkCheck:checked");
        if(selected.length===0){alert("⚠️ Keine Paare ausgewählt");return;}
        if(!confirm(selected.length+" Paare wirklich zusammenführen?")) return;

        const pairs=[];
        selected.forEach(cb=>{
          const [id1,id2]=cb.value.split("_");
          const keep_id=document.querySelector(`input[name='keep_${id1}_${id2}']:checked`).value;
          pairs.push({ org1_id: parseInt(id1), org2_id: parseInt(id2), keep_id: parseInt(keep_id) });
        });

        const res=await fetch("/bulk_merge",{
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify(pairs)
        });
        const data=await res.json();
        if(data.ok){
          alert("✅ Bulk-Merge fertig.\\nErgebnisse: "+JSON.stringify(data.results,null,2));
          loadData();
        } else {
          alert("❌ Fehler: "+data.error);
        }
      }

      async function ignorePair(org1,org2){
        if(!confirm("Paar ignorieren?")) return;
        await fetch(`/ignore_pair?org1_id=${org1}&org2_id=${org2}`,{method:"POST"});
        alert("✅ Paar ignoriert");
        loadData();
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









