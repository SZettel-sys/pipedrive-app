 # =============================================================
# MODUL 1 – BOOTSTRAP, API, HELPERS (FINAL 2025.12)
# =============================================================
import os, asyncio, httpx
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

# -------------------------------------------------------------
# PIPEDRIVE – BASIS
# -------------------------------------------------------------
PIPEDRIVE_BASE = "https://api.pipedrive.com/v1"
PIPEDRIVE_TOKEN = os.getenv("PD_API_TOKEN", "").strip()

def append_token(url: str) -> str:
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}api_token={PIPEDRIVE_TOKEN}"

def get_headers() -> dict:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

# -------------------------------------------------------------
# SAFE REQUEST (mit external client support)
# -------------------------------------------------------------
async def safe_request(
    method: str,
    url: str,
    headers=None,
    client: Optional[httpx.AsyncClient] = None,
    max_retries: int = 10,
    initial_delay: float = 1.0
):
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            if client:
                r = await client.request(method, url, headers=headers)
            else:
                async with httpx.AsyncClient(timeout=60.0) as c:
                    r = await c.request(method, url, headers=headers)

            # Erfolg
            if r.status_code == 200:
                return r

            # Rate Limit
            if r.status_code == 429:
                print(f"[429] Warte {delay:.1f}s … {url}")
                await asyncio.sleep(delay)
                delay *= 1.6
                continue

            # Andere Fehler
            print(f"[WARN] API Fehler {r.status_code}: {url}")
            print(r.text)
            return r

        except Exception as e:
            print(f"[ERR] safe_request Exception: {e}")

        await asyncio.sleep(delay)
        delay *= 1.5

    raise Exception(f"API dauerhaft fehlgeschlagen: {url}")

# -------------------------------------------------------------
# EXCEL EXPORT (eine Datei, zwei Tabellenblätter)
# -------------------------------------------------------------
async def export_to_excel(master_df: pd.DataFrame, excluded_df: pd.DataFrame, job_id: str):
    """
    Erzeugt EINE Excel-Datei mit zwei Tabellenblättern:
    - NF Master
    - Excluded
    """
    filename = f"nf_export_{job_id}.xlsx"
    filepath = f"/tmp/{filename}"

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        master_df.to_excel(writer, sheet_name="NF Master", index=False)
        excluded_df.to_excel(writer, sheet_name="Excluded", index=False)

    return filepath

# -------------------------------------------------------------
# FELD-HILFSFUNKTION
# -------------------------------------------------------------
def cf(p: dict, key: str):
    try:
        return (p.get("custom_fields") or {}).get(key)
    except:
        return None

# -------------------------------------------------------------
# NAME SPLIT
# -------------------------------------------------------------
def split_name(first, last, full_name):
    """saubere Trennung, wenn first/last fehlen."""
    if first or last:
        return first or "", last or ""

    if not full_name:
        return "", ""

    parts = full_name.strip().split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])
# =============================================================
# MODUL 2 – FILTER-FIRST ENGINE (2025.12)
# Schnellster und stabilster NF-Loader
# =============================================================

FILTER_3024_ID = 3024
BATCH_FIELD_KEY = "5ac34dad3ea917fdef4087caebf77ba275f87eec"


# -------------------------------------------------------------
# SCHRITT 1 – IDs aus Pipedrive Filter 3024 holen
# -------------------------------------------------------------
async def nf_get_ids_from_filter(filter_id: int) -> List[int]:
    url = f"{PD_API_BASE}/persons?filter_id={filter_id}&limit=500&start=0"

    all_ids = []

    while True:
        r = await safe_request("GET", url)
        data = r.get("data") or []
        additional = r.get("additional_data") or {}

        for p in data:
            all_ids.append(p["id"])

        # Pagination
        next_start = additional.get("pagination", {}).get("next_start")
        if next_start is None:
            break

        url = f"{PD_API_BASE}/persons?filter_id={filter_id}&limit=500&start={next_start}"

    print(f"[NF] IDs aus Filter {filter_id}: {len(all_ids)}")
    return all_ids


# -------------------------------------------------------------
# SCHRITT 2 – Personen-Light laden (nur IDs aus Filter)
# -------------------------------------------------------------
async def nf_load_persons_light_from_filter(ids: List[int]) -> List[dict]:
    persons = []

    async def worker(pid):
        try:
            url = f"{PD_API_BASE}/persons/{pid}"
            r = await safe_request("GET", url)
            if r and r.get("data"):
                persons.append(r["data"])
        except:
            pass

    tasks = [worker(pid) for pid in ids]
    await asyncio.gather(*tasks)

    print(f"[NF] Personen (Light) geladen: {len(persons)}")
    return persons


# -------------------------------------------------------------
# SCHRITT 3 – Batch-ID prüfen & herausfiltern
# -------------------------------------------------------------
def nf_filter_persons_with_batch(persons: List[dict]) -> List[dict]:
    with_batch = []

    for p in persons:
        batch_value = p.get(BATCH_FIELD_KEY)
        if batch_value and str(batch_value).strip() != "":
            with_batch.append(p)

    print(f"[NF] Personen mit Batch-ID: {len(with_batch)}")
    return with_batch


# -------------------------------------------------------------
# EXTERNE FUNKTION (wird von Modul 6 verwendet)
# Kompletter Filter-First Personensatz
# -------------------------------------------------------------
async def nf_filter_first_pipeline():
    # IDs holen
    ids = await nf_get_ids_from_filter(FILTER_3024_ID)

    # Personen Light laden
    persons_light = await nf_load_persons_light_from_filter(ids)

    # Nur Personen mit Batch-ID
    persons_filtered = nf_filter_persons_with_batch(persons_light)

    return persons_filtered

# =============================================================
# MODUL 3 – MASTER BUILDER (Filter-First Engine)
# =============================================================

ORG_FIELD_LEVEL = "0ab03885d6792086a0bb007d6302d14b13b0c7d1"
ORG_FIELD_STOP  = "61d238b86784db69f7300fe8f12f54c601caeff8"

PROSPECT_FIELD_KEY = "f9138f9040c44622808a4b8afda2b1b75ee5acd0"
TITLE_FIELD_KEY     = "0343bc43a91159aaf33a463ca603dc5662422ea5"
POSITION_FIELD_KEY  = "4585e5de11068a3bccf02d8b93c126bcf5c257ff"
XING_FIELD_KEY      = "44ebb6feae2a670059bc5261001443a2878a2b43"
LINKEDIN_FIELD_KEY  = "25563b12f847a280346bba40deaf527af82038cc"
GENDER_FIELD_KEY    = "c4f5f434cdb0cfce3f6d62ec7291188fe968ac72"

NEXT_ACTIVITY_KEY   = "next_activity_date"


# -------------------------------------------------------------
# Organisationen Light laden
# -------------------------------------------------------------
async def nf_load_orgs_light_for_persons(persons: List[dict]) -> Dict[str, dict]:
    org_ids = {str(p.get("org_id")) for p in persons if p.get("org_id")}
    orgs = {}

    async def load_one(oid):
        try:
            url = f"{PD_API_BASE}/organizations/{oid}"
            r = await safe_request("GET", url)
            if r and r.get("data"):
                orgs[str(oid)] = r["data"]
        except:
            pass

    await asyncio.gather(*[load_one(oid) for oid in org_ids])

    print(f"[NF] Organisationen geladen: {len(orgs)}")
    return orgs


# -------------------------------------------------------------
# Kandidatenliste kombinieren
# -------------------------------------------------------------
def nf_merge_persons_orgs(persons: List[dict], orgs: Dict[str, dict]) -> List[dict]:
    merged = []

    for p in persons:
        oid = str(p.get("org_id") or "")
        org = orgs.get(oid, {})
        merged.append({
            "person": p,
            "org": org
        })

    print(f"[NF] Kandidaten kombiniert: {len(merged)}")
    return merged


# -------------------------------------------------------------
# Ausschlüsse anwenden (max 2 Kontakte, Aktivität > 3 Monate)
# -------------------------------------------------------------
def nf_apply_exclusions(merged: List[dict]) -> Tuple[List[dict], List[dict]]:
    selected = []
    excluded = []

    counter_org = defaultdict(int)
    now = datetime.now()

    for item in merged:
        p = item["person"]
        org = item["org"]

        pid = str(p.get("id"))
        oid = str(org.get("id") or "")
        oname = org.get("name") or "-"

        # 1) Next Activity Check
        dt_raw = p.get(NEXT_ACTIVITY_KEY)
        if dt_raw:
            try:
                dt = datetime.fromisoformat(str(dt_raw).split(" ")[0])
                if (now - dt).days <= 90:
                    excluded.append({
                        "Kontakt ID": pid,
                        "Name": p.get("name"),
                        "Organisation": oname,
                        "Grund": "Nächste Aktivität < 3 Monate"
                    })
                    continue
            except:
                pass

        # 2) Max zwei Personen pro Organisation
        if oid:
            counter_org[oid] += 1
            if counter_org[oid] > 2:
                excluded.append({
                    "Kontakt ID": pid,
                    "Name": p.get("name"),
                    "Organisation": oname,
                    "Grund": "Mehr als 2 Kontakte pro Organisation"
                })
                continue

        selected.append(item)

    print(f"[NF] Ausgewählt: {len(selected)}, Excluded: {len(excluded)}")
    return selected, excluded


# -------------------------------------------------------------
# MASTER-TABELLE bauen
# -------------------------------------------------------------
def nf_build_master(selected: List[dict], batch_id: str, campaign: str) -> pd.DataFrame:
    rows = []

    for item in selected:
        p = item["person"]
        org = item["org"]

        pid = str(p.get("id"))
        oname = org.get("name") or "-"
        oid = str(org.get("id") or "")

        first, last = split_name(p.get("first_name"), p.get("last_name"), p.get("name"))

        # Email
        email = ""
        e = p.get("email") or p.get("emails") or []
        if isinstance(e, list) and e:
            if isinstance(e[0], dict):
                email = e[0].get("value") or ""
            elif isinstance(e[0], str):
                email = e[0]

        rows.append({
            "Person - Batch ID": batch_id,
            "Person - Organisation": oname,
            "Organisation - ID": oid,

            "Person - Prospect ID": p.get(PROSPECT_FIELD_KEY) or "",
            "Person - Geschlecht": p.get(GENDER_FIELD_KEY) or "",
            "Person - Titel":      p.get(TITLE_FIELD_KEY) or "",
            "Person - Vorname":    first,
            "Person - Nachname":   last,
            "Person - Position":   p.get(POSITION_FIELD_KEY) or "",

            "Person - XING-Profil":     p.get(XING_FIELD_KEY) or "",
            "Person - LinkedIn Profil": p.get(LINKEDIN_FIELD_KEY) or "",

            "Person - E-Mail": email,
            "Person - ID": pid,
            "Cold-Mailing Import": campaign,
        })

    return pd.DataFrame(rows)

# =============================================================
# MODUL 4 – NF Filter 3024 (Python-Replikation, FINAL 2025.12)
# Filtert Organisations + Personen nach deinen Regeln
# =============================================================

# deine Field Keys (aus deinen Angaben)
PERSON_BATCH_KEY       = "5ac34dad3ea917fdef4087caebf77ba275f87eec"
PERSON_PROSPECT_KEY    = "f9138f9040c44622808a4b8afda2b1b75ee5acd0"
ORG_VERTRIEBSSTOP_KEY  = "61d238b86784db69f7300fe8f12f54c601caeff8"
ORG_LEVEL_KEY          = "0ab03885d6792086a0bb007d6302d14b13b0c7d1"


# -------------------------------------------------------------
# ORGANISATIONEN nach 3024 filtern
# -------------------------------------------------------------
def nf_filter_organization(org: dict) -> bool:
    if not org:
        return False

    # 1) Name darf nicht "Freelancer" sein
    if org.get("name", "").strip().lower() == "freelancer":
        return False

    # 2) Verkaufsstop Labels
    labels = org.get("label_ids") or []
    if any("VERTRIEBSSTOPP DAUERHAFT" in str(l) for l in labels):
        return False
    if any("VERTRIEBSSTOPP VORÜBERGEHEND" in str(l) for l in labels):
        return False

    # 3) Organisations-Level muss leer sein
    if cf(org, ORG_LEVEL_KEY) not in (None, "", 0):
        return False

    # 4) Deals checks
    if org.get("open_deals_count", 0) != 0:
        return False
    if org.get("closed_deals_count", 0) != 0:
        return False
    if org.get("won_deals_count", 0) != 0:
        return False
    if org.get("lost_deals_count", 0) != 0:
        return False

    # 5) Vertriebsstopp-Feld
    vst = cf(org, ORG_VERTRIEBSSTOP_KEY)
    if vst and vst != "keine Freelancer-Anstellung":
        return False

    return True


# -------------------------------------------------------------
# PERSONEN vorfiltern (Batch-ID / Email / Label)
# -------------------------------------------------------------
def nf_precheck_person_personside(p: dict) -> bool:
    """
    Grober Personenfilter, bevor wir Organisationsfilter anwenden:
    - batch-id oder prospect-id muss vorhanden sein
    - email muss vorhanden sein
    - darf nicht gesperrt sein
    """

    if not p:
        return False

    # A) Muss eine Batch-ID haben
    if not cf(p, PERSON_BATCH_KEY):
        return False

    # Email vorhanden?
    emails = p.get("email") or []
    if not emails or not emails[0].get("value"):
        return False

    # "BIZFORWARD SPERRE" Label?
    labels = p.get("label_ids") or []
    if any("BIZFORWARD SPERRE" in str(l) for l in labels):
        return False

    # Muss eine Organisation besitzen
    if not p.get("org_id"):
        return False

    return True


# -------------------------------------------------------------
# MERGE PERSON ↔ ORGANISATION
# -------------------------------------------------------------
def nf_merge_persons_and_orgs(persons_light: List[dict], orgs_light: Dict[str, dict]) -> List[dict]:
    """
    Liefert eine reduzierte Kandidatenliste:
    - Person erfüllt Personenbedingungen
    - Zugehörige Organisation erfüllt Orga-Bedingungen
    """

    # 1. Organisationen filtern
    valid_orgs = {oid for oid,odata in orgs_light.items() if nf_filter_organization(odata)}

    print(f"[NF] Gültige Organisationen nach Filter 3024: {len(valid_orgs)}")

    # 2. Personen filtern + mergen
    candidates = []

    for p in persons_light:
        if not nf_precheck_person_personside(p):
            continue

        oid = str(p.get("org_id"))
        if oid in valid_orgs:
            candidates.append(p)

    print(f"[NF] Personen nach Orga + Person Filter: {len(candidates)}")
    return candidates
# =============================================================
# MODUL 5 – DETAIL-LOADER (Persons & Organizations)
# Lädt NUR Details für die gefilterten NF-Kandidaten
# =============================================================

DETAIL_SEMAPHORE = 40    # hoch, aber sicher
ORG_DETAIL_SEMAPHORE = 30

# -------------------------------------------------------------
# PERSONEN-DETAILS
# -------------------------------------------------------------
async def nf_load_person_details(person_ids: List[str]) -> Dict[str, dict]:
    """
    Lädt vollständige Personendetails NUR für die Kandidaten.
    """
    results = {}

    sem = asyncio.Semaphore(DETAIL_SEMAPHORE)

    async with httpx.AsyncClient(timeout=60.0) as client:

        async def load_one(pid):
            async with sem:
                url = append_token(
                    f"{PIPEDRIVE_BASE}/persons/{pid}?fields=*"
                )

                r = await safe_request("GET", url, headers=get_headers(), client=client)
                if r.status_code == 200:
                    data = r.json().get("data")
                    if data:
                        results[str(pid)] = data

                await asyncio.sleep(0.001)

        await asyncio.gather(*(load_one(pid) for pid in person_ids))

    print(f"[NF] Personendetails geladen: {len(results)}")
    return results


# -------------------------------------------------------------
# ORGANISATIONS-DETAILS
# -------------------------------------------------------------
async def nf_load_org_details(org_ids: List[str]) -> Dict[str, dict]:
    """
    Lädt vollständige Organisationsdetails NUR für Organisationen,
    die in der Kandidatenliste vorkommen.
    """
    results = {}

    sem = asyncio.Semaphore(ORG_DETAIL_SEMAPHORE)

    async with httpx.AsyncClient(timeout=60.0) as client:

        async def load_one(oid):
            async with sem:
                url = append_token(
                    f"{PIPEDRIVE_BASE}/organizations/{oid}?fields=*"
                )

                r = await safe_request("GET", url, headers=get_headers(), client=client)
                if r.status_code == 200:
                    data = r.json().get("data")
                    if data:
                        results[str(oid)] = data

                await asyncio.sleep(0.001)

        await asyncio.gather(*(load_one(oid) for oid in org_ids))

    print(f"[NF] Organisationsdetails geladen: {len(results)}")
    return results


# -------------------------------------------------------------
# GESAMT-FUNKTION: Details laden für NF-Kandidaten
# -------------------------------------------------------------
async def nf_load_details_for_candidates(candidates: List[dict]):
    """
    Kombi-Funktion: Lädt Person- und Orga-Details für alle NF-Kandidaten.
    Liefert (persons_full, orgs_full)
    """

    # 1. Personendetails
    person_ids = [str(p.get("id")) for p in candidates if p.get("id")]
    print(f"[NF] Lade Personendetails für {len(person_ids)} Kandidaten …")

    persons_full = await nf_load_person_details(person_ids)

    # 2. Organisationsdetails
    org_ids = list({str(p.get("org_id")) for p in candidates if p.get("org_id")})
    print(f"[NF] Lade Organisationsdetails für {len(org_ids)} Organisationen …")

    orgs_full = await nf_load_org_details(org_ids)

    return persons_full, orgs_full

# =============================================================
# UI – TEIL 1/3: JOB MANAGER (In-Memory)
# =============================================================

class NFJob:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.phase = "Initialisierung…"
        self.percent = 0
        self.file_path = None
        self.error = None
        self.start_ts = datetime.now()

    def to_dict(self):
        return {
            "job_id": self.job_id,
            "phase": self.phase,
            "percent": self.percent,
            "file_path": self.file_path,
            "error": self.error
        }


NF_JOBS: Dict[str, NFJob] = {}


def create_job() -> NFJob:
    job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    job = NFJob(job_id)
    NF_JOBS[job_id] = job
    return job


def get_job(job_id: str) -> Optional[NFJob]:
    return NF_JOBS.get(job_id)
# =============================================================
# UI – TEIL 2/3: ROUTER + BACKGROUND-JOB
# =============================================================
from fastapi import BackgroundTasks, Request
from fastapi.responses import JSONResponse, HTMLResponse


ui_router = APIRouter()


# -------------------------------------------------------------
# UI: STARTSEITE (HTML)
# -------------------------------------------------------------
@ui_router.get("/", response_class=HTMLResponse)
async def ui_home(request: Request):
    """Liefert die Premium-Nachfass-UI."""
    return await render_premium_ui()


# -------------------------------------------------------------
# UI: Job starten
# -------------------------------------------------------------
@ui_router.post("/ui/nachfass/start")
async def ui_nf_start(request: Request, background: BackgroundTasks):
    data = await request.json()

    batch_ids = data.get("batch_ids", "")
    export_batch = data.get("export_batch", "")
    campaign = data.get("campaign", "")

    job = create_job()
    job.phase = "Starte Nachfass …"
    job.percent = 1

    background.add_task(run_nf_pipeline_background,
                        job.job_id,
                        batch_ids,
                        export_batch,
                        campaign)

    return {"job_id": job.job_id}


# -------------------------------------------------------------
# UI: Job Status Polling
# -------------------------------------------------------------
@ui_router.get("/ui/nachfass/status")
async def ui_nf_status(job_id: str):
    job = get_job(job_id)
    if not job:
        return JSONResponse({"error": "Job nicht gefunden"}, status_code=404)
    return job.to_dict()


# -------------------------------------------------------------
# UI: Datei herunterladen
# -------------------------------------------------------------
@ui_router.get("/ui/nachfass/download")
async def ui_nf_download(job_id: str):
    job = get_job(job_id)
    if not job or not job.file_path:
        raise HTTPException(400, "Datei noch nicht verfügbar")

    filename = os.path.basename(job.file_path)
    return FileResponse(
        job.file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# -------------------------------------------------------------
# Hintergrund-JOB: NF PIPELINE
# -------------------------------------------------------------
async def run_nf_pipeline_background(job_id: str):
    job = get_job(job_id)
    if not job:
        return

    try:
        # PHASE 1 – Personen Light
        job.phase = "Lade Personen (Light)…"
        job.percent = 10
        persons_light = await nf_load_persons_light()

        # PHASE 2 – Organisationen Light
        job.phase = "Lade Organisationen (Light)…"
        job.percent = 25
        orgs_light = await nf_load_orgs_light()

        # PHASE 3 – Filter 3024
        job.phase = "Filtere nach 3024…"
        job.percent = 45
        candidates = nf_merge_persons_and_orgs(persons_light, orgs_light)

        if not candidates:
            job.error = "Keine passenden Kandidaten."
            job.phase = "Fehlgeschlagen"
            job.percent = 100
            return

        # PHASE 4 – Details laden
        job.phase = "Lade Details…"
        job.percent = 65
        persons_full, orgs_full = await nf_load_details_for_candidates(candidates)

        # PHASE 5 – Master bauen
        job.phase = "Baue NF-Master…"
        job.percent = 80
        master_df, excluded_df = build_nf_master(persons_full, orgs_full)

        # PHASE 6 – Export
        job.phase = "Erstelle Excel-Datei…"
        job.percent = 90
        file_path = await export_to_excel(master_df, excluded_df, job.job_id)

        job.file_path = file_path
        job.phase = "Fertig"
        job.percent = 100

    except Exception as e:
        job.error = str(e)
        job.phase = "Fehler"
        job.percent = 100
# =============================================================
# UI – TEIL 3/3: PREMIUM HTML TEMPLATE (FINAL)
# =============================================================

async def render_premium_ui():
    return HTMLResponse(
        content="""
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8" />
<title>Nachfass Export – Premium UI</title>

<style>
    body {
        margin: 0;
        padding: 0;
        font-family: Inter, Arial, sans-serif;
        background: #f1f3f6;
        color: #222;
    }
    .container {
        max-width: 780px;
        margin: 80px auto;
        background: #fff;
        padding: 40px 50px;
        border-radius: 16px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.10);
    }
    h1 {
        margin-top: 0;
        font-size: 28px;
    }
    p {
        font-size: 15px;
        color: #555;
        margin-bottom: 25px;
    }
    .btn {
        background: #0078ff;
        color: #fff;
        padding: 14px 24px;
        border-radius: 10px;
        font-size: 16px;
        cursor: pointer;
        border: none;
        outline: none;
        transition: 0.2s ease;
    }
    .btn:hover {
        background: #005fcc;
    }
    .hidden {
        display: none;
    }
    .status-box {
        background: #f9fafb;
        border-left: 4px solid #0078ff;
        padding: 15px 20px;
        border-radius: 8px;
        margin-top: 25px;
        font-size: 15px;
    }
    .progress {
        margin-top: 20px;
        width: 100%;
        background: #eaecef;
        border-radius: 50px;
        height: 14px;
        overflow: hidden;
    }
    .progress-inner {
        height: 100%;
        width: 0%;
        background: #0078ff;
        transition: width 0.3s ease;
    }
    .download-btn {
        margin-top: 25px;
        padding: 14px 26px;
        background: #28a745;
        border-radius: 10px;
        color: white;
        font-size: 16px;
        text-decoration: none;
        display: inline-block;
    }
    .download-btn:hover {
        background: #1e8f39;
    }
    .error {
        background: #ffe5e5;
        border-left: 4px solid #ff4444;
        padding: 15px;
        margin-top: 25px;
        border-radius: 8px;
        color: #a40000;
    }
</style>

</head>
<body>

<div class="container">
    <h1>Nachfass Export</h1>
    <p>Erstelle jetzt den vollständigen Nachfass-Export inkl. Filter 3024, Batch-ID, Prospect-ID, und aller relevanten Organisationsdaten.</p>

    <button class="btn" id="startBtn" onclick="startJob()">🚀 Nachfass starten</button>

    <div id="statusBox" class="status-box hidden">
        <strong>Status:</strong> <span id="phaseText">–</span>
        <div class="progress">
            <div class="progress-inner" id="progBar"></div>
        </div>
    </div>

    <div id="downloadBox" class="hidden">
        <a id="downloadLink" class="download-btn" href="#">⬇️ Export herunterladen</a>
    </div>

    <div id="errorBox" class="error hidden"></div>
</div>


<script>
let jobId = null;
let pollInterval = null;

function startJob() {
    document.getElementById("startBtn").disabled = true;

    fetch("/ui/nachfass/start", {
        method: "POST"
    })
    .then(r => r.json())
    .then(data => {
        jobId = data.job_id;

        document.getElementById("statusBox").classList.remove("hidden");

        pollInterval = setInterval(checkStatus, 1200);
    })
    .catch(err => {
        showError("Konnte Job nicht starten: " + err);
    });
}

function checkStatus() {
    fetch(`/ui/nachfass/status?job_id=${jobId}`)
    .then(r => r.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
            clearInterval(pollInterval);
            return;
        }

        document.getElementById("phaseText").innerText = data.phase;
        document.getElementById("progBar").style.width = data.percent + "%";

        if (data.percent >= 100) {
            clearInterval(pollInterval);

            if (data.file_path) {
                document.getElementById("downloadBox").classList.remove("hidden");
                document.getElementById("downloadLink").href = `/ui/nachfass/download?job_id=${jobId}`;
            } else {
                showError("Fertig, aber keine Datei gefunden!");
            }
        }
    })
    .catch(err => {
        showError("Status-Abfrage fehlgeschlagen: " + err);
    });
}

function showError(msg) {
    const box = document.getElementById("errorBox");
    box.classList.remove("hidden");
    box.innerText = msg;
}
</script>

</body>
</html>
        """
    )
async def render_premium_ui():
    return HTMLResponse(
        content="""
<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8" />
<title>Nachfass Export – Premium UI</title>

<style>
    body {
        margin: 0;
        padding: 0;
        font-family: Inter, Arial, sans-serif;
        background: #f1f3f6;
        color: #222;
    }
    .container {
        max-width: 780px;
        margin: 80px auto;
        background: #fff;
        padding: 40px 50px;
        border-radius: 16px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.10);
    }
    h1 {
        margin-top: 0;
        font-size: 28px;
    }
    p {
        font-size: 15px;
        color: #555;
        margin-bottom: 25px;
    }
    label {
        display: block;
        margin-top: 20px;
        margin-bottom: 6px;
        font-weight: 600;
    }
    input {
        width: 100%;
        padding: 12px 14px;
        font-size: 15px;
        border-radius: 10px;
        border: 1px solid #ddd;
        margin-bottom: 10px;
    }
    .btn {
        background: #0078ff;
        color: #fff;
        padding: 14px 24px;
        border-radius: 10px;
        font-size: 16px;
        cursor: pointer;
        border: none;
        outline: none;
        transition: 0.2s ease;
        margin-top: 20px;
    }
    .btn:hover {
        background: #005fcc;
    }
    .hidden {
        display: none;
    }
    .status-box {
        background: #f9fafb;
        border-left: 4px solid #0078ff;
        padding: 15px 20px;
        border-radius: 8px;
        margin-top: 25px;
        font-size: 15px;
    }
    .progress {
        margin-top: 20px;
        width: 100%;
        background: #eaecef;
        border-radius: 50px;
        height: 14px;
        overflow: hidden;
    }
    .progress-inner {
        height: 100%;
        width: 0%;
        background: #0078ff;
        transition: width 0.3s ease;
    }
    .download-btn {
        margin-top: 25px;
        padding: 14px 26px;
        background: #28a745;
        border-radius: 10px;
        color: white;
        font-size: 16px;
        text-decoration: none;
        display: inline-block;
    }
    .download-btn:hover {
        background: #1e8f39;
    }
    .error {
        background: #ffe5e5;
        border-left: 4px solid #ff4444;
        padding: 15px;
        margin-top: 25px;
        border-radius: 8px;
        color: #a40000;
    }
</style>

</head>
<body>

<div class="container">
    <h1>Nachfass Export</h1>
    <p>Erstelle jetzt den vollständigen Nachfass-Export inkl. Filter 3024 und Batch-Feldern.</p>

    <!-- Neue Eingabefelder -->
    <label for="batchList">Batch-IDs (Liste):</label>
    <input id="batchList" type="text" placeholder="z.B. B443,B448,B449" />

    <label for="exportBatch">Export-Batch-ID:</label>
    <input id="exportBatch" type="text" placeholder="z.B. B000" />

    <label for="campaign">Kampagne:</label>
    <input id="campaign" type="text" placeholder="z.B. Test-Kampagne Q1/25" />

    <button class="btn" id="startBtn" onclick="startJob()">🚀 Nachfass starten</button>

    <div id="statusBox" class="status-box hidden">
        <strong>Status:</strong> <span id="phaseText">–</span>
        <div class="progress">
            <div class="progress-inner" id="progBar"></div>
        </div>
    </div>

    <div id="downloadBox" class="hidden">
        <a id="downloadLink" class="download-btn" href="#">⬇️ Export herunterladen</a>
    </div>

    <div id="errorBox" class="error hidden"></div>
</div>


<script>
let jobId = null;
let pollInterval = null;

function startJob() {
    document.getElementById("startBtn").disabled = true;

    const payload = {
        batch_ids: document.getElementById("batchList").value,
        export_batch: document.getElementById("exportBatch").value,
        campaign: document.getElementById("campaign").value
    };

    fetch("/ui/nachfass/start", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
    })
    .then(r => r.json())
    .then(data => {
        jobId = data.job_id;

        document.getElementById("statusBox").classList.remove("hidden");

        pollInterval = setInterval(checkStatus, 1200);
    })
    .catch(err => {
        showError("Konnte Job nicht starten: " + err);
    });
}

function checkStatus() {
    fetch(`/ui/nachfass/status?job_id=${jobId}`)
    .then(r => r.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
            clearInterval(pollInterval);
            return;
        }

        document.getElementById("phaseText").innerText = data.phase;
        document.getElementById("progBar").style.width = data.percent + "%";

        if (data.percent >= 100) {
            clearInterval(pollInterval);

            if (data.file_path) {
                document.getElementById("downloadBox").classList.remove("hidden");
                document.getElementById("downloadLink").href = `/ui/nachfass/download?job_id=${jobId}`;
            } else {
                showError("Fertig, aber keine Datei gefunden!");
            }
        }
    })
    .catch(err => {
        showError("Status-Abfrage fehlgeschlagen: " + err);
    });
}

function showError(msg) {
    const box = document.getElementById("errorBox");
    box.classList.remove("hidden");
    box.innerText = msg;
}
</script>

</body>
</html>
        """
    )

# =============================================================
# MODUL 6 – FILTER-FIRST NACHFASS PIPELINE (UI-INTEGRATION)
# =============================================================

import pandas as pd
import numpy as np
from fastapi import HTTPException


# -------------------------------------------------------------
# Excel speichern
# -------------------------------------------------------------
async def nf_export_to_excel(master_df, excluded_df, job_id: str) -> str:
    export_dir = "/tmp"
    filename = f"nachfass_export_{job_id}.xlsx"
    file_path = os.path.join(export_dir, filename)

    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        master_df.to_excel(writer, sheet_name="Master", index=False)
        excluded_df.to_excel(writer, sheet_name="Excluded", index=False)

    print(f"[NF] Export gespeichert: {file_path}")
    return file_path



# -------------------------------------------------------------
# Hauptpipeline (von UI gestartet)
# -------------------------------------------------------------
async def run_nf_pipeline_background(job_id: str, batch_ids: str, export_batch: str, campaign: str):
    job = get_job(job_id)
    if not job:
        return

    try:
        # ================================================
        # STEP 1 – IDs aus Filter laden
        # ================================================
        job.phase = "Lade IDs aus Filter 3024 …"
        job.percent = 10

        ids = await nf_get_ids_from_filter(FILTER_3024_ID)
        if not ids:
            job.error = "Filter 3024 enthält keine Personen."
            job.phase = "Fehler"
            job.percent = 100
            return

        # ================================================
        # STEP 2 – Personen Light
        # ================================================
        job.phase = "Lade Personen (Light) …"
        job.percent = 25

        persons_light = await nf_load_persons_light_from_filter(ids)
        if not persons_light:
            job.error = "Keine Personen aus Filter gefunden."
            job.phase = "Fehler"
            job.percent = 100
            return

        # ================================================
        # STEP 3 – Batch-ID Prüfung
        # ================================================
        job.phase = "Filtere Personen mit Batch-ID …"
        job.percent = 35

        persons_batch = nf_filter_persons_with_batch(persons_light)

        # Wenn der Benutzer eine Batch-Liste angegeben hat: (z.B. B443,B448)
        if batch_ids.strip():
            allowed_batches = [b.strip() for b in batch_ids.split(",")]
            persons_batch = [p for p in persons_batch
                             if str(p.get(BATCH_FIELD_KEY)) in allowed_batches]

        if not persons_batch:
            job.error = "Nach Batch-Filter keine Personen übrig."
            job.phase = "Fehler"
            job.percent = 100
            return

        # ================================================
        # STEP 4 – Organisationen laden
        # ================================================
        job.phase = "Lade Organisationen …"
        job.percent = 50

        orgs = await nf_load_orgs_light_for_persons(persons_batch)

        # ================================================
        # STEP 5 – Merge
        # ================================================
        job.phase = "Kombiniere Personen + Organisationen …"
        job.percent = 60

        merged = nf_merge_persons_orgs(persons_batch, orgs)

        # ================================================
        # STEP 6 – Ausschlüsse anwenden
        # ================================================
        job.phase = "Wende Ausschlussregeln an …"
        job.percent = 70

        selected, excluded = nf_apply_exclusions(merged)

        # ================================================
        # STEP 7 – Master bauen (Export-Batch & Kampagne nutzen)
        # ================================================
        job.phase = "Baue Master-Tabelle …"
        job.percent = 80

        master_df = nf_build_master(
            selected,
            batch_id=export_batch,
            campaign=campaign
        )

        # ================================================
        # STEP 8 – Excel speichern
        # ================================================
        job.phase = "Erstelle Excel …"
        job.percent = 90

        excluded_df = pd.DataFrame(excluded).replace({np.nan: None})
        if excluded_df.empty:
            excluded_df = pd.DataFrame([{
                "Kontakt ID": "-",
                "Name": "-",
                "Organisation": "-",
                "Grund": "Keine Ausschlüsse"
            }])

        file_path = await nf_export_to_excel(master_df, excluded_df, job_id)

        job.file_path = file_path
        job.phase = "Fertig"
        job.percent = 100

    except Exception as e:
        job.error = str(e)
        job.phase = "Fehler"
        job.percent = 100
        print("[NF ERROR]", e)


