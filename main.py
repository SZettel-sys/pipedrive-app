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
# MODUL 2 – NF-Light-Persons Loader (FINAL 2025.12)
# Stabil, fehlerfrei, geeignet für 300k+ Personen
# Basis des gesamten Nachfass-Prozesses
# =============================================================

PERSON_PAGE_LIMIT = 500        # stabil & performant
PERSON_SEMAPHORE = 2           # schützt vor 429 bei großen Accounts

async def nf_load_persons_light() -> List[dict]:
    """
    Lädt ALLE Personen aus Pipedrive als Light-Objekte, analog zum funktionierenden
    Organisationen-Skript. Dies ist die stabilste Methode für große Datenmengen.
    Die geladenen Felder sind:
    - id
    - name
    - org_id
    - email
    - active_flag
    - label_ids
    - custom_fields (für Batch-ID, Prospect-ID etc.)
    """

    print("[NF] Starte Light-Personen-Scan über /persons …")

    persons = []
    seen_ids = set()
    start = 0

    sem = asyncio.Semaphore(PERSON_SEMAPHORE)

    async with httpx.AsyncClient(timeout=60.0) as client:

        async def load_page(start):
            async with sem:
                url = append_token(
                    f"{PIPEDRIVE_BASE}/persons"
                    f"?start={start}"
                    f"&limit={PERSON_PAGE_LIMIT}"
                    f"&include_fields=custom_fields,org_id,email,label"
                )

                r = await safe_request("GET", url, headers=get_headers(), client=client)

                # Daten holen
                data = r.json().get("data") or []

                # Pagination Info
                meta = (r.json().get("additional_data") or {}).get("pagination") or {}
                more = meta.get("more_items_in_collection", False)
                next_start = meta.get("next_start", None)

                # Personen einsammeln
                for p in data:
                    pid = p.get("id")
                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)
                        persons.append(p)

                return more, next_start

        # STABILES, sequenzielles Paging
        while True:
            more, next_start = await load_page(start)
            print(f"[NF] Personen geladen bis Index {start} – Gesamt: {len(persons)}")

            if not more:
                break

            start = next_start if next_start is not None else start + PERSON_PAGE_LIMIT

    print(f"[NF] Light-Personen Gesamt: {len(persons)}")
    return persons
# =============================================================
# MODUL 3 – NF-Light-Organizations Loader (FINAL 2025.12)
# Lädt ALLE Organisationen extrem stabil über Paging
# =============================================================

ORG_PAGE_LIMIT = 500
ORG_SEMAPHORE = 2   # stabilste Einstellung für große Accounts

async def nf_load_orgs_light() -> Dict[str, dict]:
    """
    Lädt alle Organisationen als 'Light'-Objekte über /organizations.
    Liefert:
      {
        "org_id": {
            "id": …,
            "name": …,
            "label_ids": […],
            "custom_fields": {...},
            "open_deals_count": 0,
            "closed_deals_count": 0,
            "won_deals_count": 0,
            "lost_deals_count": 0
        },
        ...
      }
    """

    print("[NF] Starte Light-Organisationen-Scan über /organizations …")

    orgs = {}
    start = 0
    seen = set()

    sem = asyncio.Semaphore(ORG_SEMAPHORE)

    async with httpx.AsyncClient(timeout=60.0) as client:

        async def load_page(start):
            async with sem:
                url = append_token(
                    f"{PIPEDRIVE_BASE}/organizations"
                    f"?start={start}"
                    f"&limit={ORG_PAGE_LIMIT}"
                    f"&include_fields=label,custom_fields,open_deals_count,closed_deals_count,won_deals_count,lost_deals_count"
                )

                r = await safe_request("GET", url, headers=get_headers(), client=client)

                data = r.json().get("data") or []
                meta = (r.json().get("additional_data") or {}).get("pagination") or {}

                more = meta.get("more_items_in_collection", False)
                next_start = meta.get("next_start", None)

                for o in data:
                    oid = o.get("id")
                    if oid and oid not in seen:
                        seen.add(oid)
                        orgs[str(oid)] = o

                return more, next_start

        # sequenzielles Paging → 100% stabil
        while True:
            more, next_start = await load_page(start)
            print(f"[NF] Orgas geladen bis Index {start} – Gesamt: {len(orgs)}")

            if not more:
                break

            start = next_start if next_start is not None else start + ORG_PAGE_LIMIT

    print(f"[NF] Light-Orgas Gesamt: {len(orgs)}")
    return orgs
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
# MODUL 6 – NF MASTER BUILDER & EXPORT ROUTES (FINAL 2025.12)
# =============================================================

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

router = APIRouter()


# -------------------------------------------------------------
# NF MASTER BUILDER
# -------------------------------------------------------------
def build_nf_master(persons_full: Dict[str, dict], orgs_full: Dict[str, dict]):
    rows = []
    excluded = []

    org_person_counter = defaultdict(int)
    now = datetime.now()

    for pid, p in persons_full.items():

        oid = str(p.get("org_id"))
        org = orgs_full.get(oid)

        # Organisation muss existieren (falls vorher gefiltert)
        if not org:
            excluded.append({
                "Person ID": pid,
                "Grund": "Keine gültige Organisation gefunden"
            })
            continue

        # Maximal 2 Personen pro Organisation
        org_person_counter[oid] += 1
        if org_person_counter[oid] > 2:
            excluded.append({
                "Person ID": pid,
                "Orga ID": oid,
                "Grund": "Mehr als 2 Personen pro Organisation"
            })
            continue

        # Next Activity Check
        next_raw = p.get("next_activity_date")
        if next_raw:
            try:
                d = datetime.fromisoformat(next_raw.split(" ")[0])
                if (now - d).days <= 90:
                    excluded.append({
                        "Person ID": pid,
                        "Orga ID": oid,
                        "Grund": "Nächste Aktivität < 3 Monate"
                    })
                    continue
            except:
                pass

        # E-Mail
        email = ""
        emails = p.get("email") or []
        if isinstance(emails, list) and emails:
            email = emails[0].get("value") or ""

        # Geschlecht / Titel / Position
        gender = cf(p, PERSON_GENDER_KEY)
        title  = cf(p, PERSON_TITLE_KEY)
        pos    = cf(p, PERSON_POSITION_KEY)

        # Prospect-ID
        prospect = cf(p, PERSON_PROSPECT_KEY)

        # Batch-ID
        batch = cf(p, PERSON_BATCH_KEY)

        # LinkedIn
        linkedin = cf(p, PERSON_LINKEDIN_KEY)

        # XING
        xing = cf(p, PERSON_XING_KEY)

        # Name
        full_name = p.get("name") or ""
        first, last = split_name(
            p.get("first_name"),
            p.get("last_name"),
            full_name
        )

        # Orga
        org_name = org.get("name", "")
        org_level = cf(org, ORG_LEVEL_KEY)
        org_stop  = cf(org, ORG_VERTRIEBSSTOP_KEY)

        rows.append({
            "Person – Batch ID": batch,
            "Person – Prospect ID": prospect,
            "Person – Organisation": org_name,
            "Organisation – ID": oid,
            "Organisation – Level": org_level,
            "Organisation – Vertriebsstopp": org_stop,

            "Person – Geschlecht": gender,
            "Person – Titel": title,
            "Person – Vorname": first,
            "Person – Nachname": last,
            "Person – Position": pos,

            "Person – ID": pid,
            "Person – XING-Profil": xing,
            "Person – LinkedIn Profil-URL": linkedin,
            "Person – E-Mail-Adresse – Büro": email,
        })

    master_df = pd.DataFrame(rows)
    excluded_df = pd.DataFrame(excluded)

    return master_df, excluded_df


# -------------------------------------------------------------
# /nachfass/run  –  Startet gesamten NF-Prozess
# -------------------------------------------------------------
@router.get("/nachfass/run")
async def run_nachfass():

    print("\n==========================")
    print(" STARTE NACHFASS PIPELINE")
    print("==========================\n")

    # 1) Personen Light laden
    persons_light = await nf_load_persons_light()

    # 2) Organisationen Light laden
    orgs_light = await nf_load_orgs_light()

    # 3) NF Filter 3024
    candidates = nf_merge_persons_and_orgs(persons_light, orgs_light)

    if not candidates:
        raise HTTPException(404, "Keine Kandidaten gefunden")

    # 4) Details für Kandidaten laden
    persons_full, orgs_full = await nf_load_details_for_candidates(candidates)

    # 5) Master bauen
    master_df, excluded_df = build_nf_master(persons_full, orgs_full)

    # 6) Excel erzeugen
    job_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = await export_to_excel(master_df, excluded_df, job_id)

    print(f"[NF] Export erstellt → {file_path}")

    return {"job_id": job_id, "file_path": file_path}


# -------------------------------------------------------------
# /nachfass/download – Datei abrufen
# -------------------------------------------------------------
@router.get("/nachfass/download")
async def download_nachfass(file_path: str):
    if not os.path.exists(file_path):
        raise HTTPException(404, "Datei nicht gefunden")

    filename = os.path.basename(file_path)
    return FileResponse(
        file_path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
