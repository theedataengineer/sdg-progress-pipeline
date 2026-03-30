"""
World Bank SDG Indicator Extractor
====================================
Pulls SDG indicator data from the World Bank Open Data API.
API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392

This script is used in two modes:
  1. Batch mode   — full historical pull (called by Airflow weekly DAG)
  2. Stream mode  — recent updates only (called by Kafka producer every 6hrs)

The World Bank API is free, public, and requires no authentication.
"""

import requests
import json
import os
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
WB_API_BASE = os.getenv("WORLD_BANK_API_BASE", "https://api.worldbank.org/v2")

# All 54 African country ISO3 codes
# These are the countries whose SDG progress we track
AFRICAN_COUNTRIES = [
    "DZA","AGO","BEN","BWA","BFA","BDI","CPV","CMR","CAF","TCD",
    "COM","COD","COG","CIV","DJI","EGY","GNQ","ERI","SWZ","ETH",
    "GAB","GMB","GHA","GIN","GNB","KEN","LSO","LBR","LBY","MDG",
    "MWI","MLI","MRT","MUS","MAR","MOZ","NAM","NER","NGA","RWA",
    "STP","SEN","SLE","SOM","ZAF","SSD","SDN","TZA","TGO","TUN",
    "UGA","ZMB","ZWE","SHN"
]

# SDG indicators we track — mapped to their SDG goal
# These are official World Bank indicator codes for SDG monitoring
SDG_INDICATORS = {
    # SDG 1 — No Poverty
    "SI.POV.DDAY":  {"goal": 1, "name": "Poverty headcount ratio $2.15/day (%)"},
    "SI.POV.LMIC":  {"goal": 1, "name": "Poverty headcount ratio $3.65/day (%)"},
    "SI.POV.NAHC":  {"goal": 1, "name": "Poverty headcount ratio national (%)"},

    # SDG 2 — Zero Hunger
    "SN.ITK.DEFC.ZS": {"goal": 2, "name": "Prevalence of undernourishment (%)"},
    "SH.STA.STNT.ZS": {"goal": 2, "name": "Stunting prevalence under 5 (%)"},
    "SH.STA.WAST.ZS": {"goal": 2, "name": "Wasting prevalence under 5 (%)"},

    # SDG 3 — Good Health and Well-Being
    "SH.DYN.MORT":  {"goal": 3, "name": "Under-5 mortality rate per 1000"},
    "SH.MMR.RISK":  {"goal": 3, "name": "Lifetime risk of maternal death (%)"},
    "SH.MMR.DTHS":  {"goal": 3, "name": "Number of maternal deaths"},
    "SH.HIV.INCD":  {"goal": 3, "name": "Incidence of HIV per 1000 uninfected"},
    "SH.TBS.INCD":  {"goal": 3, "name": "Incidence of tuberculosis per 100000"},
    "SH.MLR.INCD.P3": {"goal": 3, "name": "Incidence of malaria per 1000"},
    "SP.DYN.LE00.IN": {"goal": 3, "name": "Life expectancy at birth (years)"},

    # SDG 4 — Quality Education
    "SE.PRM.ENRR":  {"goal": 4, "name": "Primary school enrollment gross (%)"},
    "SE.SEC.ENRR":  {"goal": 4, "name": "Secondary school enrollment gross (%)"},
    "SE.ADT.LITR.ZS": {"goal": 4, "name": "Literacy rate adults (%)"},
    "SE.PRM.CMPT.ZS": {"goal": 4, "name": "Primary completion rate (%)"},

    # SDG 5 — Gender Equality
    "SG.GEN.PARL.ZS": {"goal": 5, "name": "Women in parliament (%)"},
    "SE.ENR.PRSC.FM.ZS": {"goal": 5, "name": "Gender parity index primary school"},
    "SL.TLF.CACT.FE.ZS": {"goal": 5, "name": "Female labour force participation (%)"},

    # SDG 6 — Clean Water and Sanitation
    "SH.H2O.BASW.ZS": {"goal": 6, "name": "Access to basic drinking water (%)"},
    "SH.STA.BASS.ZS": {"goal": 6, "name": "Access to basic sanitation (%)"},

    # SDG 7 — Affordable and Clean Energy
    "EG.ELC.ACCS.ZS": {"goal": 7, "name": "Access to electricity (%)"},
    "EG.FEC.RNEW.ZS": {"goal": 7, "name": "Renewable energy share (%)"},

    # SDG 8 — Decent Work and Economic Growth
    "NY.GDP.MKTP.KD.ZG": {"goal": 8, "name": "GDP growth annual (%)"},
    "SL.UEM.TOTL.ZS":   {"goal": 8, "name": "Unemployment total (%)"},
    "SL.UEM.1524.ZS":   {"goal": 8, "name": "Youth unemployment (%)"},

    # SDG 10 — Reduced Inequalities
    "SI.POV.GINI":  {"goal": 10, "name": "Gini index"},
    "SI.DST.10TH.10": {"goal": 10, "name": "Income share top 10% (%)"},
    "SI.DST.FRST.20": {"goal": 10, "name": "Income share bottom 20% (%)"},

    # SDG 13 — Climate Action
    "EN.ATM.CO2E.PC":   {"goal": 13, "name": "CO2 emissions per capita (tonnes)"},
    "EN.ATM.CO2E.KT":   {"goal": 13, "name": "CO2 emissions total (kt)"},
    "EN.CLC.MDAT.ZS":   {"goal": 13, "name": "Countries with climate adaptation plans (%)"},

    # SDG 16 — Peace, Justice and Strong Institutions
    "VC.IHR.PSRC.P5": {"goal": 16, "name": "Homicide rate per 100000"},
    "IQ.CPA.IRAI.XQ": {"goal": 16, "name": "CPIA transparency rating"},
}


def fetch_indicator(indicator_code: str,
                    country_code: str = "all",
                    start_year: int = 2000,
                    end_year: int = 2024,
                    retries: int = 3) -> list[dict]:
    """
    Fetch a single indicator for one or all countries from the World Bank API.

    The API returns paginated results. This function handles pagination
    automatically and returns all pages as a flat list.

    Args:
        indicator_code : World Bank indicator code e.g. "SI.POV.DDAY"
        country_code   : ISO3 country code or "all" for all countries
        start_year     : First year to fetch (default 2000 = SDG baseline era)
        end_year       : Last year to fetch
        retries        : Number of retry attempts on failure

    Returns:
        List of indicator records as dicts
    """
    url = f"{WB_API_BASE}/country/{country_code}/indicator/{indicator_code}"
    params = {
        "format": "json",
        "date": f"{start_year}:{end_year}",
        "per_page": 1000,   # max per page — reduces number of API calls
        "page": 1
    }

    all_records = []

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # World Bank API returns [metadata, data] as a 2-element list
            if not data or len(data) < 2:
                log.warning(f"Empty response for {indicator_code} / {country_code}")
                return []

            metadata = data[0]
            records  = data[1] or []
            total_pages = metadata.get("pages", 1)

            all_records.extend(records)
            log.info(f"  Page 1/{total_pages} — {indicator_code} / {country_code} "
                     f"({len(records)} records)")

            # Fetch remaining pages if data is paginated
            for page in range(2, total_pages + 1):
                params["page"] = page
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                page_data = resp.json()
                page_records = page_data[1] or []
                all_records.extend(page_records)
                log.info(f"  Page {page}/{total_pages} — {len(page_records)} records")
                time.sleep(0.2)   # polite rate limiting

            return all_records

        except requests.exceptions.RequestException as e:
            log.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)   # exponential backoff
            else:
                log.error(f"All retries exhausted for {indicator_code}")
                return []


def normalise_record(raw: dict, indicator_code: str) -> dict | None:
    """
    Transform a raw World Bank API record into our standard pipeline schema.

    This is the schema that flows through Kafka, lands in MinIO,
    loads into PostgreSQL, and gets transformed by dbt.

    Raw World Bank record looks like:
    {
        "indicator": {"id": "SI.POV.DDAY", "value": "Poverty headcount..."},
        "country": {"id": "KE", "value": "Kenya"},
        "countryiso3code": "KEN",
        "date": "2021",
        "value": 26.8,
        "unit": "",
        "obs_status": "",
        "decimal": 1
    }
    """
    if raw is None:
        return None

    value = raw.get("value")

    # Skip records with no value — common for recent years not yet published
    if value is None:
        return None

    indicator_meta = SDG_INDICATORS.get(indicator_code, {})

    return {
        # SDG classification
        "goal":           indicator_meta.get("goal"),
        "indicator_code": indicator_code,
        "indicator_name": indicator_meta.get("name", raw.get("indicator", {}).get("value", "")),

        # Geography
        "country_code":   raw.get("countryiso3code", ""),
        "country_name":   raw.get("country", {}).get("value", ""),
        "country_id":     raw.get("country", {}).get("id", ""),

        # Data
        "year":           int(raw.get("date", 0)),
        "value":          float(value),
        "unit":           raw.get("unit", ""),
        "obs_status":     raw.get("obs_status", ""),

        # Metadata
        "source":         "World Bank Open Data",
        "extracted_at":   datetime.now(timezone.utc).isoformat() + "Z",
    }


def extract_africa_sdg_data(start_year: int = 2000,
                             end_year: int = 2024,
                             output_dir: str = "data/raw/world_bank") -> dict:
    """
    Main extraction function — pulls all SDG indicators for all African countries.

    This is called by:
    - Airflow batch_ingestion_dag (weekly full refresh)
    - The Kafka producer for streaming updates

    Returns a summary dict with record counts per goal.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    summary = {"total_records": 0, "by_goal": {}, "failed_indicators": []}
    all_records = []

    log.info(f"Starting World Bank extraction: {len(SDG_INDICATORS)} indicators, "
             f"{len(AFRICAN_COUNTRIES)} African countries, {start_year}-{end_year}")

    for indicator_code, meta in SDG_INDICATORS.items():
        goal = meta["goal"]
        log.info(f"Fetching SDG {goal} — {indicator_code}: {meta['name']}")

        # Fetch for all African countries in one API call using pipe-separated codes
        # World Bank supports up to ~50 countries per call
        country_string = ";".join(AFRICAN_COUNTRIES)
        raw_records = fetch_indicator(
            indicator_code=indicator_code,
            country_code=country_string,
            start_year=start_year,
            end_year=end_year
        )

        if not raw_records:
            summary["failed_indicators"].append(indicator_code)
            continue

        # Normalise each record to our standard schema
        normalised = []
        for raw in raw_records:
            record = normalise_record(raw, indicator_code)
            if record:
                normalised.append(record)

        all_records.extend(normalised)

        # Track counts by goal
        goal_key = f"goal_{goal}"
        summary["by_goal"][goal_key] = summary["by_goal"].get(goal_key, 0) + len(normalised)
        summary["total_records"] += len(normalised)

        log.info(f"  ✓ {len(normalised)} valid records for {indicator_code}")

        # Save per-indicator file for incremental loading
        indicator_file = Path(output_dir) / f"{indicator_code.replace('.', '_')}.json"
        with open(indicator_file, "w") as f:
            json.dump(normalised, f, indent=2)

        # Be polite to the API — 0.5s between indicator calls
        time.sleep(0.5)

    # Save complete combined file
    combined_file = Path(output_dir) / "all_indicators_africa.json"
    with open(combined_file, "w") as f:
        json.dump(all_records, f, indent=2)

    summary["output_file"] = str(combined_file)
    log.info(f"\n{'='*60}")
    log.info(f"Extraction complete!")
    log.info(f"Total records: {summary['total_records']}")
    log.info(f"By goal: {summary['by_goal']}")
    log.info(f"Failed indicators: {summary['failed_indicators']}")
    log.info(f"Output: {combined_file}")

    return summary


def extract_recent_updates(days_back: int = 7) -> list[dict]:
    """
    Fetch only the most recently published indicators.
    Used by the Kafka producer for streaming mode —
    polls every 6 hours and publishes only new records.

    Args:
        days_back: How many days back to check for updates

    Returns:
        List of normalised records published recently
    """
    current_year = datetime.now(timezone.utc).year
    # World Bank publishes annual data — check current and previous year
    recent_records = []

    log.info(f"Fetching recent updates (last {days_back} days)...")

    for indicator_code, meta in SDG_INDICATORS.items():
        country_string = ";".join(AFRICAN_COUNTRIES)
        raw_records = fetch_indicator(
            indicator_code=indicator_code,
            country_code=country_string,
            start_year=current_year - 1,
            end_year=current_year
        )

        for raw in raw_records:
            record = normalise_record(raw, indicator_code)
            if record:
                recent_records.append(record)

        time.sleep(0.3)

    log.info(f"Found {len(recent_records)} recent records")
    return recent_records


# ── Script entry point ────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    if mode == "full":
        # Full historical extraction — used by Airflow batch DAG
        summary = extract_africa_sdg_data(start_year=2000, end_year=2024)
        print(json.dumps(summary, indent=2))

    elif mode == "recent":
        # Recent updates only — used by Kafka producer
        records = extract_recent_updates(days_back=7)
        print(f"Found {len(records)} recent records")
        if records:
            print(json.dumps(records[0], indent=2))

    else:
        # Test mode — fetch just one indicator to confirm API is working
        log.info("TEST MODE — fetching one indicator for Kenya")
        records = fetch_indicator(
            indicator_code="SI.POV.DDAY",
            country_code="KEN",
            start_year=2015,
            end_year=2024
        )
        normalised = [normalise_record(r, "SI.POV.DDAY") for r in records if r]
        normalised = [r for r in normalised if r]
        print(f"\nFetched {len(normalised)} records")
        if normalised:
            print("\nSample record:")
            print(json.dumps(normalised[0], indent=2))
        print("\nAll years found:")
        for r in sorted(normalised, key=lambda x: x["year"]):
            print(f"  {r['year']}: {r['value']} {r['unit']}")
