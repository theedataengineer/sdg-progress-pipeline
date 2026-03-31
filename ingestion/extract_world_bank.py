"""
World Bank SDG Indicator Extractor
====================================
Pulls SDG indicator data from the World Bank Open Data API.
Fetches countries in batches of 10 to avoid URL length limits.
"""

import requests
import json
import os
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

WB_API_BASE = os.getenv("WORLD_BANK_API_BASE", "https://api.worldbank.org/v2")

AFRICAN_COUNTRIES = [
    "DZA","AGO","BEN","BWA","BFA","BDI","CPV","CMR","CAF","TCD",
    "COM","COD","COG","CIV","DJI","EGY","GNQ","ERI","SWZ","ETH",
    "GAB","GMB","GHA","GIN","GNB","KEN","LSO","LBR","LBY","MDG",
    "MWI","MLI","MRT","MUS","MAR","MOZ","NAM","NER","NGA","RWA",
    "STP","SEN","SLE","SOM","ZAF","SSD","SDN","TZA","TGO","TUN",
    "UGA","ZMB","ZWE","SHN"
]

SDG_INDICATORS = {
    "SI.POV.DDAY":       {"goal": 1,  "name": "Poverty headcount ratio $2.15/day (%)"},
    "SI.POV.LMIC":       {"goal": 1,  "name": "Poverty headcount ratio $3.65/day (%)"},
    "SI.POV.NAHC":       {"goal": 1,  "name": "Poverty headcount ratio national (%)"},
    "SN.ITK.DEFC.ZS":   {"goal": 2,  "name": "Prevalence of undernourishment (%)"},
    "SH.STA.STNT.ZS":   {"goal": 2,  "name": "Stunting prevalence under 5 (%)"},
    "SH.STA.WAST.ZS":   {"goal": 2,  "name": "Wasting prevalence under 5 (%)"},
    "SH.DYN.MORT":      {"goal": 3,  "name": "Under-5 mortality rate per 1000"},
    "SH.MMR.RISK":      {"goal": 3,  "name": "Lifetime risk of maternal death (%)"},
    "SH.MMR.DTHS":      {"goal": 3,  "name": "Number of maternal deaths"},
    "SH.HIV.INCD":      {"goal": 3,  "name": "Incidence of HIV per 1000 uninfected"},
    "SH.TBS.INCD":      {"goal": 3,  "name": "Incidence of tuberculosis per 100000"},
    "SH.MLR.INCD.P3":   {"goal": 3,  "name": "Incidence of malaria per 1000"},
    "SP.DYN.LE00.IN":   {"goal": 3,  "name": "Life expectancy at birth (years)"},
    "SE.PRM.ENRR":      {"goal": 4,  "name": "Primary school enrollment gross (%)"},
    "SE.SEC.ENRR":      {"goal": 4,  "name": "Secondary school enrollment gross (%)"},
    "SE.ADT.LITR.ZS":   {"goal": 4,  "name": "Literacy rate adults (%)"},
    "SE.PRM.CMPT.ZS":   {"goal": 4,  "name": "Primary completion rate (%)"},
    "SG.GEN.PARL.ZS":   {"goal": 5,  "name": "Women in parliament (%)"},
    "SE.ENR.PRSC.FM.ZS":{"goal": 5,  "name": "Gender parity index primary school"},
    "SL.TLF.CACT.FE.ZS":{"goal": 5,  "name": "Female labour force participation (%)"},
    "SH.H2O.BASW.ZS":   {"goal": 6,  "name": "Access to basic drinking water (%)"},
    "SH.STA.BASS.ZS":   {"goal": 6,  "name": "Access to basic sanitation (%)"},
    "EG.ELC.ACCS.ZS":   {"goal": 7,  "name": "Access to electricity (%)"},
    "EG.FEC.RNEW.ZS":   {"goal": 7,  "name": "Renewable energy share (%)"},
    "NY.GDP.MKTP.KD.ZG":{"goal": 8,  "name": "GDP growth annual (%)"},
    "SL.UEM.TOTL.ZS":   {"goal": 8,  "name": "Unemployment total (%)"},
    "SL.UEM.1524.ZS":   {"goal": 8,  "name": "Youth unemployment (%)"},
    "SI.POV.GINI":      {"goal": 10, "name": "Gini index"},
    "SI.DST.10TH.10":   {"goal": 10, "name": "Income share top 10% (%)"},
    "SI.DST.FRST.20":   {"goal": 10, "name": "Income share bottom 20% (%)"},
    "EN.ATM.CO2E.PC":   {"goal": 13, "name": "CO2 emissions per capita (tonnes)"},
    "EN.ATM.CO2E.KT":   {"goal": 13, "name": "CO2 emissions total (kt)"},
    "EN.CLC.MDAT.ZS":   {"goal": 13, "name": "Countries with climate adaptation plans (%)"},
    "VC.IHR.PSRC.P5":   {"goal": 16, "name": "Homicide rate per 100000"},
    "IQ.CPA.IRAI.XQ":   {"goal": 16, "name": "CPIA transparency rating"},
}


def fetch_indicator_for_countries(indicator_code, country_batch,
                                   start_year, end_year, retries=3):
    """
    Fetch one indicator for a batch of up to 10 countries.
    Returns list of raw records.
    """
    country_string = ";".join(country_batch)
    url = f"{WB_API_BASE}/country/{country_string}/indicator/{indicator_code}"
    params = {
        "format":   "json",
        "date":     f"{start_year}:{end_year}",
        "per_page": 1000,
        "page":     1
    }

    all_records = []

    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if not data or len(data) < 2 or not data[1]:
                return []

            metadata    = data[0]
            records     = data[1]
            total_pages = metadata.get("pages", 1)
            all_records.extend(records)

            for page in range(2, total_pages + 1):
                params["page"] = page
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                page_data = resp.json()
                if page_data[1]:
                    all_records.extend(page_data[1])
                time.sleep(0.1)

            return all_records

        except requests.exceptions.RequestException as e:
            log.warning(f"Attempt {attempt+1}/{retries} failed: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return []


def normalise_record(raw, indicator_code):
    """Convert raw API record to standard pipeline schema."""
    if raw is None:
        return None
    value = raw.get("value")
    if value is None:
        return None

    meta = SDG_INDICATORS.get(indicator_code, {})
    return {
        "goal":           meta.get("goal"),
        "indicator_code": indicator_code,
        "indicator_name": meta.get("name", ""),
        "country_code":   raw.get("countryiso3code", ""),
        "country_name":   raw.get("country", {}).get("value", ""),
        "country_id":     raw.get("country", {}).get("id", ""),
        "year":           int(raw.get("date", 0)),
        "value":          float(value),
        "unit":           raw.get("unit", ""),
        "obs_status":     raw.get("obs_status", ""),
        "source":         "World Bank Open Data",
        "extracted_at":   datetime.now(timezone.utc).isoformat() + "Z",
    }


def extract_africa_sdg_data(start_year=2000, end_year=2024,
                             output_dir="data/raw/world_bank",
                             country_batch_size=10):
    """
    Full extraction: all indicators, all African countries.
    Fetches countries in batches to avoid URL length limits.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    summary = {
        "total_records":     0,
        "by_goal":           {},
        "failed_indicators": []
    }
    all_records = []

    # Split countries into batches of country_batch_size
    country_batches = [
        AFRICAN_COUNTRIES[i:i+country_batch_size]
        for i in range(0, len(AFRICAN_COUNTRIES), country_batch_size)
    ]

    log.info(f"Extracting {len(SDG_INDICATORS)} indicators for "
             f"{len(AFRICAN_COUNTRIES)} countries in "
             f"{len(country_batches)} batches of {country_batch_size}")

    for indicator_code, meta in SDG_INDICATORS.items():
        goal = meta["goal"]
        log.info(f"SDG {goal} — {indicator_code}: {meta['name']}")

        indicator_records = []

        for batch_num, batch in enumerate(country_batches):
            raw_records = fetch_indicator_for_countries(
                indicator_code, batch, start_year, end_year
            )

            for raw in raw_records:
                record = normalise_record(raw, indicator_code)
                if record:
                    indicator_records.append(record)

            time.sleep(0.2)   # polite rate limiting between batches

        if not indicator_records:
            summary["failed_indicators"].append(indicator_code)
            log.warning(f"  No data returned for {indicator_code}")
            continue

        all_records.extend(indicator_records)
        goal_key = f"goal_{goal}"
        summary["by_goal"][goal_key] = (
            summary["by_goal"].get(goal_key, 0) + len(indicator_records)
        )
        summary["total_records"] += len(indicator_records)

        log.info(f"  ✓ {len(indicator_records)} records")

        # Save per-indicator file
        out_file = Path(output_dir) / f"{indicator_code.replace('.','_')}.json"
        with open(out_file, "w") as f:
            json.dump(indicator_records, f, indent=2)

        time.sleep(0.3)

    # Save combined file
    combined = Path(output_dir) / "all_indicators_africa.json"
    with open(combined, "w") as f:
        json.dump(all_records, f, indent=2)

    summary["output_file"] = str(combined)

    log.info("=" * 60)
    log.info(f"Extraction complete! Total: {summary['total_records']} records")
    log.info(f"By goal: {summary['by_goal']}")
    if summary["failed_indicators"]:
        log.warning(f"Failed: {summary['failed_indicators']}")

    return summary


def extract_recent_updates(days_back=7):
    """Fetch recent updates for Kafka producer streaming mode."""
    current_year = datetime.now(timezone.utc).year
    recent_records = []

    country_batches = [
        AFRICAN_COUNTRIES[i:i+10]
        for i in range(0, len(AFRICAN_COUNTRIES), 10)
    ]

    log.info(f"Fetching recent updates ({current_year-1}-{current_year})...")

    for indicator_code in SDG_INDICATORS:
        for batch in country_batches:
            raw_records = fetch_indicator_for_countries(
                indicator_code, batch,
                current_year - 1, current_year
            )
            for raw in raw_records:
                record = normalise_record(raw, indicator_code)
                if record:
                    recent_records.append(record)
            time.sleep(0.1)

    log.info(f"Found {len(recent_records)} recent records")
    return recent_records


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "test"

    if mode == "full":
        summary = extract_africa_sdg_data(start_year=2000, end_year=2024)
        print(json.dumps(summary, indent=2))

    elif mode == "recent":
        records = extract_recent_updates()
        print(f"Found {len(records)} recent records")
        if records:
            print(json.dumps(records[0], indent=2))

    else:
        # Test mode — single country, single indicator
        log.info("TEST MODE — Kenya, life expectancy 2015-2022")
        batch = fetch_indicator_for_countries(
            "SP.DYN.LE00.IN", ["KEN"], 2015, 2022
        )
        normalised = [normalise_record(r, "SP.DYN.LE00.IN")
                      for r in batch if r]
        normalised = [r for r in normalised if r]
        print(f"Fetched {len(normalised)} records")
        for r in sorted(normalised, key=lambda x: x["year"]):
            print(f"  {r['year']}: {r['value']}")
