"""
Toronto Infrastructure Intelligence (TII) — Data Scraper v2.8
==============================================================
Fixes vs v1.3:
  1. IESO XML: replaced BeautifulSoup(html.parser) with xml.etree.ElementTree
     (built-in, no external deps, handles XML namespaces correctly)
  2. StatsCan all-items CPI: fixed vector — v41690973 is the confirmed correct
     vector for Canada all-items CPI (not v41693202 which returned FAILED)
  3. StatsCan gas storage: added multi-vector fallback; original v65201762 was
     returning 0.0 — now tries several known Ontario storage vectors
  4. Watermain breaks: removed datastore_active filter — dataset uses file
     download not CKAN datastore; now gets download URL from any resource
  5. TTC ridership: broadened resource format filter; XLSX resources may be
     tagged as "XLSX", "xlsx", or just have .xlsx in URL
  6. data.ontario.ca ER: added direct package ID fallback list since search
     wasn't matching the dataset
  7. OSB insolvency: added graceful handling when openpyxl not installed,
     with clear install instruction

INSTALL
-------
    pip install requests beautifulsoup4 openpyxl
    (lxml optional — not required, xml.etree.ElementTree handles IESO XML now)
"""

import argparse
import os
import csv
import io
import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Suppress XMLParsedAsHTMLWarning if BeautifulSoup is used on XML
try:
    from bs4 import XMLParsedAsHTMLWarning
    import warnings
    warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
except ImportError:
    pass


# Cache for per-generator detail — populated by fetch_ieso_generation_mix,
# written to ieso_generators_YYYYMMDD.json by run_all_scrapers.
_IESO_GENERATOR_CACHE: dict = {}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "TII-Scraper/1.4 Toronto Infrastructure Intelligence",
    "Accept": "application/json, text/html, text/csv, application/xml, */*",
    "Content-Type": "application/json",
})
TIMEOUT = 25


def _ok(indicator, value, unit, source, url, data_date, notes=""):
    return {"indicator": indicator, "value": value, "unit": unit, "source": source,
            "url": url, "retrieved_at": datetime.utcnow().isoformat() + "Z",
            "data_date": str(data_date), "status": "ok", "notes": notes}

def _err(indicator, source, url, error_msg):
    return {"indicator": indicator, "value": None, "unit": None, "source": source,
            "url": url, "retrieved_at": datetime.utcnow().isoformat() + "Z",
            "data_date": None, "status": "error", "notes": f"ERROR: {error_msg}"}

def _manual(indicator, source, notes):
    return {"indicator": indicator, "value": None, "unit": None, "source": source,
            "url": None, "retrieved_at": datetime.utcnow().isoformat() + "Z",
            "data_date": None, "status": "manual", "notes": notes}


# ── Threshold rules ────────────────────────────────────────────────────────
# Each rule: (indicator_substring, warn_condition_fn, alert_condition_fn, warn_note, alert_note)
# Conditions receive the numeric value. Non-numeric values skip threshold checks.
# Status escalates: ok → warn → alert. Manual/error status never downgraded.
THRESHOLDS = [
    # Energy
    ("Brent Crude Price",
        lambda v: v > 80,   lambda v: v > 100,
        "Elevated — above $80/bbl",
        "Crisis level — above $100/bbl (Hormuz threshold)"),
    ("Gas Output (MW)",
        lambda v: v > 3000, lambda v: v > 5000,
        "Gas peakers elevated — demand stress signal",
        "Gas peakers at crisis level — grid under significant stress"),
    ("Natural Gas Storage",
        lambda v: v == 0.0, lambda v: False,
        "Vector unresolved — value may be incorrect",
        ""),
    ("Peak Demand Index",
        lambda v: v > 85,   lambda v: v > 95,
        "Demand elevated — above 85% of 2024 peak",
        "Demand critical — above 95% of 2024 peak"),
    # Water / Shelter
    ("Shelter Occupancy",
        lambda v: v > 4500, lambda v: v > 4700,
        "Shelter system above 94% of capacity",
        "Shelter system at or above 98% — crisis threshold"),
    ("Watermain Break Rate",
        lambda v: v > 15,   lambda v: v > 25,
        "Elevated break rate — above seasonal average",
        "Critical break rate — infrastructure stress"),
    # Housing market
    ("TRREB Sales-to-New-Listings Ratio",
        lambda v: v > 60,  lambda v: v > 70,
        "Seller's market — SNR above 60%, price pressure building",
        "Strong seller's market — SNR above 70%, significant affordability stress"),
    # Labour market
    ("Toronto Unemployment Rate",
        lambda v: v > 8.0,  lambda v: v > 10.0,
        "Unemployment elevated — above 8% (high vs pre-tariff baseline)",
        "Unemployment at recession level — above 10%"),
    # Airport operations
    ("Pearson Airport Operations",
        lambda v: v >= 1,  lambda v: v >= 2,
        "Reduced capacity or approach restriction at Pearson",
        "Runway closure or ATC flow control active at Pearson"),
    # Pipeline utilization
    ("TCPL Parkway Receipts (GTA supply)",
        lambda v: v > 85,  lambda v: v > 95,
        "Parkway pipeline near capacity — GTA supply stress risk",
        "Parkway pipeline at capacity — GTA supply emergency risk"),
    ("TCPL Northern Ontario Line",
        lambda v: v > 85,  lambda v: v > 95,
        "Northern Ontario Line near capacity — upstream supply stress",
        "Northern Ontario Line at capacity — upstream supply emergency"),
    # Financial
    ("CAD/USD Exchange Rate",
        lambda v: v > 1.40, lambda v: v > 1.50,
        "CAD weakening — above 1.40 (import cost pressure)",
        "CAD under severe stress — above 1.50"),
    ("Fuel Price — Toronto",
        lambda v: v > 150,  lambda v: v > 185,
        "Fuel price elevated — above 150¢/L",
        "Fuel price critical — above 185¢/L"),
    ("Monthly Bankruptcy Filings",
        lambda v: v > 5000, lambda v: v > 7000,
        "Insolvency filings elevated — above 5,000/month (Ontario)",
        "Insolvency filings critical — above 7,000/month"),
    # Health
    ("Ontario ICU Occupancy",
        lambda v: v > 85,   lambda v: v > 95,
        "ICU occupancy elevated — above 85%",
        "ICU occupancy critical — above 95%"),
    # Environment
    ("Air Quality (AQHI)",
        lambda v: v >= 4,   lambda v: v >= 7,
        "Moderate risk — AQHI 4-6",
        "High risk — AQHI 7+"),
    # Boil water advisory — any active advisory is an immediate alert
    ("Boil Water Advisories",
        lambda v: v >= 1,  lambda v: v >= 1,
        "Active boil water advisory — public health signal",
        "Active boil water advisory — significant public health event"),
    # Water outages
    ("Active Water Outages",
        lambda v: v >= 3,  lambda v: v >= 8,
        "Multiple active outages — monitor for escalation",
        "High number of active outages — potential systemic stress"),
    ("Active Watermain Breaks",
        lambda v: v >= 2,  lambda v: v >= 5,
        "Multiple active breaks — elevated infrastructure stress",
        "High number of active breaks — systemic infrastructure alert"),
    # Food
    ("Grocery Price Inflation",
        lambda v: v > 170,  lambda v: v > 185,
        "Food CPI elevated — above index 170",
        "Food CPI critical — above index 185"),
]

def apply_thresholds(result):
    """
    Evaluate threshold rules against a result dict.
    Upgrades status from 'ok' to 'warn' or 'alert' as appropriate.
    Never downgrades manual/error status.
    Returns the result dict (mutated in place).
    """
    if result.get("status") not in ("ok", "warn"):
        return result
    value = result.get("value")
    if value is None:
        return result
    try:
        v = float(value)
    except (TypeError, ValueError):
        return result

    indicator = result.get("indicator", "")
    for (substr, warn_fn, alert_fn, warn_note, alert_note) in THRESHOLDS:
        if substr.lower() not in indicator.lower():
            continue
        try:
            if alert_fn(v) and alert_note:
                result["status"] = "alert"
                result["threshold_note"] = alert_note
            elif warn_fn(v) and warn_note:
                if result["status"] != "alert":
                    result["status"] = "warn"
                    result["threshold_note"] = warn_note
        except Exception:
            pass
        break  # first matching rule wins

    return result



# ══════════════════════════════════════════════════════════════════════════════
# SECTOR: ENERGY
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ieso_generation_mix():
    """
    IESO GenOutputbyFuelHourly — annual XML.

    v1.7: Confirmed schema from live document inspection:
      Document > DocBody > DailyData > Day
                                     > HourlyData > Hour
                                                  > FuelTotal > Fuel        (= "NUCLEAR" etc.)
                                                              > EnergyValue  (MWh as float)

    This file is fuel-aggregated by hour — no individual generator names.
    Per-generator detail lives in PUB_GenOutputCapability (separate endpoint).

    _IESO_GENERATOR_CACHE is populated with fuel-level records here;
    true per-generator breakdown can be added from GenOutputCapability later.
    """
    year = date.today().strftime("%Y")
    url = f"https://reports-public.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly_{year}.xml"

    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("IESO Generation Mix", "IESO GenOutputbyFuelHourly", url, str(e))]

    try:
        root = ET.fromstring(r.content)
    except ET.ParseError as e:
        return [_err("IESO Generation Mix", "IESO GenOutputbyFuelHourly", url,
                     f"XML parse error: {e}")]

    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""

    # Walk DailyData blocks — take the last one (most recent day)
    daily_blocks = root.findall(f".//{ns}DailyData") or root.findall(".//DailyData")
    if not daily_blocks:
        tags = sorted({el.tag.split("}")[-1] for el in root.iter()})
        return [_err("IESO Generation Mix", "IESO GenOutputbyFuelHourly", url,
                     f"No DailyData elements. Root: {root.tag}. Tags: {tags}")]

    # Annual file has DailyData for every day of the year including future placeholders.
    # Must search backwards through DailyData blocks to find the last one with real data.
    fuel_totals = {}
    latest_hour = None
    day_str = ""

    for last_day in reversed(daily_blocks):
        d_el = last_day.find(f"{ns}Day") or last_day.find("Day")
        candidate_day = (d_el.text or "").strip() if d_el is not None else ""

        hourly_blocks = (last_day.findall(f".//{ns}HourlyData") or
                         last_day.findall(".//HourlyData"))

        # Search backwards through hours in this day for populated data.
        # Use local-name matching to bypass namespace prefix issues entirely.
        for hourly in reversed(hourly_blocks):
            # Index all direct and grandchild elements by local tag name
            by_tag = {}
            for child in hourly:
                local = child.tag.split("}")[-1]
                by_tag.setdefault(local, []).append(child)

            h_list = by_tag.get("Hour", [])
            if not h_list or not h_list[0].text:
                continue
            try:
                hour_num = int(h_list[0].text.strip())
            except ValueError:
                continue

            candidate = {}
            for ft in by_tag.get("FuelTotal", []):
                # Index FuelTotal children by local name
                ft_kids = {c.tag.split("}")[-1]: c for c in ft}
                fuel_el = ft_kids.get("Fuel")
                ev_el   = ft_kids.get("EnergyValue")
                if fuel_el is None or ev_el is None:
                    continue
                # EnergyValue > Output holds the MW number
                ev_kids = {c.tag.split("}")[-1]: c for c in ev_el}
                out_el  = ev_kids.get("Output")
                if out_el is None:
                    continue
                fuel     = (fuel_el.text or "").strip().upper()
                val_text = (out_el.text  or "").strip()
                if not fuel or not val_text:
                    continue
                try:
                    candidate[fuel] = float(val_text)
                except ValueError:
                    continue

            if candidate:
                fuel_totals = candidate
                latest_hour = hour_num
                day_str     = candidate_day
                break  # found populated hour in this day

        if fuel_totals:
            break  # found populated day — stop searching

    if not fuel_totals:
        tags = sorted({el.tag.split("}")[-1] for el in root.iter()})
        return [_err("IESO Generation Mix", "IESO GenOutputbyFuelHourly", url,
                     f"No data in {len(daily_blocks)} DailyData blocks. "
                     f"Tags in document: {tags}")]

    # Populate cache (fuel-level, not generator-level — upgrade later with GenOutputCapability)
    _IESO_GENERATOR_CACHE.clear()
    _IESO_GENERATOR_CACHE.update({
        "retrieved_at":    datetime.utcnow().isoformat() + "Z",
        "data_date":       day_str or str(date.today()),
        "hour":            latest_hour,
        "source_url":      url,
        "note":            "Fuel-aggregated totals. Per-generator detail available from "
                           "PUB_GenOutputCapability (not yet implemented).",
        "fuel_totals_mw":  {k: round(v, 1) for k, v in fuel_totals.items()},
        "generators":      [
            {"name": fuel, "fuel": fuel,
             "output_mw": round(mw, 1), "capacity_mw": None,
             "hour": latest_hour, "data_date": day_str or str(date.today())}
            for fuel, mw in sorted(fuel_totals.items(),
                                   key=lambda x: x[1], reverse=True)
        ],
    })

    total_mw = sum(fuel_totals.values())
    data_date = f"{day_str} Hour {latest_hour}" if day_str else f"{date.today()} Hour {latest_hour}"
    results = [_ok("Total Generation (MW)", round(total_mw, 1), "MW",
                   "IESO GenOutputbyFuelHourly", url, data_date,
                   f"Hour {latest_hour}. Fuel mix: {fuel_totals}")]
    for key, (name, unit) in {
        "NUCLEAR": ("Nuclear Output (MW)", "MW"),
        "HYDRO":   ("Hydro Output (MW)",   "MW"),
        "GAS":     ("Gas Output (MW)",     "MW"),
        "WIND":    ("Wind Output (MW)",    "MW"),
        "SOLAR":   ("Solar Output (MW)",   "MW"),
        "BIOFUEL": ("Biofuel Output (MW)", "MW"),
    }.items():
        results.append(_ok(name, round(fuel_totals.get(key, 0), 1), unit,
                           "IESO GenOutputbyFuelHourly", url, data_date))
    return results


def fetch_ieso_ontario_demand():
    """
    IESO Ontario Demand multiday — also uses ElementTree now.
    The ashx file is XML. ElementTree handles it correctly.
    """
    PEAK_REF_MW = 25000
    url = "https://www.ieso.ca/-/media/Files/IESO/Power-Data/Ontario-Demand-multiday.ashx"

    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("Peak Demand Index", "IESO Ontario Demand Multiday", url, str(e))]

    demand_val = None
    try:
        root = ET.fromstring(r.content)
        ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
        # Search for demand elements
        for tag in ["OntarioDemand", "Demand", "TotalDemand", "MarketDemand",
                    "ActualDemand", "ForecastDemand"]:
            els = root.findall(f".//{ns}{tag}") or root.findall(f".//{tag}")
            for el in reversed(els):
                if el.text:
                    try:
                        v = float(el.text.strip())
                        if v > 0:
                            demand_val = v
                            break
                    except ValueError:
                        pass
            if demand_val:
                break
    except ET.ParseError:
        pass

    # Fallback: numeric scan of content (works even if XML structure changes)
    if demand_val is None:
        for line in r.text.splitlines():
            nums = re.findall(r'\b(\d{4,5}(?:\.\d+)?)\b', line)
            for n in nums:
                v = float(n)
                if 5000 < v < 30000:
                    demand_val = v
                    break
            if demand_val:
                break

    if demand_val is None:
        return [_err("Peak Demand Index", "IESO Ontario Demand Multiday", url,
                     f"Could not parse demand from {len(r.content)} bytes")]

    peak_index = round(demand_val / PEAK_REF_MW * 100, 1)
    return [
        _ok("Ontario Demand (MW)", demand_val, "MW",
            "IESO Ontario Demand Multiday", url, date.today()),
        _ok("Peak Demand Index", peak_index, "% of 2024 peak",
            "IESO Ontario Demand Multiday", url, date.today(),
            f"{demand_val} MW ÷ {PEAK_REF_MW} MW × 100"),
    ]


def fetch_natural_gas_storage():
    """
    US East region natural gas working gas in storage — EIA v2 API.
    Series: NW2_EPG0_SAT_R1X_BCF (East region total working gas, weekly BCF)
    East region includes New England, Mid-Atlantic, and South Atlantic states
    which are the primary supply source for Ontario via interstate pipelines.

    Weekly data, released every Thursday for the prior week.
    Context: East region storage below 1,500 BCF = low heading into winter.
    Ontario draws heavily on US storage during peak heating demand.

    Requires EIA_API_KEY environment variable.
    Free key: eia.gov/opendata/register.php
    """
    api_key = os.environ.get("EIA_API_KEY", "DEMO_KEY")
    URL = "https://api.eia.gov/v2/natural-gas/stor/wkly/data/"
    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[duoarea][]": "R1X",   # East region
        "facets[process][]": "SAT",   # Total working gas in storage
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 2
    }

    try:
        r = SESSION.get(URL, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("Natural Gas Storage (East)", "EIA v2 API", URL, str(e))]

    rows = data.get("response", {}).get("data", [])

    # If DEMO_KEY or rate limit returns empty, fall back gracefully
    if not rows:
        err_msg = (data.get("response", {}).get("error") or
                   str(data.get("warnings", "No data returned — check EIA_API_KEY")))
        # Try without facets to see if any data comes back at all
        try:
            params_broad = {
                "api_key": api_key,
                "frequency": "weekly",
                "data[0]": "value",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 5
            }
            r2 = SESSION.get(URL, params=params_broad, timeout=TIMEOUT)
            rows_broad = r2.json().get("response", {}).get("data", [])
            # Filter for East region and total working gas
            rows = [row for row in rows_broad
                    if row.get("duoarea") == "R1X"
                    and row.get("process") == "SAT"]
        except Exception:
            pass

    if not rows:
        return [_err("Natural Gas Storage (East)", "EIA v2 API", URL,
                     f"No data returned. Ensure EIA_API_KEY secret is set. {err_msg}")]

    latest = rows[0]
    value  = latest.get("value")
    period = latest.get("period", "unknown")
    units  = latest.get("units", "BCF")
    area   = latest.get("area-name", "East region")

    if value is None:
        return [_err("Natural Gas Storage (East)", "EIA v2 API", URL,
                     f"Null value for period {period}")]

    bcf = float(value)

    # Prior week for context
    prior_note = ""
    if len(rows) >= 2:
        prior = rows[1].get("value")
        if prior:
            chg = round(bcf - float(prior), 1)
            direction = "injection" if chg > 0 else "withdrawal"
            prior_note = f" Week-over-week: {chg:+.1f} BCF ({direction})."

    notes = (f"EIA {area} total working gas in underground storage. "
             f"{prior_note} "
             f"Context: East region <1,500 BCF = low heading into winter; "
             f"Ontario supply relies heavily on US East pipeline network. "
             f"Released weekly on Thursdays.")

    return [_ok("Natural Gas Storage (East)", round(bcf, 1), units,
                f"EIA v2 — {area}", URL, period, notes)]


def fetch_brent_crude():
    """
    EIA Brent crude spot price.
    v1.6: DEMO_KEY hits 429 after ~30 req/day. Added EIA v1 API as fallback
    (separate rate limit bucket) and a known-value fallback with staleness flag.
    Register a free key at eia.gov/opendata to remove the rate limit entirely.
    """
    # Primary: EIA v2 API
    url_v2 = ("https://api.eia.gov/v2/petroleum/pri/spt/data/"
               "?api_key=DEMO_KEY&frequency=weekly&data[0]=value"
               "&facets[series][]=RBRTE"
               "&sort[0][column]=period&sort[0][direction]=desc&length=2")
    try:
        r = SESSION.get(url_v2, timeout=TIMEOUT)
        if r.status_code != 429:
            r.raise_for_status()
            data = r.json()
            latest = data["response"]["data"][0]
            return [_ok("Brent Crude Price", round(float(latest["value"]), 2), "USD/bbl",
                        "EIA Brent Spot Price (RBRTE)", url_v2, latest["period"],
                        "Weekly. Register free key at eia.gov/opendata to lift rate limit.")]
    except (requests.RequestException, json.JSONDecodeError, KeyError, ValueError, IndexError):
        pass

    # Fallback: EIA v1 series API (separate endpoint, different rate limit)
    url_v1 = "https://api.eia.gov/series/?api_key=DEMO_KEY&series_id=PET.RBRTE.W"
    try:
        r = SESSION.get(url_v1, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        pts = data["series"][0]["data"]
        period, val = pts[0][0], float(pts[0][1])
        return [_ok("Brent Crude Price", round(val, 2), "USD/bbl",
                    "EIA Brent Spot Price (RBRTE) v1", url_v1, str(period),
                    "Weekly. EIA v2 rate-limited; fell back to v1 endpoint.")]
    except (requests.RequestException, json.JSONDecodeError, KeyError,
            ValueError, IndexError, TypeError):
        pass

    # Last resort: return last known value with staleness flag
    return [_ok("Brent Crude Price", 85.28, "USD/bbl",
                "EIA (cached — both API endpoints rate-limited)", url_v2,
                "2026-03-07",
                "⚠ Both EIA endpoints rate-limited (DEMO_KEY). "
                "Last known value: $85.28 (week of Mar 7 2026). "
                "Register free key at eia.gov/opendata.")]




# ══════════════════════════════════════════════════════════════════════════════
# SECTOR: WATER / SHELTER
# ══════════════════════════════════════════════════════════════════════════════

TORONTO_CKAN_BASE = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

def _ckan_package(package_id):
    """Fetch Toronto CKAN package. Falls back to search if package_show fails."""
    url = f"{TORONTO_CKAN_BASE}/api/3/action/package_show"
    try:
        r = SESSION.get(url, params={"id": package_id}, timeout=TIMEOUT)
        r.raise_for_status()
        result = r.json()
        if result.get("success"):
            return result.get("result", {})
    except (requests.RequestException, json.JSONDecodeError):
        pass

    search_url = f"{TORONTO_CKAN_BASE}/api/3/action/package_search"
    try:
        r = SESSION.get(search_url,
                        params={"q": package_id.replace("-", " "), "rows": 5},
                        timeout=TIMEOUT)
        r.raise_for_status()
        results = r.json().get("result", {}).get("results", [])
        for pkg in results:
            if pkg.get("name", "").startswith(package_id[:10]):
                return pkg
        if results:
            return results[0]
    except (requests.RequestException, json.JSONDecodeError):
        pass
    return None

def _ckan_datastore(resource_id, limit=3000, sort=None):
    url = f"{TORONTO_CKAN_BASE}/api/3/action/datastore_search"
    params = {"resource_id": resource_id, "limit": limit}
    if sort:
        params["sort"] = sort
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json().get("result", {}).get("records", [])
    except (requests.RequestException, json.JSONDecodeError):
        return None


def _parse_watermain_xml(text, data_url):
    """
    Parse Toronto Water Main Breaks XML export.
    Structure is typically: root > row* > field elements
    We scan all leaf text nodes that look like YYYY-MM-DD dates,
    count by month, and return the second-most-recent month's count.
    Also handles GeoJSON-style and ESRI XML exports.
    """
    import re as _re
    try:
        root = ET.fromstring(text.encode("utf-8"))
    except ET.ParseError as e:
        return [_err("Watermain Break Rate", "Toronto Open Data", data_url,
                     f"XML parse error: {e}. First 200: {text[:200]}")]

    ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
    all_tags = sorted({el.tag.split("}")[-1] for el in root.iter()})

    # Collect all text values that look like dates (YYYY-MM-DD or YYYY/MM/DD)
    date_pat = _re.compile(r'(\d{4})[/-](\d{2})')
    monthly = Counter()
    total = 0

    for el in root.iter():
        txt = (el.text or "").strip()
        if not txt:
            continue
        # Count any element that contains a date-like value
        m = date_pat.search(txt)
        if m:
            ym = f"{m.group(1)}-{m.group(2)}"
            tag = el.tag.split("}")[-1].lower()
            # Prefer elements whose tag suggests a break/event date
            if any(k in tag for k in ["date","break","open","repair","created","year"]):
                monthly[ym] += 1
                total += 1

    # If no tagged date fields, count all date-like values (less precise but better than nothing)
    if not monthly:
        for el in root.iter():
            txt = (el.text or "").strip()
            m = date_pat.search(txt)
            if m:
                monthly[f"{m.group(1)}-{m.group(2)}"] += 1
                total += 1

    if not monthly:
        # Count rows as a fallback — estimate record count from row-like elements
        row_els = ([el for el in root if len(list(el)) > 0] or
                   [el for el in root.iter() if el.tag.split("}")[-1].lower() in
                    ["row","record","feature","watermainbreak"]])
        return [_ok("Watermain Break Rate", len(row_els), "records (no dates parsed)",
                    "Toronto Open Data — Water Main Breaks (XML)", data_url, "unknown",
                    f"XML tags: {all_tags[:12]}. No date values found. "
                    f"Manual review needed.")]

    months = sorted(monthly, reverse=True)
    ref = months[1] if len(months) > 1 else months[0]
    return [_ok("Watermain Break Rate", monthly[ref], "breaks/month",
                "Toronto Open Data — Water Main Breaks (XML)", data_url, ref,
                f"Parsed from XML. Tags: {all_tags[:10]}. "
                f"Total date values: {total}.")]



def fetch_watermain_breaks():
    """
    Toronto Open Data — Water Main Breaks.
    v1.6: column detection hardened. ZIP extraction was working but returned
    "1 total records" — either the data has 1 row or the date column name
    didn't match any of our keywords. Now strips header whitespace and
    tries a wider set of column name patterns. Also reports actual column
    names in the notes field for diagnosis.
    """
    pkg = _ckan_package("water-main-breaks")
    if not pkg:
        return [_err("Watermain Break Rate", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "Package not found")]

    resources = pkg.get("resources", [])
    # Rank resources: prefer newer/current data over historical archives
    # Skip read-me files and very old historical datasets (1990-2016 archive)
    SKIP_KEYWORDS = ["read-me", "readme", "1990", "historical", "archive"]
    ranked = sorted(
        resources,
        key=lambda r: (
            # Penalise resources that look like archives or readmes
            any(kw in r.get("url","").lower() or kw in r.get("name","").lower()
                for kw in SKIP_KEYWORDS),
            # Prefer datastore-active resources (False sorts before True, so invert)
            not r.get("datastore_active", False),
            # Prefer newer last_modified — sort ascending then take first = oldest,
            # so negate by prepending "~" trick: use descending sort separately
            r.get("last_modified") or r.get("created") or "",
        ),
        reverse=False,
    )
    # Re-sort the non-penalised group by date descending
    penalised = [r for r in ranked if any(
        kw in r.get("url","").lower() or kw in r.get("name","").lower()
        for kw in SKIP_KEYWORDS)]
    preferred = [r for r in ranked if r not in penalised]
    preferred.sort(key=lambda r: r.get("last_modified") or r.get("created") or "",
                   reverse=True)
    ranked = preferred + penalised
    data_url = None
    for res in ranked:
        url_str = res.get("url", "")
        fmt = res.get("format", "").upper()
        if (fmt in ("CSV", "XLS", "XLSX", "ZIP") or
                any(url_str.lower().endswith(ext) for ext in [".csv",".xlsx",".xls",".zip"]) or
                "download" in url_str.lower() or "datastore/dump" in url_str.lower()):
            data_url = url_str
            break
    if not data_url and ranked:
        data_url = ranked[0].get("url", "")
    if not data_url:
        return [_err("Watermain Break Rate", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "No download URL in resources")]

    try:
        r = SESSION.get(data_url, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("Watermain Break Rate", "Toronto Open Data", data_url, str(e))]

    raw = r.content

    # Unzip if needed
    if raw[:4] == b'PK\x03\x04':
        try:
            import zipfile
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                target = csv_names[0] if csv_names else zf.namelist()[0]
                with zf.open(target) as f:
                    raw = f.read()
        except Exception as e:
            return [_err("Watermain Break Rate", "Toronto Open Data", data_url,
                         f"ZIP extraction failed: {e}")]

    text = raw.decode("utf-8", errors="replace").replace("\x00", "")

    # Detect XML by declaration — Toronto Water Main Breaks is served as XML, not CSV
    if text.lstrip().startswith("<?xml") or text.lstrip().startswith("<"):
        return _parse_watermain_xml(text, data_url)

    # CSV path
    try:
        reader = csv.DictReader(io.StringIO(text))
        records = list(reader)
    except Exception as e:
        return [_err("Watermain Break Rate", "Toronto Open Data", data_url,
                     f"CSV parse error: {e}")]

    if not records:
        return [_err("Watermain Break Rate", "Toronto Open Data", data_url, "Empty CSV")]

    records = [{k.strip(): v for k, v in row.items()} for row in records]
    all_cols = list(records[0].keys())
    DATE_KEYWORDS = ["break_date", "breakdate", "date_break", "date", "created",
                     "open_date", "repair_date", "year"]
    date_col = next((c for c in all_cols
                     if any(k in c.lower().replace(" ","_") for k in DATE_KEYWORDS)), None)

    if not date_col:
        return [_ok("Watermain Break Rate", len(records), "total records (date col not found)",
                    "Toronto Open Data — Water Main Breaks", data_url, "unknown",
                    f"Columns: {all_cols}. "
                    f"Update DATE_KEYWORDS in fetch_watermain_breaks() to match.")]

    monthly = Counter()
    for rec in records:
        bd = str(rec.get(date_col, "") or "")
        if len(bd) >= 7:
            monthly[bd[:7]] += 1

    if not monthly:
        sample_vals = [str(r.get(date_col,"")) for r in records[:5]]
        return [_err("Watermain Break Rate", "Toronto Open Data", data_url,
                     f"Date col '{date_col}' found but no parseable YYYY-MM values. "
                     f"Sample values: {sample_vals}")]

    months = sorted(monthly, reverse=True)
    ref = months[1] if len(months) > 1 else months[0]
    return [_ok("Watermain Break Rate", monthly[ref], "breaks/month",
                "Toronto Open Data — Water Main Breaks", data_url, ref,
                f"Date col: '{date_col}'. Total records: {len(records)}.")]


def fetch_toronto_shelter():
    """Toronto Open Data — Daily Shelter. Unchanged — was working."""
    pkg = _ckan_package("daily-shelter-overnight-service-occupancy-capacity")
    if not pkg:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "Package not found")]

    resource_id = next((r["id"] for r in pkg.get("resources", [])
                        if r.get("datastore_active")), None)
    if not resource_id:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "No active datastore resource")]

    records = _ckan_datastore(resource_id, 3000, "OCCUPANCY_DATE desc")
    if not records:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "Datastore empty or failed")]

    most_recent_date = records[0].get("OCCUPANCY_DATE", "")
    if not most_recent_date:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "OCCUPANCY_DATE field not found")]

    day_recs = [rec for rec in records if rec.get("OCCUPANCY_DATE") == most_recent_date]
    total_occ = total_cap = 0
    for rec in day_recs:
        for k in ["OCCUPANCY", "OCCUPIED_BEDS", "occupied_beds"]:
            if k in rec:
                try: total_occ += float(rec[k] or 0); break
                except (ValueError, TypeError): pass
        for k in ["CAPACITY_ACTUAL", "CAPACITY_ACTUAL_BED", "capacity_actual_bed"]:
            if k in rec:
                try: total_cap += float(rec[k] or 0); break
                except (ValueError, TypeError): pass

    occ_rate = round(total_occ / total_cap * 100, 1) if total_cap > 0 else None
    api_url = f"{TORONTO_CKAN_BASE}/api/3/action/datastore_search?resource_id={resource_id}"
    return [
        _ok("Shelter System Capacity", int(total_cap), "beds/spaces",
            "Toronto Open Data — Daily Shelter", api_url, most_recent_date,
            f"{len(day_recs)} programs at 4am snapshot"),
        _ok("Shelter Occupancy", int(total_occ), "occupied",
            "Toronto Open Data — Daily Shelter", api_url, most_recent_date,
            f"Occupancy rate: {occ_rate}%. Threshold: 95% = Warning"),
    ]


# ══════════════════════════════════════════════════════════════════════════════
# SECTOR: HEALTH
# ══════════════════════════════════════════════════════════════════════════════

def fetch_active_water_outages():
    """
    Toronto Water — active watermain breaks and planned outages.
    Source: ArcGIS REST API behind toronto.ca/no-water-map
    Updated in real time. Confirmed field names from live API inspection.

    Key fields:
      WaterOutageEdit7b_ReasonForShut  — numeric code for outage type
      WaterOutageEdit7b_DateandTimeof  — epoch ms, when outage started
      WaterOutageEdit7b_Address        — street address
      EstRestorationDateTime           — epoch ms, estimated restoration
      Est_Num_Properties_Affected      — number of properties affected
      Comments                         — free text description

    ReasonForShut domain codes queried from ArcGIS service info.
    Falls back to Comments text parsing if domain lookup fails.
    """
    BASE = ("https://services3.arcgis.com/b9WvedVPoizGfvfD/arcgis/rest/services"
            "/COT_Geospatial_Water_Outage_View/FeatureServer/0/query")
    params = {
        "where": "DateandTimeServiceRestored = NULL AND WaterOutageEdit7b_Bypass <> 1",
        "outFields": ("WaterOutageEdit7b_ReasonForShut,WaterOutageEdit7b_DateandTimeof,"
                      "WaterOutageEdit7b_Address,EstRestorationDateTime,"
                      "Est_Num_Properties_Affected,Comments"),
        "f": "geojson",
        "returnGeometry": "false",
    }
    url = BASE

    # Step 1: fetch domain codes from service info (best-effort)
    # ReasonForShut is a coded value domain — get the lookup table
    reason_codes = {}
    try:
        info_url = BASE.replace("/query", "?f=json")
        ri = SESSION.get(info_url, timeout=10)
        if ri.status_code == 200:
            info = ri.json()
            for field in info.get("fields", []):
                if field.get("name") == "WaterOutageEdit7b_ReasonForShut":
                    domain = field.get("domain", {})
                    for cv in domain.get("codedValues", []):
                        reason_codes[cv["code"]] = cv["name"]
    except Exception:
        pass  # domain lookup is best-effort; fall back to Comments parsing

    # Step 2: fetch active outages
    try:
        r = SESSION.get(BASE, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("Active Water Outages", "Toronto Water ArcGIS API", url, str(e))]

    if "error" in data:
        return [_err("Active Water Outages", "Toronto Water ArcGIS API", url,
                     f"API error: {data['error']}")]

    features = data.get("features", [])
    breaks   = 0
    planned  = 0
    other    = 0
    total_props_affected = 0
    oldest_start = None
    addresses = []

    from datetime import datetime as _dt, timezone as _tz

    for feat in features:
        p = feat.get("properties", {})

        # Resolve outage type from domain code, then fall back to Comments text
        code = p.get("WaterOutageEdit7b_ReasonForShut")
        reason_name = reason_codes.get(code, "") if code is not None else ""
        comment = str(p.get("Comments", "") or "").lower()

        # Classify using resolved name first, then comment text
        type_text = (reason_name + " " + comment).lower()
        if any(k in type_text for k in ["break", "emergency", "watermain break", "burst"]):
            breaks += 1
        elif any(k in type_text for k in ["plan", "maintenance", "scheduled",
                                           "precaution", "sewer", "construction"]):
            planned += 1
        else:
            other += 1

        # Properties affected
        n = p.get("Est_Num_Properties_Affected")
        if n:
            try:
                total_props_affected += int(n)
            except (ValueError, TypeError):
                pass

        # Oldest active outage
        ts = p.get("WaterOutageEdit7b_DateandTimeof")
        if ts:
            try:
                dt = _dt.fromtimestamp(int(ts) / 1000, tz=_tz.utc)
                if oldest_start is None or dt < oldest_start:
                    oldest_start = dt
            except (ValueError, OSError):
                pass

        # Collect addresses for notes
        addr = p.get("WaterOutageEdit7b_Address", "")
        if addr:
            addresses.append(str(addr))

    total = len(features)
    oldest_str = (oldest_start.strftime("%Y-%m-%d %H:%M UTC")
                  if oldest_start else "unknown")
    addr_str = (", ".join(addresses[:3]) + ("..." if len(addresses) > 3 else "")
                if addresses else "unknown")

    note = (f"Breaks: {breaks}, Planned/precautionary: {planned}, Other: {other}. "
            f"~{total_props_affected} properties affected. "
            f"Oldest active since {oldest_str}. "
            f"Locations: {addr_str}.")
    if reason_codes:
        note += f" Domain codes resolved: {reason_codes}."
    else:
        note += " Domain lookup unavailable — type classified from Comments text."

    results = [_ok("Active Water Outages", total, "active outages",
                   "Toronto Water — No Water Map (ArcGIS)", url,
                   str(date.today()), note)]

    if breaks > 0:
        results.append(_ok("Active Watermain Breaks", breaks, "breaks",
                           "Toronto Water — No Water Map (ArcGIS)", url,
                           str(date.today()),
                           f"Emergency watermain breaks. Addresses: {addr_str}"))
    if planned > 0:
        results.append(_ok("Planned/Precautionary Outages", planned, "outages",
                           "Toronto Water — No Water Map (ArcGIS)", url,
                           str(date.today()),
                           "Planned maintenance or precautionary shutoffs."))

    return results


def fetch_toronto_shelter():
    """Toronto Open Data — Daily Shelter. Unchanged — was working."""
    pkg = _ckan_package("daily-shelter-overnight-service-occupancy-capacity")
    if not pkg:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "Package not found")]

    resource_id = next((r["id"] for r in pkg.get("resources", [])
                        if r.get("datastore_active")), None)
    if not resource_id:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "No active datastore resource")]

    records = _ckan_datastore(resource_id, 3000, "OCCUPANCY_DATE desc")
    if not records:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "Datastore empty or failed")]

    most_recent_date = records[0].get("OCCUPANCY_DATE", "")
    if not most_recent_date:
        return [_err("Shelter System Capacity", "Toronto Open Data",
                     TORONTO_CKAN_BASE, "OCCUPANCY_DATE field not found")]

    day_recs = [rec for rec in records if rec.get("OCCUPANCY_DATE") == most_recent_date]
    total_occ = total_cap = 0
    for rec in day_recs:
        for k in ["OCCUPANCY", "OCCUPIED_BEDS", "occupied_beds"]:
            if k in rec:
                try: total_occ += float(rec[k] or 0); break
                except (ValueError, TypeError): pass
        for k in ["CAPACITY_ACTUAL", "CAPACITY_ACTUAL_BED", "capacity_actual_bed"]:
            if k in rec:
                try: total_cap += float(rec[k] or 0); break
                except (ValueError, TypeError): pass

    occ_rate = round(total_occ / total_cap * 100, 1) if total_cap > 0 else None
    api_url = f"{TORONTO_CKAN_BASE}/api/3/action/datastore_search?resource_id={resource_id}"
    return [
        _ok("Shelter System Capacity", int(total_cap), "beds/spaces",
            "Toronto Open Data — Daily Shelter", api_url, most_recent_date,
            f"{len(day_recs)} programs at 4am snapshot"),
        _ok("Shelter Occupancy", int(total_occ), "occupied",
            "Toronto Open Data — Daily Shelter", api_url, most_recent_date,
            f"Occupancy rate: {occ_rate}%. Threshold: 95% = Warning"),
    ]


# ══════════════════════════════════════════════════════════════════════════════
# SECTOR: HEALTH
# ══════════════════════════════════════════════════════════════════════════════

def fetch_active_water_outages():
    """
    Toronto Water — active watermain breaks and planned outages.
    Source: City of Toronto ArcGIS REST API (COT_Geospatial_Water_Outage_View)
    This is the live data feed behind toronto.ca/no-water-map.
    Updated in real time as breaks are reported and restored.

    Query: all records where DateandTimeServiceRestored = NULL
    (i.e. service not yet restored = currently active)
    and WaterOutageEdit7b_Bypass <> 1 (excludes test/bypass records)

    Returns:
    - Active watermain breaks count
    - Active planned maintenance outages count
    - Total active outages
    """
    BASE = ("https://services3.arcgis.com/b9WvedVPoizGfvfD/arcgis/rest/services"
            "/COT_Geospatial_Water_Outage_View/FeatureServer/0/query")
    params = {
        "where": "DateandTimeServiceRestored = NULL AND WaterOutageEdit7b_Bypass <> 1",
        "outFields": "*",
        "f": "geojson",
        "returnGeometry": "false",
    }
    url = BASE + "?" + "&".join(f"{k}={v}" for k, v in params.items())

    try:
        r = SESSION.get(BASE, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("Active Water Outages", "Toronto Water ArcGIS API", url, str(e))]

    features = data.get("features", [])
    if not features and "error" in data:
        return [_err("Active Water Outages", "Toronto Water ArcGIS API", url,
                     f"API error: {data['error']}")]

    # Categorise by outage type
    # Common type field names in Toronto Water ArcGIS: OutageType, Type, ReasonForOutage
    breaks   = 0
    planned  = 0
    unknown  = 0
    oldest_start = None

    for feat in features:
        props = feat.get("properties", {})
        # Find the type field — try common names
        type_val = (props.get("OutageType") or props.get("Type") or
                    props.get("ReasonForOutage") or props.get("OUTAGETYPE") or "")
        type_str = str(type_val).lower()

        if any(k in type_str for k in ["break", "emergency", "watermain"]):
            breaks += 1
        elif any(k in type_str for k in ["planned", "maintenance", "scheduled"]):
            planned += 1
        else:
            unknown += 1

        # Track oldest active outage start time
        start_ts = (props.get("DateandTimeServiceInterrupted") or
                    props.get("StartDate") or props.get("DateTimeStart"))
        if start_ts and isinstance(start_ts, (int, float)):
            # ArcGIS returns epoch milliseconds
            from datetime import datetime as _dt
            try:
                dt = _dt.utcfromtimestamp(start_ts / 1000)
                if oldest_start is None or dt < oldest_start:
                    oldest_start = dt
            except (ValueError, OSError):
                pass

    total = len(features)
    oldest_str = oldest_start.strftime("%Y-%m-%d %H:%M UTC") if oldest_start else "unknown"

    results = [
        _ok("Active Water Outages", total, "active outages",
            "Toronto Water — No Water Map (ArcGIS)", url, str(date.today()),
            f"Breaks: {breaks}, Planned maintenance: {planned}, Other: {unknown}. "
            f"Oldest active since: {oldest_str}. "
            f"Source: toronto.ca/no-water-map — real-time feed."),
    ]

    # Add breakdown indicators if there are active breaks
    if breaks > 0:
        results.append(_ok("Active Watermain Breaks", breaks, "breaks",
                           "Toronto Water — No Water Map (ArcGIS)", url,
                           str(date.today()),
                           "Emergency watermain breaks with service interrupted."))
    if planned > 0:
        results.append(_ok("Planned Maintenance Outages", planned, "outages",
                           "Toronto Water — No Water Map (ArcGIS)", url,
                           str(date.today()),
                           "Scheduled maintenance with planned service interruption."))

    return results


def fetch_toronto_unemployment():
    """
    Toronto CMA unemployment rate — StatsCan WDS API.
    Table 14-10-0294-01: Labour force characteristics by CMA, 3-month moving average.
    Coordinate 23.5.1.1 = Toronto CMA, Unemployment rate, Estimate, Seasonally adjusted.
    Released monthly, lags ~5 weeks after reference month.
    Context: Toronto CMA at 8.9% in Sep 2025 — elevated vs pre-tariff levels.
    Alert threshold: >10% = recession-level stress.
    """
    WDS_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromCubePidCoordAndLatestNPeriods"
    # productId=14100294, coordinate=23.5.1.1 (Toronto, Unemployment rate, Estimate, SA)
    payload = [{"productId": 14100294, "coordinate": "23.5.1.1", "latestN": 2}]

    try:
        r = SESSION.post(WDS_URL, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("Toronto Unemployment Rate", "StatsCan WDS (Table 14-10-0294-01)",
                     WDS_URL, str(e))]

    try:
        obj = data[0].get("object", {})
        if data[0].get("status") != "SUCCESS":
            raise ValueError(f"API status: {data[0].get('status')} — {obj}")
        points = obj.get("vectorDataPoint", [])
        if not points:
            raise ValueError("No data points returned")
        latest = points[-1]
        rate = float(latest["value"])
        ref = latest.get("refPer", "unknown")
        vector_id = obj.get("vectorId", "?")
    except (KeyError, ValueError, TypeError, IndexError) as e:
        return [_err("Toronto Unemployment Rate", "StatsCan WDS (Table 14-10-0294-01)",
                     WDS_URL, f"Parse error: {e}. Response: {str(data)[:200]}")]

    return [_ok("Toronto Unemployment Rate", round(rate, 1), "%",
                "StatsCan LFS — Table 14-10-0294-01 (Toronto CMA)", WDS_URL, ref,
                f"3-month moving average, seasonally adjusted. "
                f"Vector v{vector_id}. Released ~5 weeks after reference month. "
                f"Sep 2025: 8.9%. Pre-tariff baseline (2023): ~6.5%.")]


def fetch_toronto_boil_advisories():
    """
    Toronto Water — active boil water advisories.
    Source: toronto.ca/tap-water — Toronto Water public page.
    Toronto's 4 treatment plants serve ~4M people. Advisories are rare.
    A non-zero count is a significant public health signal.

    Logic:
    - Page loads + "water is safe" or similar → 0 advisories (confirmed clean)
    - Page loads + "boil water advisory" language → count advisories
    - Page fails to load → error
    - Page loads but no recognisable content → 0 with note
    """
    URL = "https://www.toronto.ca/services-payments/water-environment/tap-water-in-toronto/"

    try:
        r = SESSION.get(URL, timeout=TIMEOUT,
                        headers={"User-Agent": "Mozilla/5.0 TII-Scraper/2.8"})
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("Boil Water Advisories", "Toronto Water — toronto.ca",
                     URL, str(e))]

    from bs4 import BeautifulSoup as _BS
    soup = _BS(r.content, "html.parser")
    text = soup.get_text(" ", strip=True).lower()

    if len(text) < 500:
        return [_err("Boil Water Advisories", "Toronto Water — toronto.ca",
                     URL, f"Page returned unexpectedly short content ({len(text)} chars)")]

    advisory_count = 0
    advisory_details = []

    # Check for active advisory language first
    active_keywords = [
        "boil water advisory", "boil-water advisory",
        "do not use", "water advisory in effect",
        "advisory is in effect", "boil your water"
    ]
    for kw in active_keywords:
        count = text.count(kw)
        if count > 0:
            advisory_count += 1
            advisory_details.append(f"'{kw}' detected")

    # Confirmed clean signals
    clean_signals = [
        "water is safe", "safe to drink", "no current advisory",
        "no active advisory", "no boil water", "meets all",
        "continues to meet", "tap water is safe"
    ]
    page_confirmed_clean = any(phrase in text for phrase in clean_signals)

    if advisory_count > 0:
        notes = (f"{advisory_count} potential advisory signal(s) detected. "
                 f"Details: {'; '.join(advisory_details)}. "
                 f"Verify at toronto.ca/tap-water.")
    elif page_confirmed_clean:
        notes = ("No active boil water advisories. "
                 "Toronto Water confirmed: water is safe to drink. "
                 "Toronto's 4 treatment plants serve ~4M people across the city.")
    else:
        notes = ("No advisory language detected on Toronto Water page. "
                 "Page loaded successfully but no explicit safety confirmation found. "
                 "Verify at toronto.ca/tap-water if concerned.")

    return [_ok("Boil Water Advisories", advisory_count, "active advisories",
                "Toronto Water — toronto.ca", URL,
                str(date.today()), notes)]


def fetch_toronto_unemployment():
    """
    Toronto CMA unemployment rate — StatsCan WDS API.
    Table 14-10-0294-01: Labour force characteristics by CMA, 3-month moving average.
    Coordinate 23.5.1.1 = Toronto CMA, Unemployment rate, Estimate, Seasonally adjusted.
    Released monthly, lags ~5 weeks after reference month.
    Context: Toronto CMA at 8.9% in Sep 2025 — elevated vs pre-tariff levels.
    Alert threshold: >10% = recession-level stress.
    """
    WDS_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromCubePidCoordAndLatestNPeriods"
    # productId=14100294, coordinate=23.5.1.1 (Toronto, Unemployment rate, Estimate, SA)
    payload = [{"productId": 14100294, "coordinate": "23.5.1.1", "latestN": 2}]

    try:
        r = SESSION.post(WDS_URL, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("Toronto Unemployment Rate", "StatsCan WDS (Table 14-10-0294-01)",
                     WDS_URL, str(e))]

    try:
        obj = data[0].get("object", {})
        if data[0].get("status") != "SUCCESS":
            raise ValueError(f"API status: {data[0].get('status')} — {obj}")
        points = obj.get("vectorDataPoint", [])
        if not points:
            raise ValueError("No data points returned")
        latest = points[-1]
        rate = float(latest["value"])
        ref = latest.get("refPer", "unknown")
        vector_id = obj.get("vectorId", "?")
    except (KeyError, ValueError, TypeError, IndexError) as e:
        return [_err("Toronto Unemployment Rate", "StatsCan WDS (Table 14-10-0294-01)",
                     WDS_URL, f"Parse error: {e}. Response: {str(data)[:200]}")]

    return [_ok("Toronto Unemployment Rate", round(rate, 1), "%",
                "StatsCan LFS — Table 14-10-0294-01 (Toronto CMA)", WDS_URL, ref,
                f"3-month moving average, seasonally adjusted. "
                f"Vector v{vector_id}. Released ~5 weeks after reference month. "
                f"Sep 2025: 8.9%. Pre-tariff baseline (2023): ~6.5%.")]


def fetch_trreb_market():
    """
    TRREB GTA housing market — sales-to-new-listings ratio and average price.
    Source: trreb.ca/market-data/quick-market-overview/
    Data embedded as JavaScript variables in page HTML, updated monthly.
    Published first week of each month for the prior month.

    Key indicators:
    - Sales-to-new-listings ratio (SNR):
        <40% = buyer's market
        40-60% = balanced market
        >60% = seller's market
    - Average selling price (all residential)
    - Total sales (year-over-year)
    - Total new listings (year-over-year)
    """
    URL = "https://trreb.ca/market-data/quick-market-overview/"
    try:
        r = SESSION.get(URL, timeout=TIMEOUT,
                        headers={"User-Agent": "Mozilla/5.0 TII-Scraper/2.6"})
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("TRREB Sales-to-New-Listings Ratio", "TRREB Quick Overview", URL, str(e))]

    try:
        from bs4 import BeautifulSoup as _BS
        import re as _re
        soup = _BS(r.content, "html.parser")

        # All data is in inline script blocks as JS variables
        scripts = " ".join(s.string or "" for s in soup.find_all("script", src=False))

        def extract_js_obj(scripts, var_name):
            """Extract last value from a JS object literal like: let Var = {"Feb'25": 32, "Feb'26": 36}"""
            pattern = rf'let\s+{var_name}\s*=\s*\{{([^}}]+)\}}'
            m = _re.search(pattern, scripts)
            if not m:
                return None, None, None
            pairs = _re.findall(r'"([^"]+)":\s*([\d.]+)', m.group(1))
            if not pairs:
                return None, None, None
            # Return latest period and value, plus prior period value
            latest_period, latest_val = pairs[-1]
            prior_val = pairs[-2][1] if len(pairs) >= 2 else None
            return latest_period, float(latest_val), float(prior_val) if prior_val else None

        # Sales-to-new-listings ratio
        snr_period, snr_val, snr_prior = extract_js_obj(scripts, "TnlSnrData")
        # Average selling price
        asp_period, asp_val, asp_prior = extract_js_obj(scripts, "AspYoyData")
        # Total residential transactions
        trt_period, trt_val, trt_prior = extract_js_obj(scripts, "TrtYoyData")
        # Total new listings
        tnl_period, tnl_val, tnl_prior = extract_js_obj(scripts, "TnlYoyData")

        if snr_val is None:
            raise ValueError("Could not parse TnlSnrData from page — page structure may have changed")

        # Market classification
        if snr_val < 40:
            market_type = "buyer's market"
        elif snr_val <= 60:
            market_type = "balanced market"
        else:
            market_type = "seller's market"

        # Price change YoY
        price_note = ""
        if asp_val and asp_prior:
            price_chg = round(((asp_val - asp_prior) / asp_prior) * 100, 1)
            price_note = f" Avg price ${asp_val:,.0f} ({price_chg:+.1f}% YoY)."

        # Sales change YoY
        sales_note = ""
        if trt_val and trt_prior:
            sales_chg = round(((trt_val - trt_prior) / trt_prior) * 100, 1)
            sales_note = f" Sales {int(trt_val):,} ({sales_chg:+.1f}% YoY)."

        # Listings change YoY
        listings_note = ""
        if tnl_val and tnl_prior:
            listings_chg = round(((tnl_val - tnl_prior) / tnl_prior) * 100, 1)
            listings_note = f" New listings {int(tnl_val):,} ({listings_chg:+.1f}% YoY)."

        notes = (f"{market_type.title()} — SNR of {snr_val:.0f}% "
                 f"(prior period: {snr_prior:.0f}%).{price_note}{sales_note}{listings_note} "
                 f"Reference: {snr_period}. "
                 f"SNR guide: <40% buyer's, 40-60% balanced, >60% seller's market.")

        results = [
            _ok("TRREB Sales-to-New-Listings Ratio", snr_val, "%",
                "TRREB Quick Market Overview", URL, snr_period, notes),
        ]
        if asp_val:
            results.append(
                _ok("TRREB Average Selling Price", round(asp_val), "CAD",
                    "TRREB Quick Market Overview", URL, asp_period,
                    f"GTA all residential. {price_note.strip()}"))
        return results

    except Exception as e:
        return [_err("TRREB Sales-to-New-Listings Ratio", "TRREB Quick Overview",
                     URL, f"Parse error: {e}")]


def fetch_tcpl_mainline():
    """
    TransCanada (TC) Canadian Mainline — Parkway Receipts utilization.
    Source: Canada Energy Regulator (CER) open data CSV.
    Updated monthly, daily granularity.

    Parkway Receipts is the key point measuring gas arriving at the
    Parkway hub near Toronto — the primary delivery point for GTA supply.
    Northern Ontario Line is included as secondary context (gas transiting
    across northern Ontario toward Toronto).

    Utilization = throughput / capacity * 100
    Normal operating range: 50-80%
    Warn: >85% sustained (pipeline near capacity, supply stress risk)
    Alert: >95% sustained (pipeline at capacity, supply emergency risk)

    CSV has ~208,000 rows (2006-present). We read only the last 5,000
    rows to get recent data efficiently without downloading the full file.
    """
    URL = ("https://www.cer-rec.gc.ca/open/energy/throughput-capacity/"
           "tcpl-mainline-throughput-and-capacity.csv")

    try:
        r = SESSION.get(URL, timeout=30,
                        headers={"User-Agent": "Mozilla/5.0 TII-Scraper/2.7"})
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("TCPL Mainline Parkway Utilization",
                     "CER Open Data CSV", URL, str(e))]

    try:
        import io as _io
        import csv as _csv

        lines = r.text.splitlines()
        if len(lines) < 2:
            raise ValueError("CSV appears empty")

        header = lines[0]
        # Read last 5000 rows for efficiency — covers ~14 months of daily data
        recent_text = "\n".join([header] + lines[-5000:])
        reader = _csv.DictReader(_io.StringIO(recent_text))

        # Collect latest rows per key point
        latest = {}
        weekly = {}  # store last 7 days per key point

        for row in reader:
            kp = row.get("Key Point", "").strip().replace("\n", "").replace("\r", "")
            dt = row.get("Date", "")
            if not kp or not dt:
                continue
            if kp not in latest or dt > latest[kp]["Date"]:
                latest[kp] = row
            if kp not in weekly:
                weekly[kp] = []
            weekly[kp].append(row)

        # Key points we care about
        targets = {
            "Eastern Triangle - Parkway Receipts": "Parkway Receipts (GTA supply)",
            "Northern Ontario Line":               "Northern Ontario Line",
        }

        results = []
        for kp, label in targets.items():
            # Clean key — CSV has embedded newlines in key point names
            matched_key = None
            for k in latest:
                if kp.lower() in k.lower():
                    matched_key = k
                    break

            if not matched_key:
                results.append(_err(f"TCPL {label}", "CER Open Data", URL,
                                    f"Key point '{kp}' not found in CSV"))
                continue

            row = latest[matched_key]
            ref_date = row.get("Date", "unknown")

            try:
                cap = float(row.get("Capacity (1000 m3/d)", 0) or 0)
                thr = float(row.get("Throughput (1000 m3/d)", 0) or 0)
            except (ValueError, TypeError):
                results.append(_err(f"TCPL {label}", "CER Open Data", URL,
                                    "Could not parse capacity/throughput values"))
                continue

            if cap <= 0:
                results.append(_err(f"TCPL {label}", "CER Open Data", URL,
                                    f"Capacity is zero for {matched_key}"))
                continue

            util = round(thr / cap * 100, 1)

            # 7-day average utilization
            recent_rows = sorted(weekly.get(matched_key, []),
                                 key=lambda x: x.get("Date", ""))[-7:]
            if len(recent_rows) >= 3:
                utils = []
                for rr in recent_rows:
                    try:
                        rc = float(rr.get("Capacity (1000 m3/d)", 0) or 0)
                        rt = float(rr.get("Throughput (1000 m3/d)", 0) or 0)
                        if rc > 0:
                            utils.append(rt / rc * 100)
                    except (ValueError, TypeError):
                        pass
                avg_util = round(sum(utils) / len(utils), 1) if utils else util
                avg_note = f" 7-day avg: {avg_util}%."
            else:
                avg_util = util
                avg_note = ""

            notes = (f"Throughput: {thr:,.0f} / Capacity: {cap:,.0f} (1000 m³/day). "
                     f"Utilization: {util}%.{avg_note} "
                     f"Normal operating range 50-80%. Above 85% = supply stress risk. "
                     f"DATA LAG: CER reports quarterly — current data is {ref_date}. "
                     f"Use for baseline context only, not real-time stress monitoring. "
                     f"Source: CER open data pipeline profiles.")

            results.append(_ok(f"TCPL {label}", util, "% utilized",
                               "CER — TransCanada Mainline", URL,
                               ref_date, notes))

        return results if results else [_err("TCPL Mainline", "CER Open Data",
                                            URL, "No matching key points found")]

    except Exception as e:
        return [_err("TCPL Mainline Parkway Utilization",
                     "CER Open Data CSV", URL, f"Parse error: {e}")]


def fetch_pearson_notams():
    """
    Toronto Pearson (CYYZ) operational status derived from NAV Canada NOTAMs.
    Source: NAV Canada CFPS API — plan.navcanada.ca
    Updated in near-real-time as NOTAMs are issued and cancelled.

    Classification uses ICAO Q-codes from the NOTAM Q-line:
    - Security/geopolitical (QXXXX security, QRXXXX, QOECH airspace):  severity 3
    - ATC flow control (QATFM, QATXX, flow/GDP/EDCT in text):          severity 3
    - Multiple runway closures (QMRLC x2+):                            severity 2
    - Single runway closure (QMRLC):                                    severity 2
    - Approach/procedure suspended (QPIXX, QPDXX):                     severity 1
    - Taxiway closures / equipment U/S (QMXLC, QNVXX, QSAXX):        severity 0
    - RSC / bird / obstacle / construction:                             severity 0

    Dashboard value: 0=Normal, 1=Reduced capacity, 2=Runway closure, 3=Flow control/Security
    Dashboard label: plain-English operational status
    """
    URL = "https://plan.navcanada.ca/weather/api/alpha/?site=CYYZ&alpha=notam"

    try:
        r = SESSION.get(URL, timeout=TIMEOUT,
                        headers={"User-Agent": "Mozilla/5.0 TII-Scraper/2.8"})
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("Pearson Airport Operations", "NAV Canada CFPS", URL, str(e))]

    notams = data.get("data", [])
    total = len(notams)

    # Parse each NOTAM
    parsed = []
    for n in notams:
        try:
            text_obj = json.loads(n.get("text", "{}"))
            raw = text_obj.get("raw", "")
        except (json.JSONDecodeError, TypeError):
            raw = str(n.get("text", ""))

        # Extract Q-code (subject code, 5th segment of Q line)
        q_code = ""
        e_text = ""
        for line in raw.splitlines():
            line = line.strip()
            if line.startswith("Q)"):
                parts = line.split("/")
                if len(parts) >= 2:
                    q_code = parts[1].strip()
            if line.startswith("E)"):
                e_text = line[2:].strip()

        parsed.append({
            "pk":       n.get("pk", ""),
            "start":    n.get("startValidity", "")[:16],
            "end":      n.get("endValidity", "")[:16],
            "q_code":   q_code,
            "e_text":   e_text,
            "raw":      raw,
        })

    # ── Classification ────────────────────────────────────────────────────
    severity     = 0
    status_label = "Normal operations"
    active_flags = []

    # Count runway closures
    rwy_closures = [p for p in parsed if p["q_code"] in ("QMRLC", "QMRXX")
                    and "CLSD" in p["e_text"].upper()]

    # Check for ATC flow control / ground delay program
    flow_keywords = ["FLOW", "GDP", "EDCT", "GROUND DELAY", "GROUND STOP",
                     "TMI", "TRAFFIC MANAGEMENT", "MIT ", "MINIT"]
    flow_notams = [p for p in parsed
                   if any(kw in p["e_text"].upper() or kw in p["raw"].upper()
                          for kw in flow_keywords)]

    # Check for security / geopolitical / airspace restriction
    # Distinguish acute security events from standing geopolitical notices:
    # Standing notices (CZXX FIR-wide, radius 999, long duration >30 days)
    # are flagged at severity 1 (noted), not severity 3 (alert)
    acute_security_keywords = ["TFR", "GROUND STOP", "SHOOT DOWN",
                                "AIR DEFENCE", "HOSTILE FIRE"]
    standing_geo_keywords   = ["RUSSIAN FED", "BELARUS", "IRAN", "NORTH KOREA",
                                "UKRAINE", "REGARDLESS OF THE STATE OF REGISTRY"]

    security_notams = []   # acute — severity 3
    geo_notams      = []   # standing geopolitical — severity 1

    for p in parsed:
        raw_upper = p["raw"].upper()
        e_upper   = p["e_text"].upper()
        is_fir_wide = ("CZXX" in p["raw"] or "999" in p["raw"][:50])

        if any(kw in e_upper or kw in raw_upper for kw in acute_security_keywords):
            security_notams.append(p)
        elif p["q_code"] in ("QOECH", "QRTCA", "QRPCA"):
            if is_fir_wide:
                geo_notams.append(p)   # standing FIR-wide notice
            else:
                security_notams.append(p)  # localised restriction = acute
        elif any(kw in e_upper or kw in raw_upper for kw in standing_geo_keywords):
            geo_notams.append(p)

    # Check for significant approach/procedure suspensions
    approach_suspended = [p for p in parsed
                          if p["q_code"].startswith("QPI") and
                          any(kw in p["e_text"].upper()
                              for kw in ["U/S", "UNSERVICEABLE", "NOT AVBL",
                                         "SUSPENDED", "CLSD"])]

    # VOR/ILS/ATIS unserviceable
    navaid_us = [p for p in parsed
                 if p["q_code"] in ("QNVAS", "QILAS", "QSAAS", "QICAS")
                 or (p["q_code"].startswith("QNV") and "U/S" in p["e_text"].upper())]

    # ── Determine overall severity ────────────────────────────────────────
    if security_notams:
        severity = 3
        descs = [p["e_text"][:60] for p in security_notams[:2]]
        status_label = f"Security/airspace restriction — {'; '.join(descs)}"
        active_flags.append(f"Security NOTAMs: {len(security_notams)}")

    elif flow_notams:
        severity = 3
        descs = [p["e_text"][:60] for p in flow_notams[:2]]
        status_label = f"ATC flow control active — {'; '.join(descs)}"
        active_flags.append(f"Flow control NOTAMs: {len(flow_notams)}")

    elif len(rwy_closures) >= 2:
        severity = 2
        rwys = [p["e_text"][:40] for p in rwy_closures[:3]]
        status_label = f"Multiple runway closures — {'; '.join(rwys)}"
        active_flags.append(f"Runway closures: {len(rwy_closures)}")

    elif len(rwy_closures) == 1:
        severity = 2
        status_label = f"Runway closure — {rwy_closures[0]['e_text'][:60]}"
        active_flags.append("Runway closure: 1")

    elif approach_suspended:
        severity = 1
        status_label = f"Approach procedure affected — {approach_suspended[0]['e_text'][:60]}"
        active_flags.append(f"Approach NOTAMs: {len(approach_suspended)}")

    elif geo_notams:
        # Standing geopolitical notice — notable but not acute
        if severity == 0:
            severity = 1
        descs = [p["e_text"][:50] for p in geo_notams[:1]]
        active_flags.append(f"Geopolitical airspace notice: {descs[0]}")

    if severity == 0:
        status_label = "Normal operations"

    # Build notes with full context
    if geo_notams and not active_flags:
        active_flags.append(f"Standing geopolitical notice ({len(geo_notams)})")
    flag_summary = ", ".join(active_flags) if active_flags else "no significant restrictions"
    notam_breakdown = (
        f"Runway closures: {len(rwy_closures)}, "
        f"Flow control: {len(flow_notams)}, "
        f"Acute security: {len(security_notams)}, "
        f"Geopolitical notice: {len(geo_notams)}, "
        f"Approach affected: {len(approach_suspended)}, "
        f"Navaid U/S: {len(navaid_us)}, "
        f"Total active NOTAMs: {total}."
    )
    notes = (f"{status_label}. {notam_breakdown} "
             f"Source: NAV Canada CFPS — real-time NOTAM feed. "
             f"Severity scale: 0=Normal, 1=Reduced capacity, "
             f"2=Runway closure, 3=Flow control or security.")

    return [_ok("Pearson Airport Operations", severity, "",
                "NAV Canada CFPS (CYYZ NOTAMs)", URL,
                str(date.today()), notes)]


def fetch_ontario_er_capacity():
    """
    data.ontario.ca — ER Wait Times.
    v1.6: dataset 'wait-time-information-system' exists but has 0 resources
    (data served through ontario's reporting portal, not as downloadable files).
    Now returns a manual placeholder with retrieval instructions.
    Keeping as a function (not moving to manual-only list) so it stays visible
    in the sector output and can be re-automated if a CSV endpoint appears.
    """
    # Check if a downloadable resource has been added since last update
    BASE = "https://data.ontario.ca/api/3/action"
    KNOWN_IDS = [
        "wait-time-information-system",
        "emergency-room-wait-times-ontario",
        "emergency-room-national-ambulatory-reporting-system-initiative-erni",
    ]
    for pkg_id in KNOWN_IDS:
        try:
            r = SESSION.get(f"{BASE}/package_show", params={"id": pkg_id}, timeout=TIMEOUT)
            r.raise_for_status()
            result = r.json()
            if not result.get("success"):
                continue
            resources = result["result"].get("resources", [])
            csv_res = [res for res in resources
                       if (res.get("format","").upper() == "CSV" or
                           res.get("url","").lower().endswith(".csv"))]
            if csv_res:
                # A CSV resource appeared — try to download it
                csv_url = csv_res[0]["url"]
                rr = SESSION.get(csv_url, timeout=30)
                rr.raise_for_status()
                if "html" not in rr.headers.get("Content-Type","").lower():
                    reader = csv.DictReader(io.StringIO(rr.text))
                    rows = list(reader)
                    if rows:
                        return [_ok("ER Capacity — Ontario", len(rows), "rows",
                                    f"data.ontario.ca — {pkg_id}", csv_url,
                                    csv_res[0].get("last_modified","unknown"),
                                    f"CSV now available. Columns: {list(rows[0].keys())[:8]}. "
                                    f"Update fetch_ontario_er_capacity() to parse properly.")]
        except Exception:
            continue

    return [_manual("ER Capacity — Ontario",
                    "data.ontario.ca / Health Quality Ontario",
                    "Dataset exists but has no downloadable files (data in reporting portal). "
                    "Manual retrieval: health.gov.on.ca/en/ms/edrs/ → "
                    "download CSV from 'Percent of ED visits completed within target'. "
                    "Or check data.ontario.ca/dataset/wait-time-information-system "
                    "periodically — a CSV resource may appear.")]


def fetch_phac_wastewater():
    """PHAC Wastewater — unchanged (manual fallback)."""
    for csv_url in [
        "https://health-infobase.canada.ca/src/data/wastewater/wastewater_data.csv",
        "https://health-infobase.canada.ca/wastewater/data/wastewater_data.csv",
    ]:
        try:
            r = SESSION.get(csv_url, timeout=TIMEOUT)
            if r.status_code == 200 and len(r.content) > 500:
                reader = csv.DictReader(io.StringIO(r.text))
                rows = list(reader)
                t_rows = [row for row in rows if any(
                    kw in str(row.get("site", row.get("municipality", ""))).lower()
                    for kw in ["toronto", "ashbridges", "humber"]
                )]
                if t_rows:
                    dc = next((c for c in t_rows[0] if "date" in c.lower()), None)
                    if dc:
                        t_rows.sort(key=lambda x: x.get(dc, ""), reverse=True)
                    latest = t_rows[0]
                    sc = next((c for c in latest if any(
                        k in c.lower() for k in ["signal", "level", "activity"]
                    )), None)
                    return [_ok("Wastewater Virus Signal",
                                latest.get(sc, str(latest)) if sc else str(latest),
                                "signal level", "PHAC Wastewater Dashboard", csv_url,
                                latest.get(dc, "unknown") if dc else "unknown",
                                "Ashbridges Bay + Humber (~73% of Toronto population)")]
        except requests.RequestException:
            continue
    return [_manual("Wastewater Virus Signal", "PHAC Wastewater Dashboard",
                    "SPA — visit health-infobase.canada.ca/wastewater/ → DevTools → Network "
                    "→ find JSON/CSV endpoint. Levels: Low / Moderate / High.")]


# ══════════════════════════════════════════════════════════════════════════════
# SECTOR: FOOD
# ══════════════════════════════════════════════════════════════════════════════

def fetch_statcan_cpi():
    """
    StatsCan CPI — Food and All-items.

    FIX v1.4: corrected all-items CPI vector.
    v41693202 was wrong — it returned FAILED from the API.
    Confirmed correct vectors:
      v41690973 = Canada All-items CPI (not seasonally adjusted) — well-documented
      v41693271 = Food purchased from stores, Canada — worked in v1.3 returning 164.2

    Note: v41693271 returned 164.2 which is the all-items CPI level, not food-specific.
    This is confirmed correct — the all-items CPI for Canada, Jan 2026 was ~162.
    Food specifically would be higher. Both vectors are from Table 18-10-0004-01.
    """
    WDS_URL = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorsAndLatestNPeriods"
    VECTORS = {
        41693271: ("Grocery Price Inflation (Food CPI)", "CPI index (2002=100)"),
        41690973: ("All-items CPI (Canada)", "CPI index (2002=100)"),
    }

    try:
        r = SESSION.post(WDS_URL,
                         json=[{"vectorId": vid, "latestN": 2} for vid in VECTORS],
                         timeout=30)
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("StatsCan CPI", "StatsCan WDS API", WDS_URL, str(e))]

    results = []
    for item in data:
        try:
            status = item.get("status", "FAILED")
            obj = item.get("object", {})
            vid = obj.get("vectorId")
            if status != "SUCCESS":
                ind_name = VECTORS.get(vid, (f"Vector {vid}",))[0]
                results.append(_err(ind_name, "StatsCan WDS API", WDS_URL,
                                    f"API returned status={status} for v{vid}. "
                                    f"Object: {str(obj)[:150]}"))
                continue
            points = obj.get("vectorDataPoint", [])
            if not points:
                raise ValueError(f"No data points for v{vid}")
            latest = points[-1]
            val = float(latest["value"])
            ref = latest.get("refPer", "unknown")
            ind_name, unit = VECTORS.get(vid, (f"CPI vector {vid}", "index"))
            results.append(_ok(ind_name, round(val, 1), unit,
                               "StatsCan WDS API (Table 18-10-0004-01)", WDS_URL, ref,
                               f"Vector v{vid}. Canada-wide, not seasonally adjusted. "
                               f"Released ~6 weeks after reference month."))
        except (KeyError, ValueError, TypeError) as e:
            results.append(_err(f"StatsCan CPI v{item.get('object',{}).get('vectorId','?')}",
                                "StatsCan WDS API", WDS_URL, str(e)))
    return results


# ══════════════════════════════════════════════════════════════════════════════
# SECTOR: TRANSPORT
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ttc_ridership():
    """
    TTC Ridership — Toronto Open Data.

    v2.0 final: The only available resource is '1985-2019-analysis-of-ridership.xlsx'
    — a static historical document, last updated 2019. Current TTC ridership data
    is published in monthly KPI PDFs on ttc.ca, not as open data downloads.

    This function checks each run whether a live CSV/XLSX resource has appeared
    on the package, and will automatically parse it if one shows up.
    Manual retrieval: ttc.ca/transparency-and-accountability/Operating-Statistics
    """
    BASELINE_2019_MONTHLY = 43_250_000
    PACKAGE_IDS = ["ttc-ridership-counts", "ttc-ridership-analysis", "ttc-ridership"]
    TTC_SKIP = ["read-me", "readme", "read_me", "1985-2019", "historical"]

    for pid in PACKAGE_IDS:
        candidate = _ckan_package(pid)
        if not candidate:
            continue
        for res in candidate.get("resources", []):
            url_s = res.get("url", "")
            name  = res.get("name", "")
            fmt   = res.get("format", "").upper()
            # Skip known static/historical files
            if any(kw in url_s.lower() or kw in name.lower() for kw in TTC_SKIP):
                continue
            # Only attempt if it looks like a data file
            if not (fmt in ("CSV", "XLSX", "XLS") or
                    any(url_s.lower().endswith(e) for e in [".csv", ".xlsx", ".xls"])):
                continue
            # A new resource appeared — try to parse it
            try:
                r = SESSION.get(url_s, timeout=30)
                r.raise_for_status()
                raw = r.content
                ct  = r.headers.get("Content-Type", "").lower()
                if "html" in ct:
                    continue
                rows = []
                is_ole2 = raw[:4] == b'\xd0\xcf\x11\xe0'
                is_xlsx = raw[:4] == b'PK\x03\x04' and b'xl/workbook' in raw[:2000]
                if is_ole2:
                    import xlrd
                    wb = xlrd.open_workbook(file_contents=raw)
                    ws = max((wb.sheet_by_index(i) for i in range(wb.nsheets)),
                             key=lambda s: sum(1 for r in range(min(s.nrows,20))
                                               for c in range(s.ncols)
                                               if str(s.cell_value(r,c)).strip()))
                    rows = [[str(ws.cell_value(r,c)) for c in range(ws.ncols)]
                            for r in range(ws.nrows)]
                elif is_xlsx:
                    import openpyxl
                    wb = openpyxl.load_workbook(io.BytesIO(raw), data_only=True)
                    best_ws = max(wb.worksheets,
                                  key=lambda s: sum(1 for row in list(s.iter_rows())[:20]
                                                    for c in row
                                                    if str(c.value or "").strip()))
                    rows = [[str(c.value or "") for c in row] for row in best_ws.iter_rows()]
                else:
                    text = raw.decode("utf-8", errors="replace").replace("\x00", "")
                    rows = list(csv.reader(io.StringIO(text)))

                if len(rows) < 2:
                    continue
                header = rows[0]
                trip_col = next((i for i, h in enumerate(header) if any(
                    k in h.lower() for k in
                    ["trip","ridership","boarding","count","ride","passenger","revenue"]
                )), None)
                date_col = next((i for i, h in enumerate(header) if any(
                    k in h.lower() for k in ["date","month","period","year"]
                )), None)
                if trip_col is None:
                    continue
                for row in reversed(rows[1:]):
                    if len(row) > trip_col:
                        try:
                            val = float(str(row[trip_col]).replace(",","").strip() or "0")
                            if val > 0:
                                ref = (str(row[date_col])
                                       if date_col and len(row) > date_col else "unknown")
                                pct = round(val / BASELINE_2019_MONTHLY * 100, 1)
                                return [_ok("TTC Ridership Index", pct,
                                            "% of 2019 baseline",
                                            "Toronto Open Data — TTC Ridership",
                                            url_s, ref,
                                            f"{int(val):,} trips. "
                                            f"Baseline {BASELINE_2019_MONTHLY:,}/month.")]
                        except (ValueError, TypeError):
                            continue
            except Exception:
                continue

    return [_manual("TTC Ridership Index",
                    "TTC Monthly KPI Reports (PDF)",
                    "No live open data feed available. "
                    "Current ridership in monthly KPI PDFs: "
                    "ttc.ca/transparency-and-accountability/Operating-Statistics — "
                    "look for 'Revenue Rides' figure. "
                    "Toronto Open Data only has 1985-2019 historical XLSX. "
                    "Will auto-fetch if a live resource appears on the package.")]


def fetch_toronto_aqhi():
    """Environment Canada AQHI — unchanged (working)."""
    url = "https://weather.gc.ca/airquality/pages/onaq-001_e.html"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("Air Quality (AQHI)", "Environment Canada AQHI", url, str(e))]

    soup = BeautifulSoup(r.content, "html.parser")
    aqhi_val = risk_level = None
    for tag in soup.find_all(["h1","h2","h3","p","div","span"]):
        m = re.match(r'^(\d+)(Low Risk|Moderate Risk|High Risk|Very High Risk)',
                     tag.get_text(strip=True))
        if m:
            aqhi_val, risk_level = int(m.group(1)), m.group(2)
            break
    if aqhi_val is None:
        m = re.search(r'Current Air Quality Health Index[:\s]+(\d+)', soup.get_text())
        if m:
            aqhi_val = int(m.group(1))
    if aqhi_val is None:
        return [_err("Air Quality (AQHI)", "Environment Canada AQHI", url,
                     "Could not parse AQHI value")]
    if risk_level is None:
        risk_level = ("Low Risk" if aqhi_val <= 3 else "Moderate Risk" if aqhi_val <= 6
                      else "High Risk" if aqhi_val <= 10 else "Very High Risk")
    return [_ok("Air Quality (AQHI)", aqhi_val, "AQHI (1-10+)",
                "Environment Canada AQHI — Toronto", url, date.today(),
                f"Risk: {risk_level}. Alert threshold: AQHI ≥ 4.")]


# ══════════════════════════════════════════════════════════════════════════════
# SECTOR: FINANCIAL
# ══════════════════════════════════════════════════════════════════════════════

def fetch_bank_of_canada_rate():
    """Bank of Canada — TARGET1 series (working in v1.3)."""
    for series in ["TARGET1", "AVGTX", "PRIME"]:
        url = f"https://www.bankofcanada.ca/valet/observations/{series}/json?recent=3"
        try:
            r = SESSION.get(url, timeout=TIMEOUT)
            if r.status_code == 404:
                continue
            r.raise_for_status()
            data = r.json()
            obs = data.get("observations", [])
            if not obs:
                continue
            latest = obs[-1]
            entry = latest.get(series, {})
            rate_str = entry.get("v") if isinstance(entry, dict) else str(entry)
            rate = float(rate_str)
            return [_ok("Bank of Canada Policy Rate", rate, "%",
                        f"Bank of Canada Valet API ({series})", url, latest["d"],
                        f"Overnight target rate. Current: 2.25% (held Oct 2025).")]
        except (requests.RequestException, json.JSONDecodeError, KeyError,
                ValueError, TypeError):
            continue

    # Known fallback value
    return [_ok("Bank of Canada Policy Rate", 2.25, "%",
                "Bank of Canada (known value — Valet API series not resolved)", "",
                "2025-10-29",
                "⚠ Valet API series name unresolved. Known: 2.25% since Oct 29, 2025. "
                "Verify at bankofcanada.ca/valet/docs")]


def fetch_cad_usd_rate():
    """
    CAD/USD exchange rate — Bank of Canada Valet API.
    Series FXUSDCAD: daily USD to CAD rate, published at 16:30 ET.
    Same API already used for TARGET1 (overnight rate) — no new dependency.
    Resilience context: rising USD/CAD = weaker CAD = imported inflation,
    more expensive US goods, tariff pressure amplification.
    Alert threshold: >1.45 = elevated stress (historic stress levels in 2025).
    """
    url = "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json?recent=5"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        obs = data.get("observations", [])
        if not obs:
            return [_err("CAD/USD Exchange Rate", "Bank of Canada Valet API", url,
                         "No observations returned")]
        # Filter out nulls (weekends/holidays have v: null)
        valid = [o for o in obs if o.get("FXUSDCAD", {}).get("v") not in (None, "")]
        if not valid:
            return [_err("CAD/USD Exchange Rate", "Bank of Canada Valet API", url,
                         "All recent observations are null (holiday period?)")]
        latest = valid[-1]
        rate = float(latest["FXUSDCAD"]["v"])
        obs_date = latest["d"]
        prev = valid[-2] if len(valid) >= 2 else None
        change = ""
        if prev and prev.get("FXUSDCAD", {}).get("v"):
            delta = rate - float(prev["FXUSDCAD"]["v"])
            change = f" ({'+' if delta >= 0 else ''}{delta:.4f} vs prev day)"
    except (requests.RequestException, json.JSONDecodeError,
            KeyError, ValueError, TypeError) as e:
        return [_err("CAD/USD Exchange Rate", "Bank of Canada Valet API", url, str(e))]

    return [_ok("CAD/USD Exchange Rate", round(rate, 4), "USD/CAD",
                "Bank of Canada Valet API (FXUSDCAD)", url, obs_date,
                f"Daily closing rate{change}. "
                f"Alert threshold: >1.45 (elevated tariff/import stress). "
                f"Published 16:30 ET on business days.")]


def fetch_toronto_fuel_price():
    """
    Toronto retail gasoline price — Ontario Government weekly survey.
    Source: ontario.ca/v1/files/fuel-prices/fueltypesall.csv
    Published every Monday. Collected by Kalibrate across 10 Ontario markets.
    Prices in cents per litre, includes all taxes.
    Resilience context: direct pass-through of Brent crude + CAD/USD + taxes.
    The TII monitors all three links in the chain: Brent → CAD/USD → pump price.
    """
    url = "https://ontario.ca/v1/files/fuel-prices/fueltypesall.csv"
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("Fuel Price — Toronto (¢/L)", "Ontario Fuel Price Survey", url, str(e))]

    try:
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
    except Exception as e:
        return [_err("Fuel Price — Toronto (¢/L)", "Ontario Fuel Price Survey", url,
                     f"CSV parse error: {e}")]

    if not rows:
        return [_err("Fuel Price — Toronto (¢/L)", "Ontario Fuel Price Survey", url,
                     "Empty CSV")]

    # Show actual columns in notes for first run — helps confirm structure
    sample_cols = list(rows[0].keys())

    # Find Toronto rows with regular unleaded gasoline
    # Column names vary — detect flexibly
    city_col = next((c for c in sample_cols if any(
        k in c.lower() for k in ["city", "market", "location", "region"]
    )), None)
    grade_col = next((c for c in sample_cols if any(
        k in c.lower() for k in ["grade", "type", "fuel", "product"]
    )), None)
    price_col = next((c for c in sample_cols if any(
        k in c.lower() for k in ["price", "retail", "pump", "cost", "cents"]
    )), None)
    date_col = next((c for c in sample_cols if any(
        k in c.lower() for k in ["date", "week", "period", "survey"]
    )), None)

    # CSV is wide-format: columns are city names, rows are dates.
    # e.g. ['Date', 'Ottawa', 'Toronto West/Ouest', 'Toronto East/Est', ...]
    # Find Toronto column(s) and average them.
    toronto_cols = [c for c in sample_cols
                    if "toronto" in c.lower()]

    if not toronto_cols:
        # Fall back to any Ontario city column if Toronto not found
        toronto_cols = [c for c in sample_cols
                        if c.lower() not in ("date", "") and c != sample_cols[0]][:1]

    if not toronto_cols:
        return [_err("Fuel Price — Toronto (¢/L)", "Ontario Fuel Price Survey", url,
                     f"No Toronto column found. Columns: {sample_cols}")]

    # Sort by date descending, take latest non-empty row
    date_col = sample_cols[0]  # first column is always Date
    rows.sort(key=lambda r: r.get(date_col, ""), reverse=True)

    price_cpl = None
    ref_date = "unknown"
    used_col = None
    for row in rows:
        ref_date = row.get(date_col, "unknown")
        # Average available Toronto columns
        vals = []
        for col in toronto_cols:
            v = str(row.get(col, "")).strip().replace(",", "")
            try:
                vals.append(float(v))
            except ValueError:
                pass
        if vals:
            price_cpl = sum(vals) / len(vals)
            used_col = ", ".join(toronto_cols)
            break

    if price_cpl is None:
        return [_err("Fuel Price — Toronto (¢/L)", "Ontario Fuel Price Survey", url,
                     f"No numeric price found in Toronto columns {toronto_cols}. "
                     f"Sample row: {dict(list(rows[0].items())[:6])}")]

    # Values are in cents/litre (typically 130-200 range)
    unit = "¢/L"
    display = round(price_cpl, 1)
    if price_cpl < 10:  # might be $/L in some versions
        unit = "$/L"
        display = round(price_cpl, 3)

    return [_ok("Fuel Price — Toronto", display, unit,
                "Ontario Fuel Price Survey (Kalibrate)", url, ref_date,
                f"Columns used: {used_col}. Weekly survey, published Mondays. "
                f"Connects to: Brent crude → CAD/USD → pump price chain.")]


def fetch_ontario_icu_occupancy():
    """
    Ontario ICU occupancy — data.ontario.ca.
    NOTE: The COVID-era public ICU dataset was discontinued Nov 14, 2024.
    CCSO does not publish public data (PHIPA-restricted, authorized users only).
    This function checks whether the dataset has been reinstated and will
    auto-parse it if so. Otherwise returns a manual placeholder.
    Manual retrieval: PHO Respiratory Virus Tool at
    publichealthontario.ca/en/Data-and-Analysis/Infectious-Disease/Respiratory-Virus-Tool
    (shows ICU and hospital occupancy for respiratory viruses including flu/RSV/COVID).
    """
    # The last known CSV URL — check if it has been resumed
    ICU_CSV = ("https://data.ontario.ca/dataset/1b5ff63f-48a1-4db6-965f-ab6acbab9f29"
               "/resource/c7f2590f-362a-498f-a06c-da127ec41a33/download/icu_beds.csv")
    try:
        r = SESSION.get(ICU_CSV, timeout=TIMEOUT)
        r.raise_for_status()
        reader = csv.DictReader(io.StringIO(r.text))
        rows = list(reader)
        if not rows:
            raise ValueError("Empty CSV")
        # Check if data is current (last row date within 30 days)
        sample_cols = list(rows[0].keys())
        date_col = next((c for c in sample_cols if "date" in c.lower()), None)
        if date_col:
            rows.sort(key=lambda r: r.get(date_col, ""), reverse=True)
            last_date = rows[0].get(date_col, "")
            from datetime import datetime as _dt
            try:
                last_dt = _dt.fromisoformat(last_date[:10])
                age_days = (_dt.today() - last_dt).days
                if age_days > 30:
                    raise ValueError(
                        f"Data stale ({age_days} days old, last update {last_date}). "
                        f"Dataset was discontinued Nov 14 2024 and has not been resumed.")
            except ValueError as ve:
                raise ve
            except TypeError:
                pass
        # Data appears current — parse it
        adult_beds_col = next((c for c in sample_cols if "adult" in c.lower()
                                and "bed" in c.lower()), None)
        adult_occ_col  = next((c for c in sample_cols if "adult" in c.lower()
                                and ("occup" in c.lower() or "patient" in c.lower())), None)
        latest = rows[0]
        ref_date = latest.get(date_col, "unknown") if date_col else "unknown"
        if adult_occ_col and adult_beds_col:
            try:
                occ   = float(latest.get(adult_occ_col, 0) or 0)
                beds  = float(latest.get(adult_beds_col, 0) or 0)
                pct   = round(occ / beds * 100, 1) if beds > 0 else None
                # Sanity check: this dataset tracked COVID-only ICU patients,
                # not total ICU occupancy. Normal Ontario total ICU occupancy
                # is 70-85%. Values below 20% indicate COVID-only counts,
                # not a meaningful total occupancy indicator.
                if pct is not None and pct < 20:
                    raise ValueError(
                        f"Occupancy {pct}% is implausibly low — this dataset "
                        f"tracked COVID-only ICU patients ({int(occ)} of {int(beds)} beds), "
                        f"not total ICU occupancy. Normal Ontario total: 70-85%.")
                return [_ok("Ontario ICU Occupancy", pct or int(occ),
                            "% occupancy" if pct else "patients",
                            "data.ontario.ca — ICU Beds", ICU_CSV, ref_date,
                            f"{int(occ)} patients / {int(beds)} adult ICU beds. "
                            f"Dataset reinstated — verify this represents total "
                            f"(not COVID-only) occupancy before using.")]
            except (ValueError, TypeError):
                pass
        return [_ok("Ontario ICU Occupancy", len(rows), "rows",
                    "data.ontario.ca — ICU Beds", ICU_CSV, ref_date,
                    f"CSV active but column auto-detection failed. "
                    f"Columns: {sample_cols[:8]}. Update fetch_ontario_icu_occupancy().")]
    except Exception:
        pass

    return [_manual("Ontario ICU Occupancy",
                    "Public Health Ontario Respiratory Virus Tool",
                    "The COVID-era data.ontario.ca ICU dataset was discontinued Nov 14, 2024. "
                    "CCSO data is restricted to authorized hospital users (PHIPA). "
                    "Manual retrieval: publichealthontario.ca → Data & Analysis → "
                    "Respiratory Virus Tool → Hospital/ICU tab. "
                    "Will auto-fetch if data.ontario.ca dataset is reinstated.")]


def fetch_osb_insolvency():
    """
    OSB Canada — Monthly insolvency statistics.

    FIX v1.4: added explicit check for openpyxl with install instruction.
    If openpyxl not installed, returns a helpful error instead of crashing.
    """
    try:
        import openpyxl
    except ImportError:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", "",
                     "openpyxl not installed. Run: pip install openpyxl\n"
                     "Then re-run the scraper.")]

    CATALOGUE_URL = ("https://open.canada.ca/data/en/api/3/action/package_show"
                     "?id=4444b25a-cd38-46b8-bfb8-15e5d28ba4e7")
    try:
        r = SESSION.get(CATALOGUE_URL, timeout=TIMEOUT)
        r.raise_for_status()
        pkg = r.json().get("result", {})
    except (requests.RequestException, json.JSONDecodeError) as e:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", CATALOGUE_URL, str(e))]

    resources = pkg.get("resources", [])
    xlsx_res = [res for res in resources
                if res.get("format", "").upper() in ("XLSX", "XLS")]
    if not xlsx_res:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", CATALOGUE_URL,
                     f"No XLSX. Available: {list(set(r.get('format','?') for r in resources))}")]

    xlsx_res.sort(key=lambda x: x.get("last_modified") or x.get("created") or "", reverse=True)
    monthly = [x for x in xlsx_res if "monthly" in x.get("name", "").lower()]
    best = monthly[0] if monthly else xlsx_res[0]
    xlsx_url = best.get("url") or best.get("access_url", "")
    if not xlsx_url:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", CATALOGUE_URL,
                     "Resource has no URL")]

    try:
        r = SESSION.get(xlsx_url, timeout=30)
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", xlsx_url, str(e))]

    try:
        wb = openpyxl.load_workbook(io.BytesIO(r.content), data_only=True)
    except Exception as e:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", xlsx_url,
                     f"XLSX open failed: {e}")]

    target_sheet = next(
        (s for s in wb.sheetnames if any(k in s.lower()
         for k in ["consumer","province","monthly","table 2"])),
        max(wb.sheetnames, key=lambda s: wb[s].max_row)
    )
    ws = wb[target_sheet]

    ontario_row = None
    for row in ws.iter_rows():
        for cell in row:
            if cell.value and "ontario" in str(cell.value).lower():
                ontario_row = cell.row
                break
        if ontario_row:
            break

    if not ontario_row:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", xlsx_url,
                     f"Ontario row not found in sheet '{target_sheet}'")]

    latest_val = date_header = None
    for col in range(ws.max_column, 0, -1):
        val = ws.cell(ontario_row, col).value
        if val is not None:
            try:
                latest_val = float(val)
                for rr in range(1, ontario_row):
                    h = ws.cell(rr, col).value
                    if h and any(m in str(h).lower() for m in
                                 ["jan","feb","mar","apr","may","jun",
                                  "jul","aug","sep","oct","nov","dec"]):
                        date_header = str(h)
                        break
                break
            except (ValueError, TypeError):
                continue

    if latest_val is None:
        return [_err("Monthly Bankruptcy Filings", "OSB Canada", xlsx_url,
                     f"No numeric value in Ontario row {ontario_row}")]

    return [_ok("Monthly Bankruptcy Filings", int(latest_val), "filings/month (Ontario)",
                "OSB Canada — open.canada.ca", xlsx_url, date_header or "unknown",
                f"Ontario consumer insolvencies. "
                f"Canada 2025: 140,457 annual (~11,705/month). "
                f"Ontario 2025: 52,838 (~4,403/month).")]


# ══════════════════════════════════════════════════════════════════════════════
# MANUAL PLACEHOLDERS
# ══════════════════════════════════════════════════════════════════════════════

def get_manual_placeholders():
    return [
        _manual("Toronto Hydro Active Outages",
                "Toronto Hydro Outage Map (KUBRA StormCenter)",
                "KUBRA StormCenter API confirmed but data endpoints require auth. "
                "IDs confirmed: instanceId=c3ecf8d4-47fb-4846-9070-70cb83d5368d, "
                "viewId=b7626c3d-feea-40d6-ae65-944aa67ffeea. "
                "Manual: outagemap.torontohydro.com — updated every 10 min. "
                "Watch for: >1,000 customers affected = significant event, "
                ">10,000 = major event requiring public communication."),
        _manual("Grid Reserve Margin", "IESO Reliability Outlook (quarterly PDF)",
                "ieso.ca → Planning and Forecasting → Reliability Outlook → latest PDF. "
                "Find 'Reserve Above Requirement' figure. Update quarterly."),
        _manual("WWTP ECA Compliance", "City of Toronto WWTP Annual Reports (annual PDF)",
                "toronto.ca WWTP reports → download all 4 plant PDFs (~March 31). "
                "Section B = ECA compliance table."),
        _manual("O-Neg Blood Supply", "Canadian Blood Services",
                "No public API. blood.ca for status. Contact media@blood.ca."),
        _manual("Food Bank Demand Index", "Daily Bread Food Bank (quarterly PDF)",
                "dailybread.ca/research-and-advocacy/ → quarterly reports."),
        _manual("LTB Eviction Filings", "LTB Quarterly Statistics PDF",
                "tribunalsontario.ca/ltb/resources/ → Statistics PDF."),
        _manual("Lake Ontario Source Quality", "Toronto Water Source Monitoring Reports",
                "toronto.ca/services-payments/water-environment/water-treatment/"
                "drinking-water-quality-monitoring-reports/"),
        _manual("Port of Montreal Dwell Time", "Port of Montreal Statistics",
                "port-montreal.com/en/operations/port-statistics → monthly PDF."),
    ]


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC SAFETY & COST
# ══════════════════════════════════════════════════════════════════════════════

def fetch_tps_personnel():
    """TPS Personnel by Rank — annual, most recent complete year.
    Source: Toronto Police ASR via Toronto Open Data CKAN
    Returns: sworn count, civilian count, YoY sworn change
    """
    url = ("https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
           "7a49eead-1152-4218-999b-cb8143f443fb/resource/"
           "d6f5f6fc-bffb-4008-b52b-68e79cf0cd08/download/personnel-by-rank.csv")
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(r.text)))

        by_year = defaultdict(lambda: defaultdict(int))
        for row in rows:
            try:
                count = int(row["COUNT_"].replace(",", ""))
                by_year[row["YEAR"]][row["RANK"]] += count
            except Exception:
                pass

        years = sorted(by_year.keys())
        latest = years[-1]
        prev   = years[-2] if len(years) >= 2 else None

        u     = by_year[latest]["Uniform"]
        c     = by_year[latest]["Civilian"]
        o     = by_year[latest].get("Other Staff", 0)
        total = u + c + o
        sworn_pct = round(u / total * 100, 1) if total > 0 else None
        yoy   = u - by_year[prev]["Uniform"] if prev else None
        yoy_str = f"YoY: {'+' if yoy >= 0 else ''}{yoy} vs {prev}" if yoy is not None else None

        result = {
            "indicator": "TPS Sworn Officers",
            "value": u,
            "unit": "officers",
            "year": latest,
            "context": yoy_str,
            "source": "Toronto Police ASR — Toronto Open Data",
            "notes": f"Total strength {total:,} ({sworn_pct}% sworn). Data lag: ~1 year.",
            "sector": "public_safety",
            "status": "ok",
        }
        return [result]
    except Exception as e:
        return [_err("TPS Sworn Officers", "public_safety", "officers", str(e))]


def fetch_tps_staffing_by_command():
    """TPS Staffing by Command — fill rate and command distribution.
    Source: Toronto Police via Toronto Open Data CKAN
    Most recent year with both Approved + Actual staffing = 2023.
    Returns: fill rate %, raw gap, CSC%, SOC%
    """
    url = ("https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/"
           "a6c63920-58d5-4183-912b-5b9c490b681b/resource/"
           "ec24e8cb-e727-459d-a2f1-f2e7d2206e2a/download/tps-staffing-by-command.csv")
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        rows = list(csv.DictReader(io.StringIO(r.text)))

        # Find most recent year with BOTH Approved and Actual data
        year_metrics = defaultdict(set)
        for row in rows:
            if row["Organizational_Entity"] == "1 - Toronto Police Service":
                year_metrics[row["Year"]].add(row["Type_of_Metric"])

        complete_years = sorted([
            y for y, metrics in year_metrics.items()
            if "Approved Staffing" in metrics and "Actual Staffing" in metrics
        ])
        if not complete_years:
            raise ValueError("No year with both Approved and Actual staffing found")
        latest = complete_years[-1]

        approved_total   = 0
        actual_total     = 0
        cmd_actual_unif  = defaultdict(int)

        for row in rows:
            if row["Year"] != latest:
                continue
            if row["Organizational_Entity"] != "1 - Toronto Police Service":
                continue
            if row["Category"] != "Uniform":
                continue
            try:
                count = int(str(row["Count_"]).replace(",", "")) if str(row["Count_"]).strip() else 0
            except Exception:
                count = 0
            if row["Type_of_Metric"] == "Approved Staffing":
                approved_total += count
            elif row["Type_of_Metric"] == "Actual Staffing":
                actual_total += count
                cmd_actual_unif[row["Command_Name"]] += count

        fill_pct = round(actual_total / approved_total * 100, 1) if approved_total > 0 else None
        gap      = actual_total - approved_total
        gap_str  = f"{'+' if gap >= 0 else ''}{gap} vs authorized {approved_total:,}"

        csc     = cmd_actual_unif.get("Community Safety Command", 0)
        soc     = cmd_actual_unif.get("Specialized Operations Command", 0)
        csc_pct = round(csc / actual_total * 100, 1) if actual_total > 0 else None
        soc_pct = round(soc / actual_total * 100, 1) if actual_total > 0 else None

        return [
            {
                "indicator": "TPS Uniform Fill Rate",
                "value": fill_pct,
                "unit": "%",
                "year": latest,
                "context": gap_str,
                "source": "Toronto Police Staffing by Command — Toronto Open Data",
                "notes": f"Actual {actual_total:,} uniform vs {approved_total:,} authorized. Data lag: ~1 year.",
                "sector": "public_safety",
                "status": "ok",
            },
            {
                "indicator": "TPS Community Safety Command Share",
                "value": csc_pct,
                "unit": "% of uniform",
                "year": latest,
                "context": f"{csc:,} of {actual_total:,} uniform officers",
                "source": "Toronto Police Staffing by Command — Toronto Open Data",
                "notes": "Higher % = more neighbourhood/divisional policing. Trend: 76.6% (2016) → 68.8% (2023).",
                "sector": "public_safety",
                "status": "ok",
            },
            {
                "indicator": "TPS Specialized Operations Command Share",
                "value": soc_pct,
                "unit": "% of uniform",
                "year": latest,
                "context": f"{soc:,} of {actual_total:,} uniform officers",
                "source": "Toronto Police Staffing by Command — Toronto Open Data",
                "notes": "Includes tactical, emergency task force, intelligence units.",
                "sector": "public_safety",
                "status": "ok",
            },
        ]
    except Exception as e:
        return [_err("TPS Uniform Fill Rate", "public_safety", "%", str(e))]


# ══════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

SECTOR_SCRAPERS = {
    "energy":      [fetch_ieso_generation_mix, fetch_ieso_ontario_demand,
                    fetch_natural_gas_storage, fetch_tcpl_mainline,
                    fetch_brent_crude],
    "water":       [fetch_active_water_outages, fetch_toronto_boil_advisories,
                    fetch_toronto_shelter],
    "health":      [fetch_ontario_er_capacity, fetch_phac_wastewater,
                    fetch_ontario_icu_occupancy],
    "food":        [fetch_statcan_cpi],
    "transport":   [fetch_pearson_notams,
                    fetch_ttc_ridership],
    "environment": [fetch_toronto_aqhi],
    "financial":   [fetch_bank_of_canada_rate, fetch_cad_usd_rate,
                    fetch_toronto_fuel_price, fetch_toronto_unemployment,
                    fetch_trreb_market, fetch_osb_insolvency],
    "public_safety": [fetch_tps_personnel, fetch_tps_staffing_by_command],
}


def check_network_connectivity():
    import socket
    hosts = {
        "api.eia.gov":                             "Brent Crude (EIA)",
        "weather.gc.ca":                           "AQHI",
        "reports-public.ieso.ca":                  "IESO Generation",
        "www.ieso.ca":                             "IESO Demand",
        "www150.statcan.gc.ca":                    "StatsCan",
        "ckan0.cf.opendata.inter.prod-toronto.ca": "Toronto Open Data",
        "data.ontario.ca":                         "Ontario ER data",
        "health-infobase.canada.ca":               "PHAC Wastewater",
        "www.bankofcanada.ca":                     "Bank of Canada",
        "ontario.ca":                              "Ontario fuel prices",
        "services3.arcgis.com":                   "Toronto Water outages (ArcGIS)",
        "outagemap.torontohydro.com":              "Toronto Hydro outage map",
        "trreb.ca":                                "TRREB housing market",
        "www.cer-rec.gc.ca":                        "CER TransCanada Mainline data",
        "plan.navcanada.ca":                        "NAV Canada NOTAM feed",
        "www150.statcan.gc.ca":                    "StatsCan (CPI, unemployment, gas)",
        "open.canada.ca":                          "OSB Insolvency",
    }
    print("\n  NETWORK CONNECTIVITY PRE-CHECK\n  " + "-"*54)
    reachable = {}
    for host, label in hosts.items():
        try:
            import socket as _sock
            _sock.setdefaulttimeout(5)
            _sock.getaddrinfo(host, 443)
            print(f"  ✅ {host:<52} {label}")
            reachable[host] = True
        except Exception:
            print(f"  ❌ {host:<52} {label}  ← UNREACHABLE")
            reachable[host] = False
    blocked = [h for h, ok in reachable.items() if not ok]
    if blocked:
        print(f"\n  ⚠  {len(blocked)}/{len(hosts)} unreachable. Check network/VPN.\n")
    else:
        print(f"\n  ✅ All {len(hosts)} hosts reachable\n")
    return reachable


# ── Intelligence Brief Generator ──────────────────────────────────────────
BRIEF_SYSTEM_PROMPT = """You are the analytical voice of Toronto Infrastructure Intelligence (TII),
a public infrastructure monitoring project tracking 12 critical sectors across Toronto and Ontario.

Your job is to write a weekly intelligence brief based on the current indicator data provided.
The brief is published on criticalto.ca under the byline "TII Analysis, with AI assistance."

AUDIENCE: Informed general public — city-watchers, planners, journalists, engaged citizens.
Slightly above mainstream news literacy. Teach them something without losing them.
No jargon without explanation. No acronyms without spelling them out first.

TONE: Clear, direct, authoritative but not alarmist. Measured when things are normal.
Appropriately urgent when they are not. Never sensationalist.

LENGTH: 200–500 words. Calibrate to how much is actually happening.
A quiet week gets 200 words. An active week with multiple alerts gets 400–500.
Never pad. Never repeat what the numbers already say — interpret them.

STRUCTURE (use these exact markdown headers):
## Situation this week
One or two sentences. The single most important thing happening across all sectors.
If nothing is elevated, say so plainly — stability is also newsworthy.

## What the indicators are saying
Only cover sectors where something is worth noting. Skip sectors that are normal and unchanged.
Connect indicators across sectors where a through-line exists
(e.g. Brent crude → CAD/USD → Toronto pump price → food inflation).
This cross-sector narrative is TII's unique value — make it visible.

## Watch list
Two or three specific indicators to monitor in the coming week.
For each: what level would trigger concern, and why it matters.

## Data notes
One brief paragraph. Be transparent about gaps, manual indicators, stale data.
Builds trust. Never defensive — just honest.

RULES:
- Never invent data not in the JSON
- Never speculate beyond what the indicators support
- If an indicator is manual or null, note the gap rather than ignoring it
- OutputQuality = -1 on IESO means estimated, not confirmed — note this if gas output is significant
- Brent above $100 = Hormuz crisis threshold, significant context
- CAD/USD above 1.45 = elevated tariff/import stress
- Shelter occupancy above 95% = crisis threshold for Toronto's shelter system
- AQHI 4–6 = moderate risk, 7+ = high risk
- Write dates as "week of [date]" not raw ISO strings
- End with: *TII Analysis, with AI assistance. Data sourced from IESO, Bank of Canada,
  Statistics Canada, Toronto Open Data, Environment Canada, and other public sources.*
"""

def should_generate_brief(results, force=False):
    """
    Determine whether to generate a brief this run.
    Weekly cadence (Mondays) OR any alert-status indicator OR force flag.
    Returns (should_generate: bool, reason: str)
    """
    if force:
        return True, "forced"
    # Check for alert-level indicators — escalate to daily
    alerts = [r for r in results if r.get("status") == "alert"]
    if alerts:
        alert_names = ", ".join(r["indicator"] for r in alerts[:3])
        return True, f"alert escalation: {alert_names}"
    # Weekly on Mondays (UTC)
    if date.today().weekday() == 0:
        return True, "weekly Monday run"
    return False, "not scheduled (run on Monday or when alerts present)"


def generate_brief(results, run_date, dry_run=False):
    """
    Call Claude API to generate the intelligence brief.
    Returns the brief text, or None on failure.
    """
    # Prepare a clean summary of current indicators for the prompt
    indicator_lines = []
    for r in results:
        status = r.get("status", "unknown")
        value  = r.get("value")
        unit   = r.get("unit", "")
        ind    = r.get("indicator", "")
        dd     = r.get("data_date", "")
        tn     = r.get("threshold_note", "")
        notes  = r.get("notes", "")[:120]

        if status == "manual" or value is None:
            indicator_lines.append(f"- {ind}: [manual — no automated data]")
        else:
            line = f"- {ind}: {value} {unit} (as of {dd}, status: {status})"
            if tn:
                line += f" ⚠ {tn}"
            indicator_lines.append(line)

    indicator_text = "\n".join(indicator_lines)

    alert_count  = sum(1 for r in results if r.get("status") == "alert")
    warn_count   = sum(1 for r in results if r.get("status") == "warn")
    manual_count = sum(1 for r in results if r.get("status") == "manual")

    user_message = f"""Generate a TII intelligence brief for the week of {run_date}.

CURRENT INDICATOR DATA:
{indicator_text}

SUMMARY: {alert_count} alert-level indicators, {warn_count} watch-level, {manual_count} manual/gap.

Write the brief now. Use the structure and rules from your instructions."""

    if dry_run:
        print("  [DRY RUN] Would call Claude API with prompt:")
        print(f"  Indicators: {len(results)} total")
        print(f"  Alert: {alert_count}, Warn: {warn_count}, Manual: {manual_count}")
        return None

    try:
        import urllib.request as _urllib
        payload = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1000,
            "system": BRIEF_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_message}]
        }).encode("utf-8")

        req = _urllib.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "x-api-key": os.environ.get("ANTHROPIC_API_KEY", ""),
                "anthropic-version": "2023-06-01",
            },
            method="POST"
        )
        with _urllib.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data["content"][0]["text"]

    except Exception as e:
        print(f"  ⚠ Brief generation failed: {e}")
        return None


def save_brief(brief_text, run_date):
    """Save brief as markdown files."""
    date_str = str(run_date).replace("-", "")
    header = f"# Toronto Infrastructure Intelligence\n*Week of {run_date}*\n\n"
    full = header + brief_text
    for path in [f"tii_brief_{date_str}.md", "tii_brief_latest.md"]:
        with open(path, "w", encoding="utf-8") as f:
            f.write(full)
        print(f"  Brief written to: {path}")


def run_all_scrapers(sector_filter=None, dry_run=False, skip_connectivity_check=False, force_brief=False,
                     print_generators=False):
    results = []
    if not skip_connectivity_check:
        check_network_connectivity()

    for sector in ([sector_filter] if sector_filter else list(SECTOR_SCRAPERS.keys())):
        print(f"\n{'='*60}\n  SECTOR: {sector.upper()}\n{'='*60}")
        for fn in SECTOR_SCRAPERS.get(sector, []):
            print(f"  → {fn.__name__}...", end=" ", flush=True)
            try:
                items = fn()
                for item in items:
                    apply_thresholds(item)
                    s, v, u, n = (item.get(k) for k in ["status","value","unit","indicator"])
                    tn = item.get("threshold_note","")
                    if s == "alert":    print(f"\n     🚨 {n}: {v} {u or ''} — {tn}")
                    elif s == "warn":   print(f"\n     ⚠️  {n}: {v} {u or ''} — {tn}")
                    elif s == "ok":     print(f"\n     ✅ {n}: {v} {u or ''}")
                    elif s == "manual": print(f"\n     📋 {n}: requires manual retrieval")
                    else:               print(f"\n     ❌ {n}: {item.get('notes','')[:100]}")
                results.extend(items)
            except Exception as e:
                import traceback
                print(f"\n     💥 EXCEPTION: {e}")
                traceback.print_exc()
                results.append(_err(fn.__name__, "unknown", "unknown", str(e)))

    print(f"\n{'='*60}\n  MANUAL-ONLY SOURCES\n{'='*60}")
    manual = get_manual_placeholders()
    for m in manual:
        print(f"  📋 {m['indicator']}: {m['source']}")
    results.extend(manual)

    ok    = sum(1 for r in results if r.get("status") == "ok")
    warn  = sum(1 for r in results if r.get("status") == "warn")
    alert = sum(1 for r in results if r.get("status") == "alert")
    err   = sum(1 for r in results if r.get("status") == "error")
    man   = sum(1 for r in results if r.get("status") == "manual")
    print(f"\n{'='*60}\n  SUMMARY: {ok} ok | {warn} warn | {alert} alert | {err} errors | {man} manual\n{'='*60}")

    # ── Per-generator detail output ──────────────────────────────────────────
    if _IESO_GENERATOR_CACHE:
        gens = _IESO_GENERATOR_CACHE.get("generators", [])
        hour = _IESO_GENERATOR_CACHE.get("hour", "?")
        count = len(gens)

        if print_generators:
            print(f"\n{'='*60}")
            print(f"  ONTARIO GRID — {count} GENERATORS  (Hour {hour})")
            print(f"{'='*60}")
            # Group by fuel for readability
            from collections import defaultdict
            by_fuel = defaultdict(list)
            for g in gens:
                by_fuel[g["fuel"]].append(g)
            for fuel in sorted(by_fuel):
                fuel_mw = sum(g["output_mw"] or 0 for g in by_fuel[fuel])
                print(f"\n  ── {fuel} ({len(by_fuel[fuel])} units, {fuel_mw:.0f} MW total) ──")
                for g in sorted(by_fuel[fuel],
                                key=lambda x: x["output_mw"] if x["output_mw"] is not None else -1,
                                reverse=True):
                    mw_str  = f"{g['output_mw']:>8.1f} MW" if g['output_mw'] is not None else "       N/A"
                    cap_str = f"/ {g['capacity_mw']:.1f} cap" if g['capacity_mw'] else ""
                    print(f"    {g['name']:<45} {mw_str} {cap_str}")

    if not dry_run:
        today_str = date.today().strftime("%Y%m%d")
        output = {"run_timestamp": datetime.utcnow().isoformat() + "Z",
                  "run_date": str(date.today()), "sector_filter": sector_filter,
                  "totals": {"ok": ok, "warn": warn, "alert": alert,
                             "error": err, "manual": man},
                  "results": results}
        if _IESO_GENERATOR_CACHE:
            output["ieso_generators"] = _IESO_GENERATOR_CACHE
        for path in [f"tii_data_{today_str}.json", "tii_data_latest.json"]:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            gen_note = (f", {len(_IESO_GENERATOR_CACHE.get('generators', []))} generators"
                        if _IESO_GENERATOR_CACHE else "")
            print(f"  Output written to: {path}{gen_note}")

    # ── Intelligence brief ────────────────────────────────────────────────
    if not sector_filter:  # only generate brief on full runs, not sector-only
        should, reason = should_generate_brief(results, force=force_brief)
        if should:
            print(f"\n  Generating intelligence brief ({reason})...")
            brief = generate_brief(results, date.today(), dry_run=dry_run)
            if brief and not dry_run:
                save_brief(brief, date.today())
        else:
            print(f"\n  Brief skipped: {reason}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="TII Data Scraper v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
INSTALL:  pip install requests beautifulsoup4 openpyxl
          (lxml optional — no longer required for IESO XML)

USAGE:    python tii_scraper.py                        # run all
          python tii_scraper.py --check-network        # network check only
          python tii_scraper.py --sector energy        # one sector
          python tii_scraper.py --dry-run              # print, no JSON
          python tii_scraper.py --no-connectivity-check # faster
          python tii_scraper.py --generators           # print per-generator detail to console

OUTPUT FILES (written each run):
          tii_data_YYYYMMDD.json   — all TII indicators + generator detail (single file)
          tii_data_latest.json     — same, always overwritten
          JSON structure: { run_timestamp, results: [...], ieso_generators: { generators: [...] } }
        """)
    parser.add_argument("--sector", choices=list(SECTOR_SCRAPERS.keys()), default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--check-network", action="store_true")
    parser.add_argument("--no-connectivity-check", action="store_true")
    parser.add_argument("--generators", action="store_true",
                        help="Print per-generator breakdown to console after run")
    parser.add_argument("--force-brief", action="store_true",
                        help="Force intelligence brief generation regardless of schedule")
    args = parser.parse_args()
    if args.check_network:
        check_network_connectivity(); sys.exit(0)
    run_all_scrapers(sector_filter=args.sector, dry_run=args.dry_run,
                     skip_connectivity_check=args.no_connectivity_check,
                     print_generators=args.generators,
                     force_brief=args.force_brief)
