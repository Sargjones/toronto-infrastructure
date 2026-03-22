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
    ("Brent Crude Price",
        lambda v: v > 80,   lambda v: v > 100,
        "Elevated — above $80/bbl",
        "Crisis level — above $100/bbl (Hormuz threshold)"),
    # Energy
    ("Gas Output (MW)",
        lambda v: v > 3000, lambda v: v > 5000,
        "Gas peakers elevated — demand stress signal",
        "Gas peakers at crisis level — grid under significant stress"),
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




def fetch_brent_crude():
    """
    Brent crude spot price — ICE Brent futures via Stooq.com.
    Source: stooq.com (Polish financial data, no API key, no US jurisdiction).
    Ticker: cb.f (Crude Oil Brent, ICE).
    Weekly context: Brent is the global benchmark pricing ~2/3 of world oil trade.
    Ontario fuel prices track Brent via: Brent → CAD/USD → refinery margin → pump price.
    Warn: >$80/bbl. Alert: >$100/bbl (Hormuz disruption threshold).
    """
    url = "https://stooq.com/q/d/l/?s=cb.f&i=d&l=5"
    try:
        r = SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
        r.raise_for_status()
        lines = r.text.strip().splitlines()
        # Header: Date,Open,High,Low,Close,Volume
        if len(lines) < 2:
            raise ValueError("No data rows returned from Stooq")
        # Find most recent row with a valid close price
        for row in reversed(lines[1:]):
            parts = row.split(",")
            if len(parts) >= 5 and parts[4].strip() not in ("", "null", "N/D"):
                date_str = parts[0].strip()
                price = round(float(parts[4].strip()), 2)
                notes = (
                    f"ICE Brent crude spot price. Global benchmark for ~2/3 of world oil trade. "
                    f"Ontario fuel prices track: Brent → CAD/USD → pump price. "
                    f"Source: Stooq.com (ICE futures, ticker cb.f). "
                    f"Warn >$80/bbl, Alert >$100/bbl (Hormuz disruption threshold)."
                )
                return [_ok("Brent Crude Price", price, "USD/bbl",
                            "Stooq.com — ICE Brent (cb.f)", url, date_str, notes)]
        raise ValueError("All rows have null close price")
    except Exception as e:
        return [_err("Brent Crude Price", "Stooq.com — ICE Brent", url, str(e))]



def fetch_toronto_fuel_price():
    """
    Toronto retail gasoline price — Stockr.net daily forecast.
    Source: stockr.net/toronto/gasprice.aspx (Canadian site, no API key).
    Updated daily at 11am — shows today's and tomorrow's price.
    Calculated from commodity markets, not crowd-sourced.
    Prices in cents per litre, regular unleaded, includes all taxes.
    Resilience context: tracks global crude → CAD/USD → pump price chain.
    Previous source: Ontario Government weekly CSV (Kalibrate survey, Mondays only).
    Switched to Stockr for daily cadence — critical during volatile crude markets.
    """
    url = "https://stockr.net/toronto/gasprice.aspx"
    try:
        r = SESSION.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as e:
        return [_err("Fuel Price — Toronto", "Stockr.net", url, str(e))]

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, "html.parser")

        # Page structure: two h3 sections — "Today" and "Tomorrow"
        # Each followed by a plain number (e.g. "172.9") then a date string
        results = []
        sections = soup.find_all("h3")

        today_price = None
        tomorrow_price = None
        today_date = None
        tomorrow_date = None

        for h3 in sections:
            label = h3.get_text(strip=True).lower()
            # Price is the next text node sibling
            sib = h3.find_next_sibling()
            price_text = ""
            date_text = ""
            # Walk siblings to find price number and date
            node = h3.next_sibling
            while node:
                t = str(node).strip()
                if t and t not in ("\n", ""):
                    # Try float parse — that's the price
                    try:
                        candidate = float(t.replace(",", "").strip())
                        if 100 < candidate < 300:  # sanity: reasonable c/L range
                            price_text = candidate
                            # Next non-empty sibling should be the date line
                            node = node.next_sibling
                            while node:
                                dt = str(node).strip()
                                if dt and dt not in ("\n", ""):
                                    date_text = dt
                                    break
                                node = node.next_sibling
                            break
                    except (ValueError, AttributeError):
                        pass
                node = node.next_sibling

            if label == "today" and price_text:
                today_price = price_text
                today_date = date_text
            elif label == "tomorrow" and price_text:
                tomorrow_price = price_text
                tomorrow_date = date_text

        if today_price is None and tomorrow_price is None:
            # Fallback: find all large standalone numbers on the page
            text = soup.get_text()
            matches = re.findall(r"(1[4-9]\d\.\d|[2]\d{2}\.\d)", text)
            if matches:
                today_price = float(matches[0])
                today_date = str(__import__("datetime").date.today())

        if today_price is None:
            return [_err("Fuel Price — Toronto", "Stockr.net", url,
                         "Could not parse price from page")]

        tomorrow_note = (f" Tomorrow: {tomorrow_price}¢/L ({tomorrow_date})."
                         if tomorrow_price else "")

        notes = (
            f"Regular unleaded, includes all taxes. Daily forecast from commodity markets.¢"
            f"{tomorrow_note} "
            f"Source: stockr.net (Canadian, daily at 11am). "
            f"Tracks: global crude → CAD/USD → pump price chain."
        )

        return [_ok("Fuel Price — Toronto", today_price, "¢/L",
                    "Stockr.net — daily Toronto pump price", url,
                    today_date, notes)]

    except Exception as e:
        return [_err("Fuel Price — Toronto", "Stockr.net", url, str(e))]


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
        _manual("Dawn Hub Gas Storage (Ontario)",
                "CER / Statistics Canada — Monthly Natural Gas Storage",
                "Total Ontario underground storage at Dawn + Tecumseh pools (Enbridge). "
                "Total capacity: 387 Bcf. Eastern Canada 5-yr average ~300 Bcf at peak. "
                "CER publishes monthly estimates; Stats Can Table 13-10-0054-01. "
                "Warn threshold: <150 Bcf entering winter (Oct). Alert: <100 Bcf. "
                "Manual update: check cer-rec.gc.ca/en/data-analysis/energy-markets/"
                "market-snapshots for latest CER storage snapshot. "
                "Pending automated endpoint — CER does not publish open CSV for storage."),
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
                    fetch_tcpl_mainline, fetch_brent_crude],
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
        "weather.gc.ca":                           "AQHI",
        "reports-public.ieso.ca":                  "IESO Generation",
        "www.ieso.ca":                             "IESO Demand",
        "www150.statcan.gc.ca":                    "StatsCan",
        "ckan0.cf.opendata.inter.prod-toronto.ca": "Toronto Open Data",
        "data.ontario.ca":                         "Ontario ER data",
        "health-infobase.canada.ca":               "PHAC Wastewater",
        "www.bankofcanada.ca":                     "Bank of Canada",
        "stooq.com":                               "Brent Crude (Stooq)",
        "stockr.net":                              "Toronto fuel price (Stockr)",
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

def run_all_scrapers(sector_filter=None, dry_run=False, skip_connectivity_check=False,
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
    args = parser.parse_args()
    if args.check_network:
        check_network_connectivity(); sys.exit(0)
    run_all_scrapers(sector_filter=args.sector, dry_run=args.dry_run,
                     skip_connectivity_check=args.no_connectivity_check,
                     print_generators=args.generators)
