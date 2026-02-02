from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# USGS Daily Values (DV) service supports JSON output.  :contentReference[oaicite:5]{index=5}
DV_URL = "https://waterservices.usgs.gov/nwis/dv/"

@dataclass(frozen=True)
class StreamflowRecord:
    site: str
    parameter: str
    date: str            # YYYY-MM-DD
    value: Optional[float]
    unit: str
    source: str = "usgs_nwis_dv"

def fetch_usgs_daily_values(site: str, parameter: str, days: int = 30) -> Dict[str, Any]:
    """
    Fetch daily values from USGS NWIS DV service as JSON.
    """
    params = {
        "format": "json",
        "sites": site,
        "parameterCd": parameter,
        "period": f"P{days}D",  # ISO 8601 duration
        "siteStatus": "all",
    }
    r = requests.get(DV_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return None
        return float(s)
    except Exception:
        return None

def parse_usgs_dv(payload: Dict[str, Any], site: str, parameter: str) -> List[StreamflowRecord]:
    """
    Normalize USGS payload into a clean record list.
    """
    out: List[StreamflowRecord] = []

    series = payload.get("value", {}).get("timeSeries", [])
    if not series:
        return out

    # For DV requests, we usually get one timeSeries for the parameter/site.
    ts = series[0]
    unit = ts.get("variable", {}).get("unit", {}).get("unitCode", "")

    # Values live here:
    # value.timeSeries[0].values[0].value -> list of {dateTime, value, qualifiers...}
    values_block = ts.get("values", [])
    if not values_block:
        return out

    values = values_block[0].get("value", [])
    for v in values:
        dt = v.get("dateTime", "")
        # dateTime includes time; we keep the date only for daily data
        date = dt[:10] if dt else ""
        val = _safe_float(v.get("value"))
        out.append(StreamflowRecord(site=site, parameter=parameter, date=date, value=val, unit=unit))

    return out

def to_db_rows(records: Iterable[StreamflowRecord]) -> List[Tuple[str, str, str, Optional[float], str, str]]:
    return [(r.site, r.parameter, r.date, r.value, r.unit, r.source) for r in records]
