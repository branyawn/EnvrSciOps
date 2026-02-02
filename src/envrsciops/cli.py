import argparse

from envrsciops.config import get_settings
from envrsciops.db import connect, init_db, upsert_streamflow_rows
from envrsciops.pipelines.usgs_streamflow import fetch_usgs_daily_values, parse_usgs_dv, to_db_rows

def cmd_ingest_usgs_streamflow(days: int) -> None:
    s = get_settings()
    payload = fetch_usgs_daily_values(site=s.usgs_site, parameter=s.usgs_parameter, days=days)
    records = parse_usgs_dv(payload, site=s.usgs_site, parameter=s.usgs_parameter)
    rows = to_db_rows(records)

    conn = connect(s.db_path)
    init_db(conn)
    count = upsert_streamflow_rows(conn, rows)
    conn.close()

    print(f"Ingested/updated {len(rows)} rows (sqlite rowcount={count}). DB: {s.db_path}")

def main() -> None:
    p = argparse.ArgumentParser(prog="envrsciops")
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("ingest-usgs-streamflow", help="Ingest USGS daily streamflow into SQLite")
    a.add_argument("--days", type=int, default=30, help="Number of past days to pull")

    args = p.parse_args()
    if args.cmd == "ingest-usgs-streamflow":
        cmd_ingest_usgs_streamflow(days=args.days)
