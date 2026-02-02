import os
import sqlite3
from typing import Iterable, Tuple

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS usgs_daily_streamflow (
  site TEXT NOT NULL,
  parameter TEXT NOT NULL,
  date TEXT NOT NULL,            -- ISO date YYYY-MM-DD
  value REAL,
  unit TEXT,
  source TEXT NOT NULL,
  ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (site, parameter, date)
);
"""

def connect(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()

def upsert_streamflow_rows(
    conn: sqlite3.Connection,
    rows: Iterable[Tuple[str, str, str, float, str, str]],
) -> int:
    """
    rows: (site, parameter, date, value, unit, source)
    """
    sql = """
    INSERT INTO usgs_daily_streamflow (site, parameter, date, value, unit, source)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(site, parameter, date)
    DO UPDATE SET
      value=excluded.value,
      unit=excluded.unit,
      source=excluded.source,
      ingested_at=datetime('now');
    """
    cur = conn.cursor()
    cur.executemany(sql, list(rows))
    conn.commit()
    return cur.rowcount
