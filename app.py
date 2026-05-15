"""
Shopee Pipeline — FastAPI server.
Reads raw metrics from Supabase (or local SQLite); the frontend computes
all aggregations real-time. No 'total' rows are ever stored.
"""
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
STATIC_PATH  = Path(__file__).parent / "static"
DB_PATH      = Path(__file__).parent / "shopee_data.db"

USE_PG = bool(DATABASE_URL)
_startup_error: str = ""


# ─── DB helpers ──────────────────────────────────────────────────────────────
def _get_pg():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)


def _get_sqlite():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_conn():
    return _get_pg() if USE_PG else _get_sqlite()


CREATE_SQLITE = """
CREATE TABLE IF NOT EXISTS shopee_metrics (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    date                TEXT    NOT NULL,
    profile_name        TEXT    NOT NULL,
    open_channel_cost   REAL    NOT NULL DEFAULT 0,
    ads_cost            REAL    NOT NULL DEFAULT 0,
    commission          REAL    NOT NULL DEFAULT 0,
    account_group       TEXT,
    scraped_at          TEXT    NOT NULL,
    UNIQUE(profile_name, date)
)
"""

CREATE_PG = """
CREATE TABLE IF NOT EXISTS shopee_metrics (
    id                  SERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    profile_name        TEXT NOT NULL,
    open_channel_cost   NUMERIC(14,2) NOT NULL DEFAULT 0,
    ads_cost            NUMERIC(14,2) NOT NULL DEFAULT 0,
    commission          NUMERIC(14,2) NOT NULL DEFAULT 0,
    account_group       TEXT,
    scraped_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_date          ON shopee_metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_profile_name  ON shopee_metrics(profile_name);
CREATE INDEX IF NOT EXISTS idx_account_group ON shopee_metrics(account_group);
CREATE UNIQUE INDEX IF NOT EXISTS uq_profile_date ON shopee_metrics(profile_name, date);
"""


def init_db():
    conn = get_conn()
    c = conn.cursor()
    if USE_PG:
        for stmt in CREATE_PG.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                c.execute(stmt)
    else:
        c.execute(CREATE_SQLITE)
    conn.commit()
    conn.close()


# ─── Sample data (only seeded if table is empty — never overwrites real scrapes) ─
SAMPLE_PROFILES = [
    ("lonaharper",      "kshomeaxie29",   320, 1250, 2840),
    ("kingmolly",       "kshomeaxie29",   180,  890, 1620),
    ("amelia_shop",     "kshomeaxie29",   250, 1480, 3210),
    ("jayden_th",       "vinhomeaxie44",  120,  780, 1140),
    ("noah_market",     "vinhomeaxie44",  300, 1340, 2890),
    ("emma_th",         "vinhomeaxie44",  200, 1020, 1980),
    ("oliver_pro",      "homestore_a1",   400, 1820, 4120),
    ("liam_shop",       "homestore_a1",   220,  990, 2230),
]


def seed_mock_data():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM shopee_metrics")
    if c.fetchone()[0] > 0:
        conn.close()
        return

    today     = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    placeholder = "%s" if USE_PG else "?"

    rows = []
    for d in (today, yesterday):
        for name, grp, openc, ads, comm in SAMPLE_PROFILES:
            rows.append((d, name, openc, ads, comm, grp, datetime.now().isoformat()))

    sql = f"""
        INSERT INTO shopee_metrics
            (date, profile_name, open_channel_cost, ads_cost, commission, account_group, scraped_at)
        VALUES ({placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder},{placeholder})
    """
    c.executemany(sql, rows)
    conn.commit()
    conn.close()


# ─── App lifecycle ────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global USE_PG, DATABASE_URL, _startup_error
    try:
        init_db()
        seed_mock_data()
    except Exception as e:
        _startup_error = str(e)
        print(f"[startup] DB init warning: {e} — falling back to SQLite")
        USE_PG = False
        DATABASE_URL = ""
        init_db()
        seed_mock_data()
    yield


app = FastAPI(title="Shopee Pipeline Dashboard", lifespan=lifespan)


# ─── API ──────────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "db": "postgresql" if USE_PG else "sqlite",
        "err": _startup_error or None,
    }


def _rows_to_dicts(cursor):
    cols = [d[0] for d in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


@app.get("/api/data")
async def get_data(date: str | None = None, group: str | None = None):
    """Return raw rows. Filtering is server-side; aggregation is client-side."""
    conn = get_conn()
    if not USE_PG:
        conn.row_factory = sqlite3.Row
    c = conn.cursor()

    conditions, params = [], []
    placeholder = "%s" if USE_PG else "?"

    if date and date != "all":
        conditions.append(f"date = {placeholder}")
        params.append(date)
    if group and group != "all":
        conditions.append(f"account_group = {placeholder}")
        params.append(group)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    # Sort: channels alphabetical (so the same channel's days are contiguous),
    # then date DESC (newest of that channel first). Matches the manual
    # tracking spreadsheet layout the user is used to.
    c.execute(
        f"SELECT id, date, profile_name, open_channel_cost, ads_cost, "
        f"commission, account_group, scraped_at "
        f"FROM shopee_metrics {where} "
        f"ORDER BY profile_name ASC, date DESC",
        params,
    )

    rows = _rows_to_dicts(c) if USE_PG else [dict(r) for r in c.fetchall()]
    conn.close()

    # Cast numerics + date to JSON-friendly types
    for r in rows:
        for k in ("open_channel_cost", "ads_cost", "commission"):
            r[k] = float(r[k] or 0)
        if hasattr(r.get("date"), "isoformat"):
            r["date"] = r["date"].isoformat()
        if hasattr(r.get("scraped_at"), "isoformat"):
            r["scraped_at"] = r["scraped_at"].isoformat()
    return rows


@app.get("/api/dates")
async def get_dates():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT DISTINCT date FROM shopee_metrics ORDER BY date DESC")
    out = []
    for row in c.fetchall():
        v = row[0]
        out.append(v.isoformat() if hasattr(v, "isoformat") else v)
    conn.close()
    return out


@app.get("/api/groups")
async def get_groups():
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT DISTINCT account_group FROM shopee_metrics "
        "WHERE account_group IS NOT NULL AND account_group <> '' "
        "ORDER BY account_group"
    )
    groups = [row[0] for row in c.fetchall()]
    conn.close()
    return groups


# ─── Static (must mount last) ────────────────────────────────────────────────
app.mount("/", StaticFiles(directory=str(STATIC_PATH), html=True), name="static")
