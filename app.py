"""
Shopee Pipeline — FastAPI server
Supports SQLite (local dev) and PostgreSQL (production via DATABASE_URL).
"""
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
STATIC_PATH = Path(__file__).parent / "static"
DB_PATH = Path(__file__).parent / "shopee_data.db"

USE_PG = bool(DATABASE_URL)
_startup_error: str = ""

# ─── DB helpers ──────────────────────────────────────────────────────────────

def _get_pg():
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(DATABASE_URL)
    return conn


def _get_sqlite():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_conn():
    if USE_PG:
        return _get_pg()
    return _get_sqlite()


CREATE_TABLE_SQLITE = """
CREATE TABLE IF NOT EXISTS scrape_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id      TEXT    NOT NULL,
    profile_name    TEXT,
    target_date     TEXT    NOT NULL,
    scraped_at      TEXT    NOT NULL,
    ads_cost        REAL,
    est_commission  REAL,
    roas            REAL,
    status          TEXT    NOT NULL,
    error_reason    TEXT,
    error_detail    TEXT,
    duration_ms     INTEGER NOT NULL DEFAULT 0
)
"""

CREATE_TABLE_PG = """
CREATE TABLE IF NOT EXISTS scrape_results (
    id              SERIAL PRIMARY KEY,
    profile_id      TEXT    NOT NULL,
    profile_name    TEXT,
    target_date     TEXT    NOT NULL,
    scraped_at      TEXT    NOT NULL,
    ads_cost        REAL,
    est_commission  REAL,
    roas            REAL,
    status          TEXT    NOT NULL,
    error_reason    TEXT,
    error_detail    TEXT,
    duration_ms     INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_target_date ON scrape_results(target_date);
CREATE INDEX IF NOT EXISTS idx_status       ON scrape_results(status);
CREATE INDEX IF NOT EXISTS idx_profile_id   ON scrape_results(profile_id);
"""

SEED_DATA = [
    ('Shop-Bangkok-01','Shop-Bangkok-01'),('Shop-Bangkok-02','Shop-Bangkok-02'),
    ('Shop-Bangkok-03','Shop-Bangkok-03'),('Shop-CNX-01','Shop-CNX-01'),
    ('Shop-CNX-02','Shop-CNX-02'),('Shop-HCMC-01','Shop-HCMC-01'),
    ('Shop-HCMC-02','Shop-HCMC-02'),('Shop-Jakarta-01','Shop-Jakarta-01'),
    ('Shop-KL-01','Shop-KL-01'),('Shop-KL-02','Shop-KL-02'),
    ('Shop-Manila-01','Shop-Manila-01'),('Shop-Manila-02','Shop-Manila-02'),
    ('Shop-Singapore-01','Shop-Singapore-01'),('Shop-HoChiMinh-03','Shop-HoChiMinh-03'),
    ('Shop-Bandung-01','Shop-Bandung-01'),('Shop-Surabaya-01','Shop-Surabaya-01'),
]


def init_db():
    conn = get_conn()
    c = conn.cursor()
    if USE_PG:
        for stmt in CREATE_TABLE_PG.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                c.execute(stmt)
    else:
        c.execute(CREATE_TABLE_SQLITE)
    conn.commit()
    conn.close()


def seed_mock_data():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM scrape_results")
    count = c.fetchone()[0]
    if count > 0:
        conn.close()
        return

    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    reasons = ['captcha', 'nav_timeout', 'otp', 'hard_timeout', 'no_metrics']
    placeholder = "%s" if USE_PG else "?"

    for i, (pid, name) in enumerate(SEED_DATA):
        failed = i % 5 == 1 or i % 7 == 0
        reason = reasons[i % len(reasons)] if failed else None
        cost = 1200 + (i * 137 % 9200)
        comm = cost * (0.6 + (i * 73 % 60) / 100)
        roas = comm / cost if cost > 0 else 0
        target_date = yesterday if i < 3 else today

        c.execute(f"""
            INSERT INTO scrape_results
            (profile_id, profile_name, target_date, scraped_at, ads_cost,
             est_commission, roas, status, error_reason, duration_ms)
            VALUES ({placeholder},{placeholder},{placeholder},{placeholder},{placeholder},
                    {placeholder},{placeholder},{placeholder},{placeholder},{placeholder})
        """, (
            f'gl_{1000+i}', name, target_date, datetime.now().isoformat(),
            None if failed else cost, None if failed else comm,
            None if failed else roas,
            'Failed' if failed else 'Success',
            f"{reason} detected" if failed else None,
            15000 + (i * 123 % 3000) if failed else 2000 + (i * 456 % 4000),
        ))

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


# ─── API routes ───────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"ok": True, "db": "postgresql" if USE_PG else "sqlite", "err": _startup_error or None}


@app.get("/api/data")
async def get_data(date: str = None, status: str = None):
    conn = get_conn()
    if not USE_PG:
        conn.row_factory = sqlite3.Row
    c = conn.cursor()

    conditions = []
    params = []
    placeholder = "%s" if USE_PG else "?"

    if date and date != "all":
        conditions.append(f"target_date = {placeholder}")
        params.append(date)
    if status and status != "all":
        conditions.append(f"status = {placeholder}")
        params.append(status)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    c.execute(f"SELECT * FROM scrape_results {where} ORDER BY scraped_at DESC", params)

    if USE_PG:
        cols = [d[0] for d in c.description]
        rows = [dict(zip(cols, row)) for row in c.fetchall()]
    else:
        rows = [dict(row) for row in c.fetchall()]

    conn.close()
    return rows


@app.get("/api/summary")
async def get_summary(date: str = None):
    conn = get_conn()
    c = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")
    target_date = date if date and date != "all" else today
    placeholder = "%s" if USE_PG else "?"

    c.execute(f"""
        SELECT
            COUNT(*)                                                   AS total,
            SUM(CASE WHEN status='Success' THEN 1 ELSE 0 END)         AS success,
            SUM(CASE WHEN status='Failed'  THEN 1 ELSE 0 END)         AS failed,
            SUM(CASE WHEN status='Success' THEN ads_cost       ELSE 0 END) AS total_cost,
            SUM(CASE WHEN status='Success' THEN est_commission ELSE 0 END) AS total_commission,
            AVG(CASE WHEN status='Success' THEN roas           ELSE NULL END) AS avg_roas
        FROM scrape_results
        WHERE target_date = {placeholder}
    """, (target_date,))

    row = c.fetchone()
    conn.close()

    total = row[0] or 0
    success = row[1] or 0
    failed = row[2] or 0
    total_cost = float(row[3] or 0)
    total_commission = float(row[4] or 0)
    avg_roas = float(row[5] or 0)

    return {
        "target_date": target_date,
        "total": total,
        "success": success,
        "failed": failed,
        "success_rate": round(success / total * 100, 1) if total else 0,
        "total_cost": total_cost,
        "total_commission": total_commission,
        "avg_roas": avg_roas,
    }


@app.get("/api/dates")
async def get_dates():
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT DISTINCT target_date FROM scrape_results ORDER BY target_date DESC")
    dates = [row[0] for row in c.fetchall()]
    conn.close()
    return dates


# ─── Static (must be last) ────────────────────────────────────────────────────

app.mount("/", StaticFiles(directory=str(STATIC_PATH), html=True), name="static")
