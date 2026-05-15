#!/usr/bin/env python3
"""
Shopee Pipeline — Async orchestrator for 400 GoLogin profiles.
Extracts Shopee Live-Ads / Affiliate metrics (Ads Cost, Est. Commission, ROAS).

Usage:
  python scraper.py                          # defaults to today's date
  python scraper.py --target-date 2026-05-14
  python scraper.py --profiles custom_profiles.json
"""

import asyncio
import json
import sqlite3
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

GOLOGIN_TOKEN = os.getenv("GOLOGIN_TOKEN", "")
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))
NAV_TIMEOUT_MS = int(os.getenv("NAV_TIMEOUT_MS", "15000"))

DB_PATH = Path(__file__).parent / "shopee_data.db"
PROFILES_PATH = Path(__file__).parent / "profiles.json"

SEL = {
    "ads_cost":       "[data-testid='live-ads-cost'], .live-ads-cost",
    "est_commission": "[data-testid='est-commission'], .est-commission",
    "roas":           "[data-testid='roas'], .roas-value",
}

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
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
    """)
    conn.commit()
    conn.close()

async def scrape_one(profile_id, profile_name, target_date):
    """Placeholder: would orchestrate GoLogin + Playwright here."""
    # In production, this would:
    # 1. gl.start() to get debugger address
    # 2. connect via Playwright CDP
    # 3. navigate to SHOPEE_DASHBOARD_URL
    # 4. detect blockers (captcha, OTP, verify)
    # 5. extract numbers from SEL selectors
    # 6. return result or failure reason
    #
    # For now, return a mock result.
    await asyncio.sleep(0.1)
    return {
        "profile_id": profile_id,
        "profile_name": profile_name,
        "target_date": target_date,
        "ads_cost": 1200.50,
        "est_commission": 2400.00,
        "roas": 2.0,
        "status": "Success",
        "error_reason": None,
        "duration_ms": 3000,
    }

async def orchestrate(profiles, target_date):
    """Run up to CONCURRENCY profiles in parallel."""
    sem = asyncio.Semaphore(CONCURRENCY)

    async def with_sem(profile):
        async with sem:
            return await scrape_one(
                profile["id"],
                profile.get("name", profile["id"]),
                target_date
            )

    tasks = [with_sem(p) for p in profiles]
    return await asyncio.gather(*tasks)

def main():
    parser = argparse.ArgumentParser(description="Shopee Pipeline Scraper")
    parser.add_argument("--target-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--profiles", default=str(PROFILES_PATH))
    args = parser.parse_args()

    print(f"Scraping {args.profiles} for {args.target_date}...")

    if not Path(args.profiles).exists():
        print(f"Error: {args.profiles} not found. Copy profiles.example.json to profiles.json and fill in your profile IDs.")
        return

    with open(args.profiles) as f:
        profiles = json.load(f)

    init_db()
    results = asyncio.run(orchestrate(profiles, args.target_date))

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    for r in results:
        c.execute("""
            INSERT INTO scrape_results
            (profile_id, profile_name, target_date, scraped_at, ads_cost, est_commission, roas, status, error_reason, duration_ms)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r["profile_id"],
            r["profile_name"],
            r["target_date"],
            datetime.now().isoformat(),
            r.get("ads_cost"),
            r.get("est_commission"),
            r.get("roas"),
            r["status"],
            r.get("error_reason"),
            r.get("duration_ms", 0),
        ))

    conn.commit()
    conn.close()
    print(f"Wrote {len(results)} rows to {DB_PATH}")

if __name__ == "__main__":
    main()
