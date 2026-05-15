#!/usr/bin/env python3
"""
Shopee Pipeline — Production scraper.

Orchestrates GoLogin profiles via API, attaches Playwright over CDP,
extracts the three core metrics, and upserts them into Supabase Postgres.

Schema (per row):
    date              YYYY-MM-DD
    profile_name      channel name (e.g. lonaharper)
    open_channel_cost number (เปิดช่อง)
    ads_cost          number (คอยน์+แอด)
    commission        number (ค่าคอมมิชชัน)
    account_group     group name (e.g. kshomeaxie29)

Rule: NEVER store 'total' rows. The dashboard sums these client-side.

Usage:
  python scraper.py                              # today, profiles.json, concurrency from .env
  python scraper.py --target-date 2026-05-14
  python scraper.py --profiles other.json
  python scraper.py --concurrency 3
  python scraper.py --dry-run                    # scrape but don't write
  python scraper.py --limit 5                    # smoke test
"""

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from playwright.async_api import (
    async_playwright,
    TimeoutError as PWTimeout,
    Error as PWError,
)

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────
GOLOGIN_TOKEN        = os.getenv("GOLOGIN_TOKEN", "")
SHOPEE_DASHBOARD_URL = os.getenv("SHOPEE_DASHBOARD_URL", "https://affiliate.shopee.co.th/dashboard")
DATABASE_URL         = os.getenv("DATABASE_URL", "")

CONCURRENCY            = int(os.getenv("CONCURRENCY", "5"))
NAV_TIMEOUT_MS         = int(os.getenv("NAV_TIMEOUT_MS", "15000"))
WORKER_HARD_TIMEOUT_S  = int(os.getenv("WORKER_HARD_TIMEOUT_S", "60"))

GOLOGIN_API_BASE = "https://api.gologin.com"
PROFILES_PATH    = Path(__file__).parent / "profiles.json"

# CSS selectors — customise for the Shopee dashboard layout you're scraping
SEL = {
    "open_channel_cost": "[data-testid='open-channel-cost'], .open-channel-cost, [data-key='open_channel']",
    "ads_cost":          "[data-testid='ads-cost'],          .ads-cost,          [data-key='coin_ads']",
    "commission":        "[data-testid='commission'],        .commission-value,  [data-key='commission']",
}

# Blockers: if any of these are visible, mark the row as Failed with that reason
BLOCKER_PATTERNS = {
    "captcha": [
        "iframe[src*='recaptcha']",
        "iframe[src*='hcaptcha']",
        "iframe[title*='captcha' i]",
        ".captcha-container",
        "#captcha",
    ],
    "otp": [
        "input[name='otp']",
        "input[name='code']",
        "input[autocomplete='one-time-code']",
        "input[name='verification-code']",
    ],
    "verify": [
        "text=/verify your identity/i",
        ".verify-identity",
    ],
}


# ─── Data structure ───────────────────────────────────────────────────────────
@dataclass
class ScrapeResult:
    profile_id:        str
    profile_name:      str
    account_group:     str
    date:              str
    scraped_at:        str
    open_channel_cost: Optional[float] = None
    ads_cost:          Optional[float] = None
    commission:        Optional[float] = None
    status:            str = "Failed"
    error_reason:      Optional[str] = None
    error_detail:      Optional[str] = None
    duration_ms:       int = 0


# ─── GoLogin API ──────────────────────────────────────────────────────────────
class GoLoginError(Exception):
    pass


async def gl_start(http: httpx.AsyncClient, profile_id: str) -> str:
    url = f"{GOLOGIN_API_BASE}/browser/{profile_id}/web"
    r = await http.post(url, timeout=30)
    if r.status_code != 200:
        raise GoLoginError(f"start HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    ws = data.get("wsUrl") or data.get("wsEndpoint") or data.get("debuggerAddress")
    if not ws:
        raise GoLoginError(f"no debugger address: {data}")
    if not (ws.startswith("ws://") or ws.startswith("wss://")):
        ws = f"ws://{ws}"
    return ws


async def gl_stop(http: httpx.AsyncClient, profile_id: str) -> None:
    url = f"{GOLOGIN_API_BASE}/browser/{profile_id}/web"
    try:
        await http.delete(url, timeout=15)
    except Exception:
        pass


# ─── Page helpers ─────────────────────────────────────────────────────────────
async def detect_blocker(page) -> Optional[str]:
    for kind, selectors in BLOCKER_PATTERNS.items():
        for sel in selectors:
            try:
                if await page.locator(sel).first.is_visible(timeout=400):
                    return kind
            except Exception:
                continue
    return None


_NUM_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")


def parse_number(text: str) -> Optional[float]:
    """'฿ 12,345.67' → 12345.67"""
    if not text:
        return None
    m = _NUM_RE.search(text.replace(" ", ""))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


async def safe_extract_number(page, css: str, timeout_ms: int = 5000) -> Optional[float]:
    try:
        loc = page.locator(css).first
        await loc.wait_for(state="visible", timeout=timeout_ms)
        return parse_number((await loc.inner_text()).strip())
    except Exception:
        return None


# ─── Single-profile worker ────────────────────────────────────────────────────
async def _scrape_one(playwright, http, profile, target_date) -> ScrapeResult:
    pid   = profile["id"]
    name  = profile.get("name", pid)
    group = profile.get("group", "")
    res = ScrapeResult(
        profile_id=pid,
        profile_name=name,
        account_group=group,
        date=target_date,
        scraped_at=datetime.now().isoformat(),
    )
    started = time.time()
    browser = None
    ws_url  = None

    try:
        # 1. Start GoLogin profile
        try:
            ws_url = await asyncio.wait_for(gl_start(http, pid), timeout=25)
        except (asyncio.TimeoutError, GoLoginError) as e:
            res.error_reason, res.error_detail = "gologin_start", str(e)
            return res

        # 2. Attach Playwright via CDP
        try:
            browser = await playwright.chromium.connect_over_cdp(ws_url)
        except PWError as e:
            res.error_reason, res.error_detail = "gologin_start", f"CDP attach failed: {e}"
            return res

        ctx  = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # 3. Navigate
        try:
            await page.goto(SHOPEE_DASHBOARD_URL, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        except PWTimeout:
            res.error_reason = "nav_timeout"
            return res

        # 4. Detect blockers
        blocker = await detect_blocker(page)
        if blocker:
            res.error_reason = blocker
            return res

        # 5. Extract metrics
        openc = await safe_extract_number(page, SEL["open_channel_cost"])
        ads   = await safe_extract_number(page, SEL["ads_cost"])
        comm  = await safe_extract_number(page, SEL["commission"])

        if all(v is None for v in (openc, ads, comm)):
            res.error_reason = "no_metrics"
            return res

        res.open_channel_cost = openc or 0
        res.ads_cost          = ads or 0
        res.commission        = comm or 0
        res.status            = "Success"
        return res

    except PWError as e:
        res.error_reason, res.error_detail = "playwright", str(e)[:300]
        return res
    except Exception as e:
        res.error_reason = "unknown"
        res.error_detail = f"{type(e).__name__}: {str(e)[:200]}"
        return res
    finally:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        if ws_url:
            await gl_stop(http, pid)
        res.duration_ms = int((time.time() - started) * 1000)


async def scrape_with_timeout(playwright, http, profile, target_date) -> ScrapeResult:
    try:
        return await asyncio.wait_for(
            _scrape_one(playwright, http, profile, target_date),
            timeout=WORKER_HARD_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return ScrapeResult(
            profile_id=profile["id"],
            profile_name=profile.get("name", profile["id"]),
            account_group=profile.get("group", ""),
            date=target_date,
            scraped_at=datetime.now().isoformat(),
            status="Failed",
            error_reason="hard_timeout",
            duration_ms=WORKER_HARD_TIMEOUT_S * 1000,
        )


# ─── Orchestrator ─────────────────────────────────────────────────────────────
async def orchestrate(profiles, target_date, concurrency):
    sem = asyncio.Semaphore(concurrency)
    results = []
    headers = {"Authorization": f"Bearer {GOLOGIN_TOKEN}"}

    async with httpx.AsyncClient(headers=headers) as http:
        async with async_playwright() as pw:
            async def run(profile):
                async with sem:
                    return await scrape_with_timeout(pw, http, profile, target_date)

            tasks = [asyncio.create_task(run(p)) for p in profiles]
            total = len(tasks)
            done = 0
            for fut in asyncio.as_completed(tasks):
                r = await fut
                results.append(r)
                done += 1
                tag = "OK " if r.status == "Success" else "FAIL"
                err = f" [{r.error_reason}]" if r.error_reason else ""
                print(f"  [{done:>3}/{total}] {tag} {r.profile_name:<28} {r.duration_ms:>5}ms{err}")
    return results


# ─── DB writer (UPSERT — never duplicates a (profile, date) pair) ────────────
def write_results(results) -> int:
    """Upsert successful rows. Failed rows are skipped (kept clean per the rule)."""
    if not DATABASE_URL:
        print("[warn] DATABASE_URL not set — skipping DB write")
        return 0

    rows = [
        (r.date, r.profile_name, r.open_channel_cost or 0,
         r.ads_cost or 0, r.commission or 0, r.account_group, r.scraped_at)
        for r in results if r.status == "Success"
    ]
    if not rows:
        return 0

    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    try:
        c = conn.cursor()
        sql = """
            INSERT INTO shopee_metrics
                (date, profile_name, open_channel_cost, ads_cost, commission,
                 account_group, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (profile_name, date) DO UPDATE SET
                open_channel_cost = EXCLUDED.open_channel_cost,
                ads_cost          = EXCLUDED.ads_cost,
                commission        = EXCLUDED.commission,
                account_group     = EXCLUDED.account_group,
                scraped_at        = EXCLUDED.scraped_at
        """
        c.executemany(sql, rows)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


# ─── CLI ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Shopee Pipeline — production scraper")
    parser.add_argument("--target-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--profiles", default=str(PROFILES_PATH))
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY)
    parser.add_argument("--limit", type=int, default=0, help="Only scrape first N profiles")
    parser.add_argument("--dry-run", action="store_true", help="Scrape but don't write")
    args = parser.parse_args()

    if not GOLOGIN_TOKEN:
        print("ERROR: GOLOGIN_TOKEN missing in .env", file=sys.stderr); sys.exit(1)
    if not Path(args.profiles).exists():
        print(f"ERROR: {args.profiles} not found — copy profiles.example.json first", file=sys.stderr)
        sys.exit(1)

    with open(args.profiles, encoding="utf-8") as f:
        profiles = json.load(f)
    if not isinstance(profiles, list) or not profiles:
        print(f"ERROR: {args.profiles} must be a non-empty JSON list", file=sys.stderr); sys.exit(1)

    if args.limit > 0:
        profiles = profiles[:args.limit]

    db_target = "Supabase/Postgres" if DATABASE_URL else "<no DB>"
    print(f"┌─ Shopee scrape · {args.target_date}")
    print(f"│  Profiles:   {len(profiles)}  (concurrency {args.concurrency})")
    print(f"│  Target URL: {SHOPEE_DASHBOARD_URL}")
    print(f"│  DB:         {db_target}{'  [dry-run]' if args.dry_run else ''}")
    print(f"└─ Starting…\n")

    t0 = time.time()
    results = asyncio.run(orchestrate(profiles, args.target_date, args.concurrency))
    dt = time.time() - t0

    success = sum(1 for r in results if r.status == "Success")
    failed  = len(results) - success
    print(f"\nFinished in {dt:.1f}s — {success} ok · {failed} fail "
          f"({success / len(results) * 100:.1f}% success)")

    if failed:
        reasons = {}
        for r in results:
            if r.status == "Failed":
                reasons[r.error_reason] = reasons.get(r.error_reason, 0) + 1
        print("Failure breakdown:")
        for reason, count in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"  {reason:<16} {count}")

    if args.dry_run:
        print("\n--dry-run: skipping DB write")
        for r in results[:5]:
            if r.status == "Success":
                print(f"  {r.profile_name}: open={r.open_channel_cost} ads={r.ads_cost} comm={r.commission}")
    else:
        n = write_results(results)
        print(f"\nUpserted {n} rows to database")


if __name__ == "__main__":
    main()
