#!/usr/bin/env python3
"""
Shopee Pipeline — Production scraper.

Orchestrates GoLogin profiles via API, attaches Playwright over CDP,
extracts Live-Ads metrics (Ads Cost / Est. Commission / ROAS), and writes
results to Supabase Postgres.

Designed to run on your LOCAL machine (the Render dashboard only reads
the DB — it does not run Playwright).

Usage:
  python scraper.py                              # today, profiles.json, concurrency from .env
  python scraper.py --target-date 2026-05-14
  python scraper.py --profiles other.json
  python scraper.py --concurrency 3              # override .env
  python scraper.py --dry-run                    # scrape but don't write
  python scraper.py --limit 5                    # only scrape first 5 profiles (smoke test)
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
    "ads_cost":       "[data-testid='live-ads-cost'], .live-ads-cost, [data-key='ads_cost']",
    "est_commission": "[data-testid='est-commission'], .est-commission, [data-key='commission']",
    "roas":           "[data-testid='roas'], .roas-value, [data-key='roas']",
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
    profile_id: str
    profile_name: str
    target_date: str
    scraped_at: str
    ads_cost: Optional[float] = None
    est_commission: Optional[float] = None
    roas: Optional[float] = None
    status: str = "Failed"
    error_reason: Optional[str] = None
    error_detail: Optional[str] = None
    duration_ms: int = 0


# ─── GoLogin API ──────────────────────────────────────────────────────────────
class GoLoginError(Exception):
    pass


async def gl_start(http: httpx.AsyncClient, profile_id: str) -> str:
    """Start a GoLogin profile, return CDP websocket URL."""
    url = f"{GOLOGIN_API_BASE}/browser/{profile_id}/web"
    r = await http.post(url, timeout=30)
    if r.status_code != 200:
        raise GoLoginError(f"start HTTP {r.status_code}: {r.text[:200]}")
    data = r.json()
    ws = data.get("wsUrl") or data.get("wsEndpoint") or data.get("debuggerAddress")
    if not ws:
        raise GoLoginError(f"no debugger address returned: {data}")
    if not (ws.startswith("ws://") or ws.startswith("wss://")):
        ws = f"ws://{ws}"
    return ws


async def gl_stop(http: httpx.AsyncClient, profile_id: str) -> None:
    """Stop a GoLogin profile. Best-effort — never raises."""
    url = f"{GOLOGIN_API_BASE}/browser/{profile_id}/web"
    try:
        await http.delete(url, timeout=15)
    except Exception:
        pass


# ─── Page helpers ─────────────────────────────────────────────────────────────
async def detect_blocker(page) -> Optional[str]:
    """Return blocker tag (captcha/otp/verify) if visible, else None."""
    for kind, selectors in BLOCKER_PATTERNS.items():
        for sel in selectors:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible(timeout=400):
                    return kind
            except Exception:
                continue
    return None


_NUM_RE = re.compile(r"-?[\d,]+(?:\.\d+)?")


def parse_number(text: str) -> Optional[float]:
    """Extract first numeric token from a string. '฿ 12,345.67' → 12345.67"""
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
        txt = (await loc.inner_text()).strip()
        return parse_number(txt)
    except Exception:
        return None


# ─── Single-profile worker ────────────────────────────────────────────────────
async def _scrape_one(playwright, http, profile, target_date) -> ScrapeResult:
    pid  = profile["id"]
    name = profile.get("name", pid)
    res = ScrapeResult(
        profile_id=pid,
        profile_name=name,
        target_date=target_date,
        scraped_at=datetime.now().isoformat(),
    )
    started = time.time()
    browser = None
    ws_url = None

    try:
        # 1. Start GoLogin profile
        try:
            ws_url = await asyncio.wait_for(gl_start(http, pid), timeout=25)
        except (asyncio.TimeoutError, GoLoginError) as e:
            res.error_reason = "gologin_start"
            res.error_detail = str(e)
            return res

        # 2. Attach Playwright via CDP
        try:
            browser = await playwright.chromium.connect_over_cdp(ws_url)
        except PWError as e:
            res.error_reason = "gologin_start"
            res.error_detail = f"CDP attach failed: {e}"
            return res

        ctx  = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # 3. Navigate (15-second hard cap)
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
        ads_cost = await safe_extract_number(page, SEL["ads_cost"])
        est_comm = await safe_extract_number(page, SEL["est_commission"])
        roas     = await safe_extract_number(page, SEL["roas"])

        if all(v is None for v in (ads_cost, est_comm, roas)):
            res.error_reason = "no_metrics"
            return res

        # Derive ROAS if missing but cost & commission present
        if roas is None and ads_cost and est_comm and ads_cost > 0:
            roas = round(est_comm / ads_cost, 4)

        res.ads_cost = ads_cost
        res.est_commission = est_comm
        res.roas = roas
        res.status = "Success"
        return res

    except PWError as e:
        res.error_reason = "playwright"
        res.error_detail = str(e)[:300]
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
    """Wrap _scrape_one in a wall-clock hard cap so a stuck profile cannot hang the cohort."""
    try:
        return await asyncio.wait_for(
            _scrape_one(playwright, http, profile, target_date),
            timeout=WORKER_HARD_TIMEOUT_S,
        )
    except asyncio.TimeoutError:
        return ScrapeResult(
            profile_id=profile["id"],
            profile_name=profile.get("name", profile["id"]),
            target_date=target_date,
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
            done = 0
            total = len(tasks)
            for fut in asyncio.as_completed(tasks):
                r = await fut
                results.append(r)
                done += 1
                tag = "OK " if r.status == "Success" else "FAIL"
                err = f" [{r.error_reason}]" if r.error_reason else ""
                print(f"  [{done:>3}/{total}] {tag} {r.profile_name:<28} {r.duration_ms:>5}ms{err}")

    return results


# ─── DB writer ────────────────────────────────────────────────────────────────
def write_results(results, batch_size: int = 100) -> int:
    """Bulk-insert results into Supabase. Returns rows written."""
    if not DATABASE_URL:
        print("[warn] DATABASE_URL is empty — skipping DB write")
        return 0

    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    try:
        c = conn.cursor()
        sql = """
            INSERT INTO scrape_results
            (profile_id, profile_name, target_date, scraped_at, ads_cost,
             est_commission, roas, status, error_reason, error_detail, duration_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [
            (r.profile_id, r.profile_name, r.target_date, r.scraped_at,
             r.ads_cost, r.est_commission, r.roas, r.status,
             r.error_reason, r.error_detail, r.duration_ms)
            for r in results
        ]
        for i in range(0, len(rows), batch_size):
            c.executemany(sql, rows[i:i + batch_size])
        conn.commit()
        return len(rows)
    finally:
        conn.close()


# ─── CLI ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Shopee Pipeline — production scraper")
    parser.add_argument("--target-date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="YYYY-MM-DD (default: today)")
    parser.add_argument("--profiles", default=str(PROFILES_PATH),
                        help="Path to profiles.json")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY,
                        help=f"Profiles in parallel (default {CONCURRENCY})")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only scrape first N profiles (0 = all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape but don't write to DB")
    args = parser.parse_args()

    # Sanity checks
    if not GOLOGIN_TOKEN:
        print("ERROR: GOLOGIN_TOKEN missing in .env", file=sys.stderr)
        sys.exit(1)
    if not Path(args.profiles).exists():
        print(f"ERROR: profiles file not found: {args.profiles}", file=sys.stderr)
        print("  Copy profiles.example.json to profiles.json and fill in your IDs.", file=sys.stderr)
        sys.exit(1)

    with open(args.profiles, encoding="utf-8") as f:
        profiles = json.load(f)
    if not isinstance(profiles, list) or not profiles:
        print(f"ERROR: {args.profiles} must be a non-empty JSON list", file=sys.stderr)
        sys.exit(1)

    if args.limit > 0:
        profiles = profiles[:args.limit]

    db_target = "Supabase/Postgres" if DATABASE_URL else "<dry — no DB configured>"

    print(f"┌─ Shopee scrape · {args.target_date}")
    print(f"│  Profiles:   {len(profiles)}  (concurrency {args.concurrency})")
    print(f"│  Target URL: {SHOPEE_DASHBOARD_URL}")
    print(f"│  DB:         {db_target}{'  [dry-run]' if args.dry_run else ''}")
    print(f"└─ Starting…\n")

    t0 = time.time()
    results = asyncio.run(orchestrate(profiles, args.target_date, args.concurrency))
    duration = time.time() - t0

    success = sum(1 for r in results if r.status == "Success")
    failed  = len(results) - success

    print(f"\nFinished in {duration:.1f}s — {success} ok · {failed} fail "
          f"({success / len(results) * 100:.1f}% success)")

    # Group failures by reason
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
    else:
        n = write_results(results)
        print(f"\nWrote {n} rows to database")


if __name__ == "__main__":
    main()
