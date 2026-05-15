#!/usr/bin/env python3
"""
Shopee Pipeline — Production scraper.

Pipeline:
  GoLogin (local profile) ─► Playwright (CDP) ─► Shopee dashboards ─► Supabase

Per profile it visits TWO pages and extracts TWO numbers:
  1. Affiliate Dashboard  → 'ค่าคอมมิชชันโดยประมาณ'  → commission
  2. Live Ads page        → 'ค่าโฆษณา'                → ads_cost

Both extractors are resilient: any failure (timeout / element missing / parse
error) ⇒ value becomes 0. The script never crashes on a single bad profile.

Usage:
  python scraper.py                            # today, all profiles
  python scraper.py --limit 1 --dry-run        # smoke test 1 profile, no DB
  python scraper.py --test-parse               # exercise number parser only
  python scraper.py --target-date 2026-05-14
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

from dotenv import load_dotenv
from playwright.async_api import (
    async_playwright,
    Error as PWError,
    TimeoutError as PWTimeout,
)

load_dotenv()

# ─── Config ───────────────────────────────────────────────────────────────────
GOLOGIN_TOKEN        = os.getenv("GOLOGIN_TOKEN", "")
GOLOGIN_PROFILES_DIR = os.getenv(
    "GOLOGIN_PROFILES_DIR",
    r"C:\Users\SATHAPORN\AppData\Roaming\GoLogin\profiles",
)
SHOPEE_AFFILIATE_URL = os.getenv(
    "SHOPEE_AFFILIATE_URL",
    "https://affiliate.shopee.co.th/dashboard",
)
SHOPEE_LIVE_ADS_URL = os.getenv(
    "SHOPEE_LIVE_ADS_URL",
    "https://creator.shopee.co.th/insight/live-ads",
)
DEFAULT_GROUP   = os.getenv("DEFAULT_GROUP", "kshomeaxie29")
DATABASE_URL    = os.getenv("DATABASE_URL", "")
CONCURRENCY     = int(os.getenv("CONCURRENCY", "1"))     # local browser = serial by default
NAV_TIMEOUT_MS  = int(os.getenv("NAV_TIMEOUT_MS", "30000"))
LABEL_WAIT_MS   = int(os.getenv("LABEL_WAIT_MS",  "15000"))

PROFILES_PATH = Path(__file__).parent / "profiles.json"


# ════════════════════════════════════════════════════════════════════════════
#                          NUMBER PARSING (Thai + Western)
# ════════════════════════════════════════════════════════════════════════════

THAI_SUFFIX = {
    # Longest first (matters because 'หมื่น' shares chars with 'ล้าน' etc.)
    "ล้าน":  1_000_000,
    "แสน":  100_000,
    "หมื่น": 10_000,
    "พัน":  1_000,
}
WESTERN_SUFFIX = {
    "k": 1_000, "K": 1_000,
    "m": 1_000_000, "M": 1_000_000,
    "b": 1_000_000_000, "B": 1_000_000_000,
}


def parse_thai_number(text: str) -> float:
    """
    Convert localized number strings to a float.

    Handles:
      '8.4พัน'    → 8400.0
      '฿2.2k'     → 2200.0
      '1,234'     → 1234.0
      '2ล้าน'     → 2000000.0
      '3.5 หมื่น'  → 35000.0
      '฿ 8.4 พัน'  → 8400.0
      '0', '', '-' → 0.0

    Returns 0.0 on ANY failure — never raises.
    """
    if not text:
        return 0.0

    # Strip currency, commas, whitespace
    s = re.sub(r"[฿$,\s]", "", str(text).strip())
    if not s or s in ("-", "—", "–"):
        return 0.0

    # Try Thai suffixes (longest first)
    for suffix, mult in THAI_SUFFIX.items():
        if s.endswith(suffix):
            body = s[:-len(suffix)]
            try:
                return float(body) * mult
            except ValueError:
                return 0.0

    # Try Western suffixes (1 char)
    if s and s[-1] in WESTERN_SUFFIX:
        try:
            return float(s[:-1]) * WESTERN_SUFFIX[s[-1]]
        except ValueError:
            return 0.0

    # Plain number
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return 0.0
    return 0.0


# ════════════════════════════════════════════════════════════════════════════
#                          CORE EXTRACTOR (the headline function)
# ════════════════════════════════════════════════════════════════════════════

# Walk the DOM near a given label and return the closest visible numeric text.
# Runs inside the page so it sees the same layout the user does.
_FIND_NUMBER_NEAR_LABEL_JS = r"""
(labelText) => {
  // What "looks like a number" in this dashboard's locale
  const NUM_RE = /^\s*[฿$]?\s*-?[\d,.]+\s*(พัน|หมื่น|แสน|ล้าน|k|K|m|M|b|B)?\s*$/u;

  const isVisible = (el) => {
    if (!el) return false;
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) return false;
    const cs = window.getComputedStyle(el);
    return cs.visibility !== 'hidden' && cs.display !== 'none' && cs.opacity !== '0';
  };

  // 1) Find the deepest element whose OWN text contains the label.
  //    (Deepest = the actual UI label, not a wrapping container.)
  const all = Array.from(document.querySelectorAll('body *'));
  const labelEls = all.filter(el => {
    if (!isVisible(el)) return false;
    const own = Array.from(el.childNodes)
      .filter(n => n.nodeType === Node.TEXT_NODE)
      .map(n => n.textContent).join('').trim();
    return own && own.includes(labelText);
  });
  if (!labelEls.length) return null;

  // Pick the deepest match
  labelEls.sort((a, b) => {
    const depth = (n) => { let d = 0; while (n) { d++; n = n.parentElement; } return d; };
    return depth(b) - depth(a);
  });
  const labelEl = labelEls[0];

  const isNumeric = (txt) => {
    if (!txt) return false;
    const t = txt.trim();
    if (!t || t.length > 30) return false;
    return NUM_RE.test(t);
  };

  // 2) Check direct siblings of the label
  if (labelEl.parentElement) {
    for (const sib of labelEl.parentElement.children) {
      if (sib === labelEl) continue;
      if (!isVisible(sib)) continue;
      const txt = sib.textContent.trim();
      if (isNumeric(txt)) return txt;
    }
  }

  // 3) Walk up to 5 ancestors; for each, scan its descendants
  let ancestor = labelEl.parentElement;
  for (let depth = 0; depth < 5 && ancestor; depth++) {
    for (const child of ancestor.querySelectorAll('*')) {
      if (child === labelEl) continue;
      if (child.contains(labelEl) || labelEl.contains(child)) continue;
      if (!isVisible(child)) continue;
      const txt = child.textContent.trim();
      if (isNumeric(txt)) return txt;
    }
    ancestor = ancestor.parentElement;
  }

  return null;
}
"""


async def _find_number_near_label(page, label_text: str, timeout_ms: int = LABEL_WAIT_MS) -> float:
    """
    Wait for `label_text` to appear, then return the nearest numeric value
    as a float. Returns 0.0 on any failure.
    """
    try:
        loc = page.get_by_text(label_text, exact=False).first
        await loc.wait_for(state="visible", timeout=timeout_ms)
    except PWTimeout:
        print(f"  [warn] label '{label_text}' did not appear within {timeout_ms}ms")
        return 0.0
    except Exception as e:
        print(f"  [warn] waiting for '{label_text}' failed: {type(e).__name__}: {e}")
        return 0.0

    try:
        raw = await page.evaluate(_FIND_NUMBER_NEAR_LABEL_JS, label_text)
    except Exception as e:
        print(f"  [warn] DOM walk failed for '{label_text}': {e}")
        return 0.0

    if raw is None:
        print(f"  [warn] '{label_text}' found but no numeric value nearby")
        return 0.0

    value = parse_thai_number(raw)
    print(f"  [ok]   '{label_text}' → raw='{raw}' → {value}")
    return value


async def extract_shopee_data(page) -> dict:
    """
    Extract two metrics from the Shopee Affiliate platform.

    Navigates to:
      • Affiliate Dashboard  → finds the number next to 'ค่าคอมมิชชันโดยประมาณ'
      • Live Ads page        → finds the number under 'ค่าโฆษณา'

    Both values are localized (e.g. '8.4พัน', '฿2.2k') and converted to ints
    in baht (8400, 2200).

    Returns:
        {"commission": float, "ads_cost": float}

    Robust to failures:
      - page navigation timeout    → that metric = 0
      - label not found on page    → that metric = 0
      - number can't be parsed     → that metric = 0
    Never raises.
    """
    result = {"commission": 0.0, "ads_cost": 0.0}

    # ───── 1. Commission (Affiliate Dashboard) ────────────────────────
    try:
        print(f"  → goto {SHOPEE_AFFILIATE_URL}")
        await page.goto(SHOPEE_AFFILIATE_URL, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        # Some pages render KPIs lazily — give them a moment
        await page.wait_for_load_state("networkidle", timeout=8000)
    except PWTimeout:
        print(f"  [warn] affiliate page nav timeout")
    except Exception as e:
        print(f"  [warn] affiliate nav: {type(e).__name__}: {e}")
    else:
        result["commission"] = await _find_number_near_label(page, "ค่าคอมมิชชันโดยประมาณ")

    # ───── 2. Ads Cost (Live Ads page) ────────────────────────────────
    try:
        print(f"  → goto {SHOPEE_LIVE_ADS_URL}")
        await page.goto(SHOPEE_LIVE_ADS_URL, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        await page.wait_for_load_state("networkidle", timeout=8000)
    except PWTimeout:
        print(f"  [warn] live-ads page nav timeout")
    except Exception as e:
        print(f"  [warn] live-ads nav: {type(e).__name__}: {e}")
    else:
        result["ads_cost"] = await _find_number_near_label(page, "ค่าโฆษณา")

    return result


# ════════════════════════════════════════════════════════════════════════════
#                          GoLogin (local profile launcher)
# ════════════════════════════════════════════════════════════════════════════

def start_gologin_local(profile_id: str):
    """
    Start a GoLogin profile from local storage.

    Returns a tuple (gl, ws_url) where:
      gl     — GoLogin SDK instance (caller must .stop() it)
      ws_url — CDP debugger URL for Playwright.connect_over_cdp()
    """
    from gologin import GoLogin
    gl = GoLogin({
        "token":       GOLOGIN_TOKEN,
        "profile_id":  profile_id,
        "tmpdir":      GOLOGIN_PROFILES_DIR,
    })
    debug_address = gl.start()  # blocks; returns "127.0.0.1:35873"
    return gl, f"http://{debug_address}"


def stop_gologin(gl) -> None:
    """Best-effort stop — never raises."""
    try:
        gl.stop()
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════
#                          Per-profile worker
# ════════════════════════════════════════════════════════════════════════════

@dataclass
class Result:
    profile_id:        str
    profile_name:      str
    account_group:     str
    date:              str
    scraped_at:        str
    open_channel_cost: float = 0.0
    ads_cost:          float = 0.0
    commission:        float = 0.0
    status:            str = "Failed"
    error_reason:      Optional[str] = None
    error_detail:      Optional[str] = None
    duration_ms:       int = 0


async def scrape_one(playwright, profile, target_date) -> Result:
    pid   = profile["id"]
    name  = profile.get("name", pid)
    group = profile.get("group", DEFAULT_GROUP)

    res = Result(
        profile_id=pid,
        profile_name=name,
        account_group=group,
        date=target_date,
        scraped_at=datetime.now().isoformat(),
    )
    t0 = time.time()
    gl = None
    browser = None

    print(f"\n┌─ [{name}]  group={group}  ({pid})")
    try:
        # 1. Start local GoLogin profile
        try:
            gl, ws_url = await asyncio.to_thread(start_gologin_local, pid)
            print(f"│  gologin started → {ws_url}")
        except Exception as e:
            res.error_reason = "gologin_start"
            res.error_detail = str(e)[:300]
            print(f"│  ✗ gologin start failed: {e}")
            return res

        # 2. Attach Playwright via CDP
        try:
            browser = await playwright.chromium.connect_over_cdp(ws_url)
        except PWError as e:
            res.error_reason = "cdp_attach"
            res.error_detail = str(e)[:300]
            print(f"│  ✗ cdp attach failed: {e}")
            return res

        ctx  = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # 3. Extract data (returns 0 for failures — never raises)
        data = await extract_shopee_data(page)
        res.commission = data["commission"]
        res.ads_cost   = data["ads_cost"]
        res.status     = "Success"
        print(f"│  ✓ commission={res.commission}  ads_cost={res.ads_cost}")

    except Exception as e:
        res.error_reason = "unknown"
        res.error_detail = f"{type(e).__name__}: {str(e)[:200]}"
        print(f"│  ✗ unknown error: {e}")
    finally:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        if gl:
            await asyncio.to_thread(stop_gologin, gl)
        res.duration_ms = int((time.time() - t0) * 1000)
        print(f"└─ done in {res.duration_ms}ms")

    return res


# ════════════════════════════════════════════════════════════════════════════
#                          Orchestrator
# ════════════════════════════════════════════════════════════════════════════

async def orchestrate(profiles, target_date, concurrency):
    sem = asyncio.Semaphore(concurrency)

    async with async_playwright() as pw:
        async def run(profile):
            async with sem:
                return await scrape_one(pw, profile, target_date)

        return await asyncio.gather(*(run(p) for p in profiles))


# ════════════════════════════════════════════════════════════════════════════
#                          Supabase writer (UPSERT)
# ════════════════════════════════════════════════════════════════════════════

def write_results(results) -> int:
    """Upsert successful rows. Failed scrapes are NOT written (DB stays clean)."""
    if not DATABASE_URL:
        print("[warn] DATABASE_URL not set — skipping DB write")
        return 0

    rows = [
        (r.date, r.profile_name, r.open_channel_cost,
         r.ads_cost, r.commission, r.account_group, r.scraped_at)
        for r in results if r.status == "Success"
    ]
    if not rows:
        return 0

    import psycopg2
    conn = psycopg2.connect(DATABASE_URL)
    try:
        c = conn.cursor()
        c.executemany("""
            INSERT INTO shopee_metrics
                (date, profile_name, open_channel_cost, ads_cost,
                 commission, account_group, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (profile_name, date) DO UPDATE SET
                open_channel_cost = EXCLUDED.open_channel_cost,
                ads_cost          = EXCLUDED.ads_cost,
                commission        = EXCLUDED.commission,
                account_group     = EXCLUDED.account_group,
                scraped_at        = EXCLUDED.scraped_at
        """, rows)
        conn.commit()
        return len(rows)
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════════════════
#                          Parse self-test
# ════════════════════════════════════════════════════════════════════════════

PARSE_TESTS = [
    ("8.4พัน",     8400),
    ("฿2.2k",      2200),
    ("฿ 8.4 พัน",  8400),
    ("1,234",      1234),
    ("2ล้าน",      2_000_000),
    ("3.5หมื่น",   35_000),
    ("100แสน",     10_000_000),
    ("฿0",         0),
    ("",           0),
    ("-",          0),
    ("12,345.67",  12345.67),
    ("2.5M",       2_500_000),
]


def run_parse_tests() -> bool:
    print("Running parse_thai_number self-tests…\n")
    ok = 0
    for raw, expected in PARSE_TESTS:
        actual = parse_thai_number(raw)
        tag = "PASS" if abs(actual - expected) < 0.01 else "FAIL"
        if tag == "PASS":
            ok += 1
        print(f"  [{tag}] {raw!r:<18} → {actual:<12}  expected {expected}")
    total = len(PARSE_TESTS)
    print(f"\n{ok}/{total} passed")
    return ok == total


# ════════════════════════════════════════════════════════════════════════════
#                          CLI
# ════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Shopee Pipeline — local GoLogin scraper")
    parser.add_argument("--target-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--profiles", default=str(PROFILES_PATH))
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY)
    parser.add_argument("--limit", type=int, default=0, help="Only scrape first N profiles")
    parser.add_argument("--dry-run", action="store_true", help="Scrape but don't write")
    parser.add_argument("--test-parse", action="store_true", help="Run parse_thai_number tests and exit")
    args = parser.parse_args()

    if args.test_parse:
        sys.exit(0 if run_parse_tests() else 1)

    if not GOLOGIN_TOKEN:
        print("ERROR: GOLOGIN_TOKEN missing in .env", file=sys.stderr); sys.exit(1)
    if not Path(args.profiles).exists():
        print(f"ERROR: {args.profiles} not found — copy profiles.example.json first", file=sys.stderr)
        sys.exit(1)
    if not Path(GOLOGIN_PROFILES_DIR).exists():
        print(f"WARN: GoLogin profiles dir not found: {GOLOGIN_PROFILES_DIR}", file=sys.stderr)
        print("      The SDK will download profiles from cloud on first use.", file=sys.stderr)

    with open(args.profiles, encoding="utf-8") as f:
        profiles = json.load(f)
    if not isinstance(profiles, list) or not profiles:
        print(f"ERROR: {args.profiles} must be a non-empty JSON list", file=sys.stderr); sys.exit(1)

    if args.limit > 0:
        profiles = profiles[:args.limit]

    print(f"┌─ Shopee scrape · {args.target_date}")
    print(f"│  Profiles:        {len(profiles)}  (concurrency {args.concurrency})")
    print(f"│  Default group:   {DEFAULT_GROUP}")
    print(f"│  Profiles dir:    {GOLOGIN_PROFILES_DIR}")
    print(f"│  Affiliate URL:   {SHOPEE_AFFILIATE_URL}")
    print(f"│  Live Ads URL:    {SHOPEE_LIVE_ADS_URL}")
    print(f"│  DB:              {'Supabase' if DATABASE_URL else 'NONE'}{'  [dry-run]' if args.dry_run else ''}")
    print(f"└─ Starting…")

    t0 = time.time()
    results = asyncio.run(orchestrate(profiles, args.target_date, args.concurrency))
    dt = time.time() - t0

    success = sum(1 for r in results if r.status == "Success")
    failed  = len(results) - success
    print(f"\n═══════════════════════════════════════════════════════════")
    print(f"Finished in {dt:.1f}s — {success} ok · {failed} fail "
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
        for r in results:
            print(f"  {r.profile_name:<20} commission={r.commission:<10} ads_cost={r.ads_cost}")
    else:
        n = write_results(results)
        print(f"\nUpserted {n} rows to Supabase")


if __name__ == "__main__":
    main()
