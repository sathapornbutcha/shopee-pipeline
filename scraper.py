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

# Walk the DOM near a given label and return the closest numeric text.
# The label may appear MULTIPLE times on the page (e.g. KPI card + table header).
# For each occurrence, we walk up looking at siblings for a number — and pick
# the occurrence whose number is CLOSEST in DOM tree distance.
_FIND_NUMBER_NEAR_LABEL_JS = r"""
([labelText, exactMatch]) => {
  // Tags whose text content is code/markup, not user-visible data
  const SKIP = new Set(['SCRIPT', 'NOSCRIPT', 'STYLE', 'IFRAME', 'CODE', 'TEMPLATE']);

  // Either: (a) a number WITH a Thai/Western multiplier suffix, OR
  //         (b) a plain number not followed by another digit/dot or by '%'.
  const NUM_RE = /(?:-?\d[\d,]*(?:\.\d+)?\s*(?:พัน|หมื่น|แสน|ล้าน|k|K|m|M|b|B)|-?\d[\d,]*(?:\.\d+)?(?![.\d]|\s*%))/u;

  // Every element whose text matches the label is a candidate.
  // exactMatch=true ⇒ element's trimmed textContent must EQUAL the label
  //                   (so 'ค่าโฆษณา' does NOT match 'ค่าโฆษณาวันนี้').
  // exactMatch=false ⇒ substring match (more permissive).
  const isInSkippedTree = (el) => {
    let p = el;
    while (p) { if (SKIP.has(p.tagName)) return true; p = p.parentElement; }
    return false;
  };
  const all = Array.from(document.querySelectorAll('body *'));
  const candidates = all.filter(el => {
    if (isInSkippedTree(el)) return false;
    if (!el.textContent) return false;
    if (exactMatch) return el.textContent.trim() === labelText;
    return el.textContent.includes(labelText);
  });
  if (!candidates.length) return null;

  // Try DEEPEST candidates first — they're the most specific (smallest wrapper
  // around just the label). The shallow ones are page-wide wrappers and
  // their "siblings" are unrelated to the label.
  const depth = (n) => { let d = 0; while (n) { d++; n = n.parentElement; } return d; };
  candidates.sort((a, b) => depth(b) - depth(a));

  const findNumberIn = (txt) => {
    if (!txt) return null;
    const cleaned = txt.split(labelText).join('').trim();
    if (!cleaned || cleaned.length > 200) return null;
    // Skip digit-reel widgets (animated counters with all 10 digits stacked).
    const distinct = new Set(cleaned.replace(/[^\d]/g, '').split(''));
    if (distinct.size > 6) return null;
    // Skip descriptive Thai text (tooltips, captions) — a value sibling has
    // little to no Thai prose around the number.
    const thaiCount = (cleaned.match(/[฀-๿]/g) || []).length;
    if (thaiCount > 10) return null;
    const m = cleaned.match(NUM_RE);
    return m ? m[0].trim() : null;
  };

  // For each candidate, walk up. Track the shallowest level at which a
  // number is found across ALL candidates — that's the winner.
  let bestNum = null;
  let bestLevel = Infinity;

  for (const labelEl of candidates) {
    let current = labelEl;
    for (let level = 0; level < 6 && level < bestLevel; level++) {
      const parent = current.parentElement;
      if (!parent) break;
      let foundAtLevel = null;
      for (const sib of parent.children) {
        if (sib === current) continue;
        if (sib.contains(labelEl)) continue;
        if (SKIP.has(sib.tagName)) continue;            // ignore <script>/<noscript> siblings
        const num = findNumberIn(sib.textContent);
        if (num !== null) { foundAtLevel = num; break; }
      }
      if (foundAtLevel !== null) {
        if (level < bestLevel) {
          bestLevel = level;
          bestNum = foundAtLevel;
        }
        break;
      }
      current = parent;
    }
    if (bestLevel === 0) break;
  }

  return bestNum;
}
"""


_DUMP_NEAR_JS = r"""
([labelText, exactMatch]) => {
  const all = Array.from(document.querySelectorAll('body *'));
  const labelEls = all.filter(el => {
    if (!el.textContent) return false;
    if (exactMatch) return el.textContent.trim() === labelText;
    return el.textContent.includes(labelText);
  });
  if (!labelEls.length) return JSON.stringify({error: 'no candidates'});
  const depth = (n) => { let d = 0; while (n) { d++; n = n.parentElement; } return d; };
  const out = [];
  for (let i = 0; i < Math.min(labelEls.length, 6); i++) {
    const el = labelEls[i];
    // Get the parent's outerHTML (limited) and direct sibling text content
    const parent = el.parentElement;
    const siblingTexts = parent ? Array.from(parent.children).map(c => ({
      tag: c.tagName, isLabel: c === el, text: (c.textContent || '').trim().slice(0, 100)
    })) : [];
    out.push({
      idx: i,
      depth: depth(el),
      tag: el.tagName,
      classes: el.className,
      ownText: Array.from(el.childNodes).filter(n => n.nodeType === 3).map(n => n.textContent).join('').trim().slice(0, 120),
      parentTag: parent ? parent.tagName : null,
      parentClass: parent ? parent.className : null,
      siblings: siblingTexts,
    });
  }
  return JSON.stringify({count: labelEls.length, candidates: out}, null, 2);
}
"""

async def _find_number_near_label(page, label_text: str, timeout_ms: int = LABEL_WAIT_MS, debug_dump_path=None, exact: bool = False) -> float:
    """
    Wait for `label_text` to appear, then return the nearest numeric value
    as a float. Returns 0.0 on any failure.

    exact=True: only match elements whose text equals the label EXACTLY
                (prevents 'ค่าโฆษณา' from matching 'ค่าโฆษณาวันนี้').
    """
    try:
        loc = page.get_by_text(label_text, exact=exact).first
        await loc.wait_for(state="visible", timeout=timeout_ms)
    except PWTimeout:
        print(f"  [warn] label '{label_text}' did not appear within {timeout_ms}ms")
        return 0.0
    except Exception as e:
        print(f"  [warn] waiting for '{label_text}' failed: {type(e).__name__}: {e}")
        return 0.0

    try:
        raw = await page.evaluate(_FIND_NUMBER_NEAR_LABEL_JS, [label_text, exact])
    except Exception as e:
        print(f"  [warn] DOM walk failed for '{label_text}': {e}")
        return 0.0

    # Always dump candidate info when debug path is set — helps tune the algorithm
    if debug_dump_path is not None:
        try:
            html = await page.evaluate(_DUMP_NEAR_JS, [label_text, exact])
            if html:
                Path(debug_dump_path).write_text(html, encoding="utf-8")
                print(f"  [debug] candidate dump saved → {debug_dump_path}")
        except Exception:
            pass

    if raw is None:
        print(f"  [warn] '{label_text}' found but no numeric value nearby")
        return 0.0

    value = parse_thai_number(raw)
    print(f"  [ok]   '{label_text}' → raw='{raw}' → {value}")
    return value


async def _debug_dump(page, tag: str, profile_name: str) -> None:
    """Save page title + visible text + viewport screenshot to help diagnose label misses."""
    debug_dir = Path(__file__).parent / "debug"
    debug_dir.mkdir(exist_ok=True)
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", profile_name)
    png  = debug_dir / f"{safe}_{tag}.png"
    txt  = debug_dir / f"{safe}_{tag}.txt"

    # Always-on: dump URL + title + visible text (cheap, ~no timeout risk)
    try:
        url   = page.url
        title = await page.title()
        body  = (await page.locator("body").inner_text())[:5000]
        txt.write_text(
            f"URL:   {url}\nTITLE: {title}\n\n--- BODY (first 5000 chars) ---\n{body}",
            encoding="utf-8",
        )
        print(f"  [debug] saved {txt.name}")
    except Exception as e:
        print(f"  [debug] text dump failed: {e}")

    # Best-effort: viewport screenshot with short timeout
    try:
        await page.screenshot(path=str(png), full_page=False, timeout=8000)
        print(f"  [debug] saved {png.name}")
    except Exception as e:
        print(f"  [debug] screenshot skipped: {type(e).__name__}")


async def extract_shopee_data(page, profile_name: str = "x", debug: bool = False) -> dict:
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

    If debug=True, saves a full-page screenshot + page text snapshot to ./debug/
    whenever a label is not found — so you can see what the page actually showed.
    """
    result = {"commission": 0.0, "ads_cost": 0.0}

    # ───── 1. Commission (Affiliate Dashboard) ────────────────────────
    try:
        print(f"  → goto {SHOPEE_AFFILIATE_URL}")
        await page.goto(SHOPEE_AFFILIATE_URL, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except PWTimeout:
            pass  # some SPA pages never go idle — that's fine, we'll still try
        print(f"  [info] landed on: {page.url}  · title: {(await page.title())[:80]}")
    except PWTimeout:
        print(f"  [warn] affiliate page nav timeout")
    except Exception as e:
        print(f"  [warn] affiliate nav: {type(e).__name__}: {e}")
    else:
        dbg_path = Path(__file__).parent / "debug" / f"{re.sub(r'[^A-Za-z0-9_-]','_',profile_name)}_affiliate_html.html" if debug else None
        if dbg_path:
            dbg_path.parent.mkdir(exist_ok=True)
        # Note: KPI card uses "ค่าคอมมิชชั่นโดยประมาณ(฿)" with NO space before "(".
        # The table header on the same page uses "ค่าคอมมิชชั่นโดยประมาณ (฿)" WITH a space.
        # Including "(฿)" (no space) targets ONLY the KPI card.
        result["commission"] = await _find_number_near_label(page, "ค่าคอมมิชชั่นโดยประมาณ(฿)", debug_dump_path=dbg_path)
        if debug and result["commission"] == 0.0:
            await _debug_dump(page, "affiliate", profile_name)

    # ───── 2. Ads Cost (Live Ads page) ────────────────────────────────
    try:
        print(f"  → goto {SHOPEE_LIVE_ADS_URL}")
        await page.goto(SHOPEE_LIVE_ADS_URL, timeout=NAV_TIMEOUT_MS, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except PWTimeout:
            pass
        print(f"  [info] landed on: {page.url}  · title: {(await page.title())[:80]}")
    except PWTimeout:
        print(f"  [warn] live-ads page nav timeout")
    except Exception as e:
        print(f"  [warn] live-ads nav: {type(e).__name__}: {e}")
    else:
        # 'ค่าโฆษณา' appears multiple times (Today's ad cost, tab, column header, KPI).
        # exact=True ensures we only match the KPI label, not 'ค่าโฆษณาวันนี้'.
        dbg_path2 = Path(__file__).parent / "debug" / f"{re.sub(r'[^A-Za-z0-9_-]','_',profile_name)}_liveads_html.html" if debug else None
        if dbg_path2:
            dbg_path2.parent.mkdir(exist_ok=True)
        result["ads_cost"] = await _find_number_near_label(page, "ค่าโฆษณา", exact=True, debug_dump_path=dbg_path2)
        if debug and result["ads_cost"] == 0.0:
            await _debug_dump(page, "liveads", profile_name)

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


async def scrape_one(playwright, profile, target_date, debug: bool = False) -> Result:
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
        data = await extract_shopee_data(page, profile_name=name, debug=debug)
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

async def orchestrate(profiles, target_date, concurrency, debug: bool = False):
    sem = asyncio.Semaphore(concurrency)

    async with async_playwright() as pw:
        async def run(profile):
            async with sem:
                return await scrape_one(pw, profile, target_date, debug=debug)

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
    parser.add_argument("--debug", action="store_true", help="Save screenshots + page text when labels not found")
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
    results = asyncio.run(orchestrate(profiles, args.target_date, args.concurrency, debug=args.debug))
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
