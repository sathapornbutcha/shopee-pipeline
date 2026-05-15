/* ──────────────────────────────────────────────────────────────────────────
   Shopee Pipeline Dashboard — vanilla JS controller.
   All aggregations (totals, ROAS, net profit) are computed CLIENT-SIDE from
   raw rows returned by /api/data — the DB never stores totals.
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  // ─── Mock fallback (so the page is inspectable even without an API) ────
  const MOCK = (() => {
    const today = new Date().toISOString().slice(0, 10);
    const y     = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
    const tpl   = [
      ['lonaharper',  'kshomeaxie29',  320, 1250, 2840],
      ['kingmolly',   'kshomeaxie29',  180,  890, 1620],
      ['amelia_shop', 'kshomeaxie29',  250, 1480, 3210],
      ['jayden_th',   'vinhomeaxie44', 120,  780, 1140],
      ['noah_market', 'vinhomeaxie44', 300, 1340, 2890],
      ['emma_th',     'vinhomeaxie44', 200, 1020, 1980],
      ['oliver_pro',  'homestore_a1',  400, 1820, 4120],
      ['liam_shop',   'homestore_a1',  220,  990, 2230],
    ];
    const rows = [];
    let id = 1;
    for (const d of [today, y]) {
      for (const [name, grp, openc, ads, comm] of tpl) {
        rows.push({
          id: id++,
          date: d,
          profile_name: name,
          account_group: grp,
          open_channel_cost: openc,
          ads_cost: ads,
          commission: comm,
          scraped_at: new Date().toISOString(),
        });
      }
    }
    return rows;
  })();

  // ─── State ─────────────────────────────────────────────────────────────
  const state = {
    rows: [],
    filterDate:  'last3',     // default: most recent 3 dates with data
    filterGroup: 'all',
    filterQuery: '',
    apiUp: null,
    loading: false,
    knownDates: [],           // sorted desc, set by populateDates()
  };

  // ─── DOM ───────────────────────────────────────────────────────────────
  const $ = (s) => document.querySelector(s);
  const el = {
    statusPill: $('#statusPill'),
    statusText: $('#statusText'),
    dateSel:    $('#filterDate'),
    groupSel:   $('#filterGroup'),
    search:     $('#searchInput'),
    refresh:    $('#refreshBtn'),
    tbody:      $('#tbody'),
    tfootSum:   $('#tfootSum'),
    rowCount:   $('#rowCount'),
    lastSync:   $('#lastSync'),
    kpiProfiles:    $('#kpiProfiles'),
    kpiProfilesSub: $('#kpiProfilesSub'),
    kpiOpen:        $('#kpiOpen'),
    kpiOpenSub:     $('#kpiOpenSub'),
    kpiAds:         $('#kpiAds'),
    kpiAdsSub:      $('#kpiAdsSub'),
    kpiComm:        $('#kpiComm'),
    kpiCommSub:     $('#kpiCommSub'),
    kpiNet:         $('#kpiNet'),
    kpiNetSub:      $('#kpiNetSub'),
    kpiRoas:        $('#kpiRoas'),
    kpiRoasSub:     $('#kpiRoasSub'),
    kpiGroups:      $('#kpiGroups'),
  };

  // ─── Fetch helpers ─────────────────────────────────────────────────────
  async function fetchJSON(url, ms = 4000) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), ms);
    try {
      const r = await fetch(url, { signal: ctrl.signal });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return await r.json();
    } finally { clearTimeout(t); }
  }

  async function probe() {
    try { await fetchJSON('/api/health'); state.apiUp = true; }
    catch { state.apiUp = false; }
    setApiStatus();
  }

  function setApiStatus() {
    if (state.apiUp === null) {
      el.statusPill.className = 'status-pill warn';
      el.statusText.textContent = 'Connecting…';
    } else if (state.apiUp) {
      el.statusPill.className = 'status-pill';
      el.statusText.textContent = 'API · Live';
    } else {
      el.statusPill.className = 'status-pill offline';
      el.statusText.textContent = 'Mock data (API offline)';
    }
  }

  // ─── Load ──────────────────────────────────────────────────────────────
  async function load() {
    state.loading = true;
    render();

    if (state.apiUp) {
      try {
        const [rows, dates, groups] = await Promise.all([
          fetchJSON('/api/data', 8000),
          fetchJSON('/api/dates', 4000),
          fetchJSON('/api/groups', 4000),
        ]);
        state.rows = rows;
        populateDates(dates);
        populateGroups(groups);
      } catch (e) {
        console.warn('API error, falling back to mock:', e);
        state.apiUp = false;
        setApiStatus();
        useMock();
      }
    } else {
      useMock();
    }

    state.loading = false;
    render();
  }

  function useMock() {
    state.rows = MOCK.slice();
    populateDates([...new Set(MOCK.map(r => r.date))].sort().reverse());
    populateGroups([...new Set(MOCK.map(r => r.account_group))].sort());
  }

  function populateDates(dates) {
    state.knownDates = (dates || []).slice();   // sorted desc by API
    const cur = el.dateSel.value || state.filterDate;
    el.dateSel.innerHTML =
      '<option value="last3">3 วันล่าสุด</option>' +
      '<option value="all">ทุกวัน</option>' +
      dates.map(d => `<option value="${d}"${d === cur ? ' selected' : ''}>${d}</option>`).join('');
    // Restore the user's selection (or default to last3)
    el.dateSel.value = cur;
    if (el.dateSel.value === '') el.dateSel.value = 'last3';
  }

  function populateGroups(groups) {
    const cur = el.groupSel.value || 'all';
    el.groupSel.innerHTML = '<option value="all">ทุกกลุ่ม</option>' +
      groups.map(g => `<option value="${escapeHTML(g)}"${g === cur ? ' selected' : ''}>${escapeHTML(g)}</option>`).join('');
  }

  // ─── Filtering (pure function, used by both render + totals) ───────────
  function filteredRows() {
    const q = state.filterQuery.trim().toLowerCase();
    // 'last3' = the 3 most recent dates that have data
    const last3 = state.knownDates.slice(0, 3);
    return state.rows
      .filter(r => {
        if (state.filterDate === 'all')   return true;
        if (state.filterDate === 'last3') return last3.includes(r.date);
        return r.date === state.filterDate;
      })
      .filter(r => state.filterGroup === 'all' || r.account_group === state.filterGroup)
      .filter(r => !q
        || (r.profile_name  || '').toLowerCase().includes(q)
        || (r.account_group || '').toLowerCase().includes(q));
  }

  // ─── Real-time aggregations (the rule: totals NEVER come from DB) ─────
  function computeTotals(rows) {
    let openSum = 0, adsSum = 0, commSum = 0;
    const groups = new Set();
    for (const r of rows) {
      openSum += (+r.open_channel_cost) || 0;
      adsSum  += (+r.ads_cost) || 0;
      commSum += (+r.commission) || 0;
      if (r.account_group) groups.add(r.account_group);
    }
    const costSum = openSum + adsSum;
    const net     = commSum - costSum;
    const roas    = costSum > 0 ? commSum / costSum : 0;
    return {
      profiles: rows.length,
      groups: groups.size,
      open: openSum,
      ads: adsSum,
      cost: costSum,
      commission: commSum,
      net,
      roas,
    };
  }

  // ─── Render ────────────────────────────────────────────────────────────
  function render() {
    const rows = filteredRows();
    const t = computeTotals(rows);

    el.kpiProfiles.textContent    = t.profiles.toLocaleString();
    el.kpiProfilesSub.innerHTML   = `<span>${t.groups} กลุ่ม</span>`;
    el.kpiOpen.textContent        = fmtMoney(t.open);
    el.kpiOpenSub.innerHTML       = `<span>เปิดช่อง รวมทุกแถว</span>`;
    el.kpiAds.textContent         = fmtMoney(t.ads);
    el.kpiAdsSub.innerHTML        = `<span>คอยน์ + ค่าโฆษณา</span>`;
    el.kpiComm.textContent        = fmtMoney(t.commission);
    el.kpiCommSub.innerHTML       = t.cost > 0
      ? `<span class="delta ${t.commission >= t.cost ? 'up' : 'down'}">${((t.commission / t.cost - 1) * 100).toFixed(1)}%</span> เทียบต้นทุน`
      : `<span>—</span>`;
    el.kpiNet.textContent         = fmtMoney(t.net);
    el.kpiNetSub.innerHTML        = t.net >= 0
      ? `<span class="delta up">กำไร</span>`
      : `<span class="delta down">ขาดทุน</span>`;
    el.kpiRoas.textContent        = t.roas.toFixed(2) + '×';
    el.kpiRoasSub.innerHTML       = t.roas >= 1
      ? `<span class="delta up">คุ้มทุน</span>`
      : `<span class="delta down">ต่ำกว่าเป้า</span>`;
    el.kpiGroups.textContent      = t.groups.toLocaleString();

    el.rowCount.textContent       = `${rows.length} row${rows.length === 1 ? '' : 's'}`;
    el.lastSync.textContent       = 'Last sync ' + new Date().toLocaleTimeString('th-TH');

    renderTable(rows, t);
  }

  function renderTable(rows, totals) {
    if (state.loading) {
      el.tbody.innerHTML = `<tr><td colspan="7" class="empty"><span class="spinner"></span> Loading…</td></tr>`;
      el.tfootSum.innerHTML = '';
      return;
    }
    if (!rows.length) {
      el.tbody.innerHTML = `
        <tr><td colspan="7" class="empty">
          <div class="ico">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <ellipse cx="12" cy="5" rx="9" ry="3"/>
              <path d="M3 5v14c0 1.7 4 3 9 3s9-1.3 9-3V5"/>
              <path d="M3 12c0 1.7 4 3 9 3s9-1.3 9-3"/>
            </svg>
          </div>
          <div>ไม่พบข้อมูลตามตัวกรอง</div>
        </td></tr>`;
      el.tfootSum.innerHTML = '';
      return;
    }

    el.tbody.innerHTML = rows.map(rowHTML).join('');

    // Real-time sum row in <tfoot> — exact precision so totals match the table
    el.tfootSum.innerHTML = `
      <tr class="sum-row">
        <td colspan="3"><strong>รวม (${rows.length} แถว)</strong></td>
        <td class="num"><strong>${fmtMoneyExact(totals.open)}</strong></td>
        <td class="num"><strong>${fmtMoneyExact(totals.ads)}</strong></td>
        <td class="num"><strong>${fmtMoneyExact(totals.commission)}</strong></td>
        <td class="num"><strong class="${totals.net >= 0 ? 'pos' : 'neg'}">${fmtMoneyExact(totals.net)}</strong></td>
      </tr>`;
  }

  function rowHTML(r) {
    const initials = (r.profile_name || '?').slice(0, 2).toUpperCase();
    const grad = pickGradient(r.profile_name);
    const cost = (+r.open_channel_cost || 0) + (+r.ads_cost || 0);
    const net  = (+r.commission || 0) - cost;
    return `
      <tr>
        <td>
          <div class="profile">
            <div class="avatar" style="background:${grad}">${escapeHTML(initials)}</div>
            <div>
              <div>${escapeHTML(r.profile_name || '—')}</div>
            </div>
          </div>
        </td>
        <td><span class="badge muted">${r.date}</span></td>
        <td>${r.account_group ? `<span class="badge muted">${escapeHTML(r.account_group)}</span>` : '<span class="dim">—</span>'}</td>
        <td class="num">${fmtMoneyExact(+r.open_channel_cost)}</td>
        <td class="num">${fmtMoneyExact(+r.ads_cost)}</td>
        <td class="num">${fmtMoneyExact(+r.commission)}</td>
        <td class="num ${net >= 0 ? 'pos' : 'neg'}">${fmtMoneyExact(net)}</td>
      </tr>`;
  }

  // ─── Utils ─────────────────────────────────────────────────────────────
  // Abbreviated format for KPI summary cards (e.g. ฿1.2M, ฿8.4k)
  function fmtMoney(v) {
    if (v == null || isNaN(v)) return '—';
    const sign = v < 0 ? '-' : '';
    const abs = Math.abs(v);
    if (abs >= 1_000_000) return sign + '฿' + (abs / 1_000_000).toFixed(2) + 'M';
    if (abs >= 1_000)     return sign + '฿' + (abs / 1_000).toFixed(1) + 'k';
    return sign + '฿' + Math.round(abs).toLocaleString();
  }
  // Exact format for table cells — full precision with comma separators.
  // Below 1000: '฿800'.  Above: '฿1,234' or '฿2,200,000'. Never abbreviated.
  function fmtMoneyExact(v) {
    if (v == null || isNaN(v)) return '—';
    if (v === 0) return '฿0';
    const sign = v < 0 ? '-' : '';
    const abs = Math.abs(v);
    // Show decimals only if there are any (e.g. ฿1,234.56)
    const hasDecimals = Math.round(abs) !== abs;
    const formatted = hasDecimals
      ? abs.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
      : Math.round(abs).toLocaleString('en-US');
    return sign + '฿' + formatted;
  }
  function escapeHTML(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }
  const GRADIENTS = [
    'linear-gradient(135deg,#a78bfa,#60a5fa)',
    'linear-gradient(135deg,#f472b6,#fb923c)',
    'linear-gradient(135deg,#34d399,#60a5fa)',
    'linear-gradient(135deg,#fbbf24,#fb7185)',
    'linear-gradient(135deg,#22d3ee,#a78bfa)',
    'linear-gradient(135deg,#fb7185,#a78bfa)',
  ];
  function pickGradient(seed) {
    let h = 0;
    for (let i = 0; i < (seed || '').length; i++) h = (h * 31 + seed.charCodeAt(i)) >>> 0;
    return GRADIENTS[h % GRADIENTS.length];
  }

  // ─── Events ────────────────────────────────────────────────────────────
  el.dateSel.addEventListener('change', () => { state.filterDate  = el.dateSel.value;  render(); });
  el.groupSel.addEventListener('change',() => { state.filterGroup = el.groupSel.value; render(); });
  el.search.addEventListener('input',   () => { state.filterQuery = el.search.value;   render(); });
  el.refresh.addEventListener('click',  () => load());

  // ─── Boot ──────────────────────────────────────────────────────────────
  (async function boot() {
    setApiStatus();
    await probe();
    await load();
  })();
})();
