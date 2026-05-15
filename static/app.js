/* ──────────────────────────────────────────────────────────────────────────
   Shopee Pipeline Dashboard — vanilla JS controller.
   Fetches /api/data + /api/summary; falls back to mock data when the API is
   unreachable (e.g. opening this file directly with file://) so the UI is
   always inspectable.
   ────────────────────────────────────────────────────────────────────────── */
(function () {
  'use strict';

  // ─── Mock fallback ───────────────────────────────────────────────────
  const MOCK = (() => {
    const today = new Date().toISOString().slice(0, 10);
    const y = new Date(Date.now() - 86400000).toISOString().slice(0, 10);
    const names = [
      'Shop-Bangkok-01','Shop-Bangkok-02','Shop-Bangkok-03','Shop-CNX-01',
      'Shop-CNX-02','Shop-HCMC-01','Shop-HCMC-02','Shop-Jakarta-01',
      'Shop-KL-01','Shop-KL-02','Shop-Manila-01','Shop-Manila-02',
      'Shop-Singapore-01','Shop-HoChiMinh-03','Shop-Bandung-01','Shop-Surabaya-01',
    ];
    return names.map((n, i) => {
      const failed = i % 5 === 1 || i % 7 === 0;
      const reasons = ['captcha','nav_timeout','otp','hard_timeout','no_metrics'];
      const reason = failed ? reasons[i % reasons.length] : null;
      const cost = 1200 + Math.random() * 9200;
      const comm = cost * (0.6 + Math.random() * 0.6);
      const roas = comm / cost;
      return {
        id: 1000 - i,
        profile_id: 'gl_' + (1000 + i),
        profile_name: n,
        target_date: i < 3 ? y : today,
        scraped_at: new Date(Date.now() - i * 60000).toISOString(),
        ads_cost:       failed ? null : +cost.toFixed(2),
        est_commission: failed ? null : +comm.toFixed(2),
        roas:           failed ? null : +roas.toFixed(2),
        status:       failed ? 'Failed' : 'Success',
        error_reason: reason,
        error_detail: reason ? `${reason} detected on dashboard` : null,
        duration_ms:  failed ? 15000 + Math.floor(Math.random() * 3000) : 2000 + Math.floor(Math.random() * 4000),
      };
    });
  })();

  // ─── State ───────────────────────────────────────────────────────────
  const state = {
    rows: [],
    summary: null,
    filterDate: 'all',
    filterStatus: 'all',
    filterQuery: '',
    apiUp: null, // null=unknown, true/false
    loading: false,
  };

  // ─── DOM ─────────────────────────────────────────────────────────────
  const $ = (sel) => document.querySelector(sel);
  const el = {
    statusPill: $('#statusPill'),
    statusText: $('#statusText'),
    dateSel:    $('#filterDate'),
    seg:        $('#segStatus'),
    search:     $('#searchInput'),
    refresh:    $('#refreshBtn'),
    tbody:      $('#tbody'),
    rowCount:   $('#rowCount'),
    lastSync:   $('#lastSync'),
    // KPIs
    kpiTotal:    $('#kpiTotal'),
    kpiTotalSub: $('#kpiTotalSub'),
    kpiCost:     $('#kpiCost'),
    kpiComm:     $('#kpiComm'),
    kpiCommSub:  $('#kpiCommSub'),
    kpiRoas:     $('#kpiRoas'),
    kpiRoasSub:  $('#kpiRoasSub'),
  };

  // ─── Fetch helpers ───────────────────────────────────────────────────
  const API_BASE = ''; // same origin

  async function fetchJSON(url, ms = 2500) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), ms);
    try {
      const r = await fetch(url, { signal: ctrl.signal });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      return await r.json();
    } finally { clearTimeout(t); }
  }

  async function probe() {
    try {
      await fetchJSON(API_BASE + '/api/health');
      state.apiUp = true;
    } catch { state.apiUp = false; }
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

  // ─── Load ────────────────────────────────────────────────────────────
  async function load() {
    state.loading = true;
    render(); // shows the spinner

    if (state.apiUp) {
      try {
        const params = new URLSearchParams();
        if (state.filterDate !== 'all')   params.set('date',   state.filterDate);
        if (state.filterStatus !== 'all') params.set('status', state.filterStatus);
        const [rows, summary, dates] = await Promise.all([
          fetchJSON(API_BASE + '/api/data?' + params.toString(), 6000),
          fetchJSON(API_BASE + '/api/summary' + (state.filterDate !== 'all' ? `?date=${state.filterDate}` : ''), 4000),
          fetchJSON(API_BASE + '/api/dates', 4000),
        ]);
        state.rows = rows;
        state.summary = summary;
        populateDates(dates);
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
    // derive summary from MOCK + filters
    const today = new Date().toISOString().slice(0, 10);
    const date = state.filterDate === 'all' ? today : state.filterDate;
    const onDate = MOCK.filter(r => r.target_date === date);
    const success = onDate.filter(r => r.status === 'Success');
    const failed  = onDate.filter(r => r.status === 'Failed');
    const total_cost       = success.reduce((s, r) => s + (r.ads_cost || 0), 0);
    const total_commission = success.reduce((s, r) => s + (r.est_commission || 0), 0);
    const avg_roas         = success.length
      ? success.reduce((s, r) => s + (r.roas || 0), 0) / success.length : 0;
    state.summary = {
      target_date: date,
      total: onDate.length,
      success: success.length,
      failed: failed.length,
      success_rate: onDate.length ? +(success.length / onDate.length * 100).toFixed(1) : 0,
      total_cost,
      total_commission,
      avg_roas,
    };
    const allDates = [...new Set(MOCK.map(r => r.target_date))].sort().reverse();
    populateDates(allDates);
  }

  function populateDates(dates) {
    const cur = el.dateSel.value || 'all';
    el.dateSel.innerHTML = '<option value="all">All dates</option>' +
      dates.map(d => `<option value="${d}"${d === cur ? ' selected' : ''}>${d}</option>`).join('');
    if ([...el.dateSel.options].some(o => o.value === cur)) el.dateSel.value = cur;
  }

  // ─── Render ──────────────────────────────────────────────────────────
  function render() {
    renderKPIs();
    renderTable();
    el.lastSync.textContent = 'Last sync ' + new Date().toLocaleTimeString();
  }

  function renderKPIs() {
    const s = state.summary || {};
    el.kpiTotal.textContent    = (s.total ?? 0).toLocaleString();
    el.kpiTotalSub.innerHTML   = `
      <span class="delta up">${s.success ?? 0} ok</span>
      <span class="delta down">${s.failed ?? 0} fail</span>
      <span>· ${s.success_rate ?? 0}% success</span>`;
    el.kpiCost.textContent     = fmtMoney(s.total_cost);
    el.kpiComm.textContent     = fmtMoney(s.total_commission);
    el.kpiCommSub.innerHTML    = s.total_cost
      ? `<span class="delta up">${(((s.total_commission / s.total_cost) - 1) * 100).toFixed(1)}%</span> vs cost`
      : '<span>—</span>';
    el.kpiRoas.textContent     = (s.avg_roas ?? 0).toFixed(2) + '×';
    el.kpiRoasSub.innerHTML    = (s.avg_roas ?? 0) >= 1
      ? `<span class="delta up">profitable</span>`
      : `<span class="delta down">under target</span>`;
  }

  function renderTable() {
    const q = state.filterQuery.trim().toLowerCase();
    const rows = state.rows
      .filter(r => state.filterDate === 'all' || r.target_date === state.filterDate)
      .filter(r => state.filterStatus === 'all' || r.status === state.filterStatus)
      .filter(r => !q
        || (r.profile_name || '').toLowerCase().includes(q)
        || (r.profile_id   || '').toLowerCase().includes(q));

    el.rowCount.textContent = `${rows.length} row${rows.length === 1 ? '' : 's'}`;

    if (state.loading) {
      el.tbody.innerHTML = `
        <tr><td colspan="7" class="empty">
          <div><span class="spinner"></span> Loading…</div>
        </td></tr>`;
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
          <div>No results match your filters.</div>
        </td></tr>`;
      return;
    }

    el.tbody.innerHTML = rows.map(rowHTML).join('');
  }

  function rowHTML(r) {
    const initials = (r.profile_name || r.profile_id).split(/[-_ ]/).slice(0, 2)
      .map(s => s.charAt(0)).join('').toUpperCase().slice(0, 2) || '?';
    const grad = pickGradient(r.profile_id);
    const status = r.status === 'Success'
      ? `<span class="badge success"><span class="dot"></span>Success</span>`
      : `<span class="badge failed"><span class="dot"></span>Failed</span>
         ${r.error_reason ? `<div class="reason">${escapeHTML(r.error_reason)}</div>` : ''}`;
    return `
      <tr>
        <td>
          <div class="profile">
            <div class="avatar" style="background:${grad}">${initials}</div>
            <div>
              <div>${escapeHTML(r.profile_name || '—')}</div>
              <div class="profile-id">${escapeHTML(r.profile_id)}</div>
            </div>
          </div>
        </td>
        <td><span class="badge muted">${r.target_date}</span></td>
        <td class="num">${fmtMoney(r.ads_cost)}</td>
        <td class="num">${fmtMoney(r.est_commission)}</td>
        <td class="num">${r.roas != null ? r.roas.toFixed(2) + '×' : '—'}</td>
        <td>${status}</td>
        <td class="num">${(r.duration_ms / 1000).toFixed(1)}s</td>
      </tr>`;
  }

  // ─── Utils ───────────────────────────────────────────────────────────
  function fmtMoney(v) {
    if (v == null) return '—';
    if (Math.abs(v) >= 1_000_000) return '฿' + (v / 1_000_000).toFixed(2) + 'M';
    if (Math.abs(v) >= 1_000)     return '฿' + (v / 1_000).toFixed(1) + 'k';
    return '฿' + Math.round(v).toLocaleString();
  }

  function escapeHTML(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
  }

  // deterministic gradient per profile id
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

  // ─── Events ──────────────────────────────────────────────────────────
  el.dateSel.addEventListener('change', () => {
    state.filterDate = el.dateSel.value;
    load();
  });
  el.seg.querySelectorAll('button').forEach(b => {
    b.addEventListener('click', () => {
      el.seg.querySelectorAll('button').forEach(x => x.setAttribute('aria-pressed', 'false'));
      b.setAttribute('aria-pressed', 'true');
      state.filterStatus = b.dataset.value;
      load();
    });
  });
  el.search.addEventListener('input', () => {
    state.filterQuery = el.search.value;
    renderTable();
  });
  el.refresh.addEventListener('click', () => load());

  // ─── Boot ────────────────────────────────────────────────────────────
  (async function boot() {
    setApiStatus();
    await probe();
    await load();
  })();
})();
