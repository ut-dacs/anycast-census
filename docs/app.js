// ── DuckDB-WASM bootstrap ──────────────────────────────────────────────────
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

const statusEl   = document.getElementById('status');
const resultsEl  = document.getElementById('results');
const searchBtn  = document.getElementById('search-btn');
const inputEl    = document.getElementById('prefix-input');
const examplesEl       = document.getElementById('examples');
const chartSection     = document.getElementById('chart-section');
const asStatsSectionEl = document.getElementById('as-stats-section');
const asTableSectionEl = document.getElementById('as-table-section');
const dateInput  = document.getElementById('date-input');
const whenLatest  = document.getElementById('when-latest');
const whenDate    = document.getElementById('when-date');
const modeLookup  = document.getElementById('mode-lookup');
const modeCompare = document.getElementById('mode-compare');
const lookupRow   = document.getElementById('lookup-date-row');
const compareRow  = document.getElementById('compare-row');
const cmpDateA    = document.getElementById('cmp-date-a');
const cmpDateB    = document.getElementById('cmp-date-b');
const cmpBtn      = document.getElementById('cmp-btn');
let isCompareMode = false;

// ── Pagination state ────────────────────────────────────────────────────────
const pgStore = {};
let pgCounter = 0;
let currentLookupCtx  = null;  // { viewKey, dateLabel }
let currentCompareCtx = null;  // { dateA, dateB }

function setStatus(msg, isError = false) {
  statusEl.textContent = msg;
  statusEl.className = 'status' + (isError ? ' error' : '');
}

// Base URL of the data files (same directory as this page)
const baseUrl = (() => {
  const u = new URL(window.location.href);
  // Strip any filename, keep trailing slash
  return u.origin + u.pathname.replace(/[^/]*$/, '');
})();

// Set max date to today
dateInput.max = cmpDateA.max = cmpDateB.max = new Date().toISOString().slice(0, 10);

// Expand a bare IPv6 address to its enclosing /48 prefix string
function ipv6ToSlash48(addr) {
  const groups = expandIPv6(addr);
  // Zero out groups beyond the first 3 (48 bits = 3 x 16-bit groups)
  return groups.slice(0, 3).join(':') + '::/48';
}

function expandIPv6(addr) {
  // Handle '::' expansion
  const halves = addr.split('::');
  if (halves.length > 2) throw new Error('invalid');
  const left  = halves[0] ? halves[0].split(':') : [];
  const right = halves[1] ? halves[1].split(':') : [];
  const missing = 8 - left.length - right.length;
  if (missing < 0) throw new Error('invalid');
  const full = [...left, ...Array(missing).fill('0'), ...right];
  if (full.length !== 8) throw new Error('invalid');
  return full.map(g => g.padStart(1, '0'));
}

// Initialise DuckDB
let db, conn;
let currentViewKey = null; // tracks which parquet files are registered as views
let leafletMap = null;

try {
  const BUNDLES = duckdb.getJsDelivrBundles();
  const bundle  = await duckdb.selectBundle(BUNDLES);

  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );
  const worker = new Worker(workerUrl);
  const logger = new duckdb.VoidLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);

  conn = await db.connect();
  await conn.query("LOAD inet");
  await registerViews('latest');

  setStatus('Ready.');
  searchBtn.disabled = false;
  examplesEl.style.display = '';
  initASStats();
  initASTable();

  // Auto-run lookup / compare if URL has query params
  const initParams = new URLSearchParams(window.location.search);
  const initMode = initParams.get('mode');
  const initQ    = initParams.get('q');
  if (initMode === 'compare') {
    const dA = initParams.get('dateA');
    const dB = initParams.get('dateB');
    if (dA && dB) {
      isCompareMode = true;
      modeCompare.classList.add('active');
      modeLookup.classList.remove('active');
      lookupRow.style.display = 'none';
      compareRow.style.display = '';
      cmpDateA.value = dA;
      cmpDateB.value = dB;
      if (initQ) inputEl.value = initQ;
      await runCompare({ updateUrl: false });
    }
  } else if (initQ) {
    inputEl.value = initQ;
    const initDate = initParams.get('date');
    if (initDate) {
      whenDate.checked   = true;
      whenLatest.checked = false;
      dateInput.value    = initDate;
      dateInput.disabled = false;
    }
    await lookup(initQ, { updateUrl: false });
  }
} catch (err) {
  setStatus('Failed to load query engine: ' + err.message, true);
}

window.addEventListener('popstate', async e => {
  if (!conn) return;
  const params = new URLSearchParams(window.location.search);
  const mode = params.get('mode');
  const q    = params.get('q');

  function ensureLookupMode() {
    if (!isCompareMode) return;
    isCompareMode = false;
    modeLookup.classList.add('active');
    modeCompare.classList.remove('active');
    lookupRow.style.display = '';
    compareRow.style.display = 'none';
  }

  if (mode === 'compare') {
    const dateA = params.get('dateA');
    const dateB = params.get('dateB');
    if (dateA && dateB) {
      isCompareMode = true;
      modeCompare.classList.add('active');
      modeLookup.classList.remove('active');
      lookupRow.style.display = 'none';
      compareRow.style.display = '';
      cmpDateA.value = dateA;
      cmpDateB.value = dateB;
      inputEl.value = q || '';
      await runCompare({ updateUrl: false });
    }
  } else if (q) {
    ensureLookupMode();
    const date = params.get('date');
    inputEl.value = q;
    if (date) {
      whenDate.checked   = true;
      whenLatest.checked = false;
      dateInput.value    = date;
      dateInput.disabled = false;
    } else {
      whenLatest.checked = true;
      whenDate.checked   = false;
      dateInput.disabled = true;
    }
    await lookup(q, { updateUrl: false });
  } else {
    ensureLookupMode();
    inputEl.value = '';
    resultsEl.innerHTML = '';
    resetPg();
    setStatus('Ready.');
  }
});

// ── View management ────────────────────────────────────────────────────────
// viewKey is 'latest' or 'YYYY/MM/DD'
async function registerViews(viewKey) {
  if (viewKey === currentViewKey) return;
  const today = new Date().toISOString().slice(0, 10); // 'YYYY-MM-DD'
  const [ipv4Url, ipv6Url] = viewKey === 'latest'
    ? [baseUrl + `IPv4-latest.parquet?v=${today}`, baseUrl + `IPv6-latest.parquet?v=${today}`]
    : [baseUrl + `${viewKey}/IPv4.parquet`, baseUrl + `${viewKey}/IPv6.parquet`];
  await conn.query(`CREATE OR REPLACE VIEW ipv4 AS SELECT * FROM read_parquet('${ipv4Url}')`);
  await conn.query(`CREATE OR REPLACE VIEW ipv6 AS SELECT * FROM read_parquet('${ipv6Url}')`);
  currentViewKey = viewKey;
}

function selectedViewKey() {
  if (whenLatest.checked) return 'latest';
  const v = dateInput.value; // 'YYYY-MM-DD'
  return v ? v.replace(/-/g, '/') : 'latest';
}

// ── Lookup dispatcher ──────────────────────────────────────────────────────
async function lookup(raw, { updateUrl = true } = {}) {
  const input = raw.trim();
  if (!input) return;

  const viewKey   = selectedViewKey();
  const dateLabel = viewKey === 'latest' ? 'latest' : viewKey.replace(/\//g, '-');
  resultsEl.innerHTML = '';
  resetPg(); currentCompareCtx = null;
  chartSection.style.display = 'none';
  asStatsSectionEl.style.display = 'none';
  asTableSectionEl.style.display = 'none';

  if (updateUrl) {
    const params = new URLSearchParams({ q: input });
    if (viewKey !== 'latest') params.set('date', dateLabel);
    history.pushState({ q: input, date: viewKey !== 'latest' ? dateLabel : null }, '',
      '?' + params.toString());
  }

  // AS number: "AS13335" or bare "13335"
  const asnMatch = input.match(/^(?:AS)?(\d{1,10})$/i);
  if (asnMatch) { await lookupASN(asnMatch[1], viewKey, dateLabel); return; }

  // Bare IPv4 address (no prefix length) → look up enclosing /24
  const bareIPv4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(input);
  if (bareIPv4) {
    const cidr = `${bareIPv4[1]}.${bareIPv4[2]}.${bareIPv4[3]}.0/24`;
    await lookupPrefix(cidr, viewKey, dateLabel);
    return;
  }

  // Bare IPv6 address (contains ':' but no '/') → look up enclosing /48
  if (input.includes(':') && !input.includes('/')) {
    try {
      const cidr = ipv6ToSlash48(input);
      await lookupPrefix(cidr, viewKey, dateLabel);
    } catch { setStatus(`Invalid IPv6 address: ${escHtml(input)}`, true); }
    return;
  }

  // CIDR block shorter than the dataset granularity (/24 for v4, /48 for v6)
  const lenMatch = input.match(/\/(\d+)$/);
  if (lenMatch) {
    const len = parseInt(lenMatch[1]);
    const isIPv6 = input.includes(':');
    if ((!isIPv6 && len < 24) || (isIPv6 && len < 48)) {
      await lookupCIDRBlock(input, viewKey, dateLabel);
      return;
    }
  }

  // Default: exact /24 or /48 prefix
  await lookupPrefix(input, viewKey, dateLabel);
}

async function lookupPrefix(prefix, viewKey, dateLabel) {
  const isIPv6 = prefix.includes(':');
  const view   = isIPv6 ? 'ipv6' : 'ipv4';
  setStatus(`Querying ${isIPv6 ? 'IPv6' : 'IPv4'} dataset (${dateLabel})…`);
  try {
    await registerViews(viewKey);
    const result = await conn.query(
      `SELECT * FROM ${view} WHERE prefix = '${prefix.replace(/'/g, "''")}'`
    );
    const rows = result.toArray().map(r => r.toJSON());
    if (rows.length === 0) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">
        Prefix <strong>${escHtml(prefix)}</strong> not found in the census for <strong>${escHtml(dateLabel)}</strong>.
        It may be unicast-only, not yet detected, or not present on that date.
      </div>`;
      return;
    }
    const row = rows[0];
    const locations = parseLocations(row.locations);
    setStatus('');
    resultsEl.innerHTML = renderResult(row, isIPv6, locations, dateLabel);
    if (locations.length) {
      initMap(locations);
      document.getElementById('loc-csv-btn').addEventListener('click', () =>
        downloadLocCsv(locations, row.prefix));
    }
  } catch (err) { handleQueryError(err, dateLabel); }
}

async function lookupASN(asn, viewKey, dateLabel) {
  setStatus(`Searching AS${asn} prefixes (${dateLabel})…`);
  // asn is digits-only (validated by regex above), safe to interpolate
  const re = `(^|_)${asn}(_|$)`;
  try {
    await registerViews(viewKey);
    const result = await conn.query(`
      SELECT 'v4' AS ver, prefix,
        AB_ICMPv4 AS ab1, AB_TCPv4 AS ab2, AB_DNSv4 AS ab3,
        GCD_ICMPv4 AS gcd1, GCD_TCPv4 AS gcd2,
        ASN, len(locations) AS nloc, partial
      FROM ipv4 WHERE regexp_matches(ASN, '${re}')
      UNION ALL
      SELECT 'v6', prefix,
        AB_ICMPv6, AB_TCPv6, AB_DNSv6,
        GCD_ICMPv6, GCD_TCPv6,
        ASN, len(locations), NULL AS partial
      FROM ipv6 WHERE regexp_matches(ASN, '${re}')
      ORDER BY prefix
    `);
    const rows = result.toArray().map(r => r.toJSON());
    if (rows.length === 0) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">No anycast prefixes found for <strong>AS${escHtml(asn)}</strong> in census for <strong>${escHtml(dateLabel)}</strong>.</div>`;
      return;
    }
    setStatus('');
    resultsEl.innerHTML = renderPrefixList(rows, `AS${asn}`, dateLabel);
    currentLookupCtx = { viewKey, dateLabel };
  } catch (err) { handleQueryError(err, dateLabel); }
}

async function lookupCIDRBlock(cidr, viewKey, dateLabel) {
  const isIPv6 = cidr.includes(':');
  if (isIPv6) {
    await lookupCIDRBlockIPv6(cidr, viewKey, dateLabel);
  } else {
    await lookupCIDRBlockIPv4(cidr, viewKey, dateLabel);
  }
}

async function lookupCIDRBlockIPv4(cidr, viewKey, dateLabel) {
  const len = parseInt(cidr.split('/')[1]);
  if (isNaN(len) || len < 0 || len > 32) {
    setStatus(`Invalid IPv4 prefix: ${escHtml(cidr)}`, true);
    return;
  }
  setStatus(`Searching anycast /24s within ${escHtml(cidr)} (${dateLabel})…`);
  try {
    await registerViews(viewKey);
    const result = await conn.query(`
      SELECT 'v4' AS ver, prefix,
        AB_ICMPv4 AS ab1, AB_TCPv4 AS ab2, AB_DNSv4 AS ab3,
        GCD_ICMPv4 AS gcd1, GCD_TCPv4 AS gcd2,
        ASN, len(locations) AS nloc, partial
      FROM ipv4
      WHERE prefix::INET <<= '${cidr.replace(/'/g, "''")}'::INET
      ORDER BY prefix
    `);
    const rows = result.toArray().map(r => r.toJSON());
    if (rows.length === 0) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">No anycast /24s found within <strong>${escHtml(cidr)}</strong> in census for <strong>${escHtml(dateLabel)}</strong>.</div>`;
      return;
    }
    setStatus('');
    resultsEl.innerHTML = renderPrefixList(rows, cidr, dateLabel);
    currentLookupCtx = { viewKey, dateLabel };
  } catch (err) { handleQueryError(err, dateLabel); }
}

async function lookupCIDRBlockIPv6(cidr, viewKey, dateLabel) {
  const len = parseInt(cidr.split('/')[1]);
  if (isNaN(len) || len < 0 || len > 128) {
    setStatus(`Invalid IPv6 prefix: ${escHtml(cidr)}`, true);
    return;
  }
  setStatus(`Searching anycast /48s within ${escHtml(cidr)} (${dateLabel})…`);
  try {
    await registerViews(viewKey);
    const result = await conn.query(`
      SELECT 'v6' AS ver, prefix,
        AB_ICMPv6 AS ab1, AB_TCPv6 AS ab2, AB_DNSv6 AS ab3,
        GCD_ICMPv6 AS gcd1, GCD_TCPv6 AS gcd2,
        ASN, len(locations) AS nloc, NULL AS partial
      FROM ipv6
      WHERE prefix::INET <<= '${cidr.replace(/'/g, "''")}'::INET
      ORDER BY prefix
    `);
    const rows = result.toArray().map(r => r.toJSON());
    if (rows.length === 0) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">No anycast /48s found within <strong>${escHtml(cidr)}</strong> in census for <strong>${escHtml(dateLabel)}</strong>.</div>`;
      return;
    }
    setStatus('');
    resultsEl.innerHTML = renderPrefixList(rows, cidr, dateLabel);
    currentLookupCtx = { viewKey, dateLabel };
  } catch (err) { handleQueryError(err, dateLabel); }
}

function handleQueryError(err, dateLabel) {
  const msg = err.message ?? '';
  if (msg.includes('404') || msg.includes('HTTP') || msg.includes('fetch')) {
    setStatus(`No data file found for ${dateLabel}. The census may not have run on that date.`, true);
  } else {
    setStatus('Query error: ' + msg, true);
  }
}

// ── Rendering ──────────────────────────────────────────────────────────────
function getConfidence(ab_max, gcd_max) {
  if (gcd_max > 1) return { label: 'Highly confident', cls: 'confidence-high' };
  if (ab_max > 2)  return { label: 'Confident',        cls: 'confidence-medium' };
  return               { label: 'Not confident',      cls: 'confidence-low' };
}

// ── Pagination helpers ──────────────────────────────────────────────────────
function resetPg() { for (const k of Object.keys(pgStore)) delete pgStore[k]; }

function pgControlsHtml(id, total, page, pageSize) {
  if (total <= 10) return '';
  const pages = Math.ceil(total / pageSize) || 1;
  const start = page * pageSize + 1;
  const end   = Math.min((page + 1) * pageSize, total);
  const sizes = [10, 25, 50, 100];
  return `<div class="pg-ctrl">
    <span class="pg-info">${fmtN(start)}\u2013${fmtN(end)} of ${fmtN(total)}</span>
    <button class="pg-first pg-btn" ${page === 0 ? 'disabled' : ''} title="First page">\u00AB</button>
    <button class="pg-prev pg-btn"  ${page === 0 ? 'disabled' : ''} title="Previous page">\u2039</button>
    <span class="pg-page">${page + 1} / ${pages}</span>
    <button class="pg-next pg-btn"  ${page >= pages - 1 ? 'disabled' : ''} title="Next page">\u203A</button>
    <button class="pg-last pg-btn"  ${page >= pages - 1 ? 'disabled' : ''} title="Last page">\u00BB</button>
    <span class="pg-sep">\u00B7</span>
    <span class="pg-size-lbl">Per page</span>
    ${sizes.map(s => `<button class="pg-size-btn${s === pageSize ? ' active' : ''}" data-size="${s}">${s}</button>`).join('')}
  </div>`;
}

function updatePgView(id) {
  const st = pgStore[id];
  if (!st) return;
  const wrap = document.querySelector(`[data-pg-id="${id}"]`);
  if (!wrap) return;
  const { rows, page, pageSize, renderRows } = st;
  const slice = rows.slice(page * pageSize, page * pageSize + pageSize);
  const tbody = wrap.querySelector('tbody');
  if (tbody) tbody.innerHTML = renderRows(slice);
  const html = pgControlsHtml(id, rows.length, page, pageSize);
  wrap.querySelectorAll('.pg-controls-slot').forEach(el => { el.innerHTML = html; });
  const topSlot = wrap.querySelector('.pg-controls-slot');
  if (topSlot) topSlot.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function applyTableFilter(id, filterType, value) {
  const st = pgStore[id];
  if (!st || !st.allRows) return;
  if (filterType === 'conf') st.confFilter = value;
  else if (filterType === 'ver') st.verFilter = value;
  // Apply both filters then re-apply active sort
  st.rows = st.allRows.filter(r => {
    if (st.confFilter !== 'all' && r.conf !== st.confFilter) return false;
    if (st.verFilter  !== 'all' && r.ver  !== st.verFilter)  return false;
    return true;
  });
  st.rows = sortRows(st.rows, st.sortCol, st.sortAsc);
  st.page = 0;
  updatePgView(id);
  // Update filter button active states
  const wrap = document.querySelector(`[data-pg-id="${id}"]`);
  if (!wrap) return;
  wrap.querySelectorAll('.conf-filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.conf === st.confFilter);
  });
  wrap.querySelectorAll('.ver-filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.ver === st.verFilter);
  });
  // Update count display
  const countEl = wrap.querySelector('.conf-filter-count');
  if (countEl) countEl.textContent = `${fmtN(st.rows.length)} shown`;
  // Recompute confidence counts for the active ver filter
  if (filterType === 'ver') {
    const verFiltered = st.allRows.filter(r => st.verFilter === 'all' || r.ver === st.verFilter);
    let cH = 0, cM = 0, cL = 0;
    for (const r of verFiltered) {
      if (r.conf === 'high') cH++; else if (r.conf === 'medium') cM++; else cL++;
    }
    wrap.querySelectorAll('.conf-filter-btn').forEach(btn => {
      if      (btn.dataset.conf === 'high')   btn.textContent = `High (${fmtN(cH)})`;
      else if (btn.dataset.conf === 'medium') btn.textContent = `Med (${fmtN(cM)})`;
      else if (btn.dataset.conf === 'low')    btn.textContent = `Low (${fmtN(cL)})`;
    });
  }
}

function sortRows(rows, col, asc) {
  if (!col) return rows;
  const confOrder = { high: 2, medium: 1, low: 0 };
  const dir = asc ? 1 : -1;
  return [...rows].sort((a, b) => {
    let av, bv;
    if      (col === 'conf')   { av = confOrder[a.conf] ?? 0; bv = confOrder[b.conf] ?? 0; }
    else if (col === 'ab')     { av = a.ab_max;  bv = b.ab_max; }
    else if (col === 'gcd')    { av = a.gcd_max; bv = b.gcd_max; }
    else if (col === 'nloc')   { av = Number(a.nloc ?? 0); bv = Number(b.nloc ?? 0); }
    else if (col === 'ver')    { av = a.ver;     bv = b.ver; }
    else if (col === 'prefix') { av = a.prefix;  bv = b.prefix; }
    else if (col === 'n4')     { av = a.n4;      bv = b.n4; }
    else if (col === 'n6')     { av = a.n6;      bv = b.n6; }
    else if (col === 'total')  { av = a.total;   bv = b.total; }
    else if (col === 'asn')    { av = a.asn;     bv = b.asn; }
    if (av < bv) return -dir;
    if (av > bv) return  dir;
    return 0;
  });
}

function applySort(id, col) {
  const st = pgStore[id];
  if (!st) return;
  const defaultDesc = new Set(['ab', 'gcd', 'nloc', 'conf', 'n4', 'n6', 'total']);
  if (st.sortCol === col) {
    st.sortAsc = !st.sortAsc;
  } else {
    st.sortCol = col;
    st.sortAsc = !defaultDesc.has(col);
  }
  st.rows = sortRows(st.rows, st.sortCol, st.sortAsc);
  st.page = 0;
  updatePgView(id);
  const wrap = document.querySelector(`[data-pg-id="${id}"]`);
  if (!wrap) return;
  wrap.querySelectorAll('th[data-sort]').forEach(th => {
    const active = th.dataset.sort === st.sortCol;
    th.classList.toggle('sort-active', active);
    const ind = th.querySelector('.sort-ind');
    if (ind) ind.textContent = active ? (st.sortAsc ? '\u2191' : '\u2193') : '';
  });
}

function renderPrefixList(rows, searchTerm, dateLabel) {
  const n = v => Number(v ?? 0);
  const id = `pg${++pgCounter}`;
  const pageSize = 10;

  const renderRows = (slice) => slice.map(row => {
    const ab_max  = Math.max(n(row.ab1), n(row.ab2), n(row.ab3));
    const gcd_max = Math.max(n(row.gcd1), n(row.gcd2));
    const conf    = getConfidence(ab_max, gcd_max);
    const asns    = (row.ASN ?? '').toString().split('_').filter(Boolean);
    return `
      <tr class="pl-row" data-prefix="${escHtml(row.prefix)}">
        <td><span class="ver-badge">${row.ver}</span></td>
        <td><span class="prefix-link">${escHtml(row.prefix)}</span>${row.partial ? ' <span class="tag tag-warn" title="Partial anycast: this /24 contains both unicast and anycast addresses">partial</span>' : ''}</td>
        <td><span class="confidence ${conf.cls}">${conf.label}</span></td>
        <td class="${ab_max  ? 'count' : 'count-zero'}">AB&nbsp;${fmtN(ab_max)}</td>
        <td class="${gcd_max ? 'count' : 'count-zero'}">GCD&nbsp;${fmtN(gcd_max)}</td>
        <td>${asns.map(a => `<span class="tag">AS${escHtml(a)}</span>`).join(' ')}</td>
      </tr>`;
  }).join('');

  // Pre-compute derived fields for filtering and sorting
  for (const row of rows) {
    const ab_max  = Math.max(n(row.ab1), n(row.ab2), n(row.ab3));
    const gcd_max = Math.max(n(row.gcd1), n(row.gcd2));
    row.conf    = gcd_max > 1 ? 'high' : ab_max > 2 ? 'medium' : 'low';
    row.ab_max  = ab_max;
    row.gcd_max = gcd_max;
  }
  let nV4 = 0, nV6 = 0, cH = 0, cM = 0, cL = 0;
  for (const r of rows) {
    if (r.ver === 'v4') nV4++; else nV6++;
    if (r.conf === 'high') cH++; else if (r.conf === 'medium') cM++; else cL++;
  }

  pgStore[id] = { allRows: rows, rows, page: 0, pageSize, renderRows, confFilter: 'all', verFilter: 'all', sortCol: null, sortAsc: true };
  const initialBody = renderRows(rows.slice(0, pageSize));
  const controls = pgControlsHtml(id, rows.length, 0, pageSize);
  const filterBar = `<div class="conf-filter">
    <div class="filter-row">
      <span class="conf-filter-lbl">Protocol:</span>
      <button class="ver-filter-btn active" data-ver="all">All (${fmtN(rows.length)})</button>
      <button class="ver-filter-btn" data-ver="v4">IPv4 (${fmtN(nV4)})</button>
      <button class="ver-filter-btn" data-ver="v6">IPv6 (${fmtN(nV6)})</button>
      <span class="pg-sep">\u00B7</span>
      <span class="conf-filter-count">${fmtN(rows.length)} shown</span>
    </div>
    <div class="filter-row">
      <span class="conf-filter-lbl">Confidence:</span>
      <button class="conf-filter-btn active" data-conf="all">All</button>
      <button class="conf-filter-btn" data-conf="high">High (${fmtN(cH)})</button>
      <button class="conf-filter-btn" data-conf="medium">Med (${fmtN(cM)})</button>
      <button class="conf-filter-btn" data-conf="low">Low (${fmtN(cL)})</button>
    </div>
  </div>`;

  return `
    <div class="card" data-pg-id="${id}">
      <div class="card-title">${fmtN(rows.length)} anycast prefix${rows.length !== 1 ? 'es' : ''} in ${escHtml(searchTerm)} \u2014 ${escHtml(dateLabel)} <span style="font-weight:400;text-transform:none;letter-spacing:0">(${fmtN(nV4)} IPv4, ${fmtN(nV6)} IPv6)</span></div>
      <p class="loc-note">Click a prefix to see its full details.</p>
      ${filterBar}
      <div class="pg-controls-slot">${controls}</div>
      <table class="prefix-list">
        <thead><tr>
          <th data-sort="ver">Ver<span class="sort-ind"></span></th>
          <th data-sort="prefix">Prefix<span class="sort-ind"></span></th>
          <th data-sort="conf">Confidence<span class="sort-ind"></span></th>
          <th data-sort="ab">AB sites<span class="sort-ind"></span></th>
          <th data-sort="gcd">GCD sites<span class="sort-ind"></span></th>
          <th>ASN(s)</th>
        </tr></thead>
        <tbody>${initialBody}</tbody>
      </table>
      <div class="pg-controls-slot">${controls}</div>
    </div>`;
}

function renderResult(row, isIPv6, locations, dateLabel) {
  const suffix  = isIPv6 ? 'v6' : 'v4';
  // int64 columns come back as BigInt from DuckDB-WASM — coerce to Number
  const n = v => Number(v ?? 0);
  const ab_icmp  = n(row[`AB_ICMP${suffix}`]);
  const ab_tcp   = n(row[`AB_TCP${suffix}`]);
  const ab_dns   = n(row[`AB_DNS${suffix}`]);
  const gcd_icmp = n(row[`GCD_ICMP${suffix}`]);
  const gcd_tcp  = n(row[`GCD_TCP${suffix}`]);

  const ab_max  = Math.max(ab_icmp, ab_tcp, ab_dns);
  const gcd_max = Math.max(gcd_icmp, gcd_tcp);
  const conf    = getConfidence(ab_max, gcd_max);

  const asns = (row.ASN ?? '').toString().split('_').filter(Boolean);

  const partialTag = row.partial
    ? `<span class="tag tag-warn">⚠ partial (mixed unicast/anycast)</span>`
    : '';

  return `
    <div class="card">
      <div class="card-title">Result &mdash; ${escHtml(dateLabel)}</div>
      <div class="prefix-badge">${escHtml(row.prefix)}</div>
      ${partialTag}
      <span class="confidence ${conf.cls}">${conf.label}</span>
      <br><br>

      <table>
        <tr>
          <th>Method</th><th>Sites detected</th>
        </tr>
        <tr>
          <td>Anycast-based ICMP (AB)</td>
          <td class="${ab_icmp ? 'count' : 'count-zero'}">${fmtN(ab_icmp)}</td>
        </tr>
        <tr>
          <td>Anycast-based TCP (AB)</td>
          <td class="${ab_tcp ? 'count' : 'count-zero'}">${fmtN(ab_tcp)}</td>
        </tr>
        <tr>
          <td>Anycast-based DNS (AB)</td>
          <td class="${ab_dns ? 'count' : 'count-zero'}">${fmtN(ab_dns)}</td>
        </tr>
        <tr>
          <td>Latency-based ICMP (GCD)</td>
          <td class="${gcd_icmp ? 'count' : 'count-zero'}">${fmtN(gcd_icmp)}</td>
        </tr>
        <tr>
          <td>Latency-based TCP (GCD)</td>
          <td class="${gcd_tcp ? 'count' : 'count-zero'}">${fmtN(gcd_tcp)}</td>
        </tr>
      </table>
    </div>

    <div class="card">
      <div class="card-title">Routing</div>
      <table>
        <tr>
          <th>Backing prefix</th>
          <td>${row.backing_prefix
            ? `<a class="tag tag-link" data-lookup="${escHtml(row.backing_prefix)}" href="?q=${encodeURIComponent(row.backing_prefix)}">${escHtml(row.backing_prefix)}</a>`
            : '—'}</td>
        </tr>
        <tr>
          <th>ASN(s)</th>
          <td>${asns.map(a => `<a class="tag tag-link" data-lookup="AS${escHtml(a)}" href="?q=AS${encodeURIComponent(a)}">AS${escHtml(a)}</a>`).join(' ') || '—'}</td>
        </tr>
      </table>
    </div>

    ${locations.length ? `
    <div class="card">
      <div class="card-title" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.75rem">
        <span>Detected locations (${fmtN(locations.length)})</span>
        <button id="loc-csv-btn" class="csv-btn">↓ CSV</button>
      </div>
      <p class="loc-note">Lower bound — actual number of PoPs may be higher.</p>
      <div id="loc-map"></div>
      <div class="loc-grid">
        ${locations.map(loc => `
          <div class="loc-card">
            <div class="loc-iata">${escHtml(loc.id ?? '?')}</div>
            <div class="loc-city">${escHtml(loc.city ?? '')}${loc.country_code ? `, ${escHtml(loc.country_code)}` : ''}</div>
            <div class="loc-coords">${fmtCoord(loc.lat)}, ${fmtCoord(loc.lon)}</div>
          </div>
        `).join('')}
      </div>
    </div>` : ''}
  `;
}

// ── Map ────────────────────────────────────────────────────────────────────
function initMap(locations) {
  const el = document.getElementById('loc-map');
  if (!el) return;

  if (leafletMap) { leafletMap.remove(); leafletMap = null; }

  leafletMap = L.map('loc-map', {
    zoomControl: true,
    worldCopyJump: false,
    maxBounds: [[-58, -180], [85, 180]],
    maxBoundsViscosity: 1.0,
  });

  L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 19,
    noWrap: true,
  }).addTo(leafletMap);

  // Fullscreen control
  const FsControl = L.Control.extend({
    options: { position: 'topright' },
    onAdd(map) {
      const btn = L.DomUtil.create('button', 'leaflet-fs-btn');
      btn.title = 'Toggle fullscreen';
      btn.innerHTML = '⛶';
      L.DomEvent.disableClickPropagation(btn);
      L.DomEvent.on(btn, 'click', () => {
        const el = map.getContainer();
        if (!document.fullscreenElement) {
          el.requestFullscreen();
        } else {
          document.exitFullscreen();
        }
      });
      return btn;
    },
  });
  leafletMap.addControl(new FsControl());

  document.addEventListener('fullscreenchange', () => {
    setTimeout(() => leafletMap && leafletMap.invalidateSize(), 50);
  });

  const validLocs = locations.filter(l => l.lat != null && l.lon != null);
  const markers = validLocs.map(loc => {
    const label = (!loc.city || loc.id === 'NoCity')
      ? `Unknown (${fmtCoord(loc.lat)}, ${fmtCoord(loc.lon)})`
      : `${loc.city}, ${loc.country_code} — ${loc.id}`;

    return L.circleMarker([loc.lat, loc.lon], {
      radius: 6,
      fillColor: '#58a6ff',
      color: '#1f6feb',
      weight: 1.5,
      opacity: 1,
      fillOpacity: 0.85,
    }).bindTooltip(label).addTo(leafletMap);
  });

  if (markers.length) {
    leafletMap.fitBounds(L.featureGroup(markers).getBounds().pad(0.15));
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────
function downloadLocCsv(locations, prefix) {
  const rows = [['iata', 'city', 'country_code', 'latitude', 'longitude']];
  for (const loc of locations) {
    rows.push([
      loc.id ?? '',
      loc.city ?? '',
      loc.country_code ?? '',
      loc.lat ?? '',
      loc.lon ?? '',
    ]);
  }
  const csv = rows.map(r => r.map(v => `"${String(v).replace(/"/g, '""')}"`).join(',')).join('\n');
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
  a.download = `${prefix.replace(/\//g, '_')}_locations.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
}

function parseLocations(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  try { return JSON.parse(raw); } catch { return []; }
}

function fmtN(v) { return Number(v).toLocaleString(); }

function fmtCoord(v) {
  if (v == null) return '?';
  return Number(v).toFixed(2);
}

function escHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Event wiring ───────────────────────────────────────────────────────────
document.getElementById('home-link').addEventListener('click', e => {
  e.preventDefault();
  inputEl.value = '';
  resultsEl.innerHTML = '';
  resetPg();
  if (chartData) chartSection.style.display = '';
  if (asStatsReady) asStatsSectionEl.style.display = '';
  if (asTableReady) asTableSectionEl.style.display = '';
  if (isCompareMode) {
    isCompareMode = false;
    modeLookup.classList.add('active');
    modeCompare.classList.remove('active');
    lookupRow.style.display = '';
    compareRow.style.display = 'none';
  }
  setStatus('Ready.');
  history.pushState(null, '', '.');
});

resultsEl.addEventListener('click', e => {
  // ── Sortable column headers ──
  const sortTh = e.target.closest('th[data-sort]');
  if (sortTh) {
    const wrap = sortTh.closest('[data-pg-id]');
    if (!wrap) return;
    applySort(wrap.dataset.pgId, sortTh.dataset.sort);
    return;
  }

  // ── Confidence filter buttons ──
  const confBtn = e.target.closest('.conf-filter-btn');
  if (confBtn) {
    const wrap = confBtn.closest('[data-pg-id]');
    if (!wrap) return;
    applyTableFilter(wrap.dataset.pgId, 'conf', confBtn.dataset.conf);
    return;
  }

  // ── Protocol (v4/v6) filter buttons ──
  const verBtn = e.target.closest('.ver-filter-btn');
  if (verBtn) {
    const wrap = verBtn.closest('[data-pg-id]');
    if (!wrap) return;
    applyTableFilter(wrap.dataset.pgId, 'ver', verBtn.dataset.ver);
    return;
  }

  // ── Pagination first / prev / next / last ──
  const pgBtn = e.target.closest('.pg-first, .pg-prev, .pg-next, .pg-last');
  if (pgBtn) {
    const wrap = pgBtn.closest('[data-pg-id]');
    if (!wrap) return;
    const id = wrap.dataset.pgId;
    const st = pgStore[id];
    if (!st) return;
    const pages = Math.ceil(st.rows.length / st.pageSize) || 1;
    if      (pgBtn.classList.contains('pg-first') && st.page > 0)            st.page = 0;
    else if (pgBtn.classList.contains('pg-prev')  && st.page > 0)            st.page--;
    else if (pgBtn.classList.contains('pg-next')  && st.page < pages - 1)    st.page++;
    else if (pgBtn.classList.contains('pg-last')  && st.page < pages - 1)    st.page = pages - 1;
    updatePgView(id);
    return;
  }

  // ── Pagination page-size selector ──
  const sizeBtn = e.target.closest('.pg-size-btn');
  if (sizeBtn) {
    const wrap = sizeBtn.closest('[data-pg-id]');
    if (!wrap) return;
    const id = wrap.dataset.pgId;
    const st = pgStore[id];
    if (!st) return;
    st.pageSize = parseInt(sizeBtn.dataset.size);
    st.page = 0;
    updatePgView(id);
    return;
  }

  // ── Prefix row click (lookup or compare) ──
  const plRow = e.target.closest('.pl-row');
  if (plRow) {
    const prefix = plRow.dataset.prefix;
    inputEl.value = prefix;
    if (plRow.classList.contains('cmp-click') && currentCompareCtx) {
      const { dateA, dateB } = currentCompareCtx;
      const params = new URLSearchParams();
      params.set('mode', 'compare');
      params.set('dateA', dateA);
      params.set('dateB', dateB);
      params.set('q', prefix);
      history.pushState({ mode: 'compare', dateA, dateB, q: prefix }, '',
        '?' + params.toString());
      comparePrefixDetail(prefix, dateA, dateB);
    } else if (currentLookupCtx) {
      lookup(prefix);
    }
    return;
  }

  // ── Clickable tags (ASN, backing prefix) in detail view ──
  const lookupEl = e.target.closest('[data-lookup]');
  if (lookupEl) {
    e.preventDefault();
    inputEl.value = lookupEl.dataset.lookup;
    lookup(lookupEl.dataset.lookup);
  }
});

// ── AS table clicks (sort, pagination, row) ────────────────────────────────
asTableSectionEl.addEventListener('click', e => {
  const sortTh = e.target.closest('th[data-sort]');
  if (sortTh) {
    const wrap = sortTh.closest('[data-pg-id]');
    if (wrap) applySort(wrap.dataset.pgId, sortTh.dataset.sort);
    return;
  }
  const pgBtn = e.target.closest('.pg-first, .pg-prev, .pg-next, .pg-last');
  if (pgBtn) {
    const wrap = pgBtn.closest('[data-pg-id]');
    if (!wrap) return;
    const id = wrap.dataset.pgId;
    const st = pgStore[id];
    if (!st) return;
    const pages = Math.ceil(st.rows.length / st.pageSize) || 1;
    if      (pgBtn.classList.contains('pg-first') && st.page > 0)         st.page = 0;
    else if (pgBtn.classList.contains('pg-prev')  && st.page > 0)         st.page--;
    else if (pgBtn.classList.contains('pg-next')  && st.page < pages - 1) st.page++;
    else if (pgBtn.classList.contains('pg-last')  && st.page < pages - 1) st.page = pages - 1;
    updatePgView(id);
    return;
  }
  const sizeBtn = e.target.closest('.pg-size-btn');
  if (sizeBtn) {
    const wrap = sizeBtn.closest('[data-pg-id]');
    if (!wrap) return;
    const id = wrap.dataset.pgId;
    const st = pgStore[id];
    if (!st) return;
    st.pageSize = parseInt(sizeBtn.dataset.size);
    st.page = 0;
    updatePgView(id);
    return;
  }
  const asRow = e.target.closest('.as-row');
  if (asRow) {
    const q = `AS${asRow.dataset.asn}`;
    inputEl.value = q;
    lookup(q);
  }
});

searchBtn.addEventListener('click', () => lookup(inputEl.value));

inputEl.addEventListener('keydown', e => {
  if (e.key === 'Enter') lookup(inputEl.value);
});

// Toggle date input enabled state with radio buttons
document.querySelectorAll('input[name="when"]').forEach(radio => {
  radio.addEventListener('change', () => {
    dateInput.disabled = whenLatest.checked;
    if (!whenLatest.checked) {
      dateInput.focus();
    }
  });
});

// Clicking the date input automatically switches to the "date" radio
dateInput.addEventListener('focus', () => { whenDate.checked = true; dateInput.disabled = false; });

// ── Compare mode toggle ─────────────────────────────────────────────────────
modeLookup.addEventListener('click', () => {
  isCompareMode = false;
  modeLookup.classList.add('active');
  modeCompare.classList.remove('active');
  lookupRow.style.display = '';
  compareRow.style.display = 'none';
});

modeCompare.addEventListener('click', () => {
  isCompareMode = true;
  modeCompare.classList.add('active');
  modeLookup.classList.remove('active');
  lookupRow.style.display = 'none';
  compareRow.style.display = '';
});

cmpBtn.addEventListener('click', () => runCompare());

// ── Compare helpers ──────────────────────────────────────────────────────────
async function registerCompareViews(dateA, dateB) {
  const today = new Date().toISOString().slice(0, 10);
  const keyA = dateA === 'latest' ? 'latest' : dateA.replace(/-/g, '/');
  const keyB = dateB === 'latest' ? 'latest' : dateB.replace(/-/g, '/');

  const urlA4 = keyA === 'latest' ? baseUrl + `IPv4-latest.parquet?v=${today}` : baseUrl + `${keyA}/IPv4.parquet`;
  const urlA6 = keyA === 'latest' ? baseUrl + `IPv6-latest.parquet?v=${today}` : baseUrl + `${keyA}/IPv6.parquet`;
  const urlB4 = keyB === 'latest' ? baseUrl + `IPv4-latest.parquet?v=${today}` : baseUrl + `${keyB}/IPv4.parquet`;
  const urlB6 = keyB === 'latest' ? baseUrl + `IPv6-latest.parquet?v=${today}` : baseUrl + `${keyB}/IPv6.parquet`;

  await conn.query(`CREATE OR REPLACE VIEW ipv4_a AS SELECT * FROM read_parquet('${urlA4}')`);
  await conn.query(`CREATE OR REPLACE VIEW ipv6_a AS SELECT * FROM read_parquet('${urlA6}')`);
  await conn.query(`CREATE OR REPLACE VIEW ipv4_b AS SELECT * FROM read_parquet('${urlB4}')`);
  await conn.query(`CREATE OR REPLACE VIEW ipv6_b AS SELECT * FROM read_parquet('${urlB6}')`);
}

document.querySelectorAll('.examples span[data-prefix]').forEach(el => {
  el.addEventListener('click', () => {
    inputEl.value = el.dataset.prefix;
    lookup(el.dataset.prefix);
  });
});

// ── Compare logic ──────────────────────────────────────────────────────────
async function runCompare({ updateUrl = true } = {}) {
  const dateA = cmpDateA.value;
  const dateB = cmpDateB.value;
  if (!dateA || !dateB) { setStatus('Select two dates to compare.', true); return; }
  if (dateA === dateB) { setStatus('Select two different dates.', true); return; }

  const query = inputEl.value.trim();
  resultsEl.innerHTML = '';
  resetPg(); currentLookupCtx = null;
  chartSection.style.display = 'none';
  asStatsSectionEl.style.display = 'none';
  asTableSectionEl.style.display = 'none';

  if (updateUrl) {
    const params = new URLSearchParams();
    params.set('mode', 'compare');
    params.set('dateA', dateA);
    params.set('dateB', dateB);
    if (query) params.set('q', query);
    history.pushState({ mode: 'compare', dateA, dateB, q: query || null }, '',
      '?' + params.toString());
  }

  setStatus(`Loading data for ${dateA} and ${dateB}\u2026`);
  try {
    await registerCompareViews(dateA, dateB);
  } catch (err) {
    setStatus('Failed to load data for one or both dates: ' + (err.message ?? ''), true);
    return;
  }

  if (query) {
    // Prefix-level compare
    await comparePrefixDetail(query, dateA, dateB);
  } else {
    // Full census compare
    await compareCensus(dateA, dateB);
  }
}

async function compareCensus(dateA, dateB) {
  setStatus(`Comparing ${dateA} vs ${dateB}…`);
  try {
    // Helper: compute confidence columns inline via SQL
    const confExpr = (ab1, ab2, ab3, gcd1, gcd2) =>
      `CASE WHEN GREATEST(${gcd1},${gcd2}) > 1 THEN 'high'
            WHEN GREATEST(${ab1},${ab2},${ab3}) > 2 THEN 'medium'
            ELSE 'low' END AS conf`;

    // Both dates
    const bothRes = await conn.query(`
      SELECT 'v4' AS ver, a.prefix,
        ${confExpr('a.AB_ICMPv4','a.AB_TCPv4','a.AB_DNSv4','a.GCD_ICMPv4','a.GCD_TCPv4')}
      FROM ipv4_a a INNER JOIN ipv4_b b ON a.prefix = b.prefix
      UNION ALL
      SELECT 'v6', a.prefix,
        ${confExpr('a.AB_ICMPv6','a.AB_TCPv6','a.AB_DNSv6','a.GCD_ICMPv6','a.GCD_TCPv6')}
      FROM ipv6_a a INNER JOIN ipv6_b b ON a.prefix = b.prefix
      ORDER BY prefix
    `);
    const both = bothRes.toArray().map(r => r.toJSON());

    // Only in A
    const onlyARes = await conn.query(`
      SELECT 'v4' AS ver, a.prefix,
        ${confExpr('a.AB_ICMPv4','a.AB_TCPv4','a.AB_DNSv4','a.GCD_ICMPv4','a.GCD_TCPv4')}
      FROM ipv4_a a LEFT JOIN ipv4_b b ON a.prefix = b.prefix WHERE b.prefix IS NULL
      UNION ALL
      SELECT 'v6', a.prefix,
        ${confExpr('a.AB_ICMPv6','a.AB_TCPv6','a.AB_DNSv6','a.GCD_ICMPv6','a.GCD_TCPv6')}
      FROM ipv6_a a LEFT JOIN ipv6_b b ON a.prefix = b.prefix WHERE b.prefix IS NULL
      ORDER BY prefix
    `);
    const onlyA = onlyARes.toArray().map(r => r.toJSON());

    // Only in B
    const onlyBRes = await conn.query(`
      SELECT 'v4' AS ver, b.prefix,
        ${confExpr('b.AB_ICMPv4','b.AB_TCPv4','b.AB_DNSv4','b.GCD_ICMPv4','b.GCD_TCPv4')}
      FROM ipv4_b b LEFT JOIN ipv4_a a ON b.prefix = a.prefix WHERE a.prefix IS NULL
      UNION ALL
      SELECT 'v6', b.prefix,
        ${confExpr('b.AB_ICMPv6','b.AB_TCPv6','b.AB_DNSv6','b.GCD_ICMPv6','b.GCD_TCPv6')}
      FROM ipv6_b b LEFT JOIN ipv6_a a ON b.prefix = a.prefix WHERE a.prefix IS NULL
      ORDER BY prefix
    `);
    const onlyB = onlyBRes.toArray().map(r => r.toJSON());

    setStatus('');
    resultsEl.innerHTML = renderCompareResults(both, onlyA, onlyB, dateA, dateB);
    currentCompareCtx = { dateA, dateB };
  } catch (err) { setStatus('Compare error: ' + (err.message ?? ''), true); }
}

async function comparePrefixDetail(raw, dateA, dateB) {
  let prefix = raw.trim();
  // Bare IPv4 → /24
  const bareV4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(prefix);
  if (bareV4) prefix = `${bareV4[1]}.${bareV4[2]}.${bareV4[3]}.0/24`;
  // Bare IPv6 → /48
  if (prefix.includes(':') && !prefix.includes('/')) {
    try { prefix = ipv6ToSlash48(prefix); } catch { setStatus('Invalid IPv6 address.', true); return; }
  }

  const isIPv6 = prefix.includes(':');
  const viewA = isIPv6 ? 'ipv6_a' : 'ipv4_a';
  const viewB = isIPv6 ? 'ipv6_b' : 'ipv4_b';
  const safe = prefix.replace(/'/g, "''");

  setStatus(`Comparing ${prefix} between ${dateA} and ${dateB}…`);
  try {
    const resA = await conn.query(`SELECT * FROM ${viewA} WHERE prefix = '${safe}'`);
    const resB = await conn.query(`SELECT * FROM ${viewB} WHERE prefix = '${safe}'`);
    const rowsA = resA.toArray().map(r => r.toJSON());
    const rowsB = resB.toArray().map(r => r.toJSON());

    if (!rowsA.length && !rowsB.length) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">Prefix <strong>${escHtml(prefix)}</strong> not found in either date.</div>`;
      return;
    }

    const rowA = rowsA[0] ?? null;
    const rowB = rowsB[0] ?? null;
    const locsA = rowA ? parseLocations(rowA.locations) : [];
    const locsB = rowB ? parseLocations(rowB.locations) : [];

    setStatus('');
    resultsEl.innerHTML = renderPrefixCompare(prefix, rowA, rowB, locsA, locsB, isIPv6, dateA, dateB);
    initCompareMap(locsA, locsB);
  } catch (err) { setStatus('Compare error: ' + (err.message ?? ''), true); }
}

// ── Compare rendering ─────────────────────────────────────────────────────
function renderCompareResults(both, onlyA, onlyB, dateA, dateB) {
  function prefixTable(rows) {
    if (!rows.length) return '<p class="loc-note">None.</p>';
    const id = `pg${++pgCounter}`;
    const pageSize = 10;
    const renderRows = (slice) => slice.map(r => {
      const cc = r.conf === 'high' ? 'conf-col-high' : r.conf === 'medium' ? 'conf-col-medium' : 'conf-col-low';
      const cl = r.conf === 'high' ? 'High' : r.conf === 'medium' ? 'Medium' : 'Low';
      return `<tr class="pl-row cmp-click" data-prefix="${escHtml(r.prefix)}">
        <td><span class="ver-badge">${r.ver}</span></td>
        <td><span class="prefix-link">${escHtml(r.prefix)}</span></td>
        <td><span class="conf-col ${cc}">${cl}</span></td>
      </tr>`;
    }).join('');
    // Count by confidence and protocol for filter buttons
    let cH = 0, cM = 0, cL = 0, nV4 = 0, nV6 = 0;
    for (const r of rows) {
      if (r.conf === 'high') cH++; else if (r.conf === 'medium') cM++; else cL++;
      if (r.ver === 'v4') nV4++; else nV6++;
    }
    pgStore[id] = { allRows: rows, rows, page: 0, pageSize, renderRows, confFilter: 'all', verFilter: 'all' };
    const initialBody = renderRows(rows.slice(0, pageSize));
    const controls = pgControlsHtml(id, rows.length, 0, pageSize);
    const filterBar = `<div class="conf-filter">
      <span class="conf-filter-lbl">Protocol:</span>
      <button class="ver-filter-btn active" data-ver="all">All (${fmtN(rows.length)})</button>
      <button class="ver-filter-btn" data-ver="v4">IPv4 (${fmtN(nV4)})</button>
      <button class="ver-filter-btn" data-ver="v6">IPv6 (${fmtN(nV6)})</button>
      <span class="pg-sep">\u00B7</span>
      <span class="conf-filter-lbl">Confidence:</span>
      <button class="conf-filter-btn active" data-conf="all">All</button>
      <button class="conf-filter-btn" data-conf="high">High (${fmtN(cH)})</button>
      <button class="conf-filter-btn" data-conf="medium">Med (${fmtN(cM)})</button>
      <button class="conf-filter-btn" data-conf="low">Low (${fmtN(cL)})</button>
      <span class="pg-sep">\u00B7</span>
      <span class="conf-filter-count">${fmtN(rows.length)} shown</span>
    </div>`;
    return `<div data-pg-id="${id}">
      ${filterBar}
      <div class="pg-controls-slot">${controls}</div>
      <table class="prefix-list"><thead><tr><th></th><th>Prefix</th><th>Confidence</th></tr></thead><tbody>${initialBody}</tbody></table>
      <div class="pg-controls-slot">${controls}</div>
    </div>`;
  }

  // Split by protocol
  const split = (arr) => {
    const v4 = arr.filter(r => r.ver === 'v4');
    const v6 = arr.filter(r => r.ver === 'v6');
    return { v4, v6 };
  };
  const bS = split(both), aS = split(onlyA), bSo = split(onlyB);

  // Confidence breakdown
  const confBreakdown = (arr) => {
    let high = 0, medium = 0, low = 0;
    for (const r of arr) {
      if (r.conf === 'high') high++;
      else if (r.conf === 'medium') medium++;
      else low++;
    }
    return { high, medium, low };
  };

  function confBadges(cb) {
    return `<span class="confidence confidence-high" style="font-size:0.7rem;padding:0.1rem 0.35rem">${fmtN(cb.high)}</span> `
         + `<span class="confidence confidence-medium" style="font-size:0.7rem;padding:0.1rem 0.35rem">${fmtN(cb.medium)}</span> `
         + `<span class="confidence confidence-low" style="font-size:0.7rem;padding:0.1rem 0.35rem">${fmtN(cb.low)}</span>`;
  }

  function statsTable(label, v4arr, v6arr) {
    const v4c = confBreakdown(v4arr), v6c = confBreakdown(v6arr);
    return `<tr>
      <td>${label}</td>
      <td class="count">${fmtN(v4arr.length + v6arr.length)}</td>
      <td class="count">${fmtN(v4arr.length)}</td><td>${confBadges(v4c)}</td>
      <td class="count">${fmtN(v6arr.length)}</td><td>${confBadges(v6c)}</td>
    </tr>`;
  }

  return `
    <div class="card">
      <div class="card-title">Census comparison: ${escHtml(dateA)} vs ${escHtml(dateB)}</div>
      <table style="margin-bottom:1rem;font-size:0.82rem">
        <thead><tr>
          <th></th><th>Total</th>
          <th>IPv4</th><th>Confidence</th>
          <th>IPv6</th><th>Confidence</th>
        </tr></thead>
        <tbody>
          ${statsTable(`<span class="cmp-dot cmp-dot-both"></span> In both`, bS.v4, bS.v6)}
          ${statsTable(`<span class="cmp-dot cmp-dot-onlyA"></span> Only ${escHtml(dateA)}`, aS.v4, aS.v6)}
          ${statsTable(`<span class="cmp-dot cmp-dot-onlyB"></span> Only ${escHtml(dateB)}`, bSo.v4, bSo.v6)}
        </tbody>
      </table>
      <p class="loc-note">Confidence: <span class="confidence confidence-high" style="font-size:0.68rem;padding:0.05rem 0.3rem">high</span>
        <span class="confidence confidence-medium" style="font-size:0.68rem;padding:0.05rem 0.3rem">medium</span>
        <span class="confidence confidence-low" style="font-size:0.68rem;padding:0.05rem 0.3rem">low</span>.
        Enter a prefix in the search box and click Compare to see per-prefix differences.</p>
    </div>

    <div class="card">
      <div class="cmp-section-title"><span class="cmp-dot cmp-dot-onlyA"></span> Only in ${escHtml(dateA)} (${fmtN(onlyA.length)})</div>
      ${prefixTable(onlyA)}
    </div>

    <div class="card">
      <div class="cmp-section-title"><span class="cmp-dot cmp-dot-onlyB"></span> Only in ${escHtml(dateB)} (${fmtN(onlyB.length)})</div>
      ${prefixTable(onlyB)}
    </div>

    <div class="card">
      <div class="cmp-section-title"><span class="cmp-dot cmp-dot-both"></span> In both dates (${fmtN(both.length)})</div>
      ${prefixTable(both)}
    </div>`;
}

function renderPrefixCompare(prefix, rowA, rowB, locsA, locsB, isIPv6, dateA, dateB) {
  const suffix = isIPv6 ? 'v6' : 'v4';
  const n = v => Number(v ?? 0);
  const fields = [
    { key: `AB_ICMP${suffix}`,  label: 'AB ICMP' },
    { key: `AB_TCP${suffix}`,   label: 'AB TCP' },
    { key: `AB_DNS${suffix}`,   label: 'AB DNS' },
    { key: `GCD_ICMP${suffix}`, label: 'GCD ICMP' },
    { key: `GCD_TCP${suffix}`,  label: 'GCD TCP' },
  ];

  function diffCell(valA, valB) {
    const d = valB - valA;
    if (d > 0) return `<span class="cmp-val-up">+${fmtN(d)}</span>`;
    if (d < 0) return `<span class="cmp-val-down">\u2212${fmtN(Math.abs(d))}</span>`;
    return `<span class="cmp-val-same">—</span>`;
  }

  const detRows = fields.map(f => {
    const vA = rowA ? n(rowA[f.key]) : 0;
    const vB = rowB ? n(rowB[f.key]) : 0;
    return `<tr>
      <td>${f.label}</td>
      <td class="${vA ? 'count' : 'count-zero'}">${fmtN(vA)}</td>
      <td class="${vB ? 'count' : 'count-zero'}">${fmtN(vB)}</td>
      <td>${diffCell(vA, vB)}</td>
    </tr>`;
  }).join('');

  const presenceNote = !rowA
    ? `<p class="loc-note" style="color:#3fb950">This prefix only appears in <strong>${escHtml(dateB)}</strong>.</p>`
    : !rowB
    ? `<p class="loc-note" style="color:#d29922">This prefix only appears in <strong>${escHtml(dateA)}</strong>.</p>`
    : '';

  // Location sets
  const locKeyA = new Set(locsA.map(l => `${l.lat},${l.lon}`));
  const locKeyB = new Set(locsB.map(l => `${l.lat},${l.lon}`));
  const locBoth = locsA.filter(l => locKeyB.has(`${l.lat},${l.lon}`));
  const locOnlyA = locsA.filter(l => !locKeyB.has(`${l.lat},${l.lon}`));
  const locOnlyB = locsB.filter(l => !locKeyA.has(`${l.lat},${l.lon}`));

  function locGrid(locs, dotClass) {
    if (!locs.length) return '<p class="loc-note">None.</p>';
    return `<div class="loc-grid">${locs.map(loc => `
      <div class="loc-card">
        <div class="loc-iata"><span class="cmp-dot ${dotClass}" style="margin-right:0.3rem"></span>${escHtml(loc.id ?? '?')}</div>
        <div class="loc-city">${escHtml(loc.city ?? '')}${loc.country_code ? `, ${escHtml(loc.country_code)}` : ''}</div>
        <div class="loc-coords">${fmtCoord(loc.lat)}, ${fmtCoord(loc.lon)}</div>
      </div>
    `).join('')}</div>`;
  }

  return `
    <div class="card">
      <div class="card-title">Prefix comparison: ${escHtml(dateA)} vs ${escHtml(dateB)}</div>
      <div class="prefix-badge">${escHtml(prefix)}</div>
      ${presenceNote}
      <table class="cmp-diff-table">
        <thead><tr><th>Method</th><th>${escHtml(dateA)}</th><th>${escHtml(dateB)}</th><th>Δ</th></tr></thead>
        <tbody>${detRows}</tbody>
      </table>
    </div>

    <div class="card">
      <div class="card-title">Location comparison</div>
      <p class="loc-note">
        <span class="cmp-dot cmp-dot-both"></span> Both (${fmtN(locBoth.length)}) &nbsp;
        <span class="cmp-dot cmp-dot-onlyA"></span> Only ${escHtml(dateA)} (${fmtN(locOnlyA.length)}) &nbsp;
        <span class="cmp-dot cmp-dot-onlyB"></span> Only ${escHtml(dateB)} (${fmtN(locOnlyB.length)})
      </p>
      <div id="loc-map"></div>
      ${locBoth.length ? `<div class="cmp-section-title" style="margin-top:0.75rem"><span class="cmp-dot cmp-dot-both"></span> Both dates (${fmtN(locBoth.length)})</div>${locGrid(locBoth, 'cmp-dot-both')}` : ''}
      ${locOnlyA.length ? `<div class="cmp-section-title" style="margin-top:0.75rem"><span class="cmp-dot cmp-dot-onlyA"></span> Only ${escHtml(dateA)} (${fmtN(locOnlyA.length)})</div>${locGrid(locOnlyA, 'cmp-dot-onlyA')}` : ''}
      ${locOnlyB.length ? `<div class="cmp-section-title" style="margin-top:0.75rem"><span class="cmp-dot cmp-dot-onlyB"></span> Only ${escHtml(dateB)} (${fmtN(locOnlyB.length)})</div>${locGrid(locOnlyB, 'cmp-dot-onlyB')}` : ''}
    </div>`;
}

function initCompareMap(locsA, locsB) {
  const el = document.getElementById('loc-map');
  if (!el) return;
  if (leafletMap) { leafletMap.remove(); leafletMap = null; }

  leafletMap = L.map('loc-map', {
    zoomControl: true,
    worldCopyJump: false,
    maxBounds: [[-58, -180], [85, 180]],
    maxBoundsViscosity: 1.0,
  });

  L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd', maxZoom: 19, noWrap: true,
  }).addTo(leafletMap);

  const locKeyA = new Set(locsA.map(l => `${l.lat},${l.lon}`));
  const locKeyB = new Set(locsB.map(l => `${l.lat},${l.lon}`));

  const markers = [];

  function addMarkers(locs, fillColor, borderColor, labelSuffix) {
    for (const loc of locs) {
      if (loc.lat == null || loc.lon == null) continue;
      const name = (!loc.city || loc.id === 'NoCity')
        ? `Unknown (${fmtCoord(loc.lat)}, ${fmtCoord(loc.lon)})`
        : `${loc.city}, ${loc.country_code} — ${loc.id}`;
      markers.push(
        L.circleMarker([loc.lat, loc.lon], {
          radius: 6, fillColor, color: borderColor, weight: 1.5, opacity: 1, fillOpacity: 0.85,
        }).bindTooltip(`${name} ${labelSuffix}`).addTo(leafletMap)
      );
    }
  }

  // Both (blue)
  const both = locsA.filter(l => locKeyB.has(`${l.lat},${l.lon}`));
  addMarkers(both, '#58a6ff', '#1f6feb', '(both)');
  // Only A (green)
  const onlyA = locsA.filter(l => !locKeyB.has(`${l.lat},${l.lon}`));
  addMarkers(onlyA, '#3fb950', '#238636', '(A only)');
  // Only B (orange)
  const onlyB = locsB.filter(l => !locKeyA.has(`${l.lat},${l.lon}`));
  addMarkers(onlyB, '#d29922', '#b45309', '(B only)');

  if (markers.length) {
    leafletMap.fitBounds(L.featureGroup(markers).getBounds().pad(0.15));
  }
}

// ── Chart ──────────────────────────────────────────────────────────────────
const CHART_SERIES = {
  v4: [
    { key: 'ab_icmp4',  label: 'AB ICMP',  color: '#58a6ff' },
    { key: 'ab_tcp4',   label: 'AB TCP',   color: '#3fb950' },
    { key: 'ab_dns4',   label: 'AB DNS',   color: '#d29922' },
    { key: 'gcd_icmp4', label: 'GCD ICMP', color: '#79c0ff' },
    { key: 'gcd_tcp4',  label: 'GCD TCP',  color: '#7ee787' },
  ],
  v6: [
    { key: 'ab_icmp6',  label: 'AB ICMP',  color: '#bc8cff' },
    { key: 'ab_tcp6',   label: 'AB TCP',   color: '#ffa657' },
    { key: 'ab_dns6',   label: 'AB DNS',   color: '#ff9a8e' },
    { key: 'gcd_icmp6', label: 'GCD ICMP', color: '#d2a8ff' },
    { key: 'gcd_tcp6',  label: 'GCD TCP',  color: '#ffca6e' },
  ],
};

let asStatsReady  = false;
let asTableReady  = false;
let chartData     = null;
let chartVer      = 'v4';
let chartHidden   = new Set();
let chartHoverIdx = null;

async function initASStats() {
  if (!conn) return;
  try {
    const [r4, r6, rcomb, rboth, rm4, rm6] = await Promise.all([
      conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv4
        WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1)`),
      conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv6
        WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1)`),
      conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv4 WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1
        UNION
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv6 WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1)`),
      conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv4 WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1
        INTERSECT
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv6 WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1)`),
      conn.query(`SELECT COUNT(*) AS n FROM ipv4 WHERE position('_' IN ASN) > 0`),
      conn.query(`SELECT COUNT(*) AS n FROM ipv6 WHERE position('_' IN ASN) > 0`),
    ]);
    const get = r => Number(r.toArray()[0].toJSON().n ?? 0);
    const n4 = get(r4), n6 = get(r6), ncomb = get(rcomb), nboth = get(rboth);
    const m4 = get(rm4), m6 = get(rm6);
    document.getElementById('as-stat-v4').textContent    = fmtN(n4);
    document.getElementById('as-stat-v6').textContent    = fmtN(n6);
    document.getElementById('as-stat-comb').textContent  = fmtN(ncomb);
    document.getElementById('as-stat-both').textContent  = fmtN(nboth);
    document.getElementById('moas-stat-v4').textContent  = fmtN(m4);
    document.getElementById('moas-stat-v6').textContent  = fmtN(m6);
    document.getElementById('moas-stat-comb').textContent = fmtN(m4 + m6);
    asStatsReady = true;
    if (!resultsEl.innerHTML) asStatsSectionEl.style.display = '';
  } catch (_) { /* stats are optional */ }
}

async function initASTable() {
  if (!conn) return;
  try {
    const result = await conn.query(`
      SELECT asn_val,
        COUNT(DISTINCT CASE WHEN ver = 'v4' THEN prefix END) AS n4,
        COUNT(DISTINCT CASE WHEN ver = 'v6' THEN prefix END) AS n6,
        COUNT(DISTINCT prefix) AS total
      FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val, 'v4' AS ver, prefix
        FROM ipv4 WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1
        UNION ALL
        SELECT unnest(string_split(ASN, '_')) AS asn_val, 'v6' AS ver, prefix
        FROM ipv6 WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1
      )
      GROUP BY asn_val
      ORDER BY total DESC, n4 DESC, n6 DESC
    `);
    const rows = result.toArray().map(r => {
      const j = r.toJSON();
      return { asn: String(j.asn_val), n4: Number(j.n4 ?? 0), n6: Number(j.n6 ?? 0), total: Number(j.total ?? 0) };
    });
    if (!rows.length) return;

    const id = `pg${++pgCounter}`;
    const pageSize = 25;
    const renderRows = (slice) => slice.map(row => `
      <tr class="as-row pl-row" data-asn="${escHtml(row.asn)}">
        <td><span class="prefix-link">AS${escHtml(row.asn)}</span></td>
        <td class="${row.n4 ? 'count' : 'count-zero'}">${row.n4 ? fmtN(row.n4) : '\u2014'}</td>
        <td class="${row.n6 ? 'count' : 'count-zero'}">${row.n6 ? fmtN(row.n6) : '\u2014'}</td>
        <td class="count">${fmtN(row.total)}</td>
      </tr>`).join('');

    pgStore[id] = { allRows: rows, rows, page: 0, pageSize, renderRows, sortCol: 'total', sortAsc: false };
    const initialBody = renderRows(rows.slice(0, pageSize));
    const controls = pgControlsHtml(id, rows.length, 0, pageSize);
    document.getElementById('as-table-body').innerHTML = `
      <div data-pg-id="${id}">
        <div class="pg-controls-slot">${controls}</div>
        <table class="prefix-list">
          <thead><tr>
            <th data-sort="asn">ASN<span class="sort-ind"></span></th>
            <th data-sort="n4">IPv4<span class="sort-ind"></span></th>
            <th data-sort="n6">IPv6<span class="sort-ind"></span></th>
            <th data-sort="total" class="sort-active">Total<span class="sort-ind">\u2193</span></th>
          </tr></thead>
          <tbody>${initialBody}</tbody>
        </table>
        <div class="pg-controls-slot">${controls}</div>
      </div>`;
    asTableReady = true;
    if (!resultsEl.innerHTML) asTableSectionEl.style.display = '';
  } catch (_) { /* optional */ }
}

async function initChart() {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const res = await fetch(baseUrl + `stats-history.json?v=${today}`);
    if (!res.ok) return;
    chartData = await res.json();
    document.getElementById('chart-section').style.display = '';
    buildLegend();
    drawChart();
    setupChartEvents();
  } catch (_) { /* chart is optional */ }
}

function buildLegend() {
  const legendEl = document.getElementById('chart-legend');
  legendEl.innerHTML = CHART_SERIES[chartVer].map(s => `
    <button class="legend-item${chartHidden.has(s.key) ? ' hidden' : ''}" data-key="${s.key}">
      <span class="legend-dot" style="background:${s.color}"></span>${s.label}
    </button>
  `).join('');
  legendEl.querySelectorAll('.legend-item').forEach(btn => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.key;
      chartHidden.has(key) ? chartHidden.delete(key) : chartHidden.add(key);
      btn.classList.toggle('hidden');
      drawChart();
    });
  });
}

function drawChart() {
  const canvas = document.getElementById('chart-canvas');
  if (!canvas || !chartData) return;

  const dpr  = window.devicePixelRatio || 1;
  const W    = canvas.clientWidth;
  const H    = canvas.clientHeight;
  canvas.width  = W * dpr;
  canvas.height = H * dpr;

  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);

  const PAD   = { top: 12, right: 14, bottom: 40, left: 58 };
  const plotW = W - PAD.left - PAD.right;
  const plotH = H - PAD.top  - PAD.bottom;
  const series = CHART_SERIES[chartVer];
  const dates  = chartData.dates;
  const n      = dates.length;

  // Max of visible series
  let maxVal = 0;
  for (const s of series) {
    if (chartHidden.has(s.key)) continue;
    for (const v of chartData[s.key]) if (v != null && v > maxVal) maxVal = v;
  }

  const yTicks = niceYTicks(maxVal, 5);
  const yMax   = yTicks.length ? yTicks[yTicks.length - 1] : 1;

  const xPos = i => PAD.left + (n > 1 ? (i / (n - 1)) * plotW : plotW / 2);
  const yPos = v => PAD.top  + plotH * (1 - v / yMax);

  ctx.clearRect(0, 0, W, H);

  // Gridlines + Y labels
  ctx.font = `11px ui-monospace, "Cascadia Code", monospace`;
  for (const tick of yTicks) {
    const y = yPos(tick);
    ctx.strokeStyle = '#21262d';
    ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(PAD.left, y); ctx.lineTo(W - PAD.right, y); ctx.stroke();
    ctx.fillStyle = '#8b949e';
    ctx.textAlign = 'right'; ctx.textBaseline = 'middle';
    ctx.fillText(fmtK(tick), PAD.left - 6, y);
  }

  // X axis ticks + labels (~every 120 px)
  const xStep = Math.max(1, Math.round(120 / (plotW / n)));
  ctx.fillStyle = '#8b949e';
  ctx.textAlign = 'center'; ctx.textBaseline = 'top';
  for (let i = 0; i < n; i += xStep) {
    const x = xPos(i);
    ctx.strokeStyle = '#30363d'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, PAD.top + plotH); ctx.lineTo(x, PAD.top + plotH + 4); ctx.stroke();
    ctx.fillText(dates[i].slice(0, 7), x, PAD.top + plotH + 8);
  }

  // Series lines
  for (const s of series) {
    if (chartHidden.has(s.key)) continue;
    const vals = chartData[s.key];
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    let moved = false;
    for (let i = 0; i < n; i++) {
      if (vals[i] == null) { moved = false; continue; }
      if (!moved) { ctx.moveTo(xPos(i), yPos(vals[i])); moved = true; }
      else          ctx.lineTo(xPos(i), yPos(vals[i]));
    }
    ctx.stroke();
  }

  // Hover crosshair + tooltip
  if (chartHoverIdx != null) {
    const hi = chartHoverIdx;
    const x  = xPos(hi);

    ctx.strokeStyle = '#484f58';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 3]);
    ctx.beginPath(); ctx.moveTo(x, PAD.top); ctx.lineTo(x, PAD.top + plotH); ctx.stroke();
    ctx.setLineDash([]);

    const visLines = series
      .filter(s => !chartHidden.has(s.key) && chartData[s.key][hi] != null)
      .map(s => ({ s, v: chartData[s.key][hi] }))
      .sort((a, b) => b.v - a.v);

    const lh = 15, tp = 7;
    const tw = 142, th = tp * 2 + 14 + visLines.length * lh;
    let tx = x + 12, ty = PAD.top + 8;
    if (tx + tw > W - PAD.right) tx = x - tw - 12;

    // Tooltip background (manual roundRect for browser compat)
    ctx.fillStyle = '#0d1117'; ctx.strokeStyle = '#30363d'; ctx.lineWidth = 1;
    const r = 4;
    ctx.beginPath();
    ctx.moveTo(tx + r, ty);
    ctx.lineTo(tx + tw - r, ty);   ctx.arcTo(tx + tw, ty,      tx + tw, ty + r,      r);
    ctx.lineTo(tx + tw, ty + th - r); ctx.arcTo(tx + tw, ty + th, tx + tw - r, ty + th, r);
    ctx.lineTo(tx + r, ty + th);   ctx.arcTo(tx,      ty + th, tx,      ty + th - r, r);
    ctx.lineTo(tx, ty + r);        ctx.arcTo(tx,      ty,      tx + r,  ty,           r);
    ctx.closePath(); ctx.fill(); ctx.stroke();

    ctx.font = `10px ui-monospace, "Cascadia Code", monospace`;
    ctx.textAlign = 'left'; ctx.textBaseline = 'top';
    ctx.fillStyle = '#8b949e';
    ctx.fillText(dates[hi], tx + tp, ty + tp);

    for (let i = 0; i < visLines.length; i++) {
      const { s, v } = visLines[i];
      const ry = ty + tp + 14 + i * lh;
      ctx.fillStyle = s.color;
      ctx.fillRect(tx + tp, ry + 2, 8, 8);
      ctx.fillStyle = '#e6edf3';
      ctx.fillText(`${s.label}: ${v.toLocaleString()}`, tx + tp + 12, ry);
    }
  }
}

function niceYTicks(max, count) {
  if (max === 0) return [0];
  const rough = max / count;
  const exp   = Math.floor(Math.log10(rough));
  const base  = Math.pow(10, exp);
  const step  = [1, 2, 2.5, 5, 10].find(f => f * base >= rough) * base;
  const result = [];
  for (let v = step; v <= max * 1.05 + step * 0.01; v += step)
    result.push(Math.round(v * 1e6) / 1e6);
  return result;
}

function fmtK(v) {
  if (v >= 1000) return ((v / 1000) % 1 === 0 ? v / 1000 : (v / 1000).toFixed(1)) + 'k';
  return String(v);
}

function setupChartEvents() {
  const canvas  = document.getElementById('chart-canvas');
  const tabBtns = document.querySelectorAll('.chart-tab');
  const n       = chartData.dates.length;
  const PL = 58, PR = 14;

  canvas.addEventListener('mousemove', e => {
    const rect  = canvas.getBoundingClientRect();
    const relX  = e.clientX - rect.left - PL;
    const plotW = rect.width - PL - PR;
    chartHoverIdx = Math.max(0, Math.min(n - 1, Math.round((relX / plotW) * (n - 1))));
    drawChart();
  });

  canvas.addEventListener('mouseleave', () => { chartHoverIdx = null; drawChart(); });

  tabBtns.forEach(btn => btn.addEventListener('click', () => {
    chartVer = btn.dataset.ver;
    chartHidden.clear();
    tabBtns.forEach(b => b.classList.toggle('active', b === btn));
    buildLegend();
    drawChart();
  }));

  document.getElementById('chart-fs-btn').addEventListener('click', () => {
    const section = document.getElementById('chart-section');
    if (!document.fullscreenElement) section.requestFullscreen();
    else document.exitFullscreen();
  });

  document.addEventListener('fullscreenchange', () => {
    const canvas = document.getElementById('chart-canvas');
    if (document.fullscreenElement) {
      canvas.style.height = '0';
    } else {
      canvas.style.height = '260px';
    }
    setTimeout(drawChart, 50);
  });

  let resizeTimer;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(drawChart, 100);
  });
}

initChart();
