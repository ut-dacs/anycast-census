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
const btnCmpA     = document.getElementById('btn-cmp-a');
const btnCmpB     = document.getElementById('btn-cmp-b');
let isCompareMode = false;

// Date picker UI elements (declared here so syncDateUI() can reference them
// even when called from inside the top-level await init block)
const btnLatest        = document.getElementById('btn-latest');
const btnPickDate      = document.getElementById('btn-pick-date');
const btnPrevDay       = document.getElementById('btn-prev-day');
const btnNextDay       = document.getElementById('btn-next-day');
const dateNavGroup     = document.getElementById('date-nav-group');
const dateLabelDisplay = document.getElementById('date-label-display');

// ── ASN autocomplete state ──────────────────────────────────────────────────
let _asnNames = {};  // { asn: name } mapping loaded from asn-names.json
const autocompleteDropdown = document.getElementById('autocomplete-dropdown');
const autocompleteList = document.getElementById('autocomplete-list');
let autocompleteHighlightedIndex = -1;

// ── Prefix history state ────────────────────────────────────────────────────
// Cached history data (declared at top to avoid temporal dead zone)
let _histDates = [];      // string[] of ISO dates from dates.txt
let _histMeta  = null;    // { num_days, row_size } from binary file header
const _cacheBuster = `?v=${Date.now()}&r=${Math.random()}`;  // Cache-bust on page load to avoid stale files

// ── Pagination state ────────────────────────────────────────────────────────
const pgStore = {};
let pgCounter = 0;
let currentLookupCtx  = null;  // { viewKey, dateLabel }
let currentCompareCtx = null;  // { dateA, dateB }

function setStatus(msg, isError = false) {
  statusEl.textContent = msg;
  statusEl.className = 'status' +
    (isError ? ' error' : msg ? ' loading' : '');
}

// Configuration and base URL setup
let appConfig = { dataSource: '' };
let baseUrl = '';

const initConfig = async () => {
  // Load config.json to determine data source
  // Allows Docker to override to point to GitHub Pages instead of local files
  try {
    const resp = await fetch('config.json');
    if (resp.ok) {
      appConfig = await resp.json();
    }
  } catch (e) {
    // config.json not found; use defaults
  }

  // Set baseUrl based on config
  if (appConfig.dataSource) {
    // Explicit data source URL (e.g., GitHub Pages for Docker)
    baseUrl = appConfig.dataSource.endsWith('/')
      ? appConfig.dataSource
      : appConfig.dataSource + '/';
  } else {
    // Default: relative URLs (works locally and on GitHub Pages)
    const u = new URL(window.location.href);
    baseUrl = u.origin + u.pathname.replace(/[^/]*$/, '');
  }
};

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

// ── Bogon / reserved address detection ────────────────────────────────────
function getIPv4BogonInfo(ip) {
  const parts = ip.split('.').map(Number);
  if (parts.length !== 4 || parts.some(p => isNaN(p) || p < 0 || p > 255)) return null;
  const [a, b, c] = parts;
  if (a === 0)                               return { label: '"This" network',             rfc: 'RFC 1122' };
  if (a === 10)                              return { label: 'private',                    rfc: 'RFC 1918' };
  if (a === 100 && b >= 64 && b <= 127)      return { label: 'shared address space',       rfc: 'RFC 6598' };
  if (a === 127)                             return { label: 'loopback',                   rfc: 'RFC 1122' };
  if (a === 169 && b === 254)                return { label: 'link-local',                 rfc: 'RFC 3927' };
  if (a === 172 && b >= 16 && b <= 31)       return { label: 'private',                    rfc: 'RFC 1918' };
  if (a === 192 && b === 0 && c === 0)       return { label: 'IETF protocol assignments',  rfc: 'RFC 6890' };
  if (a === 192 && b === 0 && c === 2)       return { label: 'documentation (TEST-NET-1)', rfc: 'RFC 5737' };
  if (a === 192 && b === 168)                return { label: 'private',                    rfc: 'RFC 1918' };
  if (a === 198 && (b === 18 || b === 19))   return { label: 'benchmarking',               rfc: 'RFC 2544' };
  if (a === 198 && b === 51 && c === 100)    return { label: 'documentation (TEST-NET-2)', rfc: 'RFC 5737' };
  if (a === 203 && b === 0 && c === 113)     return { label: 'documentation (TEST-NET-3)', rfc: 'RFC 5737' };
  if (a >= 224 && a <= 239)                  return { label: 'multicast',                  rfc: 'RFC 3171' };
  if (a >= 240)                              return { label: 'reserved',                   rfc: 'RFC 1112' };
  return null;
}

function getIPv6BogonInfo(ip) {
  let groups;
  try { groups = expandIPv6(ip); } catch { return null; }
  const g = groups.map(h => parseInt(h, 16));
  const [g0, g1, g2, g3] = g;
  // ::/128  unspecified
  if (g.every(x => x === 0))                                          return { label: 'unspecified address',     rfc: 'RFC 4291' };
  // ::1/128  loopback
  if (g.slice(0, 7).every(x => x === 0) && g[7] === 1)               return { label: 'loopback',                rfc: 'RFC 4291' };
  // ::ffff:0:0/96  IPv4-mapped
  if (g.slice(0, 5).every(x => x === 0) && g[5] === 0xffff)          return { label: 'IPv4-mapped',             rfc: 'RFC 4291' };
  // 64:ff9b:1::/48  (check before 64:ff9b::/96)
  if (g0 === 0x64 && g1 === 0xff9b && g2 === 1)                       return { label: 'IPv4/IPv6 translation',   rfc: 'RFC 8215' };
  // 64:ff9b::/96  IPv4/IPv6 translation
  if (g0 === 0x64 && g1 === 0xff9b && g2 === 0 && g3 === 0)          return { label: 'IPv4/IPv6 translation',   rfc: 'RFC 6052' };
  // 100::/64  discard
  if (g0 === 0x100 && g1 === 0 && g2 === 0 && g3 === 0)              return { label: 'discard',                 rfc: 'RFC 6666' };
  // 2001:2::/48  benchmarking (check before 2001::/32 Teredo)
  if (g0 === 0x2001 && g1 === 2 && g2 === 0)                         return { label: 'benchmarking',            rfc: 'RFC 5180' };
  // 2001:db8::/32  documentation
  if (g0 === 0x2001 && g1 === 0x0db8)                                 return { label: 'documentation',           rfc: 'RFC 3849' };
  // 2001::/32  Teredo
  if (g0 === 0x2001 && g1 === 0)                                      return { label: 'Teredo tunneling',        rfc: 'RFC 4380' };
  // 2002::/16  6to4
  if (g0 === 0x2002)                                                  return { label: '6to4',                    rfc: 'RFC 3056' };
  // fc00::/7  unique local (fc00–fdff)
  if (g0 >= 0xfc00 && g0 <= 0xfdff)                                   return { label: 'unique local (ULA)',      rfc: 'RFC 4193' };
  // fe80::/10  link-local (fe80–febf)
  if (g0 >= 0xfe80 && g0 <= 0xfebf)                                   return { label: 'link-local',              rfc: 'RFC 4291' };
  // ff00::/8  multicast
  if (g0 >= 0xff00)                                                   return { label: 'multicast',               rfc: 'RFC 4291' };
  return null;
}

// Initialise DuckDB
let db, conn;
let currentViewKey = null; // tracks which parquet files are registered as views
let leafletMap  = null;
let locMarkers  = []; // circleMarker|null per locations[i]
let asStatsReady  = false;
let asTableReady  = false;
let chartData     = null;

try {
  // Load config first (determines data source URL)
  await initConfig();

  // Load ASN names for autocomplete
  try {
    const resp = await fetch('asn-names.json');
    if (resp.ok) {
      _asnNames = await resp.json();
    }
  } catch (e) {
    console.warn('Could not load ASN names:', e);
  }

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

  setStatus('');
  searchBtn.disabled = false;
  examplesEl.style.display = '';

  // Auto-run lookup / compare if URL has query params
  const initParams = new URLSearchParams(window.location.search);
  const initMode = initParams.get('mode');
  const initQ    = initParams.get('q');
  const initDate = initParams.get('date');

  // If on home page with a date, pre-set the picker before loading stats
  if (!initQ && !initMode && initDate) {
    whenDate.checked   = true;
    whenLatest.checked = false;
    dateInput.value    = initDate;
    syncDateUI(initDate);
  }

  const homeViewKey = (!initQ && !initMode && initDate) ? initDate.replace(/-/g, '/') : 'latest';
  if (!initQ && !initMode) refreshHomeStats(homeViewKey);

  if (initMode === 'compare') {
    const dA = initParams.get('dateA');
    const dB = initParams.get('dateB');
    if (dA && dB) {
      isCompareMode = true;
      modeCompare.classList.add('active');
      modeLookup.classList.remove('active');
      lookupRow.style.display = 'none';
      compareRow.style.display = '';
      cmpDateA.value = dA; syncCmpBtn(btnCmpA, cmpDateA);
      cmpDateB.value = dB; syncCmpBtn(btnCmpB, cmpDateB);
      if (initQ) inputEl.value = initQ;
      await runCompare({ updateUrl: false });
    }
  } else if (initQ) {
    inputEl.value = initQ;
    if (initDate) {
      whenDate.checked   = true;
      whenLatest.checked = false;
      dateInput.value    = initDate;
      syncDateUI(initDate);
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
      cmpDateA.value = dateA; syncCmpBtn(btnCmpA, cmpDateA);
      cmpDateB.value = dateB; syncCmpBtn(btnCmpB, cmpDateB);
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
      syncDateUI(date);
    } else {
      whenLatest.checked = true;
      whenDate.checked   = false;
      syncDateUI(null);
    }
    await lookup(q, { updateUrl: false });
  } else {
    ensureLookupMode();
    inputEl.value = '';
    resultsEl.innerHTML = '';
    resetPg();
    setStatus('');
    const date = params.get('date');
    if (date) {
      whenDate.checked   = true;
      whenLatest.checked = false;
      dateInput.value    = date;
      syncDateUI(date);
    } else {
      whenLatest.checked = true;
      whenDate.checked   = false;
      syncDateUI(null);
    }
    if (chartData) chartSection.style.display = '';
    refreshHomeStats(date ? date.replace(/-/g, '/') : 'latest');
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
  if (!input) {
    // Empty search — go back to home view
    resultsEl.innerHTML = '';
    resetPg();
    currentLookupCtx  = null;
    currentCompareCtx = null;
    chartSection.style.display = chartData ? '' : 'none';
    asStatsSectionEl.style.display = 'none';
    asTableSectionEl.style.display = 'none';
    document.getElementById('showcase-section').style.display = 'none';
    setStatus('');
    const vk = selectedViewKey();
    if (updateUrl) history.replaceState(null, '', vk === 'latest' ? '.' : '?date=' + dateInput.value);
    refreshHomeStats(vk);
    return;
  }

  const viewKey   = selectedViewKey();
  const dateLabel = viewKey === 'latest' ? 'latest' : viewKey.replace(/\//g, '-');
  resultsEl.innerHTML = '';
  resetPg(); currentCompareCtx = null;
  chartSection.style.display = 'none';
  asStatsSectionEl.style.display = 'none';
  asTableSectionEl.style.display = 'none';
  document.getElementById('showcase-section').style.display = 'none';

  if (updateUrl) {
    const params = new URLSearchParams({ q: input });
    if (viewKey !== 'latest') params.set('date', dateLabel);
    history.pushState({ q: input, date: viewKey !== 'latest' ? dateLabel : null }, '',
      '?' + params.toString());
  }

  // AS number: "AS13335" or bare "13335"
  const asnMatch = input.match(/^(?:AS)?(\d{1,10})$/i);
  if (asnMatch) { await lookupASN(asnMatch[1], viewKey, dateLabel); return; }

  // Root zone: bare "."
  if (input === '.') {
    await lookupDomain('.', viewKey, dateLabel);
    return;
  }

  // TLD: ".nl" or ".com" (leading dot)
  if (/^\.[a-zA-Z]{2,63}$/.test(input)) {
    await lookupDomain(input.slice(1), viewKey, dateLabel);
    return;
  }

  // Domain name (e.g., google.com, ns1.example.org) or bare TLD (e.g., "nl", "com")
  if (/^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z]{2,63}$/.test(input) &&
      !input.includes(':') && !input.includes('/')) {
    await lookupDomain(input, viewKey, dateLabel);
    return;
  }

  // Bogon / reserved address detection
  const bareIPv4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(input);
  const isIPv6Input = input.includes(':');
  // Extract host IP: bare IP → itself; CIDR → strip prefix length
  const hostIPStr = bareIPv4    ? bareIPv4[0]
                  : isIPv6Input ? (input.includes('/') ? input.split('/')[0] : input)
                  : input.includes('/') ? input.split('/')[0]
                  : null;
  if (hostIPStr) {
    const bogon = isIPv6Input ? getIPv6BogonInfo(hostIPStr) : getIPv4BogonInfo(hostIPStr);
    if (bogon) {
      setStatus('');
      resultsEl.innerHTML = `<div class="bogon-notice">
        <strong>${escHtml(input)}</strong> is a <strong>${bogon.label}</strong> address (${bogon.rfc}).
        Reserved/bogon addresses are not routed on the public internet and will not appear in the anycast census.
      </div>`;
      return;
    }
  }

  // Bare IPv4 address (no prefix length) → look up enclosing /24
  if (bareIPv4) {
    const cidr = `${bareIPv4[1]}.${bareIPv4[2]}.${bareIPv4[3]}.0/24`;
    await lookupPrefix(cidr, viewKey, dateLabel);
    return;
  }

  // Bare IPv6 address (contains ':' but no '/') → look up enclosing /48
  if (isIPv6Input && !input.includes('/')) {
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
    if ((!isIPv6Input && len < 24) || (isIPv6Input && len < 48)) {
      await lookupCIDRBlock(input, viewKey, dateLabel);
      return;
    }
  }

  // ── Input validation ────────────────────────────────────────────────────
  // At this point the input must be a CIDR with len >= 24 (v4) or >= 48 (v6).
  // If it doesn't look like one, reject it rather than returning a misleading
  // "prefix not found" from the database.
  const invalidQuery = (() => {
    if (!lenMatch) return true;               // no slash at all
    const ipPart = input.slice(0, input.lastIndexOf('/'));
    const len    = parseInt(lenMatch[1]);
    if (isIPv6Input) return len > 128;        // IPv6 CIDR: only length check (expandIPv6 will catch bad addresses later)
    // IPv4 CIDR: validate each octet and length
    const octets = ipPart.split('.');
    if (octets.length !== 4) return true;
    if (octets.some(o => !/^\d{1,3}$/.test(o) || +o > 255)) return true;
    if (len > 32) return true;
    return false;
  })();
  if (invalidQuery) {
    setStatus('');
    resultsEl.innerHTML = `<div class="not-found">
      <strong>${escHtml(input)}</strong> is not a recognised query format.<br><br>
      Supported inputs:
      <ul style="margin:0.5rem 0 0 1.2rem;line-height:1.8">
        <li>IP address — <code>8.8.8.8</code> or <code>2606:4700::1</code></li>
        <li>CIDR prefix — <code>1.1.1.0/24</code> or <code>2606:4700::/48</code></li>
        <li>Wider block — <code>1.1.0.0/16</code></li>
        <li>AS number — <code>AS13335</code></li>
        <li>Domain — <code>google.com</code></li>
        <li>TLD — <code>.edu</code></li>
        <li>Root zone — <code>.</code></li>
      </ul>
    </div>`;
    return;
  }

  // Default: exact /24 or /48 prefix
  await lookupPrefix(input, viewKey, dateLabel);
}

// ── Domain lookup via Google DoH ─────────────────────────────────────────
// Confidence level for a census row (_ver: 'v4'|'v6')
function confOf(row) {
  if (!row) return null;
  const n = v => Number(v) || 0;
  if (row._ver === 'v4') {
    if (Math.max(n(row.GCD_ICMPv4), n(row.GCD_TCPv4)) > 1) return 'high';
    if (Math.max(n(row.AB_ICMPv4), n(row.AB_TCPv4), n(row.AB_DNSv4)) > 2) return 'medium';
    return 'low';
  } else {
    if (Math.max(n(row.GCD_ICMPv6), n(row.GCD_TCPv6)) > 1) return 'high';
    if (Math.max(n(row.AB_ICMPv6), n(row.AB_TCPv6), n(row.AB_DNSv6)) > 2) return 'medium';
    return 'low';
  }
}

async function dohQuery(name, type) {
  const typeNum = { A: 1, AAAA: 28, MX: 15, NS: 2 }[type];
  try {
    const res = await fetch(
      `https://dns.google/resolve?name=${encodeURIComponent(name)}&type=${encodeURIComponent(type)}`,
      { headers: { Accept: 'application/dns-json' } }
    );
    if (!res.ok) return [];
    const json = await res.json();
    if (json.Status !== 0 || !Array.isArray(json.Answer)) return [];
    return json.Answer.filter(r => r.type === typeNum);
  } catch { return []; }
}

// Resolve all DNS records + IP→prefix mapping for a domain.
// Returns null if no records found; caller handles status messages.
async function resolveDomainPrefixes(domain) {
  const [aRecs, aaaaRecs, mxRecs, nsRecs] = await Promise.all([
    dohQuery(domain, 'A'), dohQuery(domain, 'AAAA'),
    dohQuery(domain, 'MX'), dohQuery(domain, 'NS'),
  ]);
  if (!aRecs.length && !aaaaRecs.length && !mxRecs.length && !nsRecs.length) return null;

  const nsHosts = [...new Set(nsRecs.map(r => (r.data || '').replace(/\.$/, '')).filter(Boolean))];
  const mxEntries = mxRecs.map(r => {
    const p = (r.data || '').split(' ');
    return { priority: parseInt(p[0]) || 0, host: (p[1] || '').replace(/\.$/, '') };
  }).filter(e => e.host);
  const mxHosts = [...new Set(mxEntries.map(e => e.host))];

  setStatus('Resolving nameservers and mail servers\u2026');
  const resolveHost = async host => {
    const [a, aaaa] = await Promise.all([dohQuery(host, 'A'), dohQuery(host, 'AAAA')]);
    return { host, ips4: a.map(r => r.data).filter(Boolean), ips6: aaaa.map(r => r.data).filter(Boolean) };
  };
  const [nsResolved, mxResolved] = await Promise.all([
    Promise.all(nsHosts.map(resolveHost)),
    Promise.all(mxHosts.map(resolveHost)),
  ]);

  const p4Set = new Set(), p6Set = new Set(), ip2prefix = {};
  const addIP4 = ip => {
    const m = /^(\d+)\.(\d+)\.(\d+)\./.exec(ip);
    if (!m) return;
    const p = `${m[1]}.${m[2]}.${m[3]}.0/24`; p4Set.add(p); ip2prefix[ip] = p;
  };
  const addIP6 = ip => {
    try { const p = ipv6ToSlash48(ip); p6Set.add(p); ip2prefix[ip] = p; } catch {}
  };
  aRecs.forEach(r => addIP4(r.data || ''));
  aaaaRecs.forEach(r => addIP6(r.data || ''));
  [...nsResolved, ...mxResolved].forEach(({ ips4, ips6 }) => { ips4.forEach(addIP4); ips6.forEach(addIP6); });

  return { aRecs, aaaaRecs, nsResolved, mxResolved, mxEntries, p4Set, p6Set, ip2prefix };
}

// Shared rendering helpers for domain results
function domainStatusCell(prefix, censusMap) {
  const row = prefix ? censusMap[prefix] : null;
  if (!row) return `<td class="domain-status domain-not-anycast">not in census</td>`;
  const conf = confOf(row);
  const nloc = parseLocations(row.locations).length;
  const locStr = nloc > 0 ? ` &middot; ${nloc} PoP${nloc !== 1 ? 's' : ''}` : '';
  return `<td class="domain-status domain-anycast domain-anycast-${conf}">anycast &middot; ${conf}${locStr}</td>`;
}
function domainClassifyIPs(ips, ip2prefix, censusMap) {
  if (!ips.length) return null;
  const found = ips.filter(ip => ip2prefix[ip] && censusMap[ip2prefix[ip]]).length;
  if (found === 0) return 'unicast';
  if (found === ips.length) return 'anycast';
  return 'mixed';
}
function domainClassifyBadge(cls) {
  return cls ? ` <span class="domain-classify domain-classify-${cls}">${cls}</span>` : '';
}

async function lookupDomain(domain, viewKey, dateLabel) {
  const displayName = domain === '.' ? 'Root zone (.)' : domain;
  const kindLabel   = domain === '.' ? 'root zone' : !domain.includes('.') ? 'TLD' : 'domain';
  try {
    setStatus(`Resolving ${escHtml(displayName)}\u2026`);
    const resolved = await resolveDomainPrefixes(domain);
    if (!resolved) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">No DNS records found for <strong>${escHtml(displayName)}</strong>.</div>`;
      return;
    }
    const { aRecs, aaaaRecs, nsResolved, mxResolved, mxEntries, p4Set, p6Set, ip2prefix } = resolved;

    setStatus(`Checking ${p4Set.size + p6Set.size} prefixes in census\u2026`);
    const censusMap = {};
    try {
      await registerViews(viewKey);
      if (p4Set.size) {
        const list = [...p4Set].map(p => `'${p.replace(/'/g, "''")}'`).join(',');
        (await conn.query(`SELECT * FROM ipv4 WHERE prefix IN (${list})`))
          .toArray().forEach(r => { const j = r.toJSON(); censusMap[j.prefix] = { ...j, _ver: 'v4' }; });
      }
      if (p6Set.size) {
        const list = [...p6Set].map(p => `'${p.replace(/'/g, "''")}'`).join(',');
        (await conn.query(`SELECT * FROM ipv6 WHERE prefix IN (${list})`))
          .toArray().forEach(r => { const j = r.toJSON(); censusMap[j.prefix] = { ...j, _ver: 'v6' }; });
      }
    } catch (e) { console.error('Domain census query failed:', e); }
    setStatus('');

    const statusCell = prefix => domainStatusCell(prefix, censusMap);
    const classifyIPs = ips => domainClassifyIPs(ips, ip2prefix, censusMap);
    const classifyBadge = cls => domainClassifyBadge(cls);
    const ipTableRow = ip => {
      const prefix = ip2prefix[ip];
      return `<tr class="pl-row" data-prefix="${escHtml(prefix || ip)}">
        <td class="domain-ip">${escHtml(ip)}</td>
        <td class="domain-prefix">${prefix ? escHtml(prefix) : '—'}</td>
        ${statusCell(prefix)}
      </tr>`;
    };
    const ipTable = (ips4, ips6) => {
      const rows = [...ips4.map(ipTableRow), ...ips6.map(ipTableRow)];
      return rows.length ? `<table class="domain-table">
        <thead><tr><th>IP address</th><th>Prefix</th><th>Anycast</th></tr></thead>
        <tbody>${rows.join('')}</tbody></table>` : '<p class="loc-note">Could not resolve.</p>';
    };
    const section = (title, body, cls) =>
      `<div class="domain-section"><div class="domain-rec-type">${title}${classifyBadge(cls)}</div>${body}</div>`;

    let html = `<div class="card">
      <div class="card-title">${escHtml(displayName)} <span class="stat-note">— ${kindLabel}</span></div>
      <p class="loc-note">Census: ${escHtml(dateLabel)} &nbsp;&middot;&nbsp; Resolved via Google Public DNS<br>
      <span style="font-size:0.75em;color:#d29922">&#9432; DNS records reflect today&rsquo;s resolution and may differ from the selected census date.</span><br>
      <span style="font-size:0.75em;color:#484f58">Click a row to look up the prefix in detail.</span></p>`;

    if (aRecs.length) {
      const ips = aRecs.map(r => r.data);
      html += section('A records', ipTable(ips, []), classifyIPs(ips));
    }
    if (aaaaRecs.length) {
      const ips = aaaaRecs.map(r => r.data);
      html += section('AAAA records', ipTable([], ips), classifyIPs(ips));
    }
    if (nsResolved.length) {
      const allNsIPs = nsResolved.flatMap(({ ips4, ips6 }) => [...ips4, ...ips6]);
      html += `<div class="domain-section"><div class="domain-rec-type">NS records${classifyBadge(classifyIPs(allNsIPs))}</div>`;
      nsResolved.forEach(({ host, ips4, ips6 }) => {
        const hostIPs = [...ips4, ...ips6];
        html += `<div class="domain-host-label">${escHtml(host)}${hostIPs.length ? classifyBadge(classifyIPs(hostIPs)) : ''}</div>${ipTable(ips4, ips6)}`;
      });
      html += `</div>`;
    }
    if (mxResolved.length) {
      const allMxIPs = mxResolved.flatMap(({ ips4, ips6 }) => [...ips4, ...ips6]);
      html += `<div class="domain-section"><div class="domain-rec-type">MX records${classifyBadge(classifyIPs(allMxIPs))}</div>`;
      const mxByHost = Object.fromEntries(mxResolved.map(r => [r.host, r]));
      const seen = new Set();
      for (const { priority, host } of [...mxEntries].sort((a, b) => a.priority - b.priority)) {
        if (seen.has(host)) continue; seen.add(host);
        const { ips4, ips6 } = mxByHost[host] || { ips4: [], ips6: [] };
        const hostIPs = [...ips4, ...ips6];
        html += `<div class="domain-host-label">${escHtml(host)} <span class="stat-note">(priority ${priority})</span>${hostIPs.length ? classifyBadge(classifyIPs(hostIPs)) : ''}</div>${ipTable(ips4, ips6)}`;
      }
      html += `</div>`;
    }
    html += `</div>`;
    currentLookupCtx = { viewKey, dateLabel };
    resultsEl.innerHTML = html;
  } catch (err) {
    setStatus('');
    console.error('lookupDomain error:', err);
    resultsEl.innerHTML = `<div class="not-found">Error resolving domain: ${escHtml(err.message ?? String(err))}</div>`;
  }
}

async function compareDomain(domain, dateA, dateB) {
  const displayName = domain === '.' ? 'Root zone (.)' : domain;
  const kindLabel   = domain === '.' ? 'root zone' : !domain.includes('.') ? 'TLD' : 'domain';
  try {
    setStatus(`Resolving ${escHtml(displayName)}\u2026`);
    const resolved = await resolveDomainPrefixes(domain);
    if (!resolved) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">No DNS records found for <strong>${escHtml(displayName)}</strong>.</div>`;
      return;
    }
    const { aRecs, aaaaRecs, nsResolved, mxResolved, mxEntries, p4Set, p6Set, ip2prefix } = resolved;

    setStatus(`Checking ${p4Set.size + p6Set.size} prefixes for ${dateA} and ${dateB}\u2026`);
    const censusA = {}, censusB = {};
    try {
      if (p4Set.size) {
        const list = [...p4Set].map(p => `'${p.replace(/'/g, "''")}'`).join(',');
        (await conn.query(`SELECT * FROM ipv4_a WHERE prefix IN (${list})`))
          .toArray().forEach(r => { const j = r.toJSON(); censusA[j.prefix] = { ...j, _ver: 'v4' }; });
        (await conn.query(`SELECT * FROM ipv4_b WHERE prefix IN (${list})`))
          .toArray().forEach(r => { const j = r.toJSON(); censusB[j.prefix] = { ...j, _ver: 'v4' }; });
      }
      if (p6Set.size) {
        const list = [...p6Set].map(p => `'${p.replace(/'/g, "''")}'`).join(',');
        (await conn.query(`SELECT * FROM ipv6_a WHERE prefix IN (${list})`))
          .toArray().forEach(r => { const j = r.toJSON(); censusA[j.prefix] = { ...j, _ver: 'v6' }; });
        (await conn.query(`SELECT * FROM ipv6_b WHERE prefix IN (${list})`))
          .toArray().forEach(r => { const j = r.toJSON(); censusB[j.prefix] = { ...j, _ver: 'v6' }; });
      }
    } catch (e) { console.error('Domain compare census query failed:', e); }
    setStatus('');

    const classifyA  = ips => domainClassifyIPs(ips, ip2prefix, censusA);
    const classifyB  = ips => domainClassifyIPs(ips, ip2prefix, censusB);
    const badge2     = (clsA, clsB) =>
      `${domainClassifyBadge(clsA)}<span class="domain-cmp-sep">\u2192</span>${domainClassifyBadge(clsB)}`;
    const ipTableRow = ip => {
      const prefix = ip2prefix[ip];
      return `<tr class="pl-row cmp-click" data-prefix="${escHtml(prefix || ip)}">
        <td class="domain-ip">${escHtml(ip)}</td>
        <td class="domain-prefix">${prefix ? escHtml(prefix) : '—'}</td>
        ${domainStatusCell(prefix, censusA)}
        ${domainStatusCell(prefix, censusB)}
      </tr>`;
    };
    const ipTable = (ips4, ips6) => {
      const rows = [...ips4, ...ips6].map(ipTableRow);
      return rows.length ? `<table class="domain-table">
        <thead><tr><th>IP address</th><th>Prefix</th><th>${escHtml(dateA)}</th><th>${escHtml(dateB)}</th></tr></thead>
        <tbody>${rows.join('')}</tbody></table>` : '<p class="loc-note">Could not resolve.</p>';
    };
    const section = (title, body, allIPs) =>
      `<div class="domain-section"><div class="domain-rec-type">${title}${badge2(classifyA(allIPs), classifyB(allIPs))}</div>${body}</div>`;

    let html = `<div class="card">
      <div class="card-title">${escHtml(displayName)} <span class="stat-note">— ${kindLabel} compare</span></div>
      <p class="loc-note">Census: ${escHtml(dateA)} vs ${escHtml(dateB)} &nbsp;&middot;&nbsp; Resolved via Google Public DNS<br>
      <span style="font-size:0.75em;color:#d29922">&#9432; DNS records reflect today&rsquo;s resolution and may differ from the selected census dates.</span><br>
      <span style="font-size:0.75em;color:#484f58">Click a row to look up the prefix in detail.</span></p>`;

    if (aRecs.length) {
      const ips = aRecs.map(r => r.data);
      html += section('A records', ipTable(ips, []), ips);
    }
    if (aaaaRecs.length) {
      const ips = aaaaRecs.map(r => r.data);
      html += section('AAAA records', ipTable([], ips), ips);
    }
    if (nsResolved.length) {
      const allNsIPs = nsResolved.flatMap(({ ips4, ips6 }) => [...ips4, ...ips6]);
      html += `<div class="domain-section"><div class="domain-rec-type">NS records${badge2(classifyA(allNsIPs), classifyB(allNsIPs))}</div>`;
      nsResolved.forEach(({ host, ips4, ips6 }) => {
        const hostIPs = [...ips4, ...ips6];
        html += `<div class="domain-host-label">${escHtml(host)}${hostIPs.length ? badge2(classifyA(hostIPs), classifyB(hostIPs)) : ''}</div>${ipTable(ips4, ips6)}`;
      });
      html += `</div>`;
    }
    if (mxResolved.length) {
      const allMxIPs = mxResolved.flatMap(({ ips4, ips6 }) => [...ips4, ...ips6]);
      html += `<div class="domain-section"><div class="domain-rec-type">MX records${badge2(classifyA(allMxIPs), classifyB(allMxIPs))}</div>`;
      const mxByHost = Object.fromEntries(mxResolved.map(r => [r.host, r]));
      const seen = new Set();
      for (const { priority, host } of [...mxEntries].sort((a, b) => a.priority - b.priority)) {
        if (seen.has(host)) continue; seen.add(host);
        const { ips4, ips6 } = mxByHost[host] || { ips4: [], ips6: [] };
        const hostIPs = [...ips4, ...ips6];
        html += `<div class="domain-host-label">${escHtml(host)} <span class="stat-note">(priority ${priority})</span>${hostIPs.length ? badge2(classifyA(hostIPs), classifyB(hostIPs)) : ''}</div>${ipTable(ips4, ips6)}`;
      }
      html += `</div>`;
    }
    html += `</div>`;
    currentCompareCtx = { dateA, dateB };
    resultsEl.innerHTML = html;
  } catch (err) {
    setStatus('');
    console.error('compareDomain error:', err);
    resultsEl.innerHTML = `<div class="not-found">Error: ${escHtml(err.message ?? String(err))}</div>`;
  }
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
        <br><br>
        If you know of a responsive IP in this prefix that our hitlist may be missing, feel free to
        <a href="mailto:remi.hendriks@utwente.nl" style="color:#58a6ff">reach out</a> —
        we are happy to improve coverage.
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
      document.getElementById('loc-json-btn')?.addEventListener('click', () =>
        downloadLocJson(locations, row.prefix));
    }

    // Insert presence-history card before detected locations (IPv4 only; loaded async via Range request)
    if (!isIPv6) {
      const histCard = document.createElement('div');
      histCard.className = 'card';
      const histLastDate = _histDates.length > 0 ? _histDates[_histDates.length - 1] : '—';
      const dateMismatch = histLastDate !== '—' && dateLabel !== histLastDate && dateLabel !== 'latest';
      const mismatchNote = dateMismatch
        ? `<div style="background:#1c1c2d;border:1px solid #4d3600;border-radius:4px;padding:0.6rem;` +
          `margin-bottom:0.6rem;font-size:0.82rem;color:#d4a574">` +
          `⚠️ Current lookup: <strong>${escHtml(dateLabel)}</strong> | ` +
          `Latest history: <strong>${escHtml(histLastDate)}</strong></div>`
        : '';
      histCard.innerHTML =
        `<div class="card-title">Anycast presence history` +
        `<span class="stat-note" style="font-weight:400;text-transform:none;` +
        `letter-spacing:0;margin-left:0.5rem">${escHtml(_histDates?.[0] ?? 'census start')} — ${escHtml(histLastDate)}</span></div>` +
        mismatchNote +
        `<div id="prefix-history-strip"><span class="stat-note">Loading…</span></div>`;
      // Insert before the detected locations card (which has id="loc-map" inside it)
      const locMapEl = document.getElementById('loc-map');
      if (locMapEl) {
        const locCard = locMapEl.closest('.card');
        if (locCard) resultsEl.insertBefore(histCard, locCard);
      }
      fetchPrefixHistory(row.prefix);
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
    // Add ASN organization name if available
    const asnName = _asnNames[asn];
    const asnLabel = asnName ? `AS${asn} - ${asnName}` : `AS${asn}`;
    resultsEl.innerHTML = renderPrefixList(rows, asnLabel, dateLabel);
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
  else if (filterType === 'mbr') st.mbrFilter = value;
  // Apply all active filters then re-apply active sort
  st.rows = st.allRows.filter(r => {
    if (st.confFilter !== 'all' && r.conf !== st.confFilter) return false;
    if (st.verFilter  !== 'all' && r.ver  !== st.verFilter)  return false;
    if (st.mbrFilter  && st.mbrFilter !== 'all' && r.membership !== st.mbrFilter) return false;
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
  wrap.querySelectorAll('.mbr-filter-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.mbr === (st.mbrFilter ?? 'all'));
  });
  // Update "X shown" count
  const countEl = wrap.querySelector('.conf-filter-count');
  if (countEl) countEl.textContent = `${fmtN(st.rows.length)} shown`;

  // Recompute confidence button counts (based on active ver + mbr, ignoring conf)
  {
    const base = st.allRows.filter(r =>
      (st.verFilter === 'all' || r.ver === st.verFilter) &&
      (!st.mbrFilter || st.mbrFilter === 'all' || r.membership === st.mbrFilter)
    );
    let cH = 0, cM = 0, cL = 0;
    for (const r of base) {
      if (r.conf === 'high') cH++; else if (r.conf === 'medium') cM++; else cL++;
    }
    wrap.querySelectorAll('.conf-filter-btn').forEach(btn => {
      if      (btn.dataset.conf === 'high')   btn.textContent = `High (${fmtN(cH)})`;
      else if (btn.dataset.conf === 'medium') btn.textContent = `Med (${fmtN(cM)})`;
      else if (btn.dataset.conf === 'low')    btn.textContent = `Low (${fmtN(cL)})`;
    });
  }

  // Recompute protocol button counts (based on active conf + mbr, ignoring ver)
  {
    const base = st.allRows.filter(r =>
      (st.confFilter === 'all' || r.conf === st.confFilter) &&
      (!st.mbrFilter || st.mbrFilter === 'all' || r.membership === st.mbrFilter)
    );
    let nV4 = 0, nV6 = 0;
    for (const r of base) { if (r.ver === 'v4') nV4++; else nV6++; }
    wrap.querySelectorAll('.ver-filter-btn').forEach(btn => {
      if      (btn.dataset.ver === 'all') btn.textContent = `All (${fmtN(base.length)})`;
      else if (btn.dataset.ver === 'v4')  btn.textContent = `IPv4 (${fmtN(nV4)})`;
      else if (btn.dataset.ver === 'v6')  btn.textContent = `IPv6 (${fmtN(nV6)})`;
    });
  }

  // Recompute membership button counts (based on active ver + conf, ignoring mbr)
  {
    const base = st.allRows.filter(r =>
      (st.verFilter === 'all' || r.ver === st.verFilter) &&
      (st.confFilter === 'all' || r.conf === st.confFilter)
    );
    wrap.querySelectorAll('.mbr-filter-btn').forEach(btn => {
      const mbr = btn.dataset.mbr;
      if (!mbr) return;
      const count = mbr === 'all' ? base.length : base.filter(r => r.membership === mbr).length;
      // Preserve any inner HTML (cmp-dot spans) and only update the trailing text node
      const lastNode = [...btn.childNodes].find(n => n.nodeType === Node.TEXT_NODE && n.textContent.includes('('));
      if (lastNode) lastNode.textContent = lastNode.textContent.replace(/\(\d[\d,]*\)/, `(${fmtN(count)})`);
      else btn.childNodes.length
        ? (btn.lastChild.textContent = btn.lastChild.textContent.replace(/\(\d[\d,]*\)/, `(${fmtN(count)})`))
        : (btn.textContent = btn.textContent.replace(/\(\d[\d,]*\)/, `(${fmtN(count)})`));
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
        <td><a class="prefix-link" href="?q=${encodeURIComponent(row.prefix)}">${escHtml(row.prefix)}</a>${row.partial ? ' <span class="tag tag-warn" title="Partial anycast: this /24 contains both unicast and anycast addresses">partial</span>' : ''}</td>
        <td><span class="confidence ${conf.cls}">${conf.label}</span></td>
        <td class="${ab_max  ? 'count' : 'count-zero'}">AB&nbsp;${fmtN(ab_max)}</td>
        <td class="${gcd_max ? 'count' : 'count-zero'}">GCD&nbsp;${fmtN(gcd_max)}</td>
        <td>${asns.map(a => `<a class="tag" href="?q=${encodeURIComponent('AS' + a)}">AS${escHtml(a)}</a>`).join(' ')}</td>
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
  const mixedProtocol = nV4 > 0 && nV6 > 0;
  const filterBar = `<div class="conf-filter">
    <div class="filter-row">
      ${mixedProtocol ? `
      <span class="conf-filter-lbl">Protocol:</span>
      <button class="ver-filter-btn active" data-ver="all">All (${fmtN(rows.length)})</button>
      <button class="ver-filter-btn" data-ver="v4">IPv4 (${fmtN(nV4)})</button>
      <button class="ver-filter-btn" data-ver="v6">IPv6 (${fmtN(nV6)})</button>
      <span class="pg-sep">\u00B7</span>` : ''}
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
      <div class="card-title">${fmtN(rows.length)} anycast prefix${rows.length !== 1 ? 'es' : ''} in ${escHtml(searchTerm)} \u2014 ${escHtml(dateLabel)}${mixedProtocol ? ` <span style="font-weight:400;text-transform:none;letter-spacing:0">(${fmtN(nV4)} IPv4, ${fmtN(nV6)} IPv6)</span>` : ''}</div>
      <p class="loc-note">Click a prefix to see its full details.</p>
      ${filterBar}
      <div class="pg-controls-slot">${controls}</div>
      <table class="prefix-list">
        <thead><tr>
          <th data-sort="ver">Ver<span class="sort-ind"></span></th>
          <th data-sort="prefix">Prefix<span class="sort-ind"></span></th>
          <th data-sort="conf">Confidence<span class="info-icon" tabindex="0" data-tip="High: GCD > 1. Medium: AB > 2. Low: AB &le; 2 and GCD &le; 1.">ⓘ</span><span class="sort-ind"></span></th>
          <th data-sort="ab">AB sites<span class="info-icon" tabindex="0" data-tip="Max anycast sites detected across ICMP/TCP/DNS probing (MAnycast2). AB > 2 is recommended to reduce false positives.">ⓘ</span><span class="sort-ind"></span></th>
          <th data-sort="gcd">GCD sites<span class="info-icon" tabindex="0" data-tip="Max GCD score across ICMP/TCP Ark measurements. GCD > 1 indicates anycast with high confidence.">ⓘ</span><span class="sort-ind"></span></th>
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

  const tip = t => `<span class="info-icon" tabindex="0" data-tip="${t}">ⓘ</span>`;

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
          <td>Anycast-based ICMP (AB)${tip('MAnycast2: counts distinct anycast sites reached via ICMP probes. AB > 2 is recommended to reduce false positives.')}</td>
          <td class="${ab_icmp ? 'count' : 'count-zero'}">${fmtN(ab_icmp)}</td>
        </tr>
        <tr>
          <td>Anycast-based TCP (AB)${tip('MAnycast2 using TCP SYNACK probes on a high (non-standard) port.')}</td>
          <td class="${ab_tcp ? 'count' : 'count-zero'}">${fmtN(ab_tcp)}</td>
        </tr>
        <tr>
          <td>Anycast-based DNS (AB)${tip('MAnycast2 using DNS queries.')}</td>
          <td class="${ab_dns ? 'count' : 'count-zero'}">${fmtN(ab_dns)}</td>
        </tr>
        <tr>
          <td>Latency-based ICMP (GCD)${tip('GCD: detects anycast by comparing RTTs from Ark vantage points via ICMP. GCD > 1 indicates anycast with high precision.')}</td>
          <td class="${gcd_icmp ? 'count' : 'count-zero'}">${fmtN(gcd_icmp)}</td>
        </tr>
        <tr>
          <td>Latency-based TCP (GCD)${tip('GCD using TCP SYNACK probes on a high (non-standard) port. Complements ICMP for prefixes that filter ping.')}</td>
          <td class="${gcd_tcp ? 'count' : 'count-zero'}">${fmtN(gcd_tcp)}</td>
        </tr>
      </table>
    </div>

    <div class="card">
      <div class="card-title">Routing</div>
      <table>
        <tr>
          <th>Backing prefix${tip('Associated BGP prefix as seen by RouteViews collectors.')}</th>
          <td>${row.backing_prefix
            ? row.backing_prefix === row.prefix
              ? `<span class="tag" title="Same as current prefix">${escHtml(row.backing_prefix)}</span>`
              : `<a class="tag tag-link" data-lookup="${escHtml(row.backing_prefix)}" href="?q=${encodeURIComponent(row.backing_prefix)}">${escHtml(row.backing_prefix)}</a>`
            : '—'}</td>
        </tr>
        <tr>
          <th>ASN(s)${tip('ASN(s) associated with this anycast prefix.')}</th>
          <td>${asns.map(a => `<a class="tag tag-link" data-lookup="AS${escHtml(a)}" href="?q=AS${encodeURIComponent(a)}">AS${escHtml(a)}</a>`).join(' ') || '—'}</td>
        </tr>
      </table>
    </div>

    ${locations.length ? `
    <div class="card">
      <div class="card-title" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.75rem">
        <span>Detected locations (${fmtN(locations.length)})</span>
        <div style="display:flex;gap:0.5rem">
          <button id="loc-csv-btn" class="csv-btn">↓ CSV</button>
          <button id="loc-json-btn" class="csv-btn">↓ JSON</button>
        </div>
      </div>
      <p class="loc-note">Lower bound — actual number of PoPs may be higher.</p>
      <div id="loc-map"></div>
      <div class="loc-grid">
        ${locations.map((loc, i) => `
          <div class="loc-card" data-loc-idx="${i}">
            <div class="loc-iata">${escHtml(loc.id ?? '?')}</div>
            <div class="loc-city">${escHtml(loc.city ?? '')}${loc.country_code ? `, ${escHtml(loc.country_code)}` : ''}</div>
            <div class="loc-coords">${fmtCoord(loc.lat)}, ${fmtCoord(loc.lon)}</div>
          </div>
        `).join('')}
      </div>
    </div>` : ''}
  `;
}

// ── Prefix presence history ──────────────────────────────────────────────────
// Reads from data/history/{octet}.bin (generated at deploy time).
// File layout: 16-byte header + 65 536 rows × ceil(num_days/4) bytes.
// Each row is one /24 prefix; 2 bits per day, 4 days packed per byte:
//   00 = not in census   10 = low confidence   11 = confident
// (_histDates and _histMeta are declared at the top of the file to avoid TDZ)

async function _ensureHistMeta() {
  // Always reload dates.txt to ensure we have the latest (don't cache)
  const resp = await fetch(baseUrl + `data/history/dates.txt${_cacheBuster}`, {
    cache: 'no-store'  // Force browser to not use cache
  });
  if (!resp.ok) throw new Error(`History index unavailable (HTTP ${resp.status})`);
  const text = await resp.text();
  _histDates = text.trim().split('\n').filter(Boolean);
  _histMeta  = { num_days: _histDates.length,
                 row_size: (_histDates.length + 3) >> 2 };
}

async function fetchPrefixHistory(prefix) {
  // IPv6 history is not yet generated — skip silently
  if (prefix.includes(':')) return;

  const stripEl = document.getElementById('prefix-history-strip');
  if (!stripEl) return;

  try {
    await _ensureHistMeta();
    const { num_days, row_size } = _histMeta;

    // Parse "a.b.c.0/24"
    const parts  = prefix.split('.');
    const octet  = parseInt(parts[0], 10);
    const b      = parseInt(parts[1], 10);
    const c      = parseInt(parts[2], 10);
    const localIdx = (b << 8) | c;   // 0–65 535 within this octet file

    // Fetch gzip-compressed binary file
    const resp = await fetch(baseUrl + `data/history/${octet}.bin.gz${_cacheBuster}`, {
      cache: 'no-store'  // Force browser to not use cache
    });
    if (!resp.ok) {
      stripEl.textContent = 'History not available for this prefix.';
      return;
    }

    // Decompress gzip data using pako
    const compressedData = new Uint8Array(await resp.arrayBuffer());
    const pako = (await import('https://cdn.jsdelivr.net/npm/pako@2.1.0/+esm')).default;
    const fileData = new Uint8Array(pako.inflate(compressedData));

    // Extract the specific row for this prefix
    const rowStart = 16 + localIdx * row_size;
    const bytes = fileData.slice(rowStart, rowStart + row_size);

    // Decode 2 bits per day
    const states = new Uint8Array(num_days);
    for (let d = 0; d < num_days; d++) {
      const bp = d >> 2;
      const bs = (3 - (d & 3)) << 1;
      states[d] = (bytes[bp] >> bs) & 0b11;
    }

    // Summary stats
    let firstSeen = null, lastSeen = null, nPresent = 0, nConf = 0;
    for (let d = 0; d < num_days; d++) {
      const s = states[d];
      if (s === 0) continue;
      nPresent++;
      if (s === 3) nConf++;
      if (firstSeen === null) firstSeen = _histDates[d];
      lastSeen = _histDates[d];
    }

    // Colour map: 0=not detected, 2=low-conf, 3=confident
    const colMap = { 0: '#1c2128', 2: '#7d4e00', 3: '#238636' };
    const lblMap = { 0: 'not detected', 2: 'low confidence', 3: 'confident' };

    const cells = Array.from(states).map((s, d) =>
      `<span data-hist-day="${d}" style="flex:1;min-width:1px;background:${colMap[s] ?? colMap[0]};` +
      `cursor:pointer;transition:opacity 0.2s" ` +
      `title="Click to jump to ${escHtml(_histDates[d])}: ${lblMap[s] ?? 'unknown'}"></span>`
    ).join('');

    const legendItem = (col, lbl) =>
      `<span style="display:inline-flex;align-items:center;gap:0.3rem">` +
      `<span style="width:11px;height:11px;border-radius:2px;background:${col};` +
      `border:1px solid #30363d;display:inline-block"></span>${escHtml(lbl)}</span>`;

    // Date range and zoom buttons
    const firstDate = _histDates[0] || '—';
    const lastDate  = _histDates[num_days - 1] || '—';

    stripEl.innerHTML =
      `<div style="display:flex;justify-content:space-between;align-items:center;` +
      `margin-bottom:0.5rem;font-size:0.82rem;color:#8b949e">` +
      `<span><strong style="color:#e6edf3">${escHtml(firstDate)}</strong> to ` +
      `<strong style="color:#e6edf3">${escHtml(lastDate)}</strong></span>` +
      `<div style="display:flex;gap:0.4rem" id="hist-zoom-btns"></div>` +
      `</div>` +
      `<div style="position:relative">` +
      `<div style="height:20px;font-size:0.75rem;color:#8b949e;margin-bottom:0.3rem;` +
      `min-height:1.2em;display:flex;align-items:center" id="hist-tooltip"></div>` +
      `<div style="display:flex;height:18px;border-radius:4px;overflow:hidden;` +
      `border:1px solid #30363d;background:#0d1117" id="hist-strip">` +
      `${cells}</div>` +
      `</div>` +
      `<div id="hist-zoomed" style="margin-top:0.8rem"></div>` +
      `<div style="display:flex;gap:1.2rem;flex-wrap:wrap;font-size:0.82rem;` +
      `color:#8b949e;margin-bottom:0.5rem;margin-top:0.6rem">` +
      `<span>First seen: <strong style="color:#e6edf3">${escHtml(firstSeen ?? '—')}</strong></span>` +
      `<span>Last seen: <strong style="color:#e6edf3">${escHtml(lastSeen ?? '—')}</strong></span>` +
      `<span>Present: <strong style="color:#e6edf3">${fmtN(nPresent)}/${fmtN(num_days)} days</strong></span>` +
      `<span>Confident: <strong style="color:#e6edf3">${fmtN(nConf)} days</strong></span>` +
      `</div>` +
      `<div style="display:flex;gap:0.8rem;flex-wrap:wrap;font-size:0.78rem;color:#8b949e">` +
      legendItem('#1c2128', 'not detected') +
      legendItem('#7d4e00', 'low confidence') +
      legendItem('#238636', 'confident') +
      `</div>`;

    // Helper to render a zoomed heatmap for a date range
    const renderZoomedHeatmap = (startIdx, endIdx) => {
      const zoomedStates = states.slice(startIdx, endIdx + 1);
      const zoomedDates  = _histDates.slice(startIdx, endIdx + 1);
      const zoomedCells = Array.from(zoomedStates).map((s, i) =>
        `<div data-zoomed-day="${startIdx + i}" style="flex:1;background:${colMap[s] ?? colMap[0]};` +
        `cursor:pointer;transition:opacity 0.2s;min-height:40px;border:1px solid #30363d;` +
        `display:flex;align-items:center;justify-content:center;font-size:0.7rem;` +
        `color:#8b949e;text-align:center;padding:4px" ` +
        `title="Click to jump to ${escHtml(zoomedDates[i])}: ${lblMap[s] ?? 'unknown'}">` +
        `${zoomedDates[i].split('-')[2]}</div>`
      ).join('');

      const zoomedEl = document.getElementById('hist-zoomed');
      if (zoomedEl) {
        zoomedEl.innerHTML = `
          <div style="font-size:0.82rem;color:#8b949e;margin-bottom:0.4rem">
            <strong>Zoomed: ${escHtml(zoomedDates[0])} to ${escHtml(zoomedDates[zoomedDates.length - 1])}</strong>
          </div>
          <div style="display:flex;border:1px solid #30363d;border-radius:4px;overflow:hidden;background:#0d1117">
            ${zoomedCells}
          </div>`;

        // Attach listeners to zoomed cells
        const zoomedCellEls = zoomedEl.querySelectorAll('[data-zoomed-day]');
        zoomedCellEls.forEach(el => {
          const dayIdx = parseInt(el.dataset.zoomedDay, 10);
          const dateStr = _histDates[dayIdx];
          el.addEventListener('mouseenter', () => {
            el.style.opacity = '0.7';
            el.style.filter = 'brightness(1.2)';
          });
          el.addEventListener('mouseleave', () => {
            el.style.opacity = '1';
            el.style.filter = 'brightness(1)';
          });
          el.addEventListener('click', () => {
            dateInput.value = dateStr;
            dateInput.dispatchEvent(new Event('change', { bubbles: true }));
          });
        });
      }
    };

    // Attach click handlers and custom tooltip to full-timeline cells (synchronously)
    const strip = document.getElementById('hist-strip');
    const tooltip = document.getElementById('hist-tooltip');
    if (strip && tooltip) {
      const cellEls = strip.querySelectorAll('[data-hist-day]');
      cellEls.forEach(el => {
        const dayIdx = parseInt(el.dataset.histDay, 10);
        const dateStr = _histDates[dayIdx];
        const status = lblMap[states[dayIdx]] || 'unknown';
        el.addEventListener('mouseenter', () => {
          el.style.opacity = '0.7';
          el.style.filter = 'brightness(1.2)';
          tooltip.textContent = `${dateStr}  •  ${status}`;
          tooltip.style.color = '#e6edf3';
        });
        el.addEventListener('mouseleave', () => {
          el.style.opacity = '1';
          el.style.filter = 'brightness(1)';
          tooltip.textContent = '';
        });
        el.addEventListener('click', () => {
          // Navigate to this date: set the date input and trigger applyDate
          dateInput.value = dateStr;
          dateInput.dispatchEvent(new Event('change', { bubbles: true }));
        });
      });
    }

    // Add zoom buttons and range picker
    const zoomBtns = document.getElementById('hist-zoom-btns');
    if (zoomBtns) {
      const btnStyle = 'padding:0.3rem 0.6rem;font-size:0.75rem;border:1px solid #30363d;' +
        'background:#0d1117;color:#8b949e;border-radius:3px;cursor:pointer;' +
        'transition:all 0.2s;user-select:none';
      const btnActiveStyle = btnStyle + ';background:#238636;color:#fff;border-color:#238636';

      let lastActiveBtn = null;

      const zoomRanges = [
        { label: 'First 30d',  start: 0,               length: 30 },
        { label: 'Last 30d',   start: num_days - 30,   length: 30 },
        { label: 'Last 90d',   start: num_days - 90,   length: 90 },
        { label: 'All',        start: 0,               length: num_days },
      ];

      zoomRanges.forEach(range => {
        const btn = document.createElement('button');
        btn.textContent = range.label;
        btn.style.cssText = btnStyle;
        btn.addEventListener('click', () => {
          const startIdx = Math.max(0, range.start);
          const endIdx = Math.min(num_days - 1, startIdx + range.length - 1);
          renderZoomedHeatmap(startIdx, endIdx);

          // Update button styles
          zoomBtns.querySelectorAll('button').forEach(b => {
            if (b.classList.contains('hist-range-btn')) b.style.cssText = btnStyle;
          });
          btn.style.cssText = btnActiveStyle;
          lastActiveBtn = btn;
        });
        btn.addEventListener('mouseenter', () => {
          if (btn !== lastActiveBtn) {
            btn.style.background = '#161b22';
          }
        });
        btn.addEventListener('mouseleave', () => {
          if (btn !== lastActiveBtn) {
            btn.style.background = '#0d1117';
          }
        });
        btn.classList.add('hist-range-btn');
        zoomBtns.appendChild(btn);
      });

      // Add a separator and custom range picker
      const sep = document.createElement('div');
      sep.style.cssText = 'width:1px;background:#30363d;margin:0 0.4rem';
      zoomBtns.appendChild(sep);

      const customBtn = document.createElement('button');
      customBtn.textContent = '📅 Custom';
      customBtn.style.cssText = btnStyle;
      customBtn.addEventListener('click', () => {
        const startDate = prompt(`Enter start date (YYYY-MM-DD):\n\nFirst available: ${_histDates[0]}`);
        if (!startDate) return;
        const startIdx = _histDates.indexOf(startDate);
        if (startIdx === -1) {
          alert('Start date not found in history');
          return;
        }

        const endDate = prompt(`Enter end date (YYYY-MM-DD):\n\nLast available: ${_histDates[num_days - 1]}`);
        if (!endDate) return;
        const endIdx = _histDates.indexOf(endDate);
        if (endIdx === -1) {
          alert('End date not found in history');
          return;
        }

        if (endIdx < startIdx) {
          alert('End date must be after start date');
          return;
        }

        renderZoomedHeatmap(startIdx, endIdx);

        // Update button styles
        zoomBtns.querySelectorAll('button').forEach(b => {
          if (b.classList.contains('hist-range-btn')) b.style.cssText = btnStyle;
        });
        customBtn.style.cssText = btnActiveStyle;
        lastActiveBtn = customBtn;
      });
      customBtn.addEventListener('mouseenter', () => {
        if (customBtn !== lastActiveBtn) {
          customBtn.style.background = '#161b22';
        }
      });
      customBtn.addEventListener('mouseleave', () => {
        if (customBtn !== lastActiveBtn) {
          customBtn.style.background = '#0d1117';
        }
      });
      zoomBtns.appendChild(customBtn);
    }
  } catch (err) {
    const el = document.getElementById('prefix-history-strip');
    if (el) el.innerHTML = `<span class="stat-note">History unavailable: ${escHtml(err.message)}</span>`;
  }
}

// ── Map ────────────────────────────────────────────────────────────────────
function initMap(locations) {
  const el = document.getElementById('loc-map');
  if (!el) return;

  if (leafletMap) { leafletMap.remove(); leafletMap = null; }

  leafletMap = L.map('loc-map', {
    zoomControl: true,
    worldCopyJump: false,
    minZoom: 2,           // Don't zoom out too far
    maxZoom: 19,
    maxBounds: [[-85, -180], [85, 180]],
    maxBoundsViscosity: 0.95,
  });

  // Default center/zoom before fitting to markers
  leafletMap.setView([20, 0], 2);

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

  locMarkers = locations.map(loc => {
    if (loc.lat == null || loc.lon == null) return null;
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

  const validMarkers = locMarkers.filter(Boolean);
  if (validMarkers.length) {
    leafletMap.fitBounds(L.featureGroup(validMarkers).getBounds().pad(0.15));
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

function downloadLocJson(locations, prefix) {
  const json = JSON.stringify(locations, null, 2);
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([json], { type: 'application/json' }));
  a.download = `${prefix.replace(/\//g, '_')}_locations.json`;
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
  if (isCompareMode) {
    isCompareMode = false;
    modeLookup.classList.add('active');
    modeCompare.classList.remove('active');
    lookupRow.style.display = '';
    compareRow.style.display = 'none';
  }
  // Always reset to "Latest" — Home means default state, no date context
  whenLatest.checked = true;
  whenDate.checked   = false;
  dateInput.value    = '';
  syncDateUI(null);
  setStatus('');
  history.pushState(null, '', '.');
  refreshHomeStats('latest');
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

  // ── Membership (onlyA / onlyB / both) filter buttons ──
  const mbrBtn = e.target.closest('.mbr-filter-btn');
  if (mbrBtn) {
    const wrap = mbrBtn.closest('[data-pg-id]');
    if (!wrap) return;
    applyTableFilter(wrap.dataset.pgId, 'mbr', mbrBtn.dataset.mbr);
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
    e.preventDefault();
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

  // ── Location card click — highlight marker on map ──
  const locCard = e.target.closest('.loc-card[data-loc-idx]');
  if (locCard) {
    const idx    = parseInt(locCard.dataset.locIdx);
    const marker = locMarkers[idx] ?? null;
    const sel    = locCard.classList.toggle('selected');
    if (marker) {
      marker.setStyle(sel
        ? { fillColor: '#ffa657', color: '#d18616', fillOpacity: 1, radius: 9, weight: 2 }
        : { fillColor: '#58a6ff', color: '#1f6feb', fillOpacity: 0.85, radius: 6, weight: 1.5 });
      if (sel) {
        leafletMap?.panTo(marker.getLatLng(), { animate: true });
        marker.openTooltip();
      } else {
        marker.closeTooltip();
      }
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
    if (e.button !== 0) return; // allow middle-click (new tab) and right-click naturally
    e.preventDefault();
    const q = `AS${asRow.dataset.asn}`;
    inputEl.value = q;
    lookup(q);
  }
});

// ── Autocomplete functions ─────────────────────────────────────────────────
function showAutocomplete(matches) {
  autocompleteList.innerHTML = '';
  if (matches.length === 0) {
    autocompleteDropdown.style.display = 'none';
    return;
  }

  matches.forEach((match, idx) => {
    const li = document.createElement('li');
    li.textContent = match;
    li.dataset.asn = match.split(' - ')[0];
    li.addEventListener('click', () => {
      inputEl.value = li.dataset.asn;
      hideAutocomplete();
      lookup(inputEl.value);
    });
    autocompleteList.appendChild(li);
  });

  autocompleteHighlightedIndex = -1;
  autocompleteDropdown.style.display = 'block';
}

function hideAutocomplete() {
  autocompleteDropdown.style.display = 'none';
  autocompleteList.innerHTML = '';
}

function getAutocompleteMatches(input) {
  const trimmed = input.trim();

  // Only show autocomplete for ASN input (no dots, looks like "AS13335" or partial match)
  // If input contains a dot (like "google.com"), it's a domain, skip autocomplete
  // Require at least 2 characters to avoid excessive matches
  if (trimmed.includes('.') || trimmed.length < 2) return [];

  const query = trimmed.toUpperCase();
  const cleanQuery = query.replace(/^AS/, '');
  const nameQuery = trimmed.toLowerCase();
  const matches = [];
  const seen = new Set();

  // Match by ASN number first (more specific)
  for (const [asn, name] of Object.entries(_asnNames)) {
    if (asn.includes(cleanQuery)) {
      matches.push(`AS${asn} - ${name}`);
      seen.add(asn);
    }
  }

  // Also match by name
  for (const [asn, name] of Object.entries(_asnNames)) {
    if (!seen.has(asn) && name.toLowerCase().includes(nameQuery)) {
      matches.push(`AS${asn} - ${name}`);
      seen.add(asn);
    }
  }

  return matches.sort();
}

searchBtn.addEventListener('click', () => lookup(inputEl.value));

inputEl.addEventListener('keydown', e => {
  if (e.key === 'Enter') {
    hideAutocomplete();
    lookup(inputEl.value);
  } else if (e.key === 'ArrowDown') {
    e.preventDefault();
    const items = autocompleteList.querySelectorAll('li');
    if (items.length > 0) {
      autocompleteHighlightedIndex = Math.min(autocompleteHighlightedIndex + 1, items.length - 1);
      items.forEach((item, idx) => {
        item.classList.toggle('selected', idx === autocompleteHighlightedIndex);
      });
    }
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    const items = autocompleteList.querySelectorAll('li');
    if (items.length > 0) {
      autocompleteHighlightedIndex = Math.max(autocompleteHighlightedIndex - 1, -1);
      items.forEach((item, idx) => {
        item.classList.toggle('selected', idx === autocompleteHighlightedIndex);
      });
    }
  } else if (e.key === 'Escape') {
    hideAutocomplete();
  }
});

inputEl.addEventListener('input', e => {
  const matches = getAutocompleteMatches(e.target.value);
  showAutocomplete(matches);
});

inputEl.addEventListener('blur', () => {
  // Hide after a small delay to allow click on dropdown item
  setTimeout(hideAutocomplete, 200);
});

// ── Date picker UI ───────────────────────────────────────────────────────────
// (btnLatest/btnPickDate/btnPrevDay/btnNextDay/dateNavGroup/dateLabelDisplay
//  are declared near the top of the file so syncDateUI() works inside the
//  top-level await init block)

// Position a hidden date input directly under its trigger button so the
// native calendar popup opens near the button, not at the screen corner.
function anchorPickerTo(input, btn) {
  const r = btn.getBoundingClientRect();
  input.style.left = r.left + 'px';
  input.style.top  = (r.bottom + 2) + 'px';
}

// Sync visual state to a date string ('YYYY-MM-DD') or null for Latest
function syncDateUI(date) {
  if (date) {
    dateLabelDisplay.textContent = date;
    // Brief pop to confirm the calendar selection was registered
    dateLabelDisplay.classList.remove('date-confirm-flash');
    void dateLabelDisplay.offsetWidth; // force reflow so animation restarts
    dateLabelDisplay.classList.add('date-confirm-flash');
    dateNavGroup.style.display   = 'flex';
    btnLatest.classList.remove('date-mode-active');
    btnPrevDay.disabled = !!dateInput.min && date <= dateInput.min;
    btnNextDay.disabled = !!dateInput.max && date >= dateInput.max;
  } else {
    dateNavGroup.style.display = 'none';
    btnLatest.classList.add('date-mode-active');
  }
}

// Apply a specific date: update state, sync UI, and refresh/re-run as needed
function applyDate(date) {
  whenDate.checked   = true;
  whenLatest.checked = false;
  dateInput.value    = date;
  syncDateUI(date);
  if (resultsEl.innerHTML && inputEl.value) {
    // Re-run current search on the new date (navigate day-by-day through results)
    lookup(inputEl.value);
  } else {
    history.replaceState(null, '', '?date=' + date);
    refreshHomeStats(date.replace(/-/g, '/'));
  }
}

btnLatest.addEventListener('click', () => {
  whenLatest.checked = true;
  whenDate.checked   = false;
  dateInput.value    = '';
  syncDateUI(null);
  if (!resultsEl.innerHTML) {
    history.replaceState(null, '', '.');
    refreshHomeStats('latest');
  }
});

btnPickDate.addEventListener('click', () => {
  anchorPickerTo(dateInput, btnPickDate);
  try { dateInput.showPicker(); } catch { dateInput.click(); }
});

dateInput.addEventListener('change', () => {
  if (dateInput.value) applyDate(dateInput.value);
});

btnPrevDay.addEventListener('click', () => {
  if (!dateInput.value) return;
  const d = new Date(dateInput.value + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() - 1);
  const s = d.toISOString().slice(0, 10);
  if (!dateInput.min || s >= dateInput.min) applyDate(s);
});

btnNextDay.addEventListener('click', () => {
  if (!dateInput.value) return;
  const d = new Date(dateInput.value + 'T00:00:00Z');
  d.setUTCDate(d.getUTCDate() + 1);
  const s = d.toISOString().slice(0, 10);
  if (!dateInput.max || s <= dateInput.max) applyDate(s);
});

// ── Compare date picker buttons ──────────────────────────────────────────────
function syncCmpBtn(btn, input) {
  if (input.value) {
    btn.textContent = input.value;
    btn.classList.add('date-mode-active');
    // Brief pop to confirm the calendar selection was registered
    btn.classList.remove('date-confirm-flash');
    void btn.offsetWidth; // force reflow so animation restarts
    btn.classList.add('date-confirm-flash');
  } else {
    btn.textContent = 'Pick date';
    btn.classList.remove('date-mode-active');
  }
}
btnCmpA.addEventListener('click', () => { anchorPickerTo(cmpDateA, btnCmpA); try { cmpDateA.showPicker(); } catch { cmpDateA.click(); } });
btnCmpB.addEventListener('click', () => { anchorPickerTo(cmpDateB, btnCmpB); try { cmpDateB.showPicker(); } catch { cmpDateB.click(); } });
cmpDateA.addEventListener('change', () => syncCmpBtn(btnCmpA, cmpDateA));
cmpDateB.addEventListener('change', () => syncCmpBtn(btnCmpB, cmpDateB));

// ── Compare mode toggle ─────────────────────────────────────────────────────
modeLookup.addEventListener('click', () => {
  isCompareMode = false;
  modeLookup.classList.add('active');
  modeCompare.classList.remove('active');
  lookupRow.style.display = '';
  compareRow.style.display = 'none';
  resultsEl.innerHTML = '';
  setStatus('');
  resetPg();
  currentLookupCtx = null;
  currentCompareCtx = null;
  chartSection.style.display = 'none';
  asStatsSectionEl.style.display = 'none';
  asTableSectionEl.style.display = 'none';
  document.getElementById('showcase-section').style.display = 'none';
  // Restore home-page stats for the current date selection
  refreshHomeStats(selectedViewKey());
});

modeCompare.addEventListener('click', () => {
  isCompareMode = true;
  modeCompare.classList.add('active');
  modeLookup.classList.remove('active');
  lookupRow.style.display = 'none';
  compareRow.style.display = '';
  resultsEl.innerHTML = '';
  setStatus('');
  resetPg();
  currentLookupCtx = null;
  currentCompareCtx = null;
  chartSection.style.display = 'none';
  asStatsSectionEl.style.display = 'none';
  asTableSectionEl.style.display = 'none';
  document.getElementById('showcase-section').style.display = 'none';
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

  if (query) {
    // Prefix-specific compare — hide global stats cards
    asStatsSectionEl.style.display = 'none';
    asTableSectionEl.style.display = 'none';
    document.getElementById('showcase-section').style.display = 'none';
  } else {
    // Full census compare — show global stats cards with loading placeholders
    asStatsReady = false;
    asTableReady = false;
    document.getElementById('as-stats-title').textContent =
      `Network statistics (high confidence) \u2014 ${dateB} vs ${dateA}`;
    document.getElementById('as-table-title').innerHTML =
      `ASes deploying anycast \u2014 ${dateB} vs ${dateA} <span class="stat-note">(confident or better)</span>`;
    ['as-stat-v4','as-stat-v6','as-stat-comb',
     'bgp-stat-v4','bgp-stat-v6','bgp-stat-comb',
     'moas-stat-v4','moas-stat-v6','moas-stat-comb'].forEach(id => {
      document.getElementById(id).textContent = '\u2014';
    });
    document.getElementById('as-table-body').innerHTML = '';
    asStatsSectionEl.style.display = '';
    asTableSectionEl.style.display = '';
    // Showcase — reset cells and show immediately; fill data once chartData is ready
    document.getElementById('showcase-title').textContent =
      `Anycast prefix counts \u2014 ${dateB} vs ${dateA}`;
    ['sc-vp4','sc-vp6','sc-gcd-icmp4','sc-gcd-icmp6','sc-gcd-tcp4','sc-gcd-tcp6',
     'sc-ab-icmp4','sc-ab-icmp6','sc-ab-tcp4','sc-ab-tcp6','sc-ab-dns4','sc-ab-dns6'].forEach(id => {
      document.getElementById(id).textContent = '\u2014';
    });
    document.getElementById('showcase-section').style.display = '';
  }

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
    // Domain compare — resolve DNS then check both census dates
    const isDomain = /^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z]{2,63}$/.test(query) &&
      !query.includes(':') && !query.includes('/');
    const isTLD = /^\.[a-zA-Z]{2,63}$/.test(query);
    const isRoot = query === '.';
    if (isDomain || isTLD || isRoot) {
      await compareDomain(isTLD ? query.slice(1) : query, dateA, dateB);
    } else {
      await comparePrefixDetail(query, dateA, dateB);
    }
  } else {
    // Full census compare — fire global stats in parallel
    try { initShowcaseCompare(dateA, dateB); } catch (_) {}
    initASStatsCompare(dateA, dateB);
    initASTableCompare(dateA, dateB);
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
  const safe = prefix.replace(/'/g, "''");
  const lenMatch = prefix.match(/\/(\d+)$/);
  const len = lenMatch ? parseInt(lenMatch[1]) : (isIPv6 ? 48 : 24);
  const isBroadCidr = (!isIPv6 && len < 24) || (isIPv6 && len < 48);

  setStatus(`Comparing ${prefix} between ${dateA} and ${dateB}…`);

  // ── Broad CIDR (e.g. /16): show all contained /24s or /48s in both dates ──
  if (isBroadCidr) {
    try {
      const confExpr = (ab1, ab2, ab3, gcd1, gcd2) =>
        `CASE WHEN GREATEST(${gcd1},${gcd2}) > 1 THEN 'high'
              WHEN GREATEST(${ab1},${ab2},${ab3}) > 2 THEN 'medium'
              ELSE 'low' END AS conf`;
      const [vA, vB] = isIPv6 ? ['ipv6_a', 'ipv6_b'] : ['ipv4_a', 'ipv4_b'];
      const [ab1, ab2, ab3, gcd1, gcd2] = isIPv6
        ? ['AB_ICMPv6', 'AB_TCPv6', 'AB_DNSv6', 'GCD_ICMPv6', 'GCD_TCPv6']
        : ['AB_ICMPv4', 'AB_TCPv4', 'AB_DNSv4', 'GCD_ICMPv4', 'GCD_TCPv4'];
      const ver = isIPv6 ? 'v6' : 'v4';
      const ce = (t) => confExpr(`${t}.${ab1}`,`${t}.${ab2}`,`${t}.${ab3}`,`${t}.${gcd1}`,`${t}.${gcd2}`);
      const cinetA = `a.prefix::INET <<= '${safe}'::INET`;
      const cinetB = `b.prefix::INET <<= '${safe}'::INET`;

      const both = (await conn.query(`
        SELECT '${ver}' AS ver, a.prefix, ${ce('a')}
        FROM ${vA} a INNER JOIN ${vB} b ON a.prefix = b.prefix
        WHERE ${cinetA} ORDER BY a.prefix
      `)).toArray().map(r => r.toJSON());

      const onlyA = (await conn.query(`
        SELECT '${ver}' AS ver, a.prefix, ${ce('a')}
        FROM ${vA} a LEFT JOIN ${vB} b ON a.prefix = b.prefix
        WHERE b.prefix IS NULL AND ${cinetA} ORDER BY a.prefix
      `)).toArray().map(r => r.toJSON());

      const onlyB = (await conn.query(`
        SELECT '${ver}' AS ver, b.prefix, ${ce('b')}
        FROM ${vB} b LEFT JOIN ${vA} a ON b.prefix = a.prefix
        WHERE a.prefix IS NULL AND ${cinetB} ORDER BY b.prefix
      `)).toArray().map(r => r.toJSON());

      setStatus('');
      if (!both.length && !onlyA.length && !onlyB.length) {
        resultsEl.innerHTML = `<div class="not-found">No anycast prefixes found within <strong>${escHtml(prefix)}</strong> in either date.<br><br>
          If you know of a responsive IP in this block that our hitlist may be missing, feel free to
          <a href="mailto:remi.hendriks@utwente.nl" style="color:#58a6ff">reach out</a> —
          we are happy to improve coverage.</div>`;
        return;
      }
      resultsEl.innerHTML = renderCompareResults(both, onlyA, onlyB, dateA, dateB);
      currentCompareCtx = { dateA, dateB };
    } catch (err) { setStatus('Compare error: ' + (err.message ?? ''), true); }
    return;
  }

  // ── Exact /24 or /48 prefix compare ──
  const viewA = isIPv6 ? 'ipv6_a' : 'ipv4_a';
  const viewB = isIPv6 ? 'ipv6_b' : 'ipv4_b';
  try {
    const resA = await conn.query(`SELECT * FROM ${viewA} WHERE prefix = '${safe}'`);
    const resB = await conn.query(`SELECT * FROM ${viewB} WHERE prefix = '${safe}'`);
    const rowsA = resA.toArray().map(r => r.toJSON());
    const rowsB = resB.toArray().map(r => r.toJSON());

    if (!rowsA.length && !rowsB.length) {
      setStatus('');
      resultsEl.innerHTML = `<div class="not-found">Prefix <strong>${escHtml(prefix)}</strong> not found in either date.<br><br>
        If you know of a responsive IP in this prefix that our hitlist may be missing, feel free to
        <a href="mailto:remi.hendriks@utwente.nl" style="color:#58a6ff">reach out</a> —
        we are happy to improve coverage.</div>`;
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
        <td><a class="prefix-link" href="?q=${encodeURIComponent(r.prefix)}">${escHtml(r.prefix)}</a></td>
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

  // Combined prefix list with membership filter
  const allRows = [
    ...both.map(r => ({ ...r, membership: 'both' })),
    ...onlyA.map(r => ({ ...r, membership: 'onlyA' })),
    ...onlyB.map(r => ({ ...r, membership: 'onlyB' })),
  ];
  const id = `pg${++pgCounter}`;
  const pageSize = 10;
  const cmpRenderRows = (slice) => slice.map(r => {
    const cc = r.conf === 'high' ? 'conf-col-high' : r.conf === 'medium' ? 'conf-col-medium' : 'conf-col-low';
    const cl = r.conf === 'high' ? 'High' : r.conf === 'medium' ? 'Medium' : 'Low';
    const dotCls = r.membership === 'both' ? 'cmp-dot-both' : r.membership === 'onlyA' ? 'cmp-dot-onlyA' : 'cmp-dot-onlyB';
    return `<tr class="pl-row cmp-click" data-prefix="${escHtml(r.prefix)}">
      <td><span class="cmp-dot ${dotCls}"></span></td>
      <td><span class="ver-badge">${r.ver}</span></td>
      <td><a class="prefix-link" href="?q=${encodeURIComponent(r.prefix)}">${escHtml(r.prefix)}</a></td>
      <td><span class="conf-col ${cc}">${cl}</span></td>
    </tr>`;
  }).join('');

  let cH = 0, cM = 0, cL = 0, nV4 = 0, nV6 = 0;
  for (const r of allRows) {
    if (r.conf === 'high') cH++; else if (r.conf === 'medium') cM++; else cL++;
    if (r.ver === 'v4') nV4++; else nV6++;
  }
  pgStore[id] = { allRows, rows: allRows, page: 0, pageSize, renderRows: cmpRenderRows,
                  confFilter: 'all', verFilter: 'all', mbrFilter: 'all' };
  const initialBody = cmpRenderRows(allRows.slice(0, pageSize));
  const controls = pgControlsHtml(id, allRows.length, 0, pageSize);
  const filterBar = `<div class="conf-filter">
    <div class="filter-row">
      <span class="conf-filter-lbl">Show:</span>
      <button class="mbr-filter-btn active" data-mbr="all">All (${fmtN(allRows.length)})</button>
      <button class="mbr-filter-btn" data-mbr="onlyA"><span class="cmp-dot cmp-dot-onlyA" style="margin-right:0.3em"></span>Only ${escHtml(dateA)} (${fmtN(onlyA.length)})</button>
      <button class="mbr-filter-btn" data-mbr="onlyB"><span class="cmp-dot cmp-dot-onlyB" style="margin-right:0.3em"></span>Only ${escHtml(dateB)} (${fmtN(onlyB.length)})</button>
      <button class="mbr-filter-btn" data-mbr="both"><span class="cmp-dot cmp-dot-both" style="margin-right:0.3em"></span>In both (${fmtN(both.length)})</button>
    </div>
    <div class="filter-row">
      <span class="conf-filter-lbl">Protocol:</span>
      <button class="ver-filter-btn active" data-ver="all">All (${fmtN(allRows.length)})</button>
      <button class="ver-filter-btn" data-ver="v4">IPv4 (${fmtN(nV4)})</button>
      <button class="ver-filter-btn" data-ver="v6">IPv6 (${fmtN(nV6)})</button>
    </div>
    <div class="filter-row">
      <span class="conf-filter-lbl">Confidence:</span>
      <button class="conf-filter-btn active" data-conf="all">All</button>
      <button class="conf-filter-btn" data-conf="high">High (${fmtN(cH)})</button>
      <button class="conf-filter-btn" data-conf="medium">Med (${fmtN(cM)})</button>
      <button class="conf-filter-btn" data-conf="low">Low (${fmtN(cL)})</button>
      <span class="pg-sep">\u00B7</span>
      <span class="conf-filter-count">${fmtN(allRows.length)} shown</span>
    </div>
  </div>`;

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

    <div class="card" data-pg-id="${id}">
      ${filterBar}
      <div class="pg-controls-slot">${controls}</div>
      <table class="prefix-list">
        <thead><tr><th></th><th></th><th>Prefix</th><th>Confidence</th></tr></thead>
        <tbody>${initialBody}</tbody>
      </table>
      <div class="pg-controls-slot">${controls}</div>
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
    minZoom: 2,           // Don't zoom out too far
    maxZoom: 19,
    maxBounds: [[-85, -180], [85, 180]],
    maxBoundsViscosity: 0.95,
  });

  // Default center/zoom before fitting to markers
  leafletMap.setView([20, 0], 2);

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

let chartVer      = 'v4';
let chartHidden   = new Set();
let chartHoverIdx = null;
let chartZoomStart   = 0;   // first visible date index
let chartZoomEnd     = -1;  // last visible date index (-1 = latest)
let chartDragStartX  = null;
let chartDragCurrentX = null;

async function refreshHomeStats(viewKey = 'latest') {
  asStatsReady = false;
  asTableReady = false;

  // For the section headings, prefer the concrete date from chartData so all
  // three sections display the same label (avoids "latest" vs "2026-03-17" mismatch).
  let dateLabel;
  if (viewKey === 'latest') {
    dateLabel = chartData?.dates?.at(-1) ?? 'latest';
  } else {
    dateLabel = viewKey.replace(/\//g, '-');
  }

  document.getElementById('as-stats-title').textContent =
    `Network statistics (high confidence) \u2014 ${dateLabel}`;
  document.getElementById('as-table-title').innerHTML =
    `ASes deploying anycast \u2014 ${dateLabel} <span class="stat-note">(confident or better)</span>`;
  ['as-stat-v4','as-stat-v6','as-stat-comb',
   'bgp-stat-v4','bgp-stat-v6','bgp-stat-comb',
   'moas-stat-v4','moas-stat-v6','moas-stat-comb'].forEach(id => {
    document.getElementById(id).textContent = '—';
  });
  document.getElementById('as-table-body').innerHTML = '';
  try { initShowcase(viewKey); } catch (_) {}
  await Promise.all([initASStats(viewKey), initASTable(viewKey)]);

  // After stats load, re-sync headings — chartData may now be available if it
  // wasn't when this function started (it loads in parallel with DuckDB).
  if (viewKey === 'latest' && chartData?.dates?.length) {
    const resolvedDate = chartData.dates.at(-1);
    document.getElementById('as-stats-title').textContent =
      `Network statistics (high confidence) \u2014 ${resolvedDate}`;
    document.getElementById('as-table-title').innerHTML =
      `ASes deploying anycast \u2014 ${resolvedDate} <span class="stat-note">(confident or better)</span>`;
  }
}

async function initASStats(viewKey = 'latest') {
  if (!conn) return;
  await registerViews(viewKey);
  try {
    const [r4, r6, rcomb, rm4, rm6, rbgp4, rbgp6, rbgpcomb] = await Promise.all([
      conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv4
        WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1)`),
      conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv6
        WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1)`),
      conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv4
        WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1
        UNION
        SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ipv6
        WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1)`),
      conn.query(`SELECT COUNT(*) AS n FROM ipv4
        WHERE position('_' IN ASN) > 0
        AND greatest(GCD_ICMPv4, GCD_TCPv4) > 1`),
      conn.query(`SELECT COUNT(*) AS n FROM ipv6
        WHERE position('_' IN ASN) > 0
        AND greatest(GCD_ICMPv6, GCD_TCPv6) > 1`),
      conn.query(`SELECT COUNT(DISTINCT backing_prefix) AS n FROM ipv4
        WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1`),
      conn.query(`SELECT COUNT(DISTINCT backing_prefix) AS n FROM ipv6
        WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1`),
      conn.query(`SELECT COUNT(DISTINCT bp) AS n FROM (
        SELECT backing_prefix AS bp FROM ipv4 WHERE greatest(GCD_ICMPv4, GCD_TCPv4) > 1
        UNION
        SELECT backing_prefix AS bp FROM ipv6 WHERE greatest(GCD_ICMPv6, GCD_TCPv6) > 1)`),
    ]);
    const get = r => Number(r.toArray()[0].toJSON().n ?? 0);
    const n4 = get(r4), n6 = get(r6), ncomb = get(rcomb);
    const m4 = get(rm4), m6 = get(rm6);
    const bgp4 = get(rbgp4), bgp6 = get(rbgp6), bgpcomb = get(rbgpcomb);
    document.getElementById('as-stat-v4').textContent    = fmtN(n4);
    document.getElementById('as-stat-v6').textContent    = fmtN(n6);
    document.getElementById('as-stat-comb').textContent  = fmtN(ncomb);
    document.getElementById('moas-stat-v4').textContent  = fmtN(m4);
    document.getElementById('moas-stat-v6').textContent  = fmtN(m6);
    document.getElementById('moas-stat-comb').textContent = fmtN(m4 + m6);
    document.getElementById('bgp-stat-v4').textContent   = fmtN(bgp4);
    document.getElementById('bgp-stat-v6').textContent   = fmtN(bgp6);
    document.getElementById('bgp-stat-comb').textContent = fmtN(bgpcomb);
    asStatsReady = true;
    if (!resultsEl.innerHTML) asStatsSectionEl.style.display = '';
  } catch (_) { /* stats are optional */ }
}

async function initASTable(viewKey = 'latest') {
  if (!conn) return;
  await registerViews(viewKey);
  try {
    const result = await conn.query(`
      SELECT asn_val,
        COUNT(DISTINCT CASE WHEN ver = 'v4' THEN prefix END) AS n4,
        COUNT(DISTINCT CASE WHEN ver = 'v6' THEN prefix END) AS n6,
        COUNT(DISTINCT prefix) AS total
      FROM (
        SELECT unnest(string_split(ASN, '_')) AS asn_val, 'v4' AS ver, prefix
        FROM ipv4 WHERE (greatest(GCD_ICMPv4, GCD_TCPv4) > 1 OR greatest(AB_ICMPv4, AB_TCPv4, AB_DNSv4) > 2)
        UNION ALL
        SELECT unnest(string_split(ASN, '_')) AS asn_val, 'v6' AS ver, prefix
        FROM ipv6 WHERE (greatest(GCD_ICMPv6, GCD_TCPv6) > 1 OR greatest(AB_ICMPv6, AB_TCPv6, AB_DNSv6) > 2)
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
    const pageSize = 10;
    const renderRows = (slice) => slice.map(row => {
      const href = `?q=${encodeURIComponent('AS' + row.asn)}`;
      const asnName = _asnNames[row.asn];
      const asnDisplay = asnName ? `${escHtml(asnName)} (AS${escHtml(row.asn)})` : `AS${escHtml(row.asn)}`;
      return `
      <tr class="as-row" data-asn="${escHtml(row.asn)}">
        <td><a class="prefix-link" href="${href}">${asnDisplay}</a></td>
        <td class="${row.n4 ? 'count' : 'count-zero'}"><a class="row-link" href="${href}">${row.n4 ? fmtN(row.n4) : '\u2014'}</a></td>
        <td class="${row.n6 ? 'count' : 'count-zero'}"><a class="row-link" href="${href}">${row.n6 ? fmtN(row.n6) : '\u2014'}</a></td>
        <td class="count"><a class="row-link" href="${href}">${fmtN(row.total)}</a></td>
      </tr>`;}).join('');

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

async function initASStatsCompare(dateA, dateB) {
  if (!conn) return;
  const c4 = `greatest(GCD_ICMPv4, GCD_TCPv4) > 1`;
  const c6 = `greatest(GCD_ICMPv6, GCD_TCPv6) > 1`;
  const statsQueries = (v4, v6) => [
    conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
      SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ${v4} WHERE ${c4})`),
    conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
      SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ${v6} WHERE ${c6})`),
    conn.query(`SELECT COUNT(DISTINCT asn_val) AS n FROM (
      SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ${v4} WHERE ${c4}
      UNION
      SELECT unnest(string_split(ASN, '_')) AS asn_val FROM ${v6} WHERE ${c6})`),
    conn.query(`SELECT COUNT(*) AS n FROM ${v4}
      WHERE position('_' IN ASN) > 0 AND ${c4}`),
    conn.query(`SELECT COUNT(*) AS n FROM ${v6}
      WHERE position('_' IN ASN) > 0 AND ${c6}`),
    conn.query(`SELECT COUNT(DISTINCT backing_prefix) AS n FROM ${v4} WHERE ${c4}`),
    conn.query(`SELECT COUNT(DISTINCT backing_prefix) AS n FROM ${v6} WHERE ${c6}`),
    conn.query(`SELECT COUNT(DISTINCT bp) AS n FROM (
      SELECT backing_prefix AS bp FROM ${v4} WHERE ${c4}
      UNION
      SELECT backing_prefix AS bp FROM ${v6} WHERE ${c6})`),
  ];
  try {
    const [resA, resB] = await Promise.all([
      Promise.all(statsQueries('ipv4_a', 'ipv6_a')),
      Promise.all(statsQueries('ipv4_b', 'ipv6_b')),
    ]);
    const get = r => Number(r.toArray()[0].toJSON().n ?? 0);
    const vA = resA.map(get);
    const vB = resB.map(get);
    // indices: 0=as4, 1=as6, 2=ascomb, 3=moas4, 4=moas6, 5=bgp4, 6=bgp6, 7=bgpcomb
    const setDelta = (id, b, a) => {
      const d = b - a;
      const ds = d === 0 ? '' :
        `<span class="delta ${d > 0 ? 'delta-pos' : 'delta-neg'}">${d > 0 ? '+' : ''}${fmtN(d)}</span>`;
      document.getElementById(id).innerHTML = `${fmtN(b)}${ds ? ' ' + ds : ''}`;
    };
    setDelta('as-stat-v4',     vB[0], vA[0]);
    setDelta('as-stat-v6',     vB[1], vA[1]);
    setDelta('as-stat-comb',   vB[2], vA[2]);
    setDelta('moas-stat-v4',   vB[3], vA[3]);
    setDelta('moas-stat-v6',   vB[4], vA[4]);
    setDelta('moas-stat-comb', vB[3]+vB[4], vA[3]+vA[4]);
    setDelta('bgp-stat-v4',    vB[5], vA[5]);
    setDelta('bgp-stat-v6',    vB[6], vA[6]);
    setDelta('bgp-stat-comb',  vB[7], vA[7]);
    asStatsReady = true;
  } catch (_) { /* optional */ }
}

async function initASTableCompare(dateA, dateB) {
  if (!conn) return;
  const c4 = `(greatest(GCD_ICMPv4, GCD_TCPv4) > 1 OR greatest(AB_ICMPv4, AB_TCPv4, AB_DNSv4) > 2)`;
  const c6 = `(greatest(GCD_ICMPv6, GCD_TCPv6) > 1 OR greatest(AB_ICMPv6, AB_TCPv6, AB_DNSv6) > 2)`;
  const asnQuery = (v4, v6) => conn.query(`
    SELECT asn_val,
      COUNT(DISTINCT CASE WHEN ver = 'v4' THEN prefix END) AS n4,
      COUNT(DISTINCT CASE WHEN ver = 'v6' THEN prefix END) AS n6,
      COUNT(DISTINCT prefix) AS total
    FROM (
      SELECT unnest(string_split(ASN, '_')) AS asn_val, 'v4' AS ver, prefix
      FROM ${v4} WHERE ${c4}
      UNION ALL
      SELECT unnest(string_split(ASN, '_')) AS asn_val, 'v6' AS ver, prefix
      FROM ${v6} WHERE ${c6}
    ) GROUP BY asn_val`);
  try {
    const [resA, resB] = await Promise.all([
      asnQuery('ipv4_a', 'ipv6_a'),
      asnQuery('ipv4_b', 'ipv6_b'),
    ]);
    const toMap = res => new Map(res.toArray().map(r => {
      const j = r.toJSON();
      return [String(j.asn_val), { n4: Number(j.n4??0), n6: Number(j.n6??0), total: Number(j.total??0) }];
    }));
    const mapA = toMap(resA), mapB = toMap(resB);
    const rows = [...mapB.entries()].map(([asn, b]) => {
      const a = mapA.get(asn) ?? { n4: 0, n6: 0, total: 0 };
      return { asn, n4: b.n4, n6: b.n6, total: b.total,
               d4: b.n4 - a.n4, d6: b.n6 - a.n6, dtotal: b.total - a.total };
    });
    rows.sort((a, b) => b.total - a.total || b.dtotal - a.dtotal);
    if (!rows.length) return;

    const fmtDelta = (val, d) => {
      const ds = d === 0 ? '' :
        `<span class="delta ${d > 0 ? 'delta-pos' : 'delta-neg'}">${d > 0 ? '+' : ''}${fmtN(d)}</span>`;
      return (val ? fmtN(val) : '\u2014') + (ds ? ' ' + ds : '');
    };
    const id = `pg${++pgCounter}`;
    const pageSize = 10;
    const renderRows = (slice) => slice.map(row => {
      const href = `?q=${encodeURIComponent('AS' + row.asn)}`;
      return `
      <tr class="as-row" data-asn="${escHtml(row.asn)}">
        <td><a class="prefix-link" href="${href}">AS${escHtml(row.asn)}</a></td>
        <td class="${row.n4 ? 'count' : 'count-zero'}"><a class="row-link" href="${href}">${fmtDelta(row.n4, row.d4)}</a></td>
        <td class="${row.n6 ? 'count' : 'count-zero'}"><a class="row-link" href="${href}">${fmtDelta(row.n6, row.d6)}</a></td>
        <td class="count"><a class="row-link" href="${href}">${fmtDelta(row.total, row.dtotal)}</a></td>
      </tr>`;}).join('');

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
  } catch (_) { /* optional */ }
}

function initShowcase(viewKey = 'latest') {
  if (!chartData) return;
  if (resultsEl.innerHTML) return;
  const showcaseEl = document.getElementById('showcase-section');
  if (!showcaseEl) return;

  let idx;
  if (viewKey === 'latest') {
    idx = chartData.dates.length - 1;
  } else {
    const dateStr = viewKey.replace(/\//g, '-');
    idx = chartData.dates.indexOf(dateStr);
    if (idx === -1) { showcaseEl.style.display = 'none'; return; }
  }

  const dateLabel = chartData.dates[idx];
  document.getElementById('showcase-title').textContent =
    `Anycast prefix counts \u2014 ${dateLabel}`;

  // Warn when the history file lags behind today's date (common in "latest" mode
  // before the daily stats-history.json update is deployed).
  const noteEl = document.getElementById('showcase-date-note');
  if (noteEl) {
    const today = new Date().toISOString().slice(0, 10);
    if (viewKey === 'latest' && dateLabel < today) {
      noteEl.textContent = `\u2139\uFE0F Prefix count history is updated daily and currently reflects ${dateLabel}. Network statistics below use the latest available census file.`;
      noteEl.style.display = '';
    } else {
      noteEl.style.display = 'none';
    }
  }

  const set = (id, key) => {
    const el = document.getElementById(id);
    if (el) el.textContent = fmtN(chartData[key]?.[idx] ?? 0);
  };
  set('sc-vp4',       'vp4');        set('sc-vp6',       'vp6');
  set('sc-gcd-icmp4', 'gcd_icmp4'); set('sc-gcd-icmp6', 'gcd_icmp6');
  set('sc-gcd-tcp4',  'gcd_tcp4');  set('sc-gcd-tcp6',  'gcd_tcp6');
  set('sc-ab-icmp4',  'ab_icmp4');  set('sc-ab-icmp6',  'ab_icmp6');
  set('sc-ab-tcp4',   'ab_tcp4');   set('sc-ab-tcp6',   'ab_tcp6');
  set('sc-ab-dns4',   'ab_dns4');   set('sc-ab-dns6',   'ab_dns6');

  showcaseEl.style.display = '';
}

function initShowcaseCompare(dateA, dateB) {
  if (!chartData) return;
  const showcaseEl = document.getElementById('showcase-section');
  if (!showcaseEl) return;

  const idxA = chartData.dates.indexOf(dateA);
  const idxB = chartData.dates.indexOf(dateB);
  if (idxA === -1 || idxB === -1) { showcaseEl.style.display = 'none'; return; }

  document.getElementById('showcase-title').textContent =
    `Anycast prefix counts \u2014 ${dateB} vs ${dateA}`;

  const setDelta = (id, key) => {
    const el = document.getElementById(id);
    if (!el) return;
    const vA = chartData[key]?.[idxA] ?? 0;
    const vB = chartData[key]?.[idxB] ?? 0;
    const d = vB - vA;
    const ds = d === 0 ? '' :
      `<span class="delta ${d > 0 ? 'delta-pos' : 'delta-neg'}">${d > 0 ? '+' : ''}${fmtN(d)}</span>`;
    el.innerHTML = `${fmtN(vB)}${ds ? ' ' + ds : ''}`;
  };
  setDelta('sc-vp4',       'vp4');        setDelta('sc-vp6',       'vp6');
  setDelta('sc-gcd-icmp4', 'gcd_icmp4'); setDelta('sc-gcd-icmp6', 'gcd_icmp6');
  setDelta('sc-gcd-tcp4',  'gcd_tcp4');  setDelta('sc-gcd-tcp6',  'gcd_tcp6');
  setDelta('sc-ab-icmp4',  'ab_icmp4');  setDelta('sc-ab-icmp6',  'ab_icmp6');
  setDelta('sc-ab-tcp4',   'ab_tcp4');   setDelta('sc-ab-tcp6',   'ab_tcp6');
  setDelta('sc-ab-dns4',   'ab_dns4');   setDelta('sc-ab-dns6',   'ab_dns6');

  showcaseEl.style.display = '';
}

async function initChart() {
  try {
    const res = await fetch(baseUrl + `stats-history.json`, { cache: 'no-cache' });
    if (!res.ok) return;
    chartData = await res.json();
    if (!resultsEl.innerHTML) {
      try { initShowcase(selectedViewKey()); } catch (_) {}
      document.getElementById('chart-section').style.display = '';
    }
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

  // Visible range
  const vStart = Math.max(0, chartZoomStart);
  const vEnd   = chartZoomEnd < 0 ? n - 1 : Math.min(chartZoomEnd, n - 1);
  const nVis   = vEnd - vStart + 1;

  // Max of visible series within visible range
  let maxVal = 0;
  for (const s of series) {
    if (chartHidden.has(s.key)) continue;
    for (let i = vStart; i <= vEnd; i++) {
      const v = chartData[s.key][i];
      if (v != null && v > maxVal) maxVal = v;
    }
  }

  const yTicks = niceYTicks(maxVal, 5);
  const yMax   = yTicks.length ? yTicks[yTicks.length - 1] : 1;

  const xPos = i => PAD.left + (nVis > 1 ? ((i - vStart) / (nVis - 1)) * plotW : plotW / 2);
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
  const xStep = Math.max(1, Math.round(120 / (plotW / nVis)));
  const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const fmtXLabel = d => {
    if (nVis <= 90) {
      // "Mar 15" — useful when zoomed to 1W / 1M / 3M
      const [, m, day] = d.split('-');
      return `${MON[+m - 1]} ${+day}`;
    }
    return d.slice(0, 7); // "2026-03" for wide views
  };
  ctx.fillStyle = '#8b949e';
  ctx.textAlign = 'center'; ctx.textBaseline = 'top';
  for (let i = vStart; i <= vEnd; i += xStep) {
    const x = xPos(i);
    ctx.strokeStyle = '#30363d'; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(x, PAD.top + plotH); ctx.lineTo(x, PAD.top + plotH + 4); ctx.stroke();
    ctx.fillText(fmtXLabel(dates[i]), x, PAD.top + plotH + 8);
  }

  // Series lines — clipped to plot area so lines never bleed outside the axes
  ctx.save();
  ctx.beginPath();
  ctx.rect(PAD.left, PAD.top, plotW, plotH);
  ctx.clip();

  for (const s of series) {
    if (chartHidden.has(s.key)) continue;
    const vals = chartData[s.key];
    ctx.strokeStyle = s.color;
    ctx.lineWidth = 1.5;
    ctx.lineJoin = 'round';
    ctx.beginPath();
    let moved = false;
    for (let i = vStart; i <= vEnd; i++) {
      if (vals[i] == null) { moved = false; continue; }
      if (!moved) { ctx.moveTo(xPos(i), yPos(vals[i])); moved = true; }
      else          ctx.lineTo(xPos(i), yPos(vals[i]));
    }
    ctx.stroke();
  }

  ctx.restore();

  // Drag-select range overlay
  if (chartDragStartX !== null && chartDragCurrentX !== null) {
    const rx0 = PAD.left + Math.max(0, Math.min(plotW, Math.min(chartDragStartX, chartDragCurrentX)));
    const rx1 = PAD.left + Math.max(0, Math.min(plotW, Math.max(chartDragStartX, chartDragCurrentX)));
    if (rx1 - rx0 > 2) {
      ctx.fillStyle = 'rgba(31,111,235,0.12)';
      ctx.fillRect(rx0, PAD.top, rx1 - rx0, plotH);
      ctx.strokeStyle = 'rgba(88,166,255,0.5)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(rx0, PAD.top); ctx.lineTo(rx0, PAD.top + plotH);
      ctx.moveTo(rx1, PAD.top); ctx.lineTo(rx1, PAD.top + plotH);
      ctx.stroke();
    }
  }

  // Hover crosshair + tooltip (only if within visible range)
  if (chartHoverIdx != null && chartHoverIdx >= vStart && chartHoverIdx <= vEnd) {
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
  // Always continue until the tick is >= max, so yMax is never below the data
  for (let v = step; ; v += step) {
    result.push(Math.round(v * 1e6) / 1e6);
    if (v >= max) break;
  }
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

  // Helper: current visible range
  const visRange = () => ({
    s: Math.max(0, chartZoomStart),
    e: chartZoomEnd < 0 ? n - 1 : Math.min(chartZoomEnd, n - 1),
  });

  // ── Hover ──────────────────────────────────────────────────────────────────
  canvas.addEventListener('mousemove', e => {
    if (chartDragStartX !== null) return; // handled by drag logic
    const rect  = canvas.getBoundingClientRect();
    const relX  = e.clientX - rect.left - PL;
    const plotW = rect.width - PL - PR;
    const { s, e: ve } = visRange();
    const nVis = ve - s + 1;
    chartHoverIdx = s + Math.max(0, Math.min(nVis - 1, Math.round((relX / plotW) * (nVis - 1))));
    drawChart();
  });

  canvas.addEventListener('mouseleave', () => {
    if (chartDragStartX !== null) return;
    chartHoverIdx = null;
    drawChart();
  });

  // ── Scroll-to-zoom ─────────────────────────────────────────────────────────
  canvas.addEventListener('wheel', e => {
    e.preventDefault();
    const rect  = canvas.getBoundingClientRect();
    const relX  = e.clientX - rect.left - PL;
    const plotW = rect.width - PL - PR;
    const frac  = Math.max(0, Math.min(1, relX / plotW));
    const { s, e: ve } = visRange();
    const nVis  = ve - s + 1;
    const factor = e.deltaY > 0 ? 1.25 : 0.8; // scroll down = zoom out
    const newVis = Math.min(n, Math.max(7, Math.round(nVis * factor)));
    const pivot  = s + Math.round(frac * (nVis - 1));
    let ns = Math.round(pivot - frac * (newVis - 1));
    let ne = ns + newVis - 1;
    if (ns < 0)     { ns = 0; ne = Math.min(n - 1, newVis - 1); }
    if (ne > n - 1) { ne = n - 1; ns = Math.max(0, n - newVis); }
    chartZoomStart = ns; chartZoomEnd = ne;
    clearRangeBtnActive();
    drawChart();
  }, { passive: false });

  // ── Drag-to-select-range ───────────────────────────────────────────────────
  let isDragging = false;

  canvas.addEventListener('mousedown', e => {
    if (e.button !== 0) return;
    const rect = canvas.getBoundingClientRect();
    const relX = e.clientX - rect.left - PL;
    if (relX < 0 || relX > rect.width - PL - PR) return;
    chartDragStartX   = relX;
    chartDragCurrentX = relX;
    isDragging        = false;
    chartHoverIdx     = null;
  });

  window.addEventListener('mousemove', e => {
    if (chartDragStartX === null) return;
    const rect  = canvas.getBoundingClientRect();
    const relX  = e.clientX - rect.left - PL;
    chartDragCurrentX = relX;
    if (Math.abs(chartDragCurrentX - chartDragStartX) > 3) isDragging = true;
    drawChart();
  });

  window.addEventListener('mouseup', () => {
    if (chartDragStartX === null) return;
    if (isDragging && Math.abs(chartDragCurrentX - chartDragStartX) > 10) {
      const rect  = canvas.getBoundingClientRect();
      const plotW = rect.width - PL - PR;
      const { s, e: ve } = visRange();
      const nVis  = ve - s + 1;
      const x0    = Math.max(0, Math.min(chartDragStartX,   chartDragCurrentX));
      const x1    = Math.max(0, Math.min(Math.max(chartDragStartX, chartDragCurrentX), plotW));
      const i0    = s + Math.round((x0 / plotW) * (nVis - 1));
      const i1    = s + Math.round((x1 / plotW) * (nVis - 1));
      if (i1 > i0 + 1) {
        chartZoomStart = Math.max(0, i0);
        chartZoomEnd   = Math.min(n - 1, i1);
        clearRangeBtnActive();
      }
    }
    chartDragStartX = null; chartDragCurrentX = null; isDragging = false;
    drawChart();
  });

  // Prevent text selection on drag
  canvas.addEventListener('selectstart', e => e.preventDefault());

  // ── Double-click to reset zoom ─────────────────────────────────────────────
  canvas.addEventListener('dblclick', () => {
    chartZoomStart = 0; chartZoomEnd = -1;
    document.querySelectorAll('.chart-range-btn').forEach(b =>
      b.classList.toggle('active', b.dataset.range === '0'));
    drawChart();
  });

  // ── Preset range buttons ───────────────────────────────────────────────────
  document.getElementById('chart-range-btns').addEventListener('click', e => {
    const btn = e.target.closest('.chart-range-btn');
    if (!btn) return;
    const days = parseInt(btn.dataset.range);
    if (days === 0) {
      chartZoomStart = 0; chartZoomEnd = -1;
    } else {
      const latest = new Date(chartData.dates[n - 1]);
      latest.setDate(latest.getDate() - days);
      const cutStr = latest.toISOString().slice(0, 10);
      const idx = chartData.dates.findIndex(d => d >= cutStr);
      chartZoomStart = idx < 0 ? 0 : idx;
      chartZoomEnd   = -1;
    }
    document.querySelectorAll('.chart-range-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    drawChart();
  });

  // ── Tab (IPv4/IPv6) ────────────────────────────────────────────────────────
  tabBtns.forEach(btn => btn.addEventListener('click', () => {
    chartVer = btn.dataset.ver;
    chartHidden.clear();
    tabBtns.forEach(b => b.classList.toggle('active', b === btn));
    buildLegend();
    drawChart();
  }));

  // ── Fullscreen ─────────────────────────────────────────────────────────────
  document.getElementById('chart-fs-btn').addEventListener('click', () => {
    const section = document.getElementById('chart-section');
    if (!document.fullscreenElement) section.requestFullscreen();
    else document.exitFullscreen();
  });

  document.addEventListener('fullscreenchange', () => {
    const canvas = document.getElementById('chart-canvas');
    canvas.style.height = document.fullscreenElement ? '0' : '260px';
    setTimeout(drawChart, 50);
  });

  let resizeTimer;
  window.addEventListener('resize', () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(drawChart, 100);
  });
}

function clearRangeBtnActive() {
  document.querySelectorAll('.chart-range-btn').forEach(b => b.classList.remove('active'));
}

initChart();
