// ── DuckDB-WASM bootstrap ──────────────────────────────────────────────────
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

const statusEl   = document.getElementById('status');
const resultsEl  = document.getElementById('results');
const searchBtn  = document.getElementById('search-btn');
const inputEl    = document.getElementById('prefix-input');
const examplesEl = document.getElementById('examples');
const dateInput  = document.getElementById('date-input');
const whenLatest = document.getElementById('when-latest');
const whenDate   = document.getElementById('when-date');

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
dateInput.max = new Date().toISOString().slice(0, 10);

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

  // Auto-run lookup if URL has query params
  const initParams = new URLSearchParams(window.location.search);
  const initQ    = initParams.get('q');
  const initDate = initParams.get('date');
  if (initQ) {
    inputEl.value = initQ;
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
  const q    = params.get('q');
  const date = params.get('date');
  if (q) {
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
    inputEl.value = '';
    resultsEl.innerHTML = '';
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
  document.getElementById('chart-section').style.display = 'none';

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
    wirePrefixListClicks(viewKey, dateLabel);
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
    wirePrefixListClicks(viewKey, dateLabel);
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
    wirePrefixListClicks(viewKey, dateLabel);
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

function renderPrefixList(rows, searchTerm, dateLabel) {
  const n = v => Number(v ?? 0);
  const cap = 500;
  const shown = rows.slice(0, cap);
  const moreNote = rows.length > cap
    ? `<p class="loc-note">Showing ${cap} of ${rows.length} prefixes.</p>` : '';

  const tbody = shown.map(row => {
    const ab_max  = Math.max(n(row.ab1), n(row.ab2), n(row.ab3));
    const gcd_max = Math.max(n(row.gcd1), n(row.gcd2));
    const conf    = getConfidence(ab_max, gcd_max);
    const nloc    = n(row.nloc);
    const asns    = (row.ASN ?? '').toString().split('_').filter(Boolean);
    return `
      <tr class="pl-row" data-prefix="${escHtml(row.prefix)}">
        <td><span class="ver-badge">${row.ver}</span></td>
        <td><span class="prefix-link">${escHtml(row.prefix)}</span>${row.partial ? ' <span class="tag tag-warn" title="Partial anycast: this /24 contains both unicast and anycast addresses">partial</span>' : ''}</td>
        <td><span class="confidence ${conf.cls}">${conf.label}</span></td>
        <td class="${ab_max  ? 'count' : 'count-zero'}">AB&nbsp;${ab_max}</td>
        <td class="${gcd_max ? 'count' : 'count-zero'}">GCD&nbsp;${gcd_max}</td>
        <td>${nloc}</td>
        <td>${asns.map(a => `<span class="tag">AS${escHtml(a)}</span>`).join(' ')}</td>
      </tr>`;
  }).join('');

  return `
    <div class="card">
      <div class="card-title">${rows.length} anycast prefix${rows.length !== 1 ? 'es' : ''} in ${escHtml(searchTerm)} — ${escHtml(dateLabel)}</div>
      ${moreNote}
      <p class="loc-note">Click a prefix to see its full details.</p>
      <table class="prefix-list">
        <thead><tr>
          <th></th><th>Prefix</th><th>Confidence</th>
          <th>AB sites</th><th>GCD sites</th><th>Loc</th><th>ASN(s)</th>
        </tr></thead>
        <tbody>${tbody}</tbody>
      </table>
    </div>`;
}

function wirePrefixListClicks(viewKey, dateLabel) {
  document.querySelectorAll('.pl-row').forEach(row => {
    row.addEventListener('click', () => {
      const prefix = row.dataset.prefix;
      inputEl.value = prefix;
      lookupPrefix(prefix, viewKey, dateLabel);
    });
  });
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
          <td class="${ab_icmp ? 'count' : 'count-zero'}">${ab_icmp}</td>
        </tr>
        <tr>
          <td>Anycast-based TCP (AB)</td>
          <td class="${ab_tcp ? 'count' : 'count-zero'}">${ab_tcp}</td>
        </tr>
        <tr>
          <td>Anycast-based DNS (AB)</td>
          <td class="${ab_dns ? 'count' : 'count-zero'}">${ab_dns}</td>
        </tr>
        <tr>
          <td>Latency-based ICMP (GCD)</td>
          <td class="${gcd_icmp ? 'count' : 'count-zero'}">${gcd_icmp}</td>
        </tr>
        <tr>
          <td>Latency-based TCP (GCD)</td>
          <td class="${gcd_tcp ? 'count' : 'count-zero'}">${gcd_tcp}</td>
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
        <span>Detected locations (${locations.length})</span>
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
  setStatus('Ready.');
  if (chartData) document.getElementById('chart-section').style.display = '';
  history.pushState(null, '', '.');
});

resultsEl.addEventListener('click', e => {
  const el = e.target.closest('[data-lookup]');
  if (!el) return;
  e.preventDefault();
  const q = el.dataset.lookup;
  inputEl.value = q;
  lookup(q);
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

document.querySelectorAll('.examples span[data-prefix]').forEach(el => {
  el.addEventListener('click', () => {
    inputEl.value = el.dataset.prefix;
    lookup(el.dataset.prefix);
  });
});

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

let chartData     = null;
let chartVer      = 'v4';
let chartHidden   = new Set();
let chartHoverIdx = null;

async function initChart() {
  try {
    const today = new Date().toISOString().slice(0, 10);
    const res = await fetch(baseUrl + `stats-history.json?v=${today}`);
    if (!res.ok) return;
    chartData = await res.json();
    if (!new URLSearchParams(window.location.search).get('q')) {
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
