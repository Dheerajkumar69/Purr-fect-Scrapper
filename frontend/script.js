/* =========================================================
   Universal Web Scraper — Frontend v4.0
   Supports:
     - Multi-engine mode  → POST /scrape/v2  (16 engines)
     - Classic quick mode → POST /scrape  (v1 compat.)
   ========================================================= */

'use strict';

// ─── Engine definitions ───────────────────────────────────
const ENGINE_DEFINITIONS = [
  { id: 'static_requests',      label: 'Static: requests',       category: 'Static',    description: 'Python requests + BS4/lxml (zero JS)' },
  { id: 'static_httpx',         label: 'Static: httpx',          category: 'Static',    description: 'async httpx HTTP/2 + html5lib' },
  { id: 'static_urllib',        label: 'Static: urllib',         category: 'Static',    description: 'stdlib urllib – zero dependencies' },
  { id: 'headless_playwright',  label: 'Headless Playwright',    category: 'Browser',   description: 'Full Chromium, waits for JS + skeleton screens' },
  { id: 'dom_interaction',      label: 'DOM Interaction',        category: 'Browser',   description: 'Scroll, paginate, expand accordions' },
  { id: 'network_observe',      label: 'Network Observer',       category: 'Browser',   description: 'Captures live JSON API payloads from browser' },
  { id: 'structured_metadata',  label: 'Structured Metadata',    category: 'Semantic',  description: 'JSON-LD, schema.org, OpenGraph, microdata' },
  { id: 'session_auth',         label: 'Session / Auth',         category: 'Auth',      description: 'Playwright login → session cookie reuse' },
  { id: 'file_data',            label: 'File & Data Sources',    category: 'Files',     description: 'PDF, CSV, Excel, XML, RSS extraction' },
  { id: 'crawl_discovery',      label: 'BFS Crawler',            category: 'Crawl',     description: 'Sitemap.xml + breadth-first site spider' },
  { id: 'search_index',         label: 'Search Index',           category: 'Index',     description: 'Whoosh full-text indexer + keyword scoring' },
  { id: 'visual_ocr',           label: 'Visual OCR',             category: 'Vision',    description: 'Screenshot → OpenCV preprocessing → Tesseract' },
  { id: 'hybrid',               label: 'Hybrid Fallback',        category: 'Meta',      description: 'Smart chain: static → headless → OCR' },
  { id: 'ai_assist',            label: 'AI Assist (opt-in)',     category: 'AI',        description: 'LLM semantic extraction – requires AI_SCRAPER_ENABLED=1' },
  { id: 'endpoint_probe',       label: 'Endpoint Probe',         category: 'Security',  description: 'API/route exposure, CORS, GraphQL, OpenAPI detection' },
  { id: 'secret_scan',          label: 'Secret Scanner',         category: 'Security',  description: 'Credential & API key leakage detection in HTML + JS' },
];

const CATEGORY_COLORS = {
  Static:   '#4ade80',
  Browser:  '#60a5fa',
  Semantic: '#a78bfa',
  Auth:     '#f97316',
  Files:    '#facc15',
  Crawl:    '#2dd4bf',
  Index:    '#fb7185',
  Vision:   '#e879f9',
  Meta:     '#94a3b8',
  AI:       '#f472b6',
  Security: '#f87171',
};

// ─── State ────────────────────────────────────────────────
let _lastResult   = null;
let _lastJobId    = null;
let _currentMode  = 'multi';
let _activeTab    = 'report';
let _logCount     = 0;

// ─── DOM refs ─────────────────────────────────────────────
const $ = id => document.getElementById(id);
const urlInput          = $('url-input');
const scrapeBtn         = $('scrape-btn');
const btnLabel          = $('btn-label');
const btnSpinner        = $('btn-spinner');
const errorBanner       = $('error-banner');
const resultSection     = $('result-section');
const jsonOutput        = $('json-output');
const summaryBar        = $('summary-bar');
const resultBadges      = $('result-badges');
const confidencePanel   = $('confidence-panel');
const confScoreVal      = $('conf-score-val');
const confMeta          = $('conf-meta');
const confBarFill       = $('conf-bar-fill');
const engineTableWrap   = $('engine-table-wrap');
const endpointPanel     = $('endpoint-panel');
const epSummaryBadges   = $('ep-summary-badges');
const epFilterBar       = $('ep-filter-bar');
const epTableWrap       = $('ep-table-wrap');
const epCopyJsonBtn     = $('ep-copy-json-btn');
const epDownloadCsvBtn  = $('ep-download-csv-btn');
const secretsPanel      = $('secrets-panel');
const securityBadge     = $('security-badge');
const crawlBadge        = $('crawl-badge');
const viewHtmlBtn       = $('view-html-btn');
const copyBtn           = $('copy-btn');
const downloadBtn       = $('download-btn');
const outputLabel       = $('output-label');
const multiConfig       = $('multi-config');
const classicConfig     = $('classic-config');
const engineCheckboxes  = $('engine-checkboxes');
const selectAllBtn      = $('select-all-engines');
const deselectAllBtn    = $('deselect-all-engines');
// progress
const progressSection   = $('progress-section');
const progressPhase     = $('progress-phase-label');
const progressJobId     = $('progress-job-id');
const progBarFill       = $('prog-bar-fill');
const progPctLabel      = $('prog-pct-label');
const phasePipeline     = $('phase-pipeline');
const engineLiveGrid    = $('engine-live-grid');
const liveLog           = $('live-log');
const liveLogCount      = $('live-log-count');
const cancelJobBtn      = $('cancel-job-btn');

// ─── Bootstrap ────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  renderEngineCheckboxes();
  bindModeRadios();
  bindCopyDownload();
  bindScrapeButton();
  bindSelectAll();
  bindClassicCheckboxes();
  bindTabs();
  cancelJobBtn.addEventListener('click', handleCancelJob);
  loadServerCapabilities();

  // Global CMD+K shortcut to focus input
  document.addEventListener('keydown', e => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      e.preventDefault();
      urlInput.focus();
    }
  });
});

// ─── Tab switching ────────────────────────────────────────
function bindTabs() {
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.tab));
  });
}

function switchTab(tabId) {
  _activeTab = tabId;
  // Deactivate all
  document.querySelectorAll('.tab-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.tab === tabId);
    b.setAttribute('aria-selected', b.dataset.tab === tabId ? 'true' : 'false');
  });
  document.querySelectorAll('.tab-pane').forEach(p => {
    p.classList.toggle('active', p.id === `tab-${tabId}`);
    p.classList.toggle('hidden', p.id !== `tab-${tabId}`);
  });
}

function setTabBadge(badgeEl, count, cls) {
  if (!count) { badgeEl.classList.add('hidden'); return; }
  badgeEl.textContent = count;
  badgeEl.className = `tab-badge ${cls || ''}`;
  badgeEl.classList.remove('hidden');
}

// ─── Server capability detection ──────────────────────────
// Called once on load. Greys out engine chips that require capabilities
// (tesseract, AI_SCRAPER_ENABLED) that are not available on this server.
function loadServerCapabilities() {
  fetch('/health')
    .then(r => r.json())
    .then(h => {
      if (!h.capabilities?.ai_assist) {
        _disableEngineChip(
          'ai_assist',
          'AI Assist is disabled on this server. Set AI_SCRAPER_ENABLED=1 to enable.'
        );
      }
      if (!h.capabilities?.ocr) {
        _disableEngineChip(
          'visual_ocr',
          'OCR unavailable: tesseract not found in PATH. Install tesseract-ocr to enable.'
        );
      }
    })
    .catch(() => {}); // degrade silently if /health does not expose capabilities
}

function _disableEngineChip(engineId, tooltipMsg) {
  const lbl = document.querySelector(`[data-engine-id="${engineId}"]`);
  if (!lbl) return;
  const cb = lbl.querySelector('input[type=checkbox]');
  if (cb) { cb.disabled = true; cb.checked = false; }
  lbl.classList.add('engine-chip-disabled');
  lbl.title = tooltipMsg;
}

// ─── Engine checkbox grid ─────────────────────────────────
function renderEngineCheckboxes() {
  engineCheckboxes.innerHTML = '';
  const byCategory = {};
  for (const eng of ENGINE_DEFINITIONS) {
    (byCategory[eng.category] = byCategory[eng.category] || []).push(eng);
  }
  for (const [cat, engines] of Object.entries(byCategory)) {
    const group = document.createElement('div');
    group.className = 'engine-group';
    const header = document.createElement('div');
    header.className = 'engine-group-header';
    header.style.color = CATEGORY_COLORS[cat] || '#94a3b8';
    header.textContent = cat;
    group.appendChild(header);
    for (const eng of engines) {
      const lbl = document.createElement('label');
      lbl.className = 'engine-chip';
      lbl.title = eng.description;
      lbl.dataset.engineId = eng.id;
      lbl.innerHTML = `
        <input type="checkbox" name="engine" value="${escHtml(eng.id)}" checked />
        <span class="engine-chip-label">${escHtml(eng.label)}</span>`;
      group.appendChild(lbl);
    }
    engineCheckboxes.appendChild(group);
  }
}

function getSelectedEngines() {
  return [...document.querySelectorAll('#engine-checkboxes input[type=checkbox]:checked')]
    .map(cb => cb.value);
}

function bindSelectAll() {
  selectAllBtn.addEventListener('click', () => {
    document.querySelectorAll('#engine-checkboxes input[type=checkbox]')
      .forEach(cb => { if (!cb.disabled) cb.checked = true; });
  });
  deselectAllBtn.addEventListener('click', () => {
    document.querySelectorAll('#engine-checkboxes input[type=checkbox]')
      .forEach(cb => cb.checked = false);
  });
  // Preset buttons
  $('preset-security-recon')?.addEventListener('click',   () => applyPreset('security_recon'));
  $('preset-content-research')?.addEventListener('click', () => applyPreset('content_research'));
}

const PRESETS = {
  security_recon: new Set([
    'endpoint_probe', 'secret_scan', 'static_requests',
    'static_httpx', 'structured_metadata', 'network_observe',
  ]),
  content_research: new Set([
    'static_requests', 'static_httpx', 'headless_playwright', 'dom_interaction',
    'structured_metadata', 'crawl_discovery', 'visual_ocr', 'ai_assist',
    'hybrid', 'search_index',
  ]),
};

function applyPreset(name) {
  const selected = PRESETS[name];
  if (!selected) return;
  document.querySelectorAll('#engine-checkboxes input[type=checkbox]')
    .forEach(cb => { if (!cb.disabled) cb.checked = selected.has(cb.value); });
}

// ─── Mode toggle ──────────────────────────────────────────
function bindModeRadios() {
  document.querySelectorAll('input[name="scrape-mode"]').forEach(radio => {
    radio.addEventListener('change', () => {
      _currentMode = radio.value;
      multiConfig.classList.toggle('hidden', _currentMode !== 'multi');
      classicConfig.classList.toggle('hidden', _currentMode !== 'classic');
    });
  });
}

// ─── Classic option checkboxes ────────────────────────────
function bindClassicCheckboxes() {
  const cssRow   = $('css-selector-row');
  const xpathRow = $('xpath-selector-row');
  document.querySelectorAll('input[name="option"]').forEach(cb => {
    cb.addEventListener('change', () => {
      if (cb.value === 'custom_css')   cssRow.classList.toggle('hidden', !cb.checked);
      if (cb.value === 'custom_xpath') xpathRow.classList.toggle('hidden', !cb.checked);
    });
  });
}

// ─── Main scrape dispatch ─────────────────────────────────
function bindScrapeButton() {
  scrapeBtn.addEventListener('click', () => {
    if (_currentMode === 'multi') handleScrapeV2();
    else                          handleScrapeV1();
  });
  urlInput.addEventListener('keydown', e => {
    if (e.key === 'Enter') scrapeBtn.click();
  });
  urlInput.addEventListener('paste', e => {
    e.preventDefault();
    const text = (e.clipboardData || window.clipboardData).getData('text').trim();
    urlInput.value = text;
  });
}

// ─── V2 multi-engine ──────────────────────────────────────
async function handleScrapeV2() {
  const url = urlInput.value.trim();
  if (!validateUrl(url)) return;

  const engines            = getSelectedEngines();
  const depth              = parseInt($('crawl-depth').value, 10);
  const timeout_per_engine = parseInt($('engine-timeout').value, 10);
  const respect_robots     = $('v2-respect-robots').checked;
  const full_crawl_mode    = $('v2-full-crawl').checked;

  setLoading(true, 'Submitting job…');
  clearResults();

  const body = { url, engines, depth, timeout_per_engine, respect_robots, full_crawl_mode };

  let jobId;
  try {
    const res  = await fetch('/scrape/v2', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();
    if (!res.ok) { showError(data.detail || `HTTP ${res.status}`); setLoading(false); return; }
    jobId = data.job_id;
    _lastJobId = jobId;
  } catch (err) {
    showError(`Network error: ${err.message}`);
    setLoading(false);
    return;
  }

  showProgressSection(jobId);
  addLogRow('info', 'queued', 0, `Job ${jobId} accepted`);

  // Start polling IMMEDIATELY as a safety net — it always works
  startPolling(jobId);
  // Try SSE on top for faster real-time updates
  trySSE(jobId);
}

// ─── SSE streaming (enhancement, not required) ────────────
function trySSE(jobId) {
  let es;
  try { es = new EventSource(`/jobs/${jobId}/stream`); }
  catch { return; }   // SSE unavailable — polling will handle it

  let lastDataAt = Date.now();
  const STALE_TIMEOUT_MS = 20000;  // close SSE if no data for 20s

  // Heartbeat: if SSE goes silent, close it — polling is our safety net
  const heartbeat = setInterval(() => {
    if (Date.now() - lastDataAt > STALE_TIMEOUT_MS) {
      clearInterval(heartbeat);
      es.close();
      addLogRow('info', 'sse', 0, 'SSE went silent — relying on polling');
    }
  }, 5000);

  es.onmessage = e => {
    lastDataAt = Date.now();
    let ev;
    try { ev = JSON.parse(e.data); } catch { return; }

    if (ev.phase === 'done' || ev.phase === '__DONE__') {
      clearInterval(heartbeat);
      es.close();
      updateProgressBar(1, 'done');
      if (ev.status === 'failed') { onJobFailed(ev.error || 'Job failed'); return; }
      fetchFinalResult(jobId);
      return;
    }

    handleProgressEvent(ev, jobId, null);  // null es — don't let handler close it
  };

  es.onerror = () => {
    clearInterval(heartbeat);
    es.close();
    // polling is already running — no action needed
  };
}

// ─── Polling (always-on safety net) ───────────────────────
function startPolling(jobId) {
  const INTERVAL = 2000;
  let stopped = false;

  const tick = async () => {
    if (stopped || _lastJobId !== jobId) return;
    try {
      const res  = await fetch(`/jobs/${jobId}`);
      const data = await res.json();
      const pct  = data.progress ?? 0;
      const phase = data.phase || data.status || '';
      updateProgressBar(pct, phase);
      if (pct > 0) {
        addLogRow('info', phase, pct, `status=${data.status} progress=${Math.round(pct*100)}%`);
      }
      if (data.status === 'done') {
        stopped = true;
        onJobDone(data.result || {}, jobId);
      } else if (data.status === 'failed') {
        stopped = true;
        onJobFailed(data.error || 'Unknown error');
      } else {
        setTimeout(tick, INTERVAL);
      }
    } catch (err) {
      addLogRow('err', 'poll', 0, `Poll error: ${err.message}`);
      if (!stopped) setTimeout(tick, INTERVAL * 2);
    }
  };
  setTimeout(tick, INTERVAL);
}

// ─── Handle a single SSE/stream event ────────────────────
function handleProgressEvent(ev, jobId, es) {
  const phase = ev.phase || '';
  const pct   = ev.pct   ?? 0;
  const msg   = ev.engine_id
    ? `[${ev.engine_id}] ${ev.message || ''}`
    : (ev.message || phase);

  if (phase === 'done' || phase === '__DONE__') {
    if (es) es.close();
    updateProgressBar(1, 'done');
    if (ev.status === 'failed') { onJobFailed(ev.error || 'Job failed'); return; }
    fetchFinalResult(jobId);
    return;
  }

  updateProgressBar(pct, phase);
  activatePhaseStep(phase);

  if (ev.engine_id) {
    const success = ev.success;
    if (success === true)        markEngineChip(ev.engine_id, 'ok',      '');
    else if (success === false)  markEngineChip(ev.engine_id, 'err',     ev.error || '');
    else                         markEngineChip(ev.engine_id, 'running', '');
  }

  const lvl = ev.error ? 'err' : (pct >= 0.99 ? 'ok' : 'info');
  addLogRow(lvl, phase, pct, msg || phase);
}

async function fetchFinalResult(jobId, attempt = 0) {
  const MAX_ATTEMPTS = 120;
  if (attempt >= MAX_ATTEMPTS) {
    onJobFailed(`Timed out waiting for final result after ${MAX_ATTEMPTS}s`);
    return;
  }
  try {
    const res  = await fetch(`/jobs/${jobId}`);
    const data = await res.json();
    if (data.status === 'done' && data.result) {
      onJobDone(data.result, jobId);
    } else if (data.status === 'failed') {
      onJobFailed(data.error || 'Job failed');
    } else {
      setTimeout(() => fetchFinalResult(jobId, attempt + 1), 1000);
    }
  } catch (err) {
    onJobFailed(`Could not fetch result: ${err.message}`);
  }
}

// ─── Job complete handler ─────────────────────────────────
function onJobDone(result, jobId) {
  hideProgressSection();
  setLoading(false);
  _lastResult = result;

  if (!result.engines_used && result.engine_summary)
    result.engines_used = result.engine_summary.length;
  if (!result.engines_succeeded && result.engine_summary)
    result.engines_succeeded = result.engine_summary.filter(e => e.success).length;
  result.job_id = result.job_id || jobId;

  const merged = result.merged || result;

  renderSummaryBar(result);
  renderConfidencePanel(result);
  renderReportFields(merged);
  renderEndpointTable(merged);
  renderSecretsTable(merged);
  renderCrawlMap(merged);

  // Update tab badges
  const secrets = merged.leaked_secrets || [];
  const critHigh = secrets.filter(s => s.severity === 'CRITICAL' || s.severity === 'HIGH').length;
  const endpoints = (merged.detected_endpoints || []).length;
  const totalSec = secrets.length + endpoints;
  setTabBadge(securityBadge, totalSec, critHigh > 0 ? 'tab-badge-danger' : 'tab-badge-warn');

  const crawlPages = (merged.pages || merged.crawl_pages || []).length;
  setTabBadge(crawlBadge, crawlPages || null, 'tab-badge-info');

  // Persistent security callout: shown whenever any security findings exist
  const anyFindings = secrets.length > 0 || endpoints > 0;
  const secCallout = $('security-callout');
  if (secCallout) secCallout.classList.toggle('hidden', !anyFindings);

  // Auto-switch to Security tab only on critical/high secrets
  if (critHigh > 0) {
    switchTab('security');
  } else {
    switchTab('report');
  }

  // Always populate raw JSON tab
  outputLabel.textContent = `Merged Result (${result.engines_succeeded ?? '-'}/${result.engines_used ?? '-'} engines)`;
  jsonOutput.textContent  = JSON.stringify(merged, null, 2);

  resultSection.classList.remove('hidden');
}

function onJobFailed(errorMsg) {
  hideProgressSection();
  setLoading(false);
  showError(`Job failed: ${errorMsg}`);
}

// ─── Cancel job ───────────────────────────────────────────
async function handleCancelJob() {
  if (!_lastJobId) return;
  try {
    const res = await fetch(`/jobs/${_lastJobId}/cancel`, { method: 'POST' });
    if (res.ok) {
      addLogRow('warn', 'cancel', 0, 'Job cancelled by user');
      onJobFailed('Cancelled by user');
    }
  } catch { /* ignore */ }
}

// ─── Summary bar ──────────────────────────────────────────
function renderSummaryBar(data) {
  const merged = data.merged || data;
  const secrets = (merged.leaked_secrets || []).length;
  const critHigh = (merged.leaked_secrets || []).filter(
    s => s.severity === 'CRITICAL' || s.severity === 'HIGH'
  ).length;

  const badges = [
    { label: 'Engines used',      val: data.engines_used      ?? '-',  cls: 'badge-blue'   },
    { label: 'Succeeded',         val: data.engines_succeeded ?? '-',  cls: 'badge-green'  },
    { label: 'Site type',         val: data.site_type ?? merged.site_type ?? '?',  cls: 'badge-purple' },
    { label: 'Total time',        val: data.total_elapsed_s != null ? `${data.total_elapsed_s.toFixed(1)}s` : '-', cls: 'badge-gray' },
  ];

  if (secrets > 0) {
    badges.push({
      label: critHigh > 0 ? 'CRITICAL secrets' : 'Secrets found',
      val: secrets,
      cls: critHigh > 0 ? 'badge-danger' : 'badge-warn',
    });
  }

  resultBadges.innerHTML = badges.map(b =>
    `<span class="badge ${b.cls}"><em>${escHtml(b.label)}</em>${escHtml(String(b.val))}</span>`
  ).join('');

  if (data.report_html_path) {
    viewHtmlBtn.classList.remove('hidden');
    viewHtmlBtn.onclick = () => window.open(`/reports/${data.job_id}/html`, '_blank');
  }

  summaryBar.classList.remove('hidden');
  resultSection.classList.remove('hidden');
}

// ─── Confidence panel (Report tab) ────────────────────────
function renderConfidencePanel(data) {
  if (data.confidence_score == null) return;

  const pct = Math.round(data.confidence_score * 100);
  confScoreVal.textContent = `${pct}%`;
  confScoreVal.style.color = pct >= 70 ? '#4ade80' : pct >= 40 ? '#facc15' : '#f87171';
  confBarFill.style.width  = `${pct}%`;
  confBarFill.style.background = pct >= 70 ? '#4ade80' : pct >= 40 ? '#facc15' : '#f87171';

  if (data.engines_used && data.engines_succeeded != null) {
    confMeta.innerHTML = `
      <span class="badge badge-blue">${escHtml(String(data.engines_used))} engines selected</span>
      <span class="badge badge-green">${escHtml(String(data.engines_succeeded))} succeeded</span>`;
  }

  if (data.engine_summary && data.engine_summary.length) {
    renderEngineTable(data.engine_summary);
  }

  confidencePanel.classList.remove('hidden');
}

// ─── Report fields panel (Report tab) ─────────────────────
function renderReportFields(merged) {
  const panel = $('report-fields-panel');
  const grid  = $('report-fields-grid');
  if (!merged) return;

  const fields = [
    { key: 'title',        label: 'Title',        icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline><line x1="16" y1="13" x2="8" y2="13"></line><line x1="16" y1="17" x2="8" y2="17"></line><polyline points="10 9 9 9 8 9"></polyline></svg>' },
    { key: 'description',  label: 'Description',  icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="2" x2="22" y2="6"></line><path d="M7.5 20.5 19 9l-4-4L3.5 16.5 2 22z"></path></svg>' },
    { key: 'language',     label: 'Language',     icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="2" y1="12" x2="22" y2="12"></line><path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"></path></svg>' },
    { key: 'page_type',    label: 'Page Type',    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"></path><line x1="7" y1="7" x2="7.01" y2="7"></line></svg>' },
    { key: 'canonical_url',label: 'Canonical URL',icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path></svg>' },
    { key: 'main_content', label: 'Main Content', icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 22h16a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2H8a2 2 0 0 0-2 2v16a2 2 0 0 1-2 2Zm0 0a2 2 0 0 1-2-2v-9c0-1.1.9-2 2-2h2"></path><path d="M18 14h-8"></path><path d="M15 18h-5"></path></svg>', truncate: 200 },
  ];

  const counts = [
    { key: 'links',    label: 'Links',    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path></svg>' },
    { key: 'images',   label: 'Images',   icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><circle cx="8.5" cy="8.5" r="1.5"></circle><polyline points="21 15 16 10 5 21"></polyline></svg>' },
    { key: 'headings', label: 'Headings', icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12h8"></path><path d="M4 18V6"></path><path d="M12 18V6"></path><path d="M21 12h-4"></path><path d="M21 18V6"></path><path d="M17 18V6"></path></svg>' },
    { key: 'forms',    label: 'Forms',    icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect><line x1="16" y1="2" x2="16" y2="6"></line><line x1="8" y1="2" x2="8" y2="6"></line><line x1="3" y1="10" x2="21" y2="10"></line></svg>' },
    { key: 'tables',   label: 'Tables',   icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><line x1="3" y1="9" x2="21" y2="9"></line><line x1="3" y1="15" x2="21" y2="15"></line><line x1="9" y1="9" x2="9" y2="21"></line><line x1="15" y1="9" x2="15" y2="21"></line></svg>' },
    { key: 'keywords', label: 'Keywords', icon: '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>' },
  ];

  let html = '';

  for (const f of fields) {
    const v = merged[f.key];
    if (!v) continue;
    const display = f.truncate && String(v).length > f.truncate
      ? escHtml(String(v).slice(0, f.truncate)) + '<span style="color:#64748b">…</span>'
      : escHtml(String(v));
    html += `<div class="rp-field">
      <span class="rp-field-label">${f.icon} ${escHtml(f.label)}</span>
      <span class="rp-field-val">${display}</span>
    </div>`;
  }

  // Count fields
  const countItems = counts
    .map(c => {
      const arr = merged[c.key];
      const n = Array.isArray(arr) ? arr.length : 0;
      if (!n) return '';
      return `<span class="rp-count-badge">${escHtml(c.icon)} ${escHtml(c.label)}: <strong>${n}</strong></span>`;
    })
    .filter(Boolean)
    .join('');

  if (countItems) {
    html += `<div class="rp-field rp-field-counts">${countItems}</div>`;
  }

  if (!html) return;
  grid.innerHTML = html;
  panel.classList.remove('hidden');
}

// ─── Endpoint probe table (Security tab) ──────────────────
let _epAllEndpoints = [];

function renderEndpointTable(data) {
  const raw = data.detected_endpoints;
  if (!raw || !raw.length) {
    endpointPanel.classList.add('hidden');
    _updateSecurityEmpty(data);
    return;
  }
  _epAllEndpoints = raw;

  const summary = data.endpoint_probe_summary || {};
  const riskOrder = { HIGH: 0, MEDIUM: 1, LOW: 2, INFO: 3 };

  // Summary badges
  const badges = [];
  if (summary.openapi_discovered && summary.openapi_url) {
    badges.push(`<span class="ep-ui-badge ep-badge-green"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline><line x1="16" y1="13" x2="8" y2="13"></line><line x1="16" y1="17" x2="8" y2="17"></line><polyline points="10 9 9 9 8 9"></polyline></svg> OpenAPI: ${escHtml(summary.openapi_url)}</span>`);
  }
  if (summary.graphql_discovered && summary.graphql_url) {
    badges.push(`<span class="ep-ui-badge ep-badge-purple"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path><polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline><line x1="12" y1="22.08" x2="12" y2="12"></line></svg> GraphQL: ${escHtml(summary.graphql_url)}</span>`);
  }
  if (summary.cors_exposed_count) {
    badges.push(`<span class="ep-ui-badge ep-badge-red"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> CORS Wildcard: ${escHtml(String(summary.cors_exposed_count))}</span>`);
  }
  if (summary.risk_summary) {
    const rs = summary.risk_summary;
    if (rs.high_count)   badges.push(`<span class="ep-ui-badge ep-badge-red">HIGH: ${escHtml(String(rs.high_count))}</span>`);
    if (rs.medium_count) badges.push(`<span class="ep-ui-badge ep-badge-orange">MEDIUM: ${escHtml(String(rs.medium_count))}</span>`);
    if (rs.low_count)    badges.push(`<span class="ep-ui-badge ep-badge-green">LOW: ${escHtml(String(rs.low_count))}</span>`);
  }
  if (summary.js_files_analyzed) {
    badges.push(`<span class="ep-ui-badge ep-badge-gray">JS files: ${escHtml(String(summary.js_files_analyzed))}</span>`);
  }
  epSummaryBadges.innerHTML = badges.join('');

  // Filter bar
  const filters = ['ALL', 'HIGH', 'MEDIUM', 'LOW', 'INFO'];
  const filterColors = { ALL: '#e2e8f0', HIGH: '#f87171', MEDIUM: '#fb923c', LOW: '#4ade80', INFO: '#94a3b8' };
  let activeFilter = 'ALL';
  epFilterBar.innerHTML = filters
    .map(f => `<button class="ep-filter-btn${f === 'ALL' ? ' active' : ''}" data-risk="${escHtml(f)}"
      style="color:${filterColors[f] || '#e2e8f0'}">${escHtml(f)}</button>`)
    .join('');

  function _buildTable(filter) {
    const eps = filter === 'ALL' ? raw : raw.filter(e => e.risk_level === filter);
    if (!eps.length) {
      epTableWrap.innerHTML = `<p style="color:#94a3b8;padding:.75rem 0">No endpoints match this filter.</p>`;
      return;
    }
    const methodColor = { GET:'#4ade80', POST:'#60a5fa', PUT:'#f97316', DELETE:'#f87171',
                          PATCH:'#a78bfa', WS:'#2dd4bf', GRAPHQL:'#e879f9' };
    const rowHtml = eps.slice(0, 200).map(ep => {
      const rl = (ep.risk_level || 'INFO').toUpperCase();
      const authHtml = ep.auth_required === false
        ? '<span style="color:var(--danger)"><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:middle;margin-right:2px;"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg> Open</span>'
        : ep.auth_required === true
          ? '<span style="color:var(--success)"><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:middle;margin-right:2px;"><polyline points="20 6 9 17 4 12"></polyline></svg> Auth</span>'
          : '<span style="color:var(--text-muted)">?</span>';
      const corsHtml = ep.cors_permissive
        ? '<span style="color:var(--warning)"><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:middle;margin-right:2px;"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="8" x2="12" y2="12"></line><line x1="12" y1="16" x2="12.01" y2="16"></line></svg> Wildcard</span>'
        : '&#8212;';
      const scHtml = ep.status_code
        ? `<span style="color:${ep.status_code === 200 ? '#4ade80' : ep.status_code >= 400 ? '#fb923c' : '#94a3b8'}">${escHtml(String(ep.status_code))}</span>`
        : '&#8212;';
      return `<tr>
        <td><span class="ep-ui-url" title="${escHtml(ep.url)}">${escHtml(ep.url)}</span></td>
        <td><span style="font-family:monospace;font-size:.75rem;color:${methodColor[ep.method]||'#94a3b8'}">${escHtml(ep.method||'GET')}</span></td>
        <td style="color:#94a3b8;font-size:.75rem">${escHtml((ep.source || '').replace(/_/g,' '))}</td>
        <td>${authHtml}</td>
        <td>${corsHtml}</td>
        <td>${scHtml}</td>
        <td><span class="ep-risk-badge ep-risk-${rl.toLowerCase()}">${escHtml(rl)}</span></td>
      </tr>`;
    }).join('');
    const moreNote = eps.length > 200
      ? `<tr><td colspan="7" style="color:#94a3b8;font-style:italic;padding:.5rem .7rem">+${escHtml(String(eps.length-200))} more — see JSON tab</td></tr>`
      : '';
    epTableWrap.innerHTML = `
      <table class="ep-ui-table">
        <thead><tr><th>URL</th><th>Method</th><th>Source</th><th>Auth</th><th>CORS</th><th>Status</th><th>Risk</th></tr></thead>
        <tbody>${rowHtml}${moreNote}</tbody>
      </table>`;
  }

  _buildTable('ALL');
  epFilterBar.querySelectorAll('.ep-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      epFilterBar.querySelectorAll('.ep-filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeFilter = btn.dataset.risk;
      _buildTable(activeFilter);
    });
  });

  epCopyJsonBtn.onclick = () => navigator.clipboard.writeText(JSON.stringify(_epAllEndpoints, null, 2)).catch(() => {});
  epDownloadCsvBtn.onclick = () => {
    const header = 'url,method,source,auth_required,cors_permissive,status_code,risk_level\n';
    const rows = _epAllEndpoints.map(e =>
      [e.url, e.method||'', e.source||'', e.auth_required??'', e.cors_permissive?'1':'0', e.status_code??'', e.risk_level||'']
      .map(v => `"${String(v).replace(/"/g,'""')}"`)
      .join(',')
    ).join('\n');
    _downloadBlob(header + rows, 'text/csv', 'endpoints.csv');
  };

  endpointPanel.classList.remove('hidden');
  _updateSecurityEmpty(data);
}

// ─── Secrets table (Security tab) ─────────────────────────
let _allSecrets = [];

function renderSecretsTable(data) {
  const raw = data.leaked_secrets;
  if (!raw || !raw.length) {
    secretsPanel.classList.add('hidden');
    _updateSecurityEmpty(data);
    return;
  }
  _allSecrets = raw;

  const summary = data.secret_scan_summary || {};
  const SEV_ORDER = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 };

  // Summary badges
  const sBadges = [];
  if (summary.critical_count) sBadges.push(`<span class="ep-ui-badge ep-badge-red"><svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg> CRITICAL: ${escHtml(String(summary.critical_count))}</span>`);
  if (summary.high_count)     sBadges.push(`<span class="ep-ui-badge ep-badge-red">HIGH: ${escHtml(String(summary.high_count))}</span>`);
  if (summary.medium_count)   sBadges.push(`<span class="ep-ui-badge ep-badge-orange">MEDIUM: ${escHtml(String(summary.medium_count))}</span>`);
  if (summary.low_count)      sBadges.push(`<span class="ep-ui-badge ep-badge-gray">LOW: ${escHtml(String(summary.low_count))}</span>`);
  if (summary.files_scanned)  sBadges.push(`<span class="ep-ui-badge ep-badge-gray">Files scanned: ${escHtml(String(summary.files_scanned))}</span>`);
  $('secrets-summary-badges').innerHTML = sBadges.join('');

  // Filter bar
  const filters = ['ALL', 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'];
  const fColors = { ALL:'#e2e8f0', CRITICAL:'#ff4444', HIGH:'#f87171', MEDIUM:'#fb923c', LOW:'#94a3b8' };
  let activeFilter = 'ALL';
  $('secrets-filter-bar').innerHTML = filters
    .map(f => `<button class="ep-filter-btn${f==='ALL'?' active':''}" data-sev="${escHtml(f)}"
      style="color:${fColors[f]||'#e2e8f0'}">${escHtml(f)}</button>`)
    .join('');

  function _buildSecretsTable(filter) {
    const items = filter === 'ALL' ? raw : raw.filter(s => s.severity === filter);
    if (!items.length) {
      $('secrets-table-wrap').innerHTML = `<p style="color:#94a3b8;padding:.75rem 0">No findings match this filter.</p>`;
      return;
    }
    const sorted = [...items].sort((a, b) => (SEV_ORDER[a.severity]||9) - (SEV_ORDER[b.severity]||9));
    const rows = sorted.slice(0, 200).map(s => {
      const sev = (s.severity || 'LOW').toUpperCase();
      const sevClass = sev === 'CRITICAL' ? 'sec-sev-critical'
                     : sev === 'HIGH' ? 'sec-sev-high'
                     : sev === 'MEDIUM' ? 'sec-sev-medium'
                     : 'sec-sev-low';
      const srcType = escHtml((s.source_type || '').replace(/_/g, ' '));
      const preview = escHtml(s.match_preview || '');
      const ctx     = escHtml((s.line_context || '').slice(0, 80));
      return `<tr>
        <td><span class="sec-sev-badge ${sevClass}">${escHtml(sev)}</span></td>
        <td><span class="sec-pattern">${escHtml(s.pattern_name || '')}</span></td>
        <td><code class="sec-preview">${preview}</code></td>
        <td style="color:#94a3b8;font-size:.74rem">${srcType}</td>
        <td><span class="ep-ui-url" title="${escHtml(s.source_url||'')}">${escHtml((s.source_url||'').slice(-60))}</span></td>
        <td><code class="sec-ctx" title="${escHtml(s.line_context||'')}">${ctx}</code></td>
      </tr>`;
    }).join('');
    const moreNote = sorted.length > 200
      ? `<tr><td colspan="6" style="color:#94a3b8;font-style:italic;padding:.5rem .7rem">+${escHtml(String(sorted.length-200))} more — see JSON tab</td></tr>`
      : '';
    $('secrets-table-wrap').innerHTML = `
      <table class="ep-ui-table secrets-table">
        <thead><tr><th>Severity</th><th>Pattern</th><th>Preview (redacted)</th><th>Source Type</th><th>File/URL</th><th>Context</th></tr></thead>
        <tbody>${rows}${moreNote}</tbody>
      </table>`;
  }

  _buildSecretsTable('ALL');
  $('secrets-filter-bar').querySelectorAll('.ep-filter-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      $('secrets-filter-bar').querySelectorAll('.ep-filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      activeFilter = btn.dataset.sev;
      _buildSecretsTable(activeFilter);
    });
  });

  $('secrets-copy-json-btn').onclick = () => navigator.clipboard.writeText(JSON.stringify(_allSecrets, null, 2)).catch(() => {});
  $('secrets-download-csv-btn').onclick = () => {
    const header = 'severity,pattern_name,match_preview,source_type,source_url,line_context\n';
    const rows = _allSecrets.map(s =>
      [s.severity, s.pattern_name, s.match_preview, s.source_type, s.source_url, s.line_context]
      .map(v => `"${String(v||'').replace(/"/g,'""')}"`)
      .join(',')
    ).join('\n');
    _downloadBlob(header + rows, 'text/csv', 'leaked-secrets.csv');
  };

  secretsPanel.classList.remove('hidden');
  _updateSecurityEmpty(data);
}

function _updateSecurityEmpty(data) {
  const hasEndpoints = (data.detected_endpoints || []).length > 0;
  const hasSecrets   = (data.leaked_secrets || []).length > 0;
  const emptyEl = $('security-empty');
  if (!hasEndpoints && !hasSecrets) {
    emptyEl.classList.remove('hidden');
  } else {
    emptyEl.classList.add('hidden');
  }
}

// ─── Crawl Map tab ────────────────────────────────────────
function renderCrawlMap(merged) {
  const pages = merged.pages || merged.crawl_pages || [];
  const wrapEl = $('crawl-table-wrap');
  const searchRow = $('crawl-search-row');
  const summaryBadges = $('crawl-summary-badges');

  if (!pages.length) {
    wrapEl.innerHTML = '<p style="color:#94a3b8;padding:.75rem 0">No crawl data. Enable the BFS Crawler engine with depth > 1 to discover multiple pages.</p>';
    return;
  }

  // Summary badges
  const internalLinks = (merged.internal_links || []).length;
  const externalLinks = (merged.external_links || []).length;
  let sbHtml = `<span class="ep-ui-badge ep-badge-green">Pages: ${escHtml(String(pages.length))}</span>`;
  if (internalLinks) sbHtml += `<span class="ep-ui-badge ep-badge-gray">Internal links: ${escHtml(String(internalLinks))}</span>`;
  if (externalLinks) sbHtml += `<span class="ep-ui-badge ep-badge-gray">External links: ${escHtml(String(externalLinks))}</span>`;
  summaryBadges.innerHTML = sbHtml;

  if (pages.length > 10) searchRow.classList.remove('hidden');

  function _buildCrawlTable(filter) {
    const filtered = filter
      ? pages.filter(p =>
          (p.url || '').toLowerCase().includes(filter) ||
          (p.title || '').toLowerCase().includes(filter)
        )
      : pages;
    if (!filtered.length) {
      wrapEl.innerHTML = '<p style="color:#94a3b8;padding:.5rem">No pages match the filter.</p>';
      return;
    }
    const rows = filtered.slice(0, 500).map(p => {
      const depth = p.depth != null ? `<span style="color:#94a3b8;font-size:.72rem">depth ${escHtml(String(p.depth))}</span>` : '';
      const status = p.status != null
        ? `<span style="color:${p.status === 200 ? '#4ade80' : '#fb923c'}">${escHtml(String(p.status))}</span>`
        : '&#8212;';
      const title = escHtml(p.title || p.h1 || '—');
      const desc  = p.description ? `<br><span style="color:#64748b;font-size:.72rem">${escHtml(p.description.slice(0,100))}</span>` : '';
      return `<tr>
        <td><a class="ep-ui-url" href="${escHtml(p.url)}" target="_blank" rel="noopener noreferrer">${escHtml(p.url)}</a></td>
        <td>${title}${desc}</td>
        <td>${depth}</td>
        <td>${status}</td>
      </tr>`;
    }).join('');
    const moreNote = filtered.length > 500
      ? `<tr><td colspan="4" style="color:#94a3b8;font-style:italic;padding:.5rem .7rem">+${escHtml(String(filtered.length-500))} more — see JSON tab</td></tr>`
      : '';
    wrapEl.innerHTML = `
      <table class="ep-ui-table">
        <thead><tr><th>URL</th><th>Title / Description</th><th>Depth</th><th>Status</th></tr></thead>
        <tbody>${rows}${moreNote}</tbody>
      </table>`;
  }

  _buildCrawlTable('');

  const searchInput = $('crawl-search-input');
  searchInput.addEventListener('input', () => {
    _buildCrawlTable(searchInput.value.toLowerCase().trim());
  });
}

// ─── Engine table (inside confidence panel) ───────────────
function renderEngineTable(engineSummary) {
  const rows = engineSummary.map(e => {
    const status = e.success
      ? `<span class="status-ok">&#10003; OK</span>`
      : `<span class="status-err">&#10007; ${escHtml(e.error || 'failed')}</span>`;
    const elapsed = e.elapsed_s != null ? `${e.elapsed_s.toFixed(2)}s` : '-';
    return `<tr>
      <td class="eng-name">${escHtml(e.engine_id)}</td>
      <td>${status}</td>
      <td class="mono">${elapsed}</td>
      <td>${e.content_type ? escHtml(e.content_type.split(';')[0]) : '-'}</td>
    </tr>`;
  }).join('');

  engineTableWrap.innerHTML = `
    <table class="engine-table">
      <thead><tr><th>Engine</th><th>Status</th><th>Time</th><th>Content-Type</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
}

// ─── Progress UI ──────────────────────────────────────────
function showProgressSection(jobId) {
  progressSection.classList.remove('hidden');
  progressJobId.textContent = `#${jobId}`;
  progressPhase.textContent = 'Starting…';
  progBarFill.style.width   = '0%';
  progPctLabel.textContent  = '0%';
  liveLog.innerHTML         = '';
  engineLiveGrid.innerHTML  = '';
  _logCount = 0;
  phasePipeline.querySelectorAll('.phase-step')
    .forEach(el => el.classList.remove('active', 'done'));
  progressSection.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function hideProgressSection() { progressSection.classList.add('hidden'); }

function updateProgressBar(pct, phase) {
  const p = Math.min(Math.max(pct, 0), 1);
  const pctStr = `${Math.round(p * 100)}%`;
  progBarFill.style.width  = pctStr;
  progPctLabel.textContent = pctStr;
  if (phase && phase !== '__DONE__' && phase !== 'done') {
    progressPhase.textContent = phaseLabel(phase);
  } else if (phase === 'done') {
    progressPhase.textContent = '✓ Complete';
  }
}

function activatePhaseStep(phase) {
  const steps = [...phasePipeline.querySelectorAll('.phase-step')];
  const idx = steps.findIndex(s =>
    s.dataset.phase === phase ||
    phase.includes(s.dataset.phase.replace('_', '')) ||
    s.dataset.phase.includes(phase)
  );
  if (idx === -1) return;
  steps.forEach((s, i) => {
    if (i < idx)      { s.classList.remove('active'); s.classList.add('done'); }
    else if (i === idx) { s.classList.remove('done'); s.classList.add('active'); }
    else              { s.classList.remove('active', 'done'); }
  });
}

function markEngineChip(engineId, state, errorMsg = '') {
  let chip = engineLiveGrid.querySelector(`[data-eng="${engineId}"]`);
  if (!chip) {
    chip = document.createElement('span');
    chip.className = 'eng-chip';
    chip.dataset.eng = engineId;
    chip.textContent = engineId.replace(/_/g, ' ');
    engineLiveGrid.appendChild(chip);
  }
  chip.classList.remove('running', 'ok', 'err');
  chip.classList.add(state);
  if (state === 'ok')  chip.title = '\u2713 OK';
  if (state === 'err') chip.title = errorMsg ? `\u2717 ${errorMsg}` : '\u2717 Failed';
}

function addLogRow(level, phase, pct, message) {
  _logCount++;
  liveLogCount.textContent = `${_logCount} events`;
  const now = new Date().toLocaleTimeString('en-GB', { hour:'2-digit', minute:'2-digit', second:'2-digit' });
  const row = document.createElement('div');
  row.className = `log-row log-${level}`;
  row.innerHTML =
    `<span class="log-ts">${now}</span>` +
    `<span class="log-phase">${escHtml(phaseLabel(phase))}</span>` +
    `<span class="log-pct">${Math.round((pct ?? 0) * 100)}%</span>` +
    `<span class="log-msg">${escHtml(String(message || ''))}</span>`;
  liveLog.appendChild(row);
  liveLog.scrollTop = liveLog.scrollHeight;
}

const _PHASE_LABELS = {
  site_analysis:       'Site Analysis',
  parallel_engines:    'Static Engines',
  sequential_engines:  'Browser Engines',
  normalize:           'Normalize',
  merge:               'Merge',
  reports:             'Reports',
  queued:              'Queued',
  poll:                'Poll',
  cancel:              'Cancelled',
  done:                'Done',
};
function phaseLabel(p) { return _PHASE_LABELS[p] || p || ''; }

// ─── V1 classic ───────────────────────────────────────────
async function handleScrapeV1() {
  const url = urlInput.value.trim();
  if (!validateUrl(url)) return;

  const options = [...document.querySelectorAll('input[name="option"]:checked')].map(cb => cb.value);
  const force_dynamic  = $('force-dynamic').checked;
  const respect_robots = $('v1-respect-robots').checked;
  const css_selector   = $('css-input').value.trim() || null;
  const xpath_selector = $('xpath-input').value.trim() || null;

  setLoading(true, 'Scraping…');
  clearResults();

  try {
    const res  = await fetch('/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, options, force_dynamic, respect_robots, css_selector, xpath_selector }),
    });
    const data = await res.json();
    if (!res.ok) { showError(data.detail || `HTTP ${res.status}`); return; }

    _lastResult = data;
    outputLabel.textContent = 'Quick Scrape Result';
    jsonOutput.textContent  = JSON.stringify(data, null, 2);
    summaryBar.classList.remove('hidden');
    resultSection.classList.remove('hidden');
    switchTab('rawjson');
  } catch (err) {
    showError(`Network error: ${err.message}`);
  } finally {
    setLoading(false);
  }
}

// ─── Copy / Download ──────────────────────────────────────
function bindCopyDownload() {
  copyBtn.addEventListener('click', async () => {
    if (!_lastResult) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(_lastResult, null, 2));
      flashBtn(copyBtn, '&#10003; Copied!');
    } catch { /* ignore */ }
  });

  downloadBtn.addEventListener('click', () => {
    if (!_lastResult) return;
    _downloadBlob(
      JSON.stringify(_lastResult, null, 2),
      'application/json',
      `scrape-${_lastJobId || Date.now()}.json`
    );
  });
}

function _downloadBlob(content, mimeType, filename) {
  const blob = new Blob([content], { type: mimeType });
  const a    = Object.assign(document.createElement('a'), {
    href:     URL.createObjectURL(blob),
    download: filename,
  });
  a.click();
  URL.revokeObjectURL(a.href);
}

function flashBtn(btn, html) {
  const orig = btn.innerHTML;
  btn.innerHTML = html;
  setTimeout(() => btn.innerHTML = orig, 1800);
}

// ─── Helpers ──────────────────────────────────────────────
function validateUrl(url) {
  try { new URL(url); return true; }
  catch {
    showError('Please enter a valid URL including the scheme, e.g. https://example.com');
    return false;
  }
}

function setLoading(on, label) {
  scrapeBtn.disabled = on;
  btnSpinner.classList.toggle('hidden', !on);
  btnLabel.textContent = on ? (label || 'Working…') : 'Scrape Now';
}

function clearResults() {
  hideError();
  hideProgressSection();
  summaryBar.classList.add('hidden');
  confidencePanel.classList.add('hidden');
  viewHtmlBtn.classList.add('hidden');
  engineTableWrap.innerHTML = '';
  jsonOutput.textContent    = '';
  resultSection.classList.add('hidden');
  endpointPanel.classList.add('hidden');
  secretsPanel.classList.add('hidden');
  $('security-callout')?.classList.add('hidden');
  $('security-empty').classList.add('hidden');
  $('report-fields-panel').classList.add('hidden');
  $('report-fields-grid').innerHTML = '';
  $('crawl-table-wrap').innerHTML = '';
  $('crawl-search-row').classList.add('hidden');
  securityBadge.classList.add('hidden');
  crawlBadge.classList.add('hidden');
  _lastResult = null;
  _lastJobId  = null;
  // Reset to report tab
  switchTab('report');
}

function showError(msg) {
  errorBanner.textContent = msg;
  errorBanner.classList.remove('hidden');
}

function hideError() {
  errorBanner.classList.add('hidden');
  errorBanner.textContent = '';
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
