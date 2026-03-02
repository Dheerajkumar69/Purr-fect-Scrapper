/* =========================================================
   Universal Web Scraper — Frontend v3.0
   Supports:
     - Multi-engine mode  → POST /scrape/v2
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
};

// ─── State ────────────────────────────────────────────────
let _lastResult   = null;
let _lastJobId    = null;
let _currentMode  = 'multi';

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
const viewHtmlBtn       = $('view-html-btn');
const copyBtn           = $('copy-btn');
const downloadBtn       = $('download-btn');
const outputLabel       = $('output-label');
const multiConfig       = $('multi-config');
const classicConfig     = $('classic-config');
const engineCheckboxes  = $('engine-checkboxes');
const selectAllBtn      = $('select-all-engines');
const deselectAllBtn    = $('deselect-all-engines');

// ─── Bootstrap ────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  renderEngineCheckboxes();
  bindModeRadios();
  bindCopyDownload();
  bindScrapeButton();
  bindSelectAll();
  bindClassicCheckboxes();
});

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
      lbl.innerHTML = `
        <input type="checkbox" name="engine" value="${eng.id}" checked />
        <span class="engine-chip-label">${eng.label}</span>`;
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
      .forEach(cb => cb.checked = true);
  });
  deselectAllBtn.addEventListener('click', () => {
    document.querySelectorAll('#engine-checkboxes input[type=checkbox]')
      .forEach(cb => cb.checked = false);
  });
}

// ─── Mode toggle ──────────────────────────────────────────
function bindModeRadios() {
  document.querySelectorAll('input[name="scrape-mode"]').forEach(radio => {
    radio.addEventListener('change', () => {
      _currentMode = radio.value;
      if (_currentMode === 'multi') {
        multiConfig.classList.remove('hidden');
        classicConfig.classList.add('hidden');
      } else {
        multiConfig.classList.add('hidden');
        classicConfig.classList.remove('hidden');
      }
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

  setLoading(true, 'Running engines…');
  clearResults();

  const body = { url, engines, depth, timeout_per_engine, respect_robots };

  try {
    const res  = await fetch('/scrape/v2', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();

    if (!res.ok) {
      showError(data.detail || `HTTP ${res.status}`);
      return;
    }

    _lastResult = data;
    _lastJobId  = data.job_id;

    renderSummaryBar(data);
    renderConfidencePanel(data);
    showJsonOutput(data.merged || data, `Merged Result (${data.engines_succeeded}/${data.engines_used} engines)`);

  } catch (err) {
    showError(`Network error: ${err.message}`);
  } finally {
    setLoading(false);
  }
}

// ─── V1 classic ───────────────────────────────────────────
async function handleScrapeV1() {
  const url = urlInput.value.trim();
  if (!validateUrl(url)) return;

  const options = [...document.querySelectorAll('input[name="option"]:checked')]
    .map(cb => cb.value);
  const force_dynamic   = $('force-dynamic').checked;
  const respect_robots  = $('v1-respect-robots').checked;
  const css_selector    = $('css-input').value.trim() || null;
  const xpath_selector  = $('xpath-input').value.trim() || null;

  setLoading(true, 'Scraping…');
  clearResults();

  const body = { url, options, force_dynamic, respect_robots, css_selector, xpath_selector };

  try {
    const res  = await fetch('/scrape', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await res.json();

    if (!res.ok) {
      showError(data.detail || `HTTP ${res.status}`);
      return;
    }

    _lastResult = data;
    showJsonOutput(data, 'Quick Scrape Result');

  } catch (err) {
    showError(`Network error: ${err.message}`);
  } finally {
    setLoading(false);
  }
}

// ─── Summary bar ──────────────────────────────────────────
function renderSummaryBar(data) {
  const badges = [
    { label: 'Engines used',      val: data.engines_used      ?? '-',  cls: 'badge-blue'   },
    { label: 'Succeeded',         val: data.engines_succeeded ?? '-',  cls: 'badge-green'  },
    { label: 'Site type',         val: data.site_type         ?? '?',  cls: 'badge-purple' },
    { label: 'Total time',        val: data.total_elapsed_s   != null ? `${data.total_elapsed_s.toFixed(1)}s` : '-', cls: 'badge-gray' },
  ];
  resultBadges.innerHTML = badges.map(b =>
    `<span class="badge ${b.cls}"><em>${b.label}</em>${b.val}</span>`
  ).join('');

  if (data.report_html_path) {
    viewHtmlBtn.classList.remove('hidden');
    viewHtmlBtn.onclick = () => window.open(`/reports/${data.job_id}/html`, '_blank');
  }

  summaryBar.classList.remove('hidden');
  resultSection.classList.remove('hidden');
}

// ─── Confidence panel ─────────────────────────────────────
function renderConfidencePanel(data) {
  if (data.confidence_score == null) return;

  const pct = Math.round(data.confidence_score * 100);
  confScoreVal.textContent = `${pct}%`;
  confScoreVal.style.color = pct >= 70 ? '#4ade80' : pct >= 40 ? '#facc15' : '#f87171';
  confBarFill.style.width  = `${pct}%`;
  confBarFill.style.background = pct >= 70 ? '#4ade80' : pct >= 40 ? '#facc15' : '#f87171';

  if (data.engines_used && data.engines_succeeded != null) {
    confMeta.innerHTML = `
      <span class="badge badge-blue">${data.engines_used} engines selected</span>
      <span class="badge badge-green">${data.engines_succeeded} succeeded</span>`;
  }

  if (data.engine_summary && data.engine_summary.length) {
    renderEngineTable(data.engine_summary);
  }

  confidencePanel.classList.remove('hidden');
}

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

// ─── JSON output pane ─────────────────────────────────────
function showJsonOutput(obj, label) {
  outputLabel.textContent = label || 'Result';
  jsonOutput.textContent  = JSON.stringify(obj, null, 2);
  resultSection.classList.remove('hidden');
  summaryBar.classList.remove('hidden');
  requestAnimationFrame(() =>
    resultSection.scrollIntoView({ behavior: 'smooth', block: 'start' })
  );
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
    const blob = new Blob([JSON.stringify(_lastResult, null, 2)], { type: 'application/json' });
    const a    = Object.assign(document.createElement('a'), {
      href:     URL.createObjectURL(blob),
      download: `scrape-${_lastJobId || Date.now()}.json`,
    });
    a.click();
    URL.revokeObjectURL(a.href);
  });
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
  summaryBar.classList.add('hidden');
  confidencePanel.classList.add('hidden');
  viewHtmlBtn.classList.add('hidden');
  engineTableWrap.innerHTML = '';
  jsonOutput.textContent    = '';
  resultSection.classList.add('hidden');
  _lastResult = null;
  _lastJobId  = null;
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
