/**

* ECB Money & FX — SQL in the Browser

*

* Data source:

*  - ECB Data API (SDMX 2.1 REST), using startPeriod/endPeriod and format=csvdata.

* SQL engine:

*  - DuckDB-WASM (client-side), instantiated via jsDelivr bundles and a Web Worker.

*

* Notes:

*  - Works on GitHub Pages (static hosting).

*  - For local development, run a local web server (modules + WASM).

*/

 

import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.30.0/+esm";

 

const statusEl = document.getElementById("status");

const sqlEl = document.getElementById("sql");

const examplesEl = document.getElementById("examples");

const runBtn = document.getElementById("runBtn");

const resetBtn = document.getElementById("resetBtn");

const tableEl = document.getElementById("resultsTable");

const rowCountEl = document.getElementById("rowCount");

const chartNoteEl = document.getElementById("chartNote");

const maxRowsEl = document.getElementById("maxRows");

const buildInfoEl = document.getElementById("buildInfo");

 

let db, conn, chart;

 

// ------------------------

// Series configuration

// ------------------------

const SERIES = [

  // Monetary aggregates / cash (BSI)

  {

    code: "CIC",

    label: "Currency in circulation (stocks, SA/WDA)",

    unit: "Millions of EUR",

    flowRef: "BSI",

    key: "M.U2.Y.V.L10.X.1.U2.2300.Z01.E",

  },

  {

    code: "M1",

    label: "Monetary aggregate M1 (stocks, SA/WDA)",

    unit: "Millions of EUR",

    flowRef: "BSI",

    key: "M.U2.Y.V.M10.X.1.U2.2300.Z01.E",

  },

  {

    code: "M2",

    label: "Monetary aggregate M2 (stocks, SA/WDA)",

    unit: "Millions of EUR",

    flowRef: "BSI",

    key: "M.U2.Y.V.M20.X.1.U2.2300.Z01.E",

  },

  {

    code: "M3",

    label: "Monetary aggregate M3 (stocks, SA/WDA)",

    unit: "Millions of EUR",

    flowRef: "BSI",

    key: "M.U2.Y.V.M30.X.1.U2.2300.Z01.E",

  },

 

  // FX (EXR) — monthly USD/EUR ECB reference exchange rate

  {

    code: "USDEUR",

    label: "USD/EUR ECB reference rate (monthly average)",

    unit: "USD per EUR",

    flowRef: "EXR",

    key: "M.USD.EUR.SP00.A",

  },

];

 

// ECB Data API base entry point

const ECB_BASE = "https://data-api.ecb.europa.eu/service/data";

 

// A fixed start date; end date is computed dynamically (YYYY-MM)

const START_PERIOD = "1980-01";

 

// ------------------------

// Helpers

// ------------------------

function setStatus(msg, kind = "info") {

  statusEl.textContent = msg;

  statusEl.style.background =

    kind === "ok" ? "var(--ok)"

    : kind === "bad" ? "var(--bad)"

    : kind === "warn" ? "var(--warn)"

    : "rgba(255,255,255,0.04)";

 

  statusEl.style.color = kind === "bad" ? "#ffd2d2" : "var(--muted)";

}

 

function endPeriodYYYYMM() {

  const d = new Date();

  const yyyy = d.getUTCFullYear();

  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");

  return `${yyyy}-${mm}`;

}

 

function buildEcbUrl(flowRef, key) {

  const url = new URL(`${ECB_BASE}/${flowRef}/${key}`);

  url.searchParams.set("startPeriod", START_PERIOD);

  url.searchParams.set("endPeriod", endPeriodYYYYMM());

  url.searchParams.set("format", "csvdata");

  url.searchParams.set("detail", "dataonly");

  return url.toString();

}

 

// Normalize TIME_PERIOD to ISO date (monthly -> first day of month)

function normalizePeriodToISODate(periodStr) {

  // Monthly: YYYY-MM

  if (/^\d{4}-\d{2}$/.test(periodStr)) return `${periodStr}-01`;

  // Daily: YYYY-MM-DD

  if (/^\d{4}-\d{2}-\d{2}$/.test(periodStr)) return periodStr;

  // Other SDMX periods (rare here): fallback

  return periodStr;

}

 

function isISODateLike(x) {

  return typeof x === "string" && /^\d{4}-\d{2}-\d{2}$/.test(x);

}

 

function isNumeric(x) {

  return x !== null && x !== undefined && x !== "" && !Number.isNaN(Number(x));

}

 

// ------------------------

// DuckDB init (WASM)

// ------------------------

async function initDuckDB() {

  // Official-ish pattern: pick the best bundle for the browser, spawn worker, instantiate.

  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

 

  const worker_url = URL.createObjectURL(

    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })

  );

 

  const worker = new Worker(worker_url);

  const logger = new duckdb.ConsoleLogger();

  db = new duckdb.AsyncDuckDB(logger, worker);

 

  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  URL.revokeObjectURL(worker_url);

 

  conn = await db.connect();

  buildInfoEl.textContent = `DuckDB-WASM ready`;

}

 

// ------------------------

// Schema

// ------------------------

async function createSchema() {

  await conn.query(`

    CREATE TABLE series (

      series_code TEXT PRIMARY KEY,

      series_key  TEXT,

      label       TEXT,

      unit        TEXT,

      source      TEXT

    );

  `);

 

  await conn.query(`

    CREATE TABLE obs (

      period      DATE,

      series_code TEXT,

      value       DOUBLE,

      series_key  TEXT,

      freq        TEXT,

      ref_area    TEXT

    );

  `);

 

  const insertSeries = await conn.prepare(`

    INSERT INTO series(series_code, series_key, label, unit, source)

    VALUES (?, ?, ?, ?, ?);

  `);

 

  for (const s of SERIES) {

    await insertSeries.query([

      s.code,

      `${s.flowRef}.${s.key}`,

      s.label,

      s.unit,

      "ECB Data API (SDMX 2.1 REST)"

    ]);

  }

  await insertSeries.close();

}

 

// ------------------------

// Fetch + load ECB series

// ------------------------

async function fetchText(url) {

  const res = await fetch(url, { headers: { "Accept": "text/csv" } });

  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);

  return await res.text();

}

 

async function loadAllSeries() {

  const insertObs = await conn.prepare(`

    INSERT INTO obs(period, series_code, value, series_key, freq, ref_area)

    VALUES (?, ?, ?, ?, ?, ?);

  `);

 

  let totalRows = 0;

 

  for (const s of SERIES) {

    const url = buildEcbUrl(s.flowRef, s.key);

    const fullKey = `${s.flowRef}.${s.key}`;

 

    setStatus(`Downloading ${s.code}…`);

 

    let text;

    try {

      text = await fetchText(url);

    } catch (e) {

      setStatus(`ECB fetch failed for ${s.code}: ${e.message}`, "bad");

      throw e;

    }

 

    const parsed = Papa.parse(text, {

      header: true,

      dynamicTyping: true,

      skipEmptyLines: true,

    });

 

    if (parsed.errors?.length) {

      console.warn("CSV parse errors:", parsed.errors);

    }

 

    // ECB SDMX-CSV usually includes TIME_PERIOD and OBS_VALUE (and often FREQ, REF_AREA).

    const rows = parsed.data

      .filter(r => r && r.TIME_PERIOD && r.OBS_VALUE !== null && r.OBS_VALUE !== undefined && r.OBS_VALUE !== "")

      .map(r => ({

        period: normalizePeriodToISODate(String(r.TIME_PERIOD)),

        value: Number(r.OBS_VALUE),

        freq: r.FREQ ? String(r.FREQ) : null,

        ref_area: r.REF_AREA ? String(r.REF_AREA) : null,

      }))

      .filter(r => !Number.isNaN(r.value));

 

    for (const r of rows) {

      await insertObs.query([r.period, s.code, r.value, fullKey, r.freq, r.ref_area]);

    }

 

    totalRows += rows.length;

  }

 

  await insertObs.close();

  setStatus(`Loaded ${totalRows.toLocaleString()} observations into DuckDB.`, "ok");

}

 

// ------------------------

// SQL examples (joins + correlation)

// ------------------------

const EXAMPLES = [

  {

    name: "01) Inspect available series",

    sql: `SELECT * FROM series ORDER BY series_code;`

  },

  {

    name: "02) Plot currency in circulation (CIC)",

    sql: `

SELECT period, value

FROM obs

WHERE series_code = 'CIC'

ORDER BY period;

`.trim()

  },

  {

    name: "03) YoY % change (CIC) — window function",

    sql: `

WITH x AS (

  SELECT

    period,

    value,

    LAG(value, 12) OVER (ORDER BY period) AS value_12m_ago

  FROM obs

  WHERE series_code = 'CIC'

)

SELECT

  period,

  100.0 * (value / value_12m_ago - 1.0) AS yoy_pct

FROM x

WHERE value_12m_ago IS NOT NULL

ORDER BY period;

`.trim()

  },

  {

    name: "04) Join CIC vs M3 (share of M3) — join",

    sql: `

WITH cic AS (

  SELECT period, value AS cic

  FROM obs WHERE series_code='CIC'

),

m3 AS (

  SELECT period, value AS m3

  FROM obs WHERE series_code='M3'

)

SELECT

  cic.period,

  cic.cic,

  m3.m3,

  (cic.cic / m3.m3) AS cic_share_of_m3

FROM cic

JOIN m3 USING(period)

ORDER BY cic.period;

`.trim()

  },

  {

    name: "05) FX series check (USD/EUR) — plot",

    sql: `

SELECT period, value AS usdeur

FROM obs

WHERE series_code = 'USDEUR'

ORDER BY period;

`.trim()

  },

  {

    name: "06) Join M3 with USD/EUR (same month) — join",

    sql: `

WITH m3 AS (

  SELECT period, value AS m3

  FROM obs WHERE series_code='M3'

),

fx AS (

  SELECT period, value AS usdeur

  FROM obs WHERE series_code='USDEUR'

)

SELECT

  m3.period,

  m3.m3,

  fx.usdeur

FROM m3

JOIN fx USING(period)

ORDER BY m3.period;

`.trim()

  },

  {

    name: "07) Correlation: M3 vs USD/EUR (log returns) — join + windows + corr()",

    sql: `

WITH joined AS (

  SELECT

    m.period,

    m.value AS m3,

    f.value AS usdeur

  FROM obs m

  JOIN obs f ON m.period = f.period

  WHERE m.series_code = 'M3'

    AND f.series_code = 'USDEUR'

),

rets AS (

  SELECT

    period,

    LN(m3) - LN(LAG(m3) OVER (ORDER BY period))         AS m3_log_ret,

    LN(usdeur) - LN(LAG(usdeur) OVER (ORDER BY period)) AS fx_log_ret

  FROM joined

)

SELECT

  COUNT(*) AS n_months,

  corr(m3_log_ret, fx_log_ret) AS corr_m3_vs_fx

FROM rets

WHERE m3_log_ret IS NOT NULL

  AND fx_log_ret IS NOT NULL;

`.trim()

  },

  {

    name: "08) Rolling 36m correlation (M3 vs USD/EUR returns) — windowed corr()",

    sql: `

WITH joined AS (

  SELECT

    m.period,

    m.value AS m3,

    f.value AS usdeur

  FROM obs m

  JOIN obs f ON m.period = f.period

  WHERE m.series_code = 'M3'

    AND f.series_code = 'USDEUR'

),

rets AS (

  SELECT

    period,

    LN(m3) - LN(LAG(m3) OVER (ORDER BY period))         AS m3_log_ret,

    LN(usdeur) - LN(LAG(usdeur) OVER (ORDER BY period)) AS fx_log_ret

  FROM joined

),

rolling AS (

  SELECT

    period,

    corr(m3_log_ret, fx_log_ret) OVER (

      ORDER BY period

      ROWS BETWEEN 35 PRECEDING AND CURRENT ROW

    ) AS corr_36m

  FROM rets

)

SELECT period, corr_36m

FROM rolling

WHERE corr_36m IS NOT NULL

ORDER BY period;

`.trim()

  },

  {

    name: "09) Annual averages by series — group by",

    sql: `

SELECT

  series_code,

  EXTRACT(YEAR FROM period) AS year,

  AVG(value) AS avg_value

FROM obs

GROUP BY 1, 2

ORDER BY 2, 1;

`.trim()

  }

];

 

let selectedExampleIndex = 1;

 

// ------------------------

// UI: populate examples

// ------------------------

function populateExamples() {

  examplesEl.innerHTML = "";

  EXAMPLES.forEach((e, idx) => {

    const opt = document.createElement("option");

    opt.value = String(idx);

    opt.textContent = e.name;

    examplesEl.appendChild(opt);

  });

 

  examplesEl.value = String(selectedExampleIndex);

  sqlEl.value = EXAMPLES[selectedExampleIndex].sql;

 

  examplesEl.addEventListener("change", () => {

    selectedExampleIndex = Number(examplesEl.value);

    sqlEl.value = EXAMPLES[selectedExampleIndex].sql;

    chartNoteEl.textContent = "";

  });

 

  resetBtn.addEventListener("click", () => {

    sqlEl.value = EXAMPLES[selectedExampleIndex].sql;

  });

}

 

// ------------------------

// Rendering: table

// ------------------------

function renderTable(rows, maxRows) {

  tableEl.innerHTML = "";

  if (!rows || rows.length === 0) {

    tableEl.innerHTML = "<tr><td style='color:var(--muted)'>No rows</td></tr>";

    return;

  }

 

  const cols = Object.keys(rows[0]);

  const thead = document.createElement("thead");

  const trh = document.createElement("tr");

  cols.forEach(c => {

    const th = document.createElement("th");

    th.textContent = c;

    trh.appendChild(th);

  });

  thead.appendChild(trh);

 

  const tbody = document.createElement("tbody");

  rows.slice(0, maxRows).forEach(r => {

    const tr = document.createElement("tr");

    cols.forEach(c => {

      const td = document.createElement("td");

      const v = r[c];

      td.textContent = v === null || v === undefined ? "" : String(v);

      tr.appendChild(td);

    });

    tbody.appendChild(tr);

  });

 

  tableEl.appendChild(thead);

  tableEl.appendChild(tbody);

}

 

// ------------------------

// Rendering: chart (smart-enough heuristics)

// ------------------------

function destroyChart() {

  if (chart) chart.destroy();

  chart = null;

}

 

function renderChart(rows) {

  destroyChart();

  chartNoteEl.textContent = "";

 

  if (!rows || rows.length === 0) return;

 

  const cols = Object.keys(rows[0]);

  if (cols.length < 2) {

    chartNoteEl.textContent = "Chart needs at least 2 columns (x, y).";

    return;

  }

 

  const xCol = cols[0];

  const yCol = cols[1];

 

  // Decide if it's a time series: x looks like ISO date and y is numeric in most rows.

  const sample = rows.slice(0, Math.min(30, rows.length));

  const dateLikeRatio = sample.filter(r => isISODateLike(String(r[xCol]))).length / sample.length;

  const numericRatio = sample.filter(r => isNumeric(r[yCol])).length / sample.length;

 

  const ctx = document.getElementById("chart").getContext("2d");

 

  // Case A: time series line chart

  if (dateLikeRatio > 0.8 && numericRatio > 0.8) {

    const labels = rows.map(r => String(r[xCol]));

    const data = rows.map(r => (isNumeric(r[yCol]) ? Number(r[yCol]) : null));

 

    chart = new Chart(ctx, {

      type: "line",

      data: {

        labels,

        datasets: [{

          label: yCol,

          data,

          borderColor: "#7aa2ff",

          pointRadius: 0,

          borderWidth: 2,

          tension: 0.15

        }]

      },

      options: {

        responsive: true,

        maintainAspectRatio: false,

        plugins: { legend: { display: true } },

        scales: {

          x: { ticks: { maxTicksLimit: 12 } },

