import * as duckdb from "https://cdn.jsdelivr.net";
// Add PapaParse import if not in HTML
import "https://cdn.jsdelivr.net";

const statusEl = document.getElementById("status");
const sqlEl = document.getElementById("sql-editor"); // ID updated to match typical HTML
const runBtn = document.getElementById("run-btn");
const tableWrap = document.getElementById("table-container");

let db, conn, chart;

const SERIES = [
  { code: "CIC", flowRef: "BSI", key: "M.U2.Y.V.L10.X.1.U2.2300.Z01.E" },
  { code: "M1",  flowRef: "BSI", key: "M.U2.Y.V.M10.R.1.E.L.X" },
  { code: "M3",  flowRef: "BSI", key: "M.U2.Y.V.M30.R.1.E.L.X" },
  { code: "USDEUR", flowRef: "EXR", key: "M.USD.EUR.SP00.A" }
];

const ECB_BASE = "https://data-api.ecb.europa.eu/service/data";
const START_PERIOD = "2010-01"; // Limited for faster MVP load

// --- Helpers ---
function setStatus(msg, kind = "info") {
    console.log(`[${kind}] ${msg}`);
    statusEl.textContent = msg;
    statusEl.className = `status ${kind}`; // Uses CSS classes for colors
}

// --- DuckDB Setup ---
async function init() {
    try {
        setStatus("Initializing Engine...");
        const MANUAL_BUNDLES = {
            mvp: {
                mainModule: "https://cdn.jsdelivr.net",
                mainWorker: "https://cdn.jsdelivr.net",
            }
        };
        const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
        const worker = new Worker(bundle.mainWorker);
        db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule);
        conn = await db.connect();
        
        await createSchema();
        await loadData();
        
        runBtn.disabled = false;
        runBtn.addEventListener('click', handleQuery);
        setStatus("Ready. Data Loaded.", "ok");
    } catch (e) {
        setStatus(`Init failed: ${e.message}`, "bad");
    }
}

async function createSchema() {
    await conn.query(`
        CREATE TABLE obs (
            period DATE,
            series_code TEXT,
            value DOUBLE
        );
    `);
}

// --- Data Loading ---
async function loadData() {
    for (const s of SERIES) {
        setStatus(`Fetching ${s.code}...`);
        const url = `${ECB_BASE}/${s.flowRef}/${s.key}?startPeriod=${START_PERIOD}&format=csvdata&detail=dataonly`;
        
        try {
            const resp = await fetch(url);
            const csvText = await resp.text();
            
            const parsed = Papa.parse(csvText, { header: true, skipEmptyLines: true });
            
            // Transform data into flat array for batch insertion
            const rows = parsed.data
                .filter(r => r.TIME_PERIOD && r.OBS_VALUE)
                .map(r => ({
                    period: r.TIME_PERIOD.length === 7 ? `${r.TIME_PERIOD}-01` : r.TIME_PERIOD,
                    series_code: s.code,
                    value: parseFloat(r.OBS_VALUE)
                }));

            // High-speed Batch Insert
            // Note: In a production app, we'd use arrow tables, 
            // but for MVP, we'll use a JSON string to table approach
            const jsonRows = JSON.stringify(rows);
            await conn.query(`
                INSERT INTO obs 
                SELECT CAST(period AS DATE), series_code, value 
                FROM read_json_auto('${jsonRows.replace(/'/g, "''")}')
            `);
        } catch (e) {
            console.error(e);
            setStatus(`Failed to load ${s.code}`, "warn");
        }
    }
}

// --- Query Execution ---
async function handleQuery() {
    const sql = sqlEl.value;
    try {
        const result = await conn.query(sql);
        renderTable(result);
        renderChart(result);
    } catch (e) {
        alert("SQL Error: " + e.message);
    }
}

function renderTable(result) {
    const rows = result.toArray().map(r => r.toJSON());
    if (rows.length === 0) return;
    
    let html = `<table><thead><tr>`;
    Object.keys(rows[0]).forEach(k => html += `<th>${k}</th>`);
    html += `</tr></thead><tbody>`;
    
    rows.slice(0, 100).forEach(row => {
        html += `<tr>`;
        Object.values(row).forEach(v => html += `<td>${v instanceof Date ? v.toISOString().split('T')[0] : v}</td>`);
        html += `</tr>`;
    });
    html += `</tbody></table>`;
    tableWrap.innerHTML = html;
}

function renderChart(result) {
    const rows = result.toArray().map(r => r.toJSON());
    const ctx = document.getElementById('main-chart').getContext('2d');
    
    if (chart) chart.destroy();
    
    // Auto-detect columns (assuming first is X, second is Y)
    const keys = Object.keys(rows[0]);
    const xLabels = rows.map(r => r[keys[0]]);
    const yValues = rows.map(r => r[keys[1]]);

    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: xLabels,
            datasets: [{
                label: keys[1],
                data: yValues,
                borderColor: '#7aa2ff',
                backgroundColor: 'rgba(122, 162, 255, 0.1)',
                fill: true,
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { grid: { color: 'rgba(255,255,255,0.05)' } },
                x: { grid: { display: false } }
            }
        }
    });
}

// Kickoff
init();
