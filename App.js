import * as duckdb from "https://cdn.jsdelivr.net";

const sqlEl = document.getElementById("sql-editor");
const runBtn = document.getElementById("run-btn");
const statusEl = document.getElementById("status");
const tableWrap = document.getElementById("table-container");
const rowCountEl = document.getElementById("rowCount");
const examplesEl = document.getElementById("examples");

let db, conn, chart;

// Data Configuration (Official ECB Keys)
const SERIES = [
    { code: 'M1', flow: 'BSI', key: 'M.U2.Y.V.M10.R.1.E.L.X' },
    { code: 'USDEUR', flow: 'EXR', key: 'M.USD.EUR.SP00.A' }
];

async function init() {
    try {
        updateStatus("Loading DuckDB Engine...");
        const BUNDLES = {
            mvp: {
                mainModule: "https://cdn.jsdelivr.net",
                mainWorker: "https://cdn.jsdelivr.net",
            }
        };
        const bundle = await duckdb.selectBundle(BUNDLES);
        const worker = new Worker(bundle.mainWorker);
        db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule);
        conn = await db.connect();

        await setupSchema();
        await ingestData();

        runBtn.disabled = false;
        runBtn.onclick = executeSQL;
        examplesEl.onchange = () => { sqlEl.value = examplesEl.value; };
        updateStatus("Analysis Engine Online. Data Synced.", "ok");
    } catch (e) {
        updateStatus(`Critical Error: ${e.message}`, "bad");
    }
}

async function setupSchema() {
    await conn.query(`CREATE TABLE obs (period DATE, series_code TEXT, value DOUBLE);`);
}

async function ingestData() {
    let totalRows = 0;
    for (const s of SERIES) {
        updateStatus(`Syncing ${s.code} from ECB...`);
        const url = `https://data-api.ecb.europa.eu{s.flow}/${s.key}?startPeriod=2015-01&format=csvdata&detail=dataonly`;
        
        const resp = await fetch(url);
        const csv = await resp.text();
        const parsed = Papa.parse(csv, { header: true, skipEmptyLines: true });

        const rows = parsed.data
            .filter(r => r.TIME_PERIOD && r.OBS_VALUE)
            .map(r => ({
                period: r.TIME_PERIOD.length === 7 ? `${r.TIME_PERIOD}-01` : r.TIME_PERIOD,
                series_code: s.code,
                value: parseFloat(r.OBS_VALUE)
            }));

        const json = JSON.stringify(rows).replace(/'/g, "''");
        await conn.query(`INSERT INTO obs SELECT CAST(period AS DATE), series_code, value FROM read_json_auto('${json}')`);
        totalRows += rows.length;
    }
    rowCountEl.innerText = `${totalRows.toLocaleString()} observations loaded for analysis`;
}

async function executeSQL() {
    const sql = sqlEl.value;
    try {
        const result = await conn.query(sql);
        const data = result.toArray().map(r => r.toJSON());
        renderTable(data);
        renderChart(data);
    } catch (e) {
        alert("SQL Error: " + e.message);
    }
}

function renderTable(data) {
    if (!data.length) return;
    const cols = Object.keys(data[0]);
    let html = `<table><thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead><tbody>`;
    data.slice(0, 100).forEach(row => {
        html += `<tr>${cols.map(c => {
            let val = row[c];
            if (val instanceof Date) val = val.toISOString().split('T')[0];
            return `<td>${val}</td>`;
        }).join('')}</tr>`;
    });
    html += `</tbody></table>`;
    tableWrap.innerHTML = html;
}

function renderChart(data) {
    const ctx = document.getElementById('main-chart');
    if (chart) chart.destroy();
    
    const cols = Object.keys(data[0]);
    const labels = data.map(d => d[cols[0]] instanceof Date ? d[cols[0]].toISOString().split('T')[0] : d[cols[0]]);
    const values = data.map(d => d[cols[1]]);

    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels.reverse(),
            datasets: [{
                label: cols[1].toUpperCase(),
                data: values.reverse(),
                borderColor: '#f5a623',
                borderWidth: 2,
                pointRadius: 0,
                tension: 0.1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: { ticks: { color: '#666' }, grid: { display: false } },
                y: { ticks: { color: '#666' }, grid: { color: '#222' } }
            }
        }
    });
}

function updateStatus(msg, type = "") {
    statusEl.innerText = msg;
    statusEl.className = `status ${type}`;
}

init();
