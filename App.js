import * as duckdb from "https://cdn.jsdelivr.net";

const SCENARIOS = {
    raw: {
        sql: "SELECT period, value FROM obs ORDER BY period DESC LIMIT 24;",
        insight: "<strong>Case: Baseline Liquidity.</strong><br>Showing the last 24 months of M1 Money Supply. This provides the 'dry powder' inventory available in the Eurozone economy.",
        label: "M1 Nominal Value"
    },
    yoy: {
        sql: "SELECT period, value, \nLAG(value, 12) OVER (ORDER BY period) as prev_year,\nROUND(((value - LAG(value, 12) OVER (ORDER BY period)) / LAG(value, 12) OVER (ORDER BY period)) * 100, 2) as yoy_growth\nFROM obs ORDER BY period DESC;",
        insight: "<strong>Case: Inflationary Risk KPI.</strong><br>Using <strong>Window Functions</strong> (LAG) to calculate Year-over-Year growth. Investment PMs use this to spot asset bubble risks.",
        label: "YoY Growth %"
    },
    ma: {
        sql: "SELECT period, value, \nAVG(value) OVER (ORDER BY period ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as m3_moving_avg\nFROM obs ORDER BY period DESC;",
        insight: "<strong>Case: Noise Reduction.</strong><br>Using a <strong>Moving Average</strong> to smooth monthly volatility. This helps leadership teams identify long-term strategic trends.",
        label: "3-Month Moving Average"
    }
};

let conn, chart, rawData = [];

async function init() {
    const resp = await fetch(`https://data-api.ecb.europa.eu`);
    const csv = await resp.text();
    rawData = Papa.parse(csv, { header: true, skipEmptyLines: true }).data
        .filter(r => r.TIME_PERIOD && r.OBS_VALUE)
        .map(r => ({ period: r.TIME_PERIOD.length === 7 ? `${r.TIME_PERIOD}-01` : r.TIME_PERIOD, value: parseFloat(r.OBS_VALUE) }));

    const BUNDLES = { mvp: { mainModule: "https://cdn.jsdelivr.net", mainWorker: "https://cdn.jsdelivr.net" } };
    const bundle = await duckdb.selectBundle(BUNDLES);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), new Worker(bundle.mainWorker));
    await db.instantiate(bundle.mainModule);
    conn = await db.connect();
    
    await conn.query(`CREATE TABLE obs (period DATE, value DOUBLE);`);
    const json = JSON.stringify(rawData).replace(/'/g, "''");
    await conn.query(`INSERT INTO obs SELECT CAST(period AS DATE), value FROM read_json_auto('${json}')`);

    document.getElementById('status').innerText = "✅ System Ready. Click a Case to begin.";
    window.runScenario('yoy'); // Авто-запуск самого сложного кейса для HR
}

window.runScenario = async (id) => {
    const s = SCENARIOS[id];
    const res = await conn.query(s.sql);
    const data = res.toArray().map(r => r.toJSON());
    
    document.getElementById('sql-display').innerText = s.sql;
    document.getElementById('insight-box').innerHTML = s.insight;
    
    // Update Chart & Table
    renderChart(data, s.label);
    renderTable(data);
};

function renderTable(data) {
    const cols = Object.keys(data[0]);
    document.getElementById("resultsTable").innerHTML = `<thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
    <tbody>${data.slice(0, 8).map(row => `<tr>${cols.map(c => `<td>${row[c] instanceof Date ? row[c].toISOString().split('T')[0] : row[c]}</td>`).join('')}</tr>`).join('')}</tbody>`;
}

function renderChart(data, label) {
    const ctx = document.getElementById('main-chart');
    if (chart) chart.destroy();
    const d = [...data].reverse();
    const cols = Object.keys(d[0]);
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: d.map(i => i[cols[0]] instanceof Date ? i[cols[0]].toISOString().split('T')[0] : i[cols[0]]),
            datasets: [{ label: label, data: d.map(i => i[cols[cols.length-1]]), borderColor: '#f5a623', backgroundColor: 'rgba(245,166,35,0.1)', fill: true, tension: 0.3, pointRadius: 0 }]
        },
        options: { responsive: true, maintainAspectRatio: false, scales: { x: { ticks: { color: '#666', font: { size: 9 } } }, y: { ticks: { color: '#666' } } } }
    });
}

init();
