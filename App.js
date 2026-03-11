import * as duckdb from "https://cdn.jsdelivr.net";

const SCENARIOS = {
    yoy: {
        sql: `SELECT period, value, 
        LAG(value, 12) OVER (ORDER BY period) as prev_year,
        ROUND(((value - LAG(value, 12) OVER (ORDER BY period)) / LAG(value, 12) OVER (ORDER BY period)) * 100, 2) as yoy_growth
        FROM obs ORDER BY period DESC;`,
        insight: "<strong>Case: Inflationary Risk KPI.</strong><br>Uses <b>SQL Window Functions (LAG)</b>. Monitoring Year-over-Year growth is essential for spotting liquidity bubbles.",
        label: "YoY Growth %"
    },
    raw: {
        sql: "SELECT period, value FROM obs ORDER BY period DESC LIMIT 24;",
        insight: "<strong>Case: Market Liquidity.</strong><br>Direct extraction of M1 nominal levels. Represents 'dry powder' available in the Eurozone economy.",
        label: "M1 Nominal Value"
    },
    ma: {
        sql: `SELECT period, value, 
        AVG(value) OVER (ORDER BY period ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as m3_moving_avg
        FROM obs ORDER BY period DESC;`,
        insight: "<strong>Case: Strategic Smoothing.</strong><br>Uses a <b>3-Month Moving Average</b> to identify long-term structural trends over seasonal noise.",
        label: "3-Month Moving Average"
    }
};

let conn, chart, db;

async function init() {
    const statusEl = document.getElementById('status');
    try {
        updateStatus("Booting SQL Engine...", "info");
        const BUNDLES = { 
            mvp: { 
                mainModule: "https://cdn.jsdelivr.net", 
                mainWorker: "https://cdn.jsdelivr.net" 
            } 
        };
        const bundle = await duckdb.selectBundle(BUNDLES);
        const worker = new Worker(bundle.mainWorker);
        db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule);
        conn = await db.connect();

        updateStatus("Fetching ECB Live Data...", "info");
        
        // Используем CORS прокси для обхода блокировок
        const proxy = "https://corsproxy.io?";
        const ecbUrl = "https://data-api.ecb.europa.eu";
        
        const resp = await fetch(proxy + encodeURIComponent(ecbUrl));
        if (!resp.ok) throw new Error("Failed to fetch data from ECB. API might be restricted.");
        const csvText = await resp.text();

        // Native CSV ingestion
        await db.registerFileText('data.csv', csvText);
        await conn.query(`
            CREATE TABLE obs AS 
            SELECT 
                CAST(strptime(TIME_PERIOD, '%Y-%m') AS DATE) as period, 
                CAST(OBS_VALUE AS DOUBLE) as value 
            FROM read_csv_auto('data.csv')
            WHERE OBS_VALUE IS NOT NULL;
        `);

        window.runScenario = async (id) => {
            const s = SCENARIOS[id];
            const res = await conn.query(s.sql);
            const data = res.toArray().map(r => r.toJSON());
            document.getElementById('sql-display').innerText = s.sql;
            document.getElementById('insight-box').innerHTML = s.insight;
            renderChart(data, s.label);
            renderTable(data);
        };

        updateStatus("✅ Analysis Ready.", "ok");
        await window.runScenario('yoy'); 

    } catch (e) {
        updateStatus("❌ Connection Blocked. Ensure you have no ad-blockers or try again later.", "bad");
        console.error("ECB Fetch Error:", e);
    }
}

function renderTable(data) {
    const table = document.getElementById("resultsTable");
    const cols = Object.keys(data || {});
    if (!cols.length) return;
    table.innerHTML = `<thead><tr>${cols.map(c => `<th>${c}</th>`).join('')}</tr></thead>
    <tbody>${data.slice(0, 10).map(row => `<tr>${cols.map(c => {
        let val = row[c];
        return `<td>${val instanceof Date ? val.toISOString().split('T')[0] : val}</td>`;
    }).join('')}</tr>`).join('')}</tbody>`;
}

function renderChart(data, label) {
    const ctx = document.getElementById('main-chart');
    if (chart) chart.destroy();
    const d = [...data].reverse();
    const cols = Object.keys(d || {});
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: d.map(i => i[cols[0]] instanceof Date ? i[cols[0]].toISOString().split('T')[0] : i[cols[0]]),
            datasets: [{ label: label, data: d.map(i => i[cols[cols.length-1]]), borderColor: '#f5a623', backgroundColor: 'rgba(245,166,35,0.1)', fill: true, tension: 0.3, pointRadius: 0 }]
        },
        options: { responsive: true, maintainAspectRatio: false, scales: { x: { ticks: { color: '#666', font: { size: 9 } } }, y: { ticks: { color: '#666' } } } }
    });
}

function updateStatus(msg, type) {
    const statusEl = document.getElementById('status');
    statusEl.innerText = msg;
    statusEl.style.color = (type === "bad") ? "#ff5a5a" : (type === "ok") ? "#4caf50" : "#f5a623";
}

init();
