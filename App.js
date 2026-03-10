import * as duckdb from "https://cdn.jsdelivr.net";

/**
 * Analytical Scenarios for HR: 
 * These demonstrate SQL proficiency (Window Functions, Aggregations)
 * and Business Intelligence (KPI calculation).
 */
const SCENARIOS = {
    yoy: {
        sql: `SELECT period, value, 
LAG(value, 12) OVER (ORDER BY period) as prev_year,
ROUND(((value - LAG(value, 12) OVER (ORDER BY period)) / LAG(value, 12) OVER (ORDER BY period)) * 100, 2) as yoy_growth
FROM obs 
ORDER BY period DESC;`,
        insight: "<strong>Case: Inflationary Risk KPI.</strong><br>Demonstrating <b>Window Functions (LAG)</b> to calculate Year-over-Year growth. This is a critical skill for Investment PMs to monitor market liquidity bubbles.",
        label: "YoY Growth %"
    },
    raw: {
        sql: "SELECT period, value FROM obs ORDER BY period DESC LIMIT 24;",
        insight: "<strong>Case: Baseline Liquidity.</strong><br>Simple extraction of the last 24 months of M1 Money Supply. Provides the 'inventory' of available capital in the Eurozone economy.",
        label: "M1 Nominal Value"
    },
    ma: {
        sql: `SELECT period, value, 
AVG(value) OVER (ORDER BY period ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as m3_moving_avg
FROM obs 
ORDER BY period DESC;`,
        insight: "<strong>Case: Trend Smoothing.</strong><br>Calculating a <b>Moving Average</b> to filter out monthly volatility. This helps stakeholders identify long-term strategic trends over seasonal noise.",
        label: "3-Month Moving Average"
    }
};

let conn, chart, db;

/**
 * Main Initialization:
 * 1. Fetch live data from ECB
 * 2. Boot DuckDB-WASM
 * 3. Ingest data into SQL engine
 * 4. Trigger the first analytical case automatically
 */
async function init() {
    const statusEl = document.getElementById('status');
    
    try {
        // 1. Fetch live data directly from ECB (No API Key required)
        updateStatus("Syncing with ECB Data API...", "info");
        const resp = await fetch(`https://data-api.ecb.europa.eu`);
        if (!resp.ok) throw new Error("ECB API Connection Failed");
        const csv = await resp.text();
        
        // Parse CSV into structured objects
        const rawData = Papa.parse(csv, { header: true, skipEmptyLines: true }).data
            .filter(r => r.TIME_PERIOD && r.OBS_VALUE)
            .map(r => ({ 
                period: r.TIME_PERIOD.length === 7 ? `${r.TIME_PERIOD}-01` : r.TIME_PERIOD, 
                value: parseFloat(r.OBS_VALUE) 
            }));

        // 2. Initialize DuckDB WASM Engine
        updateStatus("Booting SQL Analytical Engine...", "info");
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
        
        // 3. Create Table and Ingest Data
        await conn.query(`CREATE TABLE obs (period DATE, value DOUBLE);`);
        const json = JSON.stringify(rawData).replace(/'/g, "''");
        await conn.query(`INSERT INTO obs SELECT CAST(period AS DATE), value FROM read_json_auto('${json}')`);

        updateStatus("✅ System Operational. Analytical Cases Ready.", "ok");
        
        // Expose scenario runner to global scope for button clicks
        window.runScenario = async (id) => {
            // UI Visual Feedback for Buttons
            const buttons = document.querySelectorAll('#scenario-controls button');
            buttons.forEach(btn => btn.classList.remove('active'));
            
            const s = SCENARIOS[id];
            try {
                // Execute SQL Query via DuckDB
                const res = await conn.query(s.sql);
                const data = res.toArray().map(r => r.toJSON());
                
                // Update UI Components
                document.getElementById('sql-display').innerText = s.sql;
                document.getElementById('insight-box').innerHTML = s.insight;
                
                renderChart(data, s.label);
                renderTable(data);
            } catch (err) {
                console.error("Analysis Execution Error:", err);
            }
        };

        // Automatically trigger the most complex SQL case (YoY) for the HR visitor
        await window.runScenario('yoy');

    } catch (e) {
        updateStatus("❌ Initialization Error: " + e.message, "bad");
        console.error(e);
    }
}

/**
 * Renders SQL results into a clean HTML table
 */
function renderTable(data) {
    const table = document.getElementById("resultsTable");
    if (!data || data.length === 0) return;
    
    const cols = Object.keys(data[0]);
    table.innerHTML = `
        <thead><tr>${cols.map(c => `<th>${c.toUpperCase()}</th>`).join('')}</tr></thead>
        <tbody>${data.slice(0, 8).map(row => `
            <tr>${cols.map(c => {
                let val = row[c];
                if (val instanceof Date) val = val.toISOString().split('T')[0];
                return `<td>${val}</td>`;
            }).join('')}</tr>
        `).join('')}</tbody>`;
}

/**
 * Renders SQL results into a Chart.js time-series
 */
function renderChart(data, label) {
    const ctx = document.getElementById('main-chart');
    if (chart) chart.destroy();
    
    // Reverse data for correct chronological display (Left-to-Right)
    const displayData = [...data].reverse();
    const cols = Object.keys(displayData[0]);
    
    chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: displayData.map(i => {
                let d = i[cols[0]]; // First column is always date/period
                return d instanceof Date ? d.toISOString().split('T')[0] : d;
            }),
            datasets: [{ 
                label: label, 
                data: displayData.map(i => i[cols[cols.length-1]]), // Last column is always the metric
                borderColor: '#f5a623', 
                backgroundColor: 'rgba(245,166,35,0.1)', 
                fill: true, 
                tension: 0.3, 
                pointRadius: 0 
            }]
        },
        options: { 
            responsive: true, 
            maintainAspectRatio: false, 
            plugins: { legend: { labels: { color: '#888', font: { size: 10 } } } },
            scales: { 
                x: { ticks: { color: '#666', font: { size: 9 } }, grid: { display: false } }, 
                y: { ticks: { color: '#666' }, grid: { color: '#222' } } 
            } 
        }
    });
}

function updateStatus(msg, type) {
    const statusEl = document.getElementById('status');
    statusEl.innerText = msg;
    // Styling based on status type
    if (type === "bad") statusEl.style.color = "#ff5a5a";
    else if (type === "ok") statusEl.style.color = "#4caf50";
    else statusEl.style.color = "#f5a623";
}

// Kickoff the application
init();
