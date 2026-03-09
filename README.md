# ecb-sql-monetary-analytics
MVP demo
# ECB Money & FX — SQL in the Browser (DuckDB‑WASM)

A static web app (HTML/CSS/JS only) that:

- fetches **ECB open data** using the **ECB Data API (SDMX 2.1 REST)**

- loads it into **DuckDB‑WASM** (client‑side SQL engine)

- provides a SQL console with examples demonstrating:

  - window functions (YoY, returns)

  - joins (money vs FX)

  - correlation (`corr()`) and rolling correlation windows

- renders query output as a chart + table

- runs on GitHub Pages (no backend)
---

## Data source: ECB Data API (SDMX 2.1)
The ECB Data API supports this query pattern:
`https://data-api.ecb.europa.eu/service/data/<flowRef>/<key>?startPeriod=...&endPeriod=...&format=csvdata`

Parameters used by this app:

- `startPeriod`: fixed start date (e.g. `1980-01` for monthly series)

- `endPeriod`: computed dynamically to the current month (`YYYY-MM`) on each page load

- `format=csvdata`: request SDMX‑CSV output

- `detail=dataonly`: reduce payload size

This design means the dashboard is automatically up to date whenever the ECB publishes new observations.
---

## Series used (MVP)
### Monetary aggregates & currency (dataset `BSI`, Euro area, monthly, SA/WDA)

- Currency in circulation: `BSI.M.U2.Y.V.L10.X.1.U2.2300.Z01.E`

- M1: `BSI.M.U2.Y.V.M10.X.1.U2.2300.Z01.E`

- M2: `BSI.M.U2.Y.V.M20.X.1.U2.2300.Z01.E`

- M3: `BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E`

### FX (dataset `EXR`, monthly)

- USD/EUR ECB reference rate (monthly average): `EXR.M.USD.EUR.SP00.A`
---

## Database schema
Two tables:
### `series`
Metadata for each loaded time series:

- `series_code` (TEXT, PK): CIC, M1, M2, M3, USDEUR

- `series_key` (TEXT): full ECB series key

- `label` (TEXT)

- `unit` (TEXT)

- `source` (TEXT)

### `obs`
Observations in long format:

- `period` (DATE)

- `series_code` (TEXT)

- `value` (DOUBLE)

- `series_key` (TEXT)

- `freq` (TEXT, when present)

- `ref_area` (TEXT, when present)

---

## SQL examples included

- Join M3 with USD/EUR by month

- Correlation (log returns): `corr(m3_log_ret, fx_log_ret)`

- Rolling 36‑month correlation (windowed `corr()`)

Tip: to get a time series chart, return `period` as the first column and a numeric measure as the second column.
---
## Run locally
Because the app uses ES modules + WASM, run via a local web server:
```bash
python -m http.server 8000
# open http://localhost:8000
---
Informationen (einschließlich Pflichtangaben) zu einzelnen, innerhalb der EU tätigen Gesellschaften und Zweigniederlassungen des Konzerns Deutsche Bank finden Sie unter https://www.db.com/
