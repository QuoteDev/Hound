# Homepage Scraper Pipeline

Run the full pipeline:

```bash
pip3 install -r requirements.txt
playwright install chromium

python3 -m scraper run --input domains.csv --output results/
# Compatibility alias:
python3 -m corgi.scraper run --input domains.csv --output results/
```

Supported inputs:
- CSV / TSV
- Parquet

Outputs:
- `results/phase1_results.parquet`
- `results/phase1_failures.parquet`
- `results/phase2_results.parquet`
- `results/merged_results.parquet`
- `results/enriched_results.parquet`
- `results/enriched_results.csv`
- `results/scrape_state.json`

Phases:
1. Async `curl_cffi` scraping with retries and batch Parquet writes.
2. Playwright headless fallback for phase-1 failures.
3. TF-IDF keyword extraction (`scraped_keywords` column).

Kennel integration:
- Import `enriched_results.csv` into Kennel/Hound.
- Build ICP rules against `scraped_keywords` and other scraped metadata columns.
