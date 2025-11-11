Shine Analytics Demo
=======================

This demo project simulates an ETL -> Data Warehouse -> Dashboard flow that matches
common requirements from Data Analyst roles working with ERP data and reporting.

Contents
--------
- data_generator.py     : generates sample ERP-like CSV files (production, inventory, orders)
- etl_pipeline.py       : extracts CSVs, performs transformations, data quality checks, loads into SQLite DW
- dw_admin.py           : simple admin CLI to inspect DW and run sanity queries
- dashboard_app.py      : Streamlit dashboard that reads from the DW and shows KPIs and plots
- requirements.txt      : Python packages used in the demo
- run_etl.sh            : convenience script to generate data and run ETL
- LICENSE               : MIT License

How to run (recommended)
------------------------
1. Create and activate a Python virtualenv (recommended):
   python -m venv .venv
   source .venv/bin/activate  # Linux / macOS
   .venv\Scripts\activate   # Windows PowerShell

2. Install dependencies:
   pip install -r requirements.txt

3. Generate sample data and run ETL:
   bash run_etl.sh
   # or
   python data_generator.py
   python etl_pipeline.py

4. Start dashboard (Streamlit):
   streamlit run dashboard_app.py

Notes
-----
- The project uses SQLite as a lightweight Data Warehouse for demo purposes.
- The ETL script performs simple data-quality checks (row counts, null checks).
- All code is intentionally simple and well-documented so you can adapt it to real ERP sources.



Enhancements added: Forecasting (ARIMA via statsmodels), drill-down filters (product, plant, date range), defect-rate alerts and CSV/Excel/PNG export options.
