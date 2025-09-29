#!/usr/bin/env bash
python data_generator.py
python etl_pipeline.py
echo "ETL complete. SQLite DW at ./data/odw_dw.db"
