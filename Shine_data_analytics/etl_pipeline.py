# etl_pipeline.py
# Simple ETL: extracts CSVs from ./data, transforms into DW schema, loads into SQLite
import pandas as pd
import sqlite3
import os

DB_PATH = 'data/odw_dw.db'
os.makedirs('data', exist_ok=True)

def extract():
    prod = pd.read_csv('data/production.csv', parse_dates=['date'])
    inv = pd.read_csv('data/inventory.csv', parse_dates=['date'])
    ords = pd.read_csv('data/orders.csv', parse_dates=['order_date'])
    return prod, inv, ords

def transform(prod, inv, ords):
    # Simple data cleaning
    prod['produced_qty'] = prod['produced_qty'].fillna(0).astype(int)
    prod['defective_qty'] = prod['defective_qty'].fillna(0).astype(int)
    # Derive KPI: defect_rate
    prod['defect_rate'] = prod.apply(lambda r: (r['defective_qty'] / r['produced_qty']) if r['produced_qty']>0 else 0, axis=1)

    # Dim product
    products = prod[['product_id']].drop_duplicates().reset_index(drop=True)
    products['product_key'] = products.index + 1
    products['product_name'] = products['product_id'].map({'P100': 'Harness-A','P200': 'Harness-B','P300':'Mechatronic-Module'})

    # Dim plant
    plants = prod[['plant']].drop_duplicates().reset_index(drop=True)
    plants['plant_key'] = plants.index + 1

    # Dim date
    dates = pd.DataFrame({'date': pd.to_datetime(prod['date']).dt.date.unique()})
    dates = dates.sort_values('date').reset_index(drop=True)
    dates['date_key'] = dates.index + 1
    dates['year'] = pd.DatetimeIndex(dates['date']).year
    dates['month'] = pd.DatetimeIndex(dates['date']).month
    dates['day'] = pd.DatetimeIndex(dates['date']).day

    # Fact production: join keys
    prod_fact = prod.merge(products, on='product_id', how='left')
    prod_fact = prod_fact.merge(plants, on='plant', how='left', suffixes=(None, None))
    prod_fact = prod_fact.merge(dates, left_on=prod_fact['date'].dt.date, right_on='date', how='left')
    prod_fact = prod_fact.rename(columns={'product_key':'product_key','plant_key':'plant_key','date_key':'date_key'})
    # Select fact columns
    fact = prod_fact[['date','product_id','product_key','plant','produced_qty','defective_qty','defect_rate']].copy()
    # Add an auto-increment surrogate key
    fact = fact.reset_index().rename(columns={'index':'fact_id'})

    return {
        'dim_product': products[['product_key','product_id','product_name']],
        'dim_plant': plants[['plant_key','plant']],
        'dim_date': dates[['date_key','date','year','month','day']],
        'fact_production': fact
    }

def quality_checks(tables):
    checks = []
    # row count checks
    for name, df in tables.items():
        checks.append((name, 'row_count', len(df)))
    # basic null checks for key columns
    checks.append(('dim_product', 'null_product_id', tables['dim_product']['product_id'].isnull().sum()))
    checks.append(('fact_production', 'null_produced_qty', tables['fact_production']['produced_qty'].isnull().sum()))
    return checks

def load(tables):
    conn = sqlite3.connect(DB_PATH)
    for name, df in tables.items():
        df.to_sql(name, conn, index=False, if_exists='replace')
    conn.close()

def main():
    prod, inv, ords = extract()
    tables = transform(prod, inv, ords)
    checks = quality_checks(tables)
    print('Data quality checks:')
    for c in checks:
        print('-', c[0], c[1], ':', c[2])
    load(tables)
    print('Loaded tables into', DB_PATH)

if __name__ == '__main__':
    main()
