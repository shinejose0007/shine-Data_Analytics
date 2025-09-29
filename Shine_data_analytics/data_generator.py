# data_generator.py
# Generates sample ERP-like CSVs: production.csv, inventory.csv, orders.csv
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

os.makedirs('data', exist_ok=True)

np.random.seed(42)
start = datetime(2025, 1, 1)
days = 180

plants = ['Steinau', 'Gelnhausen', 'München']
products = [
    {'product_id': 'P100', 'name': 'Harness-A'},
    {'product_id': 'P200', 'name': 'Harness-B'},
    {'product_id': 'P300', 'name': 'Mechatronic-Module'}
]

# production.csv: date, plant, product_id, produced_qty, defective_qty, shift
rows = []
for d in range(days):
    date = (start + timedelta(days=d)).date().isoformat()
    for plant in plants:
        for p in products:
            produced = int(np.random.poisson(lam=50))
            defective = int(np.random.binomial(n=produced, p=0.02))
            shift = np.random.choice(['A','B','C'])
            rows.append({
                'date': date,
                'plant': plant,
                'product_id': p['product_id'],
                'produced_qty': produced,
                'defective_qty': defective,
                'shift': shift
            })

df_prod = pd.DataFrame(rows)
df_prod.to_csv('data/production.csv', index=False)

# inventory.csv: snapshot per day per plant/product
inv_rows = []
for d in range(days):
    date = (start + timedelta(days=d)).date().isoformat()
    for plant in plants:
        for p in products:
            onhand = max(0, int(100 + np.random.normal(loc=0, scale=20)))
            inv_rows.append({
                'date': date,
                'plant': plant,
                'product_id': p['product_id'],
                'on_hand': onhand
            })
df_inv = pd.DataFrame(inv_rows)
df_inv.to_csv('data/inventory.csv', index=False)

# orders.csv: customer orders to plants
custs = ['OEM-A','OEM-B','Supplier-X']
order_rows = []
for d in range(days):
    date = (start + timedelta(days=d)).date().isoformat()
    for cust in custs:
        for p in products:
            qty = int(np.random.poisson(lam=30))
            order_rows.append({
                'order_date': date,
                'customer': cust,
                'product_id': p['product_id'],
                'order_qty': qty
            })
df_ord = pd.DataFrame(order_rows)
df_ord.to_csv('data/orders.csv', index=False)

print('Generated: data/production.csv (rows={}), data/inventory.csv (rows={}), data/orders.csv (rows={})'.format(
    len(df_prod), len(df_inv), len(df_ord)
))
