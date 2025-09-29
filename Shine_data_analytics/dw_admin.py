# dw_admin.py
# Small admin CLI to inspect the SQLite DW and run quick queries
import sqlite3
import pandas as pd
import sys

DB_PATH = 'data/odw_dw.db'

def show_tables():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""SELECT name FROM sqlite_master WHERE type='table' ORDER BY name""")
    tables = cur.fetchall()
    conn.close()
    print('Tables in DW:')
    for t in tables:
        print('-', t[0])

def table_size(table):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(f'SELECT COUNT(*) as cnt FROM {table}', conn)
    conn.close()
    return int(df['cnt'].iloc[0])

def top_k_products(k=5):
    conn = sqlite3.connect(DB_PATH)
    q = '''SELECT product_id, SUM(produced_qty) as total_produced, SUM(defective_qty) as total_defective
           FROM fact_production GROUP BY product_id ORDER BY total_produced DESC LIMIT ?'''
    df = pd.read_sql_query(q, conn, params=(k,))
    conn.close()
    print(df.to_string(index=False))

def usage():
    print('Usage: python dw_admin.py [show_tables|table_size <table>|top_products <k>]')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        usage()
    else:
        cmd = sys.argv[1]
        if cmd == 'show_tables':
            show_tables()
        elif cmd == 'table_size' and len(sys.argv) == 3:
            print(table_size(sys.argv[2]))
        elif cmd == 'top_products':
            k = int(sys.argv[2]) if len(sys.argv) > 2 else 5
            top_k_products(k)
        else:
            usage()
