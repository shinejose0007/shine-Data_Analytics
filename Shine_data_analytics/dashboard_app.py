# dashboard_app.py (enhanced)
# Streamlit dashboard with forecasting, drill-down filters, defect alerts, and export options.
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.io as pio
from statsmodels.tsa.arima.model import ARIMA
from datetime import timedelta
import io

DB_PATH = 'data/odw_dw.db'

st.set_page_config(page_title='ODW Demo Dashboard (Enhanced)', layout='wide')
st.title('Shine Forecasting')

conn = sqlite3.connect(DB_PATH)

# Load facts and dims
try:
    fact = pd.read_sql_query('SELECT * FROM fact_production', conn, parse_dates=['date'])
    dim_p = pd.read_sql_query('SELECT * FROM dim_product', conn)
    dim_d = pd.read_sql_query('SELECT * FROM dim_date', conn, parse_dates=['date'])
except Exception as e:
    st.error('Fehler beim Laden der Daten. Bitte vorher ETL ausführen (python etl_pipeline.py).\n' + str(e))
    st.stop()

# Ensure date column is datetime
if 'date' in fact.columns:
    fact['date'] = pd.to_datetime(fact['date'])

st.sidebar.header('Filter')
# Drilldown filters: date range, product, plant
min_date = fact['date'].min().date()
max_date = fact['date'].max().date()
date_range = st.sidebar.date_input('Datum (von - bis)', [min_date, max_date])
product_options = fact['product_id'].unique().tolist()
selected_products = st.sidebar.multiselect('Produkt(e)', options=product_options, default=product_options)
plant_options = fact['plant'].unique().tolist()
selected_plants = st.sidebar.multiselect('Werk(e)', options=plant_options, default=plant_options)

# Apply filters
fr = pd.to_datetime(date_range[0])
to = pd.to_datetime(date_range[1])
mask = (fact['date'] >= fr) & (fact['date'] <= to) & (fact['product_id'].isin(selected_products)) & (fact['plant'].isin(selected_plants))
view = fact.loc[mask].copy()

if view.empty:
    st.warning('Keine Daten für die gewählten Filter. Bitte Filter anpassen.')
    st.stop()

# KPI cards (numeric aggregations only)
total_prod = int(view['produced_qty'].sum()) if 'produced_qty' in view.columns else 0
total_def = int(view['defective_qty'].sum()) if 'defective_qty' in view.columns else 0
avg_def_rate = (total_def / total_prod) if total_prod > 0 else 0.0

col1, col2, col3 = st.columns(3)
col1.metric('Total produzierte Einheiten', total_prod)
col2.metric('Total defekte Einheiten', total_def)
col3.metric('Durchschnittliche Defektquote', f"{avg_def_rate:.2%}")

# Time series: production over time (aggregated)
prod_ts = view.groupby('date', as_index=False)['produced_qty'].sum()
fig_ts = px.line(prod_ts, x='date', y='produced_qty', title='Produktion Zeitreihe (gefiltert)')
st.plotly_chart(fig_ts, use_container_width=True)

# Forecasting: simple ARIMA on total production time series
st.subheader('Prognose: Produktion (Nächste 14 Tage)')
try:
    ts = prod_ts.set_index('date')['produced_qty'].asfreq('D').fillna(0)
    model = ARIMA(ts, order=(1,1,1))
    model_fit = model.fit()
    forecast_h = 14
    forecast = model_fit.get_forecast(steps=forecast_h)
    fc_index = pd.date_range(start=ts.index.max() + timedelta(days=1), periods=forecast_h, freq='D')
    fc_series = pd.Series(forecast.predicted_mean.values, index=fc_index)
    fc_df = fc_series.rename('forecast').reset_index().rename(columns={'index':'date'})
    combined = pd.concat([prod_ts.set_index('date')['produced_qty'], fc_series]).reset_index().rename(columns={'index':'date',0:'produced_qty'})
    # Plot actual + forecast
    fig_fc = px.line(title='Produktion: Historisch + Prognose')
    fig_fc.add_scatter(x=prod_ts['date'], y=prod_ts['produced_qty'], mode='lines', name='Historisch')
    fig_fc.add_scatter(x=fc_df['date'], y=fc_df['forecast'], mode='lines', name='Prognose')
    st.plotly_chart(fig_fc, use_container_width=True)
except Exception as e:
    st.info('Prognose konnte nicht berechnet werden: ' + str(e))

# Defect rate by product and alerting
st.subheader('Defektquote nach Produkt / Werk')
by_prod = view.groupby(['product_id','plant'], as_index=False).agg({'produced_qty':'sum','defective_qty':'sum'})
by_prod['defect_rate'] = by_prod.apply(lambda r: (r['defective_qty'] / r['produced_qty']) if r['produced_qty']>0 else 0, axis=1)
st.dataframe(by_prod.sort_values('defect_rate', ascending=False).reset_index(drop=True))

# Operational alert: show warnings for rows above threshold
threshold = st.sidebar.slider('Defektquote Warnschwelle (%)', min_value=0.0, max_value=10.0, value=2.0, step=0.1)
high = by_prod[by_prod['defect_rate'] > (threshold/100.0)]
if not high.empty:
    st.warning(f'Warnung: {len(high)} Produkt/Werk-Kombination(en) über der Schwelle von {threshold}%')
    st.table(high[['product_id','plant','defect_rate','produced_qty','defective_qty']].assign(defect_rate=lambda df: df['defect_rate'].map('{:.2%}'.format)))
else:
    st.success('Keine Produkt/Werk-Kombinationen über der eingestellten Schwelle.')

# Export options: CSV and Excel for the current view and by_prod
st.subheader('Export')
csv = view.to_csv(index=False).encode('utf-8')
st.download_button('Download gefilterte Daten (CSV)', data=csv, file_name='filtered_production.csv', mime='text/csv')

# Excel export
towrite = io.BytesIO()
with pd.ExcelWriter(towrite, engine='xlsxwriter') as writer:
    view.to_excel(writer, sheet_name='filtered', index=False)
    by_prod.to_excel(writer, sheet_name='defect_by_product', index=False)
towrite.seek(0)
st.download_button('Download Bericht (Excel)', data=towrite,
                   file_name='odw_report.xlsx',
                   mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


# Chart export as PNG (requires kaleido)
st.subheader('Diagramm Export')
try:
    png_bytes = pio.to_image(fig_ts, format='png', width=1200, height=600, scale=2)
    st.download_button('Download Zeitreihendiagramm (PNG)', data=png_bytes, file_name='produktionszeitreihe.png', mime='image/png')
except Exception as e:
    st.info('PNG-Export nicht verfügbar (kann an fehlender kaleido-Installation liegen): ' + str(e))

st.markdown('---')
st.subheader('Datenqualität - einfache Checks')
nulls = view.isnull().sum().to_frame('null_count')
st.table(nulls.T)
conn.close()
