# app.py
# --------------------------------------
# Streamlit viewer til semikolon-CSV (dansk locale venlig)
# - Hurtig filtrering/søgning/sortering
# - Dato-unificering (Bogføringsdato/Dato) -> _Date
# - Beløb/Saldo normalisering (decimal-komma -> punktum)
# - Filtre: Hovedkategori, Underkategori, Detalje, Beløb-interval, Dato-interval, Skjul interne
# - Nøgletal + månedlige KPI'er
# - Flotte grafer (Plotly): månedstrend, kategori-barplots (stacked/side-by-side), top-modtagere, fordeling, saldo
# - Pivot pr. måned
# - Eksport: CSV (;) og Parquet
#
# Kør:  pip install streamlit st-aggrid pandas pyarrow plotly
#       streamlit run app.py
# --------------------------------------
import io
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st
import plotly.express as px

# (Valgfrit) AgGrid giver mere "Excel-agtig" oplevelse. Fallback til st.dataframe hvis ikke installeret.
try:
    from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
    HAS_AGGRID = True
except Exception:
    HAS_AGGRID = False

st.set_page_config(page_title="Budget / Transaktioner – Viewer", layout="wide")
st.title("📊 Interaktiv tabel – Budget & transaktioner")
st.caption("Upload din CSV/Excel, filtrér lynhurtigt og eksporter resultaterne.")

# ---------------- Helpers ----------------
def read_any(path_or_buf, sheet: Optional[str] = None) -> pd.DataFrame:
    """Robust reader:
    - CSV med ';' og evt. latin-1
    - Excel (xls/xlsx)
    """
    name = getattr(path_or_buf, 'name', str(path_or_buf))
    if name.lower().endswith(('.xls', '.xlsx')):
        return pd.read_excel(path_or_buf, sheet_name=sheet)
    # CSV: prøv UTF-8, fald tilbage til latin-1
    data = path_or_buf.read() if hasattr(path_or_buf, 'read') else Path(path_or_buf).read_bytes()
    for enc in ('utf-8', 'latin-1'):
        try:
            text = data.decode(enc) if isinstance(data, (bytes, bytearray)) else data
            df = pd.read_csv(io.StringIO(text), sep=';', engine='python')
            return df
        except Exception:
            continue
    # Sidste fallback: lad pandas gætte
    return pd.read_csv(io.BytesIO(data))

def coerce_bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().map({
        'true': True, 'false': False, '1': True, '0': False, 'ja': True, 'nej': False
    })

def unify_dates(df: pd.DataFrame) -> pd.DataFrame:
    # Find og parse en af: Bogføringsdato (YYYY/MM/DD), Dato (YYYY-MM-DD), DatoBogført, Date
    candidates = [c for c in ['Bogføringsdato', 'Dato', 'DatoBogført', 'Date'] if c in df.columns]
    date_col = None
    for col in candidates:
        s = pd.to_datetime(df[col], errors='coerce', utc=False, dayfirst=False, infer_datetime_format=True)
        if s.notna().mean() > 0.7:
            date_col = col
            df['_Date'] = s.dt.tz_localize(None) if getattr(s.dtype, "tz", None) else s
            break
    if date_col is None and candidates:
        s = pd.to_datetime(df[candidates[0]], errors='coerce')
        df['_Date'] = s
    if '_Date' not in df.columns:
        df['_Date'] = pd.NaT
    if df['_Date'].notna().any():
        df['_Month'] = df['_Date'].dt.to_period('M').astype(str)
        df['_Year'] = df['_Date'].dt.year
    else:
        df['_Month'] = None
        df['_Year'] = None
    return df

def normalize_numbers(df: pd.DataFrame) -> pd.DataFrame:
    # Beløb: numerisk (forventer punktum-decimal i input)
    if 'Beløb' in df.columns:
        df['Beløb'] = pd.to_numeric(df['Beløb'], errors='coerce')
    # Saldo: konverter decimal-komma -> punktum
    if 'Saldo' in df.columns:
        s = df['Saldo'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['Saldo'] = pd.to_numeric(s, errors='coerce')
    return df

def optimize_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include='number').columns:
        df[col] = pd.to_numeric(df[col], downcast='float')
    for col in df.columns:
        if df[col].dtype == 'object':
            nunique = df[col].nunique(dropna=True)
            if len(df) > 0 and nunique and (nunique / len(df) < 0.4):
                df[col] = df[col].astype('category')
    if 'InternOverførsel' in df.columns:
        try:
            df['InternOverførsel'] = coerce_bool(df['InternOverførsel'])
        except Exception:
            pass
    return df

def global_search_mask(df: pd.DataFrame, q: str, columns: list[str]) -> pd.Series:
    if not q or not q.strip():
        return pd.Series([True] * len(df), index=df.index)
    cols = [c for c in columns if c in df.columns]
    if not cols:
        cols = df.columns.tolist()
    sub = df[cols].astype(str).apply(lambda s: s.str.contains(q, case=False, na=False))
    return sub.any(axis=1)

# ---------------- Sidebar I/O ----------------
with st.sidebar:
    st.header("📂 Data")
    uploaded = st.file_uploader("Upload CSV/Excel (semicolon-CSV understøttes)", type=['csv', 'xls', 'xlsx'], accept_multiple_files=False)

# Byg DataFrame
if uploaded is not None:
    df = read_any(uploaded)
else:
    st.info("Upload en fil i sidepanelet")
    st.stop()

# Normaliser & berig
df = normalize_numbers(df.copy())
df = unify_dates(df)
df = optimize_types(df)

# ---------------- Sidebar filters ----------------
with st.sidebar:
    st.header("🔎 Filtre")
    # Udvidet global søgning inkl. 'Detalje'
    default_search_cols = [
        'Navn', 'Beskrivelse', '_blob', '_blob_norm',
        'Afsender', 'Modtager',
        'Hovedkategori', 'Underkategori', 'Detalje'
    ]
    q = st.text_input("Global søgning", placeholder="skriv tekst…")

    # Dato-interval
    if df['_Date'].notna().any():
        dmin = pd.to_datetime(df['_Date']).min()
        dmax = pd.to_datetime(df['_Date']).max()
        date_range = st.date_input("Datointerval", (dmin.date(), dmax.date()))
    else:
        date_range = None

    # Kategori-filtre
    hk = st.multiselect("Hovedkategori",
                        sorted(df['Hovedkategori'].dropna().astype(str).unique()) if 'Hovedkategori' in df.columns else [])
    uk = st.multiselect("Underkategori",
                        sorted(df['Underkategori'].dropna().astype(str).unique()) if 'Underkategori' in df.columns else [])

    # Detalje (afhængig af HK/UK)
    if 'Detalje' in df.columns:
        df_det = df
        if hk and 'Hovedkategori' in df.columns:
            df_det = df_det[df_det['Hovedkategori'].astype(str).isin(hk)]
        if uk and 'Underkategori' in df.columns:
            df_det = df_det[df_det['Underkategori'].astype(str).isin(uk)]
        det_options = sorted(df_det['Detalje'].dropna().astype(str).unique())
        det = st.multiselect("Detalje", det_options)
    else:
        det = []

    # Beløb-interval
    if 'Beløb' in df.columns and df['Beløb'].notna().any():
        bmin, bmax = float(df['Beløb'].min()), float(df['Beløb'].max())
        belob = st.slider("Beløb (interval)", min_value=bmin, max_value=bmax, value=(bmin, bmax))
    else:
        belob = None

    hide_internal = st.checkbox("Skjul interne overførsler", value=True)

# ---------------- Anvend filtre ----------------
mask = pd.Series([True] * len(df), index=df.index)

# Global søgning
mask &= global_search_mask(df, q, default_search_cols)

# Dato-filter
if date_range and isinstance(date_range, (tuple, list)) and len(date_range) == 2:
    start, end = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    if pd.notna(start) and pd.notna(end):
        mask &= df['_Date'].between(start, end + pd.Timedelta(days=1))  # inklusiv end-dagen

# Kategori-filtre
if hk and 'Hovedkategori' in df.columns:
    mask &= df['Hovedkategori'].astype(str).isin(hk)
if uk and 'Underkategori' in df.columns:
    mask &= df['Underkategori'].astype(str).isin(uk)
# Detalje-filter
if det and 'Detalje' in df.columns:
    mask &= df['Detalje'].astype(str).isin(det)

# Beløb-interval
if belob and 'Beløb' in df.columns:
    lo, hi = belob
    mask &= df['Beløb'].between(lo, hi)

# Interne overførsler
if hide_internal and 'InternOverførsel' in df.columns:
    mask &= (df['InternOverførsel'] != True)  # behold False/NaN

df_view = df.loc[mask].copy()

# ---------------- Tabel ----------------
st.subheader(f"Tabel – viste rækker: {len(df_view):,}")
if HAS_AGGRID:
    gb = GridOptionsBuilder.from_dataframe(df_view)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    gb.configure_side_bar()
    gb.configure_selection("multiple", use_checkbox=True)
    gb.configure_pagination(paginationAutoPageSize=True)
    grid_options = gb.build()
    AgGrid(
        df_view,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.NO_UPDATE,
        fit_columns_on_grid_load=False,
        enable_enterprise_modules=False,
        height=600,
    )
else:
    st.dataframe(df_view, use_container_width=True, height=600)

# ---------------- Quick metrics ----------------
st.markdown("### Hurtige nøgletal")
cols = st.columns(4)
with cols[0]:
    st.metric("Antal posteringer", f"{len(df_view):,}")
with cols[1]:
    if 'Beløb' in df_view.columns:
        st.metric("Sum Beløb", f"{df_view['Beløb'].sum():,.2f}")
with cols[2]:
    if 'Beløb' in df_view.columns:
        st.metric("Gns. Beløb", f"{df_view['Beløb'].mean():,.2f}")
with cols[3]:
    if 'Saldo' in df_view.columns and df_view['Saldo'].notna().any():
        st.metric("Seneste Saldo", f"{df_view['Saldo'].iloc[-1]:,.2f}")

# ---------------- Monthly KPIs ----------------
st.markdown("### Nøgletal pr. måned")
if '_Date' in df_view.columns and df_view['_Date'].notna().any() and 'Beløb' in df_view.columns:
    df_view['_Date'] = pd.to_datetime(df_view['_Date'], errors='coerce')

    show_expenses_positive = st.checkbox("Vis udgifter som positive tal", value=True)
    amt = df_view['Beløb'].copy()
    if show_expenses_positive:
        amt = amt.where(amt >= 0, -amt)

    # 1) Gns. månedligt beløb (sum pr. måned -> gennemsnit)
    monthly_sum = (
        df_view.assign(_Amt=amt)
               .resample('MS', on='_Date')['_Amt']   # robust: brug 'on' fremfor set_index
               .sum()
    )
    avg_monthly_total = monthly_sum.mean()

    # 2) Gns. transaktionsbeløb pr. måned (mean pr. måned -> gennemsnit)
    month_pi = pd.PeriodIndex(df_view['_Date'].dt.to_period('M'))
    monthly_tx_mean = (
        df_view.assign(_Amt=amt, _Month=month_pi)
               .groupby('_Month')['_Amt']
               .mean()
               .astype(float)
    )
    avg_of_tx_means = monthly_tx_mean.mean()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Antal måneder (i udsnittet)", f"{monthly_sum.shape[0]}")
    with c2:
        st.metric("Gns. månedligt beløb (sum/måned)", f"{avg_monthly_total:,.2f}")
    with c3:
        st.metric("Gns. transaktionsbeløb pr. måned", f"{avg_of_tx_means:,.2f}")

    st.markdown("**Tabel pr. måned**")
    monthly_table = pd.DataFrame({
        "Sum pr. måned": monthly_sum.round(2),
        "Gns. transaktionsbeløb": monthly_tx_mean.round(2)
    }).rename_axis("Måned").reset_index()
    monthly_table['Måned'] = monthly_table['Måned'].astype(str)
    st.dataframe(monthly_table, use_container_width=True)
else:
    st.info("Ingen gyldige datoer/beløb i det filtrerede udsnit til månedlige nøgletal.")

# ---------------- Flotte grafer (Plotly) ----------------
st.markdown("## 📈 Grafer")

if '_Date' in df_view.columns and df_view['_Date'].notna().any() and 'Beløb' in df_view.columns:
    # Ensret dato
    df_view['_Date'] = pd.to_datetime(df_view['_Date'], errors='coerce')

    # Toggle: vis udgifter positive
    graph_exp_pos = st.checkbox("Grafer: vis udgifter som positive tal", value=True, key="graph_exp_pos")
    g_amt = df_view['Beløb'].copy()
    if graph_exp_pos:
        g_amt = g_amt.where(g_amt >= 0, -g_amt)

    # (A) Månedlig trend (sum)
    monthly_sum_for_plot = (
        df_view.assign(_Amt=g_amt)
               .resample('MS', on='_Date')['_Amt']
               .sum()
               .reset_index()
    )
    fig_trend = px.line(
        monthly_sum_for_plot,
        x='_Date', y='_Amt',
        markers=True,
        title="Månedlig sum af beløb"
    )
    fig_trend.update_layout(xaxis_title="Måned", yaxis_title="Sum")
    st.plotly_chart(fig_trend, use_container_width=True)

    # (B) Kategori-barplots (stacked eller side-by-side)
    st.markdown("### Kategori over tid")
    bar_mode = st.radio("Visning", ["Stacked", "Side-by-side"], horizontal=True)
    group_dim = st.selectbox("Dimension", [c for c in ["Hovedkategori", "Underkategori", "Modtager", "Afsender", "Navn"] if c in df_view.columns], index=0)

    df_cat = df_view.copy()
    df_cat['_MonthStart'] = df_cat['_Date'].values.astype('datetime64[M]')  # måned start som datetime
    cat_month = (
        df_cat.assign(_Amt=g_amt)
              .groupby(['_MonthStart', group_dim], dropna=False)['_Amt']
              .sum()
              .reset_index()
    ).sort_values('_MonthStart')

    if not cat_month.empty:
        fig_cat = px.bar(
            cat_month,
            x='_MonthStart', y='_Amt',
            color=group_dim,
            barmode='stack' if bar_mode == "Stacked" else 'group',
            title=f"Sum pr. måned fordelt på {group_dim}"
        )
        fig_cat.update_layout(xaxis_title="Måned", yaxis_title="Sum")
        st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.info("Ingen data til kategori-grafen i det aktuelle filter.")

    # (C) Top-N modtagere / afsendere
    st.markdown("### Top-N modtagere / afsendere")
    top_dim = st.selectbox("Vælg dimension", [c for c in ["Modtager", "Afsender", "Navn"] if c in df_view.columns], index=0)
    top_n = st.slider("Top-N", 5, 50, 15)
    top_tbl = (
        df_view.assign(_Amt=g_amt)
               .groupby(top_dim, dropna=False)['_Amt']
               .sum()
               .sort_values(ascending=False)
               .head(top_n)
               .reset_index()
    )
    if not top_tbl.empty:
        fig_top = px.bar(top_tbl, x=top_dim, y='_Amt', title=f"Top-{top_n} efter sum ({top_dim})")
        fig_top.update_layout(xaxis_title=top_dim, yaxis_title="Sum")
        st.plotly_chart(fig_top, use_container_width=True)
    else:
        st.info("Ingen data til Top-N i det aktuelle filter.")

    # (D) Fordeling af beløb (histogram)
    st.markdown("### Fordeling af transaktionsbeløb")
    log_y = st.checkbox("Log-skala på y-aksen", value=False)
    fig_hist = px.histogram(df_view.assign(_Amt=g_amt), x="_Amt", nbins=60, title="Histogram af beløb")
    if log_y:
        fig_hist.update_yaxes(type="log")
    fig_hist.update_layout(xaxis_title="Beløb", yaxis_title="Antal")
    st.plotly_chart(fig_hist, use_container_width=True)

    # (E) Løbende saldo (hvis Saldo findes)
    if 'Saldo' in df_view.columns and df_view['Saldo'].notna().any():
        saldo_series = (
            df_view[['\u005fDate', 'Saldo']]
            .dropna(subset=['\u005fDate'])
            .sort_values('\u005fDate')
        )
        if not saldo_series.empty:
            fig_saldo = px.line(saldo_series, x='\u005fDate', y='Saldo', markers=True, title="Løbende saldo (som i data)")
            fig_saldo.update_layout(xaxis_title="Dato", yaxis_title="Saldo")
            st.plotly_chart(fig_saldo, use_container_width=True)

# ---------------- Pivot ----------------
st.markdown("### Pivot: Sum af Beløb pr. måned og kategori")
if '_Month' in df_view.columns and 'Beløb' in df_view.columns:
    by = st.selectbox("Grupper efter", [c for c in ["Hovedkategori", "Underkategori", "Navn", "Modtager", "Afsender"] if c in df_view.columns], index=0)
    topn = st.slider("Vis top-N (efter absolut sum)", 5, 50, 15)
    pivot = df_view.groupby(['_Month', by], dropna=False)['Beløb'].sum().unstack(by, fill_value=0.0)
    top_cols = pivot.abs().sum().sort_values(ascending=False).head(topn).index
    pivot = pivot[top_cols]
    st.dataframe(pivot, use_container_width=True)

# ---------------- Export ----------------
st.markdown("### Eksport")
csv_bytes = df_view.to_csv(index=False, sep=';').encode('utf-8')
st.download_button("Download filtreret CSV (;-separeret)", csv_bytes, file_name="filtered.csv", mime="text/csv")

parquet_buf = io.BytesIO()
df_view.to_parquet(parquet_buf, index=False)
st.download_button("Download filtreret Parquet", parquet_buf.getvalue(), file_name="filtered.parquet", mime="application/octet-stream")

st.caption("Tip: Brug Parquet for hurtigere indlæsning og mindre filer.")