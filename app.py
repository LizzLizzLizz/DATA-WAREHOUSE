import streamlit as st
import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import json
import os

# --- 1. KONFIGURASI PROJECT ---
PROJECT_ID = "datawarehouse-493606"
BUCKET_NAME = "retail-data-raw-493606"
DATASET_TABLE = "retail_warehouse.integrated_retail_data"

# --- 2. FUNGSI AUTHENTICATION (KUNCI AKSES) ---
def get_gcp_credentials():
    if "gcp_service_account" in st.secrets:
        # Langsung ambil sebagai dictionary, tidak perlu json.loads
        s_account_info = dict(st.secrets["gcp_service_account"])
        
        # Tetap bersihkan private_key untuk berjaga-jaga
        s_account_info["private_key"] = s_account_info["private_key"].replace("\\n", "\n")
        
        return service_account.Credentials.from_service_account_info(s_account_info)
    else:
        if os.path.exists("credentials.json"):
            return service_account.Credentials.from_service_account_file("credentials.json")

# Inisialisasi Credentials
credentials = get_gcp_credentials()

# --- 3. FUNGSI LOGIKA (GCS & BIGQUERY) ---
def upload_to_gcs(uploaded_file):
    try:
        client = storage.Client(credentials=credentials, project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(uploaded_file.name)
        blob.upload_from_file(uploaded_file)
        return True
    except Exception as e:
        st.error(f"Error Upload: {e}")
        return False

def fetch_data_from_bq():
    try:
        client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_TABLE}` ORDER BY processed_at DESC"
        return client.query(query).to_dataframe()
    except Exception as e:
        # Jangan tampilkan error jika tabel belum ada (pertama kali run)
        return pd.DataFrame()

# --- 4. TAMPILAN DASHBOARD STREAMLIT ---
st.set_page_config(page_title="Retail ETL Monitoring", layout="wide", page_icon="🛒")

st.title("🛒 Retail Data Integration Hub")
st.markdown(f"**Project ID:** `{PROJECT_ID}` | **Status AI:** Automated ML Mapper")
st.write("Sistem ini mengintegrasikan data heterogen dari berbagai cabang secara otomatis menggunakan Cloud Functions & Machine Learning.")

st.divider()

# --- SIDEBAR: UNTUK UPLOAD ---
with st.sidebar:
    st.header("📤 Ingestion Area")
    st.info("Upload file CSV cabang (format kolom bebas)")
    uploaded_file = st.file_uploader("Pilih File CSV", type=["csv"])
    
    if uploaded_file is not None:
        if st.button("🚀 Kirim ke Pipeline Cloud"):
            with st.spinner("Mengunggah data..."):
                success = upload_to_gcs(uploaded_file)
                if success:
                    st.success(f"Berhasil! {uploaded_file.name} telah masuk ke antrean.")
                    st.toast("Triggering Cloud Function...", icon="⚡")

# --- MAIN AREA: MONITORING & DASHBOARD ---
tab1, tab2 = st.tabs(["📊 Dashboard Monitoring", "📋 Data Warehouse (Raw)"])

with tab1:
    st.header("Pipeline Monitoring")
    if st.button("🔄 Refresh Data Warehouse"):
        st.rerun()

    df = fetch_data_from_bq()

    if not df.empty:
        # Metrik Ringkasan
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Rows", len(df))
        m2.metric("Total Branches", df['source_file'].nunique())
        m3.metric("Last Processed", str(df['processed_at'].iloc[0])[:19])

        # Grafik Distribusi Data per Cabang
        st.subheader("Distribusi Input Data per File")
        branch_counts = df['source_file'].value_counts()
        st.bar_chart(branch_counts)
        
        # Grafik Tren Harga (Contoh Visualisasi Retail)
        if 'price' in df.columns and 'product_name' in df.columns:
            st.subheader("Analisis Harga Produk")
            st.line_chart(df.set_index('product_name')['price'])

    else:
        st.warning("Belum ada data yang berhasil masuk ke BigQuery.")
        st.info("Silakan upload file di sidebar dan tunggu 10 detik agar AI memprosesnya.")

with tab2:
    st.header("Tabel Hasil Integrasi")
    if not df.empty:
        st.dataframe(df, use_container_width=True)
        
        # Tombol Download Hasil Integrasi
        csv_data = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Data Terintegrasi (CSV)",
            data=csv_data,
            file_name="integrated_retail_data.csv",
            mime="text/csv"
        )
    else:
        st.write("Tabel kosong.")

# Footer
st.divider()
st.caption("Tugas Akhir Data Warehouse - Informatics Student Project © 2026")