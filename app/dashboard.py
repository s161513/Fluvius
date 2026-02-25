import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Data Visualization", layout="wide")

st.title("Data Visualization Dashboard")

st.markdown("""
This dashboard visualizes the data ingested from the parquet file or database. 
""")

@st.cache_data
def load_parquet(sample_fraction=0.01):
    # Attempt to load from the data folder
    parquet_path = os.environ.get("PARQUET_PATH", "../ingest/data/1-19-totaal-gealloceerd-volume.parquet")
    if os.path.exists(parquet_path):
        # We load a small random sample or use specific filtering if the dataset is large
        # to prevent Streamlit from becoming unresponsive
        df = pd.read_parquet(parquet_path)
        
        # If the dataset is large, we keep it manageable for the UI
        if len(df) > 10000:
            st.warning(f"Data is very large ({len(df)} rows). Showing a 1% random sample for better performance.")
            return df.sample(frac=sample_fraction, random_state=42)
        return df
    return None

data = load_parquet()

if data is not None:
    st.success(f"Data successfully loaded! ({len(data)} rows)")
    
    st.subheader("Raw Data Preview")
    st.dataframe(data.head(100))
    
    st.subheader("Data Summary")
    st.write(data.describe())
    
    # Try to plot if there are numeric columns
    numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
    if numeric_cols:
        st.subheader("Data Visualization")
        col_to_plot = st.selectbox("Select a column to plot", numeric_cols)
        st.line_chart(data[col_to_plot])
else:
    st.error("Could not find the parquet file. Please ensure the path is correct.")
