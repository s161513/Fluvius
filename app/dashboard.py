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
    # Get the data folder path from the environment variable
    data_folder = os.getenv("DATA_FOLDER", "/app/data")

    # Resolve the absolute path of the data folder
    data_folder = os.path.abspath(data_folder)

    # Check if the data folder exists
    if not os.path.exists(data_folder):
        st.error(f"Data folder not found at '{data_folder}'. Please check the DATA_FOLDER environment variable.")
        return None

    # Find all versioned parquet files in the folder
    parquet_files = [
        f for f in os.listdir(data_folder)
        if f.startswith("1-19-totaal-gealloceerd-volume.parquet")
    ]

    if not parquet_files:
        st.error("No parquet files found in the data folder. Please check the folder and file naming.")
        return None

    # Sort files to get the latest version
    parquet_files.sort(key=lambda x: int(x.split("-")[-1]))
    latest_file = parquet_files[-1]
    parquet_path = os.path.join(data_folder, latest_file)

    # Load the parquet file
    df = pd.read_parquet(parquet_path)

    # If the dataset is large, sample it for better performance
    if len(df) > 10000:
        st.warning(f"Data is very large ({len(df)} rows). Showing a 1% random sample for better performance.")
        return df.sample(frac=sample_fraction, random_state=42)
    return df

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
