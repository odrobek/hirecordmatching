import streamlit as st
import pandas as pd
from hoa_processing import load_and_process_hoa_data, load_and_process_excel_data

# --- Configuration ---
st.set_page_config(layout="wide") # Use wide layout for better table display

# --- File Paths ---
# Assuming the files are in the same directory as app.py
HOA_CSV_PATH = "hoa.csv"
EXCEL_PATH = "HIOA Owners List March 2024.xlsx"

# --- Load Data ---
# Cache the data loading to avoid reloading on every interaction
@st.cache_data
def load_data(hoa_path, excel_path):
    try:
        # Load and process both datasets
        hoa_df = load_and_process_hoa_data(hoa_path)
        excel_df = load_and_process_excel_data(excel_path)
        
        # Convert all columns to string for consistent searching
        hoa_df = hoa_df.astype(str)
        excel_df = excel_df.astype(str)
        
        return hoa_df, excel_df
    except FileNotFoundError as e:
        st.error(f"Error loading data: {e}. Make sure '{hoa_path}' and '{excel_path}' are in the same directory as the app.")
        return None, None
    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None, None

st.title("HOA Record Matching")

hoa_df, excel_df = load_data(HOA_CSV_PATH, EXCEL_PATH)

# Stop execution if files weren't loaded successfully
if hoa_df is None or excel_df is None:
    st.stop()

# --- Display HOA Data with Search ---
st.header("HOA Data (Condensed)")
hoa_search_query = st.text_input("Search HOA Data:", key="hoa_search")

if hoa_search_query:
    # Filter rows where any column contains the search query (case-insensitive)
    filtered_hoa_df = hoa_df[hoa_df.apply(lambda row: row.str.contains(hoa_search_query, case=False, na=False).any(), axis=1)]
    st.dataframe(filtered_hoa_df)
else:
    st.dataframe(hoa_df) # Display original if search is empty

# --- Display Excel Data with Search ---
st.header("Owners List (from Excel)")
excel_search_query = st.text_input("Search Owners List:", key="excel_search")

if excel_search_query:
    # Filter rows where any column contains the search query (case-insensitive)
    filtered_excel_df = excel_df[excel_df.apply(lambda row: row.str.contains(excel_search_query, case=False, na=False).any(), axis=1)]
    st.dataframe(filtered_excel_df)
else:
    st.dataframe(excel_df) # Display original if search is empty

# --- Placeholder Button ---
if st.button("Run Matching"):
    st.info("Matching logic not implemented yet.") # Placeholder message 