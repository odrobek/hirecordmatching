import streamlit as st
import pandas as pd
# Import necessary functions from hoa_processing
from hoa_processing import (
    load_and_process_hoa_data,
    load_and_process_excel_data,
    match_records,
    analyze_matches # Added analyze_matches
)
from match_analyzer import MatchAnalyzer,MatchFlags

# --- Configuration ---
st.set_page_config(layout="wide") # Use wide layout for better table display

# --- File Paths ---
# Assuming the files are in the same directory as app.py
HOA_CSV_PATH = "hoa.csv"
EXCEL_PATH = "HIOA Owners List March 2024.xlsx"

# --- Initialize Session State ---
# Used to store matching results and highlight status across reruns
if 'match_df' not in st.session_state:
    st.session_state.match_df = None
if 'highlight' not in st.session_state:
    st.session_state.highlight = False
if 'analysis_summary' not in st.session_state:
    st.session_state.analysis_summary = ""
if 'matched_hoa_keys' not in st.session_state:
    st.session_state.matched_hoa_keys = set()
if 'matched_excel_keys' not in st.session_state:
    st.session_state.matched_excel_keys = set()


# --- Load Data ---
# Cache the data loading to avoid reloading on every interaction
@st.cache_data
def load_data(hoa_path, excel_path):
    """Loads and processes data, returning original dataframes."""
    try:
        # Load and process both datasets using functions from hoa_processing
        # These dataframes have original data types (important for matching)
        hoa_df_orig = load_and_process_hoa_data(hoa_path)
        excel_df_orig = load_and_process_excel_data(excel_path)
        return hoa_df_orig, excel_df_orig
    except FileNotFoundError as e:
        st.error(f"Error loading data: {e}. Make sure '{hoa_path}' and '{excel_path}' are in the same directory as the app.")
        return None, None
    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None, None

st.title("HOA Record Matching")

# Load original data
hoa_df_orig, excel_df_orig = load_data(HOA_CSV_PATH, EXCEL_PATH)

# Stop execution if files weren't loaded successfully
if hoa_df_orig is None or excel_df_orig is None:
    st.stop()

# --- Data Preparation for Display/Search ---
# Create string versions for display and searching
# Convert potential list columns (like Email in hoa_df) to string representation
hoa_df_display = hoa_df_orig.copy()
if 'Email' in hoa_df_display.columns:
     # Safely convert list elements to string, handle potential non-list entries
    hoa_df_display['Email'] = hoa_df_display['Email'].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x))
hoa_df_display = hoa_df_display.astype(str)

excel_df_display = excel_df_orig.copy().astype(str)


# --- Highlighting Logic ---
def highlight_matched_rows(row, keys_set, key_cols):
    """Applies highlight style if row key is in the matched keys set."""
    # Create the key tuple from the current row (which has string types)
    row_key = tuple(row[col] for col in key_cols)
    if st.session_state.highlight and row_key in keys_set:
        return ['background-color: green'] * len(row)
    else:
        return [''] * len(row)

# --- Display HOA Data with Search ---
st.header("HOA Data (Condensed)")

# Create a row with search and filter using columns
hoa_col1, hoa_col2 = st.columns([3, 1])
with hoa_col1:
    hoa_search_query = st.text_input("Search HOA Data:", key="hoa_search")
with hoa_col2:
    hide_matched_hoa = st.checkbox("Hide Exact Matches", key="hide_hoa_matches")

# Apply search filter
if hoa_search_query:
    filtered_hoa_df = hoa_df_display[hoa_df_display.apply(lambda row: row.str.contains(hoa_search_query, case=False, na=False).any(), axis=1)]
else:
    filtered_hoa_df = hoa_df_display

# Hide exact matches if checkbox is selected
if hide_matched_hoa:
    # Create key tuples for the current filtered dataframe
    hoa_key_cols = ['Last Name', 'Mailing Street', 'Mailing City', 'Mailing StateZip']
    if all(col in filtered_hoa_df.columns for col in hoa_key_cols):
        current_keys = {tuple(row[col] for col in hoa_key_cols) for _, row in filtered_hoa_df.iterrows()}
        # Filter out rows whose keys are in matched_hoa_keys
        filtered_hoa_df = filtered_hoa_df[~filtered_hoa_df.apply(lambda row: 
            tuple(row[col] for col in hoa_key_cols) in st.session_state.matched_hoa_keys, axis=1)]

# Define key columns for HOA highlighting
hoa_key_cols = ['Last Name', 'Mailing Street', 'Mailing City', 'Mailing StateZip']
# Check if all key columns exist before applying style
if all(col in filtered_hoa_df.columns for col in hoa_key_cols):
     st.dataframe(filtered_hoa_df.style.apply(
         highlight_matched_rows,
         keys_set=st.session_state.matched_hoa_keys,
         key_cols=hoa_key_cols,
         axis=1
     ))
else:
    st.warning("One or more key columns for HOA highlighting are missing. Displaying without highlights.")
    st.dataframe(filtered_hoa_df)


# --- Display Excel Data with Search ---
st.header("Owners List (from Excel)")

# Create a row with search and filter using columns
excel_col1, excel_col2 = st.columns([3, 1])
with excel_col1:
    excel_search_query = st.text_input("Search Owners List:", key="excel_search")
with excel_col2:
    hide_matched_excel = st.checkbox("Hide Exact Matches", key="hide_excel_matches")

# Apply search filter
if excel_search_query:
    filtered_excel_df = excel_df_display[excel_df_display.apply(lambda row: row.str.contains(excel_search_query, case=False, na=False).any(), axis=1)]
else:
    filtered_excel_df = excel_df_display

# Hide exact matches if checkbox is selected
if hide_matched_excel:
    # Create key tuples for the current filtered dataframe
    excel_key_cols = ['First Name', 'Last Name', 'Email']
    if all(col in filtered_excel_df.columns for col in excel_key_cols):
        current_keys = {tuple(row[col] for col in excel_key_cols) for _, row in filtered_excel_df.iterrows()}
        # Filter out rows whose keys are in matched_excel_keys
        filtered_excel_df = filtered_excel_df[~filtered_excel_df.apply(lambda row: 
            tuple(row[col] for col in excel_key_cols) in st.session_state.matched_excel_keys, axis=1)]

# Define key columns for Excel highlighting
excel_key_cols = ['First Name', 'Last Name', 'Email']
# Check if all key columns exist before applying style
if all(col in filtered_excel_df.columns for col in excel_key_cols):
    st.dataframe(filtered_excel_df.style.apply(
        highlight_matched_rows,
        keys_set=st.session_state.matched_excel_keys,
        key_cols=excel_key_cols,
        axis=1
    ))
else:
    st.warning("One or more key columns for Excel highlighting are missing. Displaying without highlights.")
    st.dataframe(filtered_excel_df)


# --- Matching Section ---
st.header("Run Matching")
if st.button("Run Matching Process"):
    with st.spinner("Matching records..."):
        try:
            # Run matching using the original dataframes with correct types
            match_df_result = match_records(excel_df_orig, hoa_df_orig)
            st.session_state.match_df = match_df_result
            st.session_state.analysis_summary = analyze_matches(match_df_result)
            st.session_state.highlight = False # Reset highlight on new match run

            # Extract keys for highlighting
            new_matched_excel_keys = set()
            new_matched_hoa_keys = set()
            if not match_df_result.empty:
                for _, row in match_df_result.iterrows():
                    if row['Match_Type'] == 'Exact':  # Only highlight exact matches
                        # Convert potential None values from match_df to empty strings for key creation
                        excel_key = (
                            str(row['Excel_First_Name'] or ''),
                            str(row['Excel_Last_Name'] or ''),
                            str(row['Excel_Email'] or '')
                        )
                        hoa_key = (
                            str(row['HOA_Last_Name'] or ''),
                            str(row['HOA_Street'] or ''), # Corresponds to Mailing Street
                            str(row['HOA_City'] or ''),     # Corresponds to Mailing City
                            str(row['HOA_StateZip'] or '') # Corresponds to Mailing StateZip
                        )
                        new_matched_excel_keys.add(excel_key)
                        new_matched_hoa_keys.add(hoa_key)

            st.session_state.matched_excel_keys = new_matched_excel_keys
            st.session_state.matched_hoa_keys = new_matched_hoa_keys

            st.success("Matching complete!")
            # Rerun to update displays immediately after matching
            st.rerun()

        except Exception as e:
            st.error(f"An error occurred during matching: {str(e)}")
            st.session_state.match_df = None # Clear results on error
            st.session_state.analysis_summary = "Matching failed."
            st.session_state.matched_excel_keys = set()
            st.session_state.matched_hoa_keys = set()

# Add flag filtering options
st.header("Filter Options")
flag_categories = {
    "Address-Related": [
        MatchFlags.ADDRESS_MISMATCH,
        MatchFlags.MULTIPLE_PROPERTIES,
        MatchFlags.ADDRESS_FORMAT_DIFF
    ],
    "Name-Related": [
        MatchFlags.NAME_MISMATCH,
        MatchFlags.MULTIPLE_RESIDENTS,
        MatchFlags.NAME_ORDER_SWAP,
        MatchFlags.COMPANY_RECORD,
        MatchFlags.HOUSEHOLD_COMPOSITION_CHANGE,
        MatchFlags.PARTIAL_HOUSEHOLD_MATCH,
        MatchFlags.NAME_COUNT_CHANGE,
        MatchFlags.COMMON_MEMBER_PRESENT
    ],
    "Email-Related": [
        MatchFlags.EMAIL_CHECK_NEEDED,
        MatchFlags.EMAIL_PRESERVED,
        MatchFlags.MULTIPLE_EMAILS,
        MatchFlags.NO_EMAILS
    ],
    "Record Existence": [
        MatchFlags.NEW_RECORD,
        MatchFlags.RECORD_REMOVED
    ],
    "Confidence-Related": [
        MatchFlags.HIGH_CONFIDENCE_MATCH,
        MatchFlags.MEDIUM_CONFIDENCE_MATCH,
        MatchFlags.LOW_CONFIDENCE_MATCH,
        MatchFlags.POTENTIAL_DUPLICATE
    ]
}

# Create columns for flag categories
cols = st.columns(len(flag_categories))
selected_flags = set()

# Create multiselect for each category
for i, (category, flags) in enumerate(flag_categories.items()):
    with cols[i]:
        st.subheader(category)
        selected = st.multiselect(
            f"Select {category} Flags",
            options=[flag.name for flag in flags],
            help=f"Filter records by {category.lower()} flags"
        )
        selected_flags.update(selected) 

# --- Display Match Results ---
if st.session_state.match_df is not None:
    st.header("Matching Results")

    # Display analysis summary
    st.text(st.session_state.analysis_summary)

    # Add highlight toggle button
    button_text = "Remove Highlights" if st.session_state.highlight else "Highlight Matched Records"
    if st.button(button_text):
        st.session_state.highlight = not st.session_state.highlight
        st.rerun() # Rerun to apply/remove highlights

    # Filter results based on selected flags if any
    filtered_df = st.session_state.match_df
    if selected_flags:
        filtered_df = filtered_df[filtered_df['Match_Flags'].apply(lambda flags: all(flag in flags for flag in selected_flags))]

    # Display the filtered match dataframe
    st.dataframe(filtered_df)

    # Display flag descriptions for selected flags
    if selected_flags:
        st.header("Selected Flag Descriptions")
        analyzer = MatchAnalyzer()
        for flag_name in selected_flags:
            flag = MatchFlags[flag_name]
            metadata = analyzer.flag_metadata[flag]
            st.markdown(f"**{flag_name}**")
            st.markdown(f"- Description: {metadata['description']}")
            st.markdown(f"- Example: {metadata['example']}")
            st.markdown("---")