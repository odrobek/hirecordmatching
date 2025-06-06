import streamlit as st
import pandas as pd
import io
# Import necessary functions from hoa_processing
from hoa_processing import (
    process_hoa_dataframe,
    process_excel_dataframe,
    match_records,
    analyze_matches
)
from match_analyzer import MatchAnalyzer,MatchFlags

# --- Configuration ---
st.set_page_config(layout="wide") # Use wide layout for better table display

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
if 'hoa_data' not in st.session_state:
    st.session_state.hoa_data = None
if 'excel_data' not in st.session_state:
    st.session_state.excel_data = None

# --- Data Loading Functions ---
def load_data_from_secrets():
    """Attempt to load data from Streamlit secrets."""
    try:
        if 'hoa_csv' in st.secrets and 'excel_file' in st.secrets:
            hoa_data = pd.read_csv(io.StringIO(st.secrets['hoa_csv']))
            excel_data = pd.read_excel(io.BytesIO(st.secrets['excel_file'].encode()))
            return hoa_data, excel_data
    except Exception as e:
        st.warning("Could not load data from secrets.")
        return None, None
    return None, None

def load_data_from_files():
    """Load data from local files if they exist."""
    try:
        hoa_data = pd.read_csv("hoa.csv")
        excel_data = pd.read_excel("HIOA Owners List March 2024.xlsx")
        return hoa_data, excel_data
    except Exception:
        return None, None

# --- Load Data ---
@st.cache_data
def process_data(hoa_df, excel_df):
    """Process the loaded dataframes."""
    try:
        hoa_df_processed = process_hoa_dataframe(hoa_df)
        excel_df_processed = process_excel_dataframe(excel_df)
        return hoa_df_processed, excel_df_processed
    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        return None, None

st.title("HOA Record Matching")

# Data Loading Section
if st.session_state.hoa_data is None or st.session_state.excel_data is None:
    st.header("Load Data")
    
    # Try loading from secrets first
    hoa_df, excel_df = load_data_from_secrets()
    
    # If secrets didn't work, try local files
    if hoa_df is None or excel_df is None:
        hoa_df, excel_df = load_data_from_files()
    
    # If neither worked, show file upload
    if hoa_df is None or excel_df is None:
        col1, col2 = st.columns(2)
        with col1:
            hoa_upload = st.file_uploader("Upload HOA CSV file", type=['csv'])
            if hoa_upload is not None:
                try:
                    hoa_df = pd.read_csv(hoa_upload)
                except Exception as e:
                    st.error(f"Error reading HOA CSV file: {str(e)}")
                    hoa_df = None
        
        with col2:
            excel_upload = st.file_uploader("Upload Excel file", type=['xlsx'])
            if excel_upload is not None:
                try:
                    excel_df = pd.read_excel(excel_upload)
                except Exception as e:
                    st.error(f"Error reading Excel file: {str(e)}")
                    excel_df = None
    
    # If we have both files, process them
    if hoa_df is not None and excel_df is not None:
        try:
            # Process the dataframes using the new functions
            hoa_df_orig = process_hoa_dataframe(hoa_df)
            excel_df_orig = process_excel_dataframe(excel_df)
            
            if hoa_df_orig is not None and excel_df_orig is not None:
                st.session_state.hoa_data = hoa_df_orig
                st.session_state.excel_data = excel_df_orig
                st.success("Data loaded successfully!")
                st.rerun()
            else:
                st.error("Error processing the uploaded files. Please check the file format and contents.")
                st.stop()
        except Exception as e:
            st.error(f"Error processing files: {str(e)}")
            st.stop()
    else:
        st.warning("Please upload both HOA CSV and Excel files to continue.")
        st.stop()

# Use the loaded data
hoa_df_orig = st.session_state.hoa_data
excel_df_orig = st.session_state.excel_data

# Validate data before proceeding
if hoa_df_orig is None or excel_df_orig is None:
    st.error("Data not properly loaded. Please refresh the page and try uploading the files again.")
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