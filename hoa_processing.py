import requests
import json
import time
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from match_analyzer import  MatchAnalyzer, MatchFlags

def is_likely_company(name):
    """Check if a name appears to be a company name"""
    company_indicators = ['llc', 'inc', 'corp', 'ltd', 'trust', 'properties', 
                         'association', 'management', 'company', 'partners']
    name_lower = name.lower()
    return any(indicator in name_lower for indicator in company_indicators)

def extract_address_info(html_content):
    """Extract both property and mailing address information from the HTML content"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    addresses = {
        'property_address': None,
        'mailing_address': None
    }
    
    # Find all address_part divs
    address_parts = soup.find_all('div', id='address_part')
    
    for address_part in address_parts:
        header = address_part.find_previous('td', class_='clsDMHeader')
        address_lines = [line.strip() for line in address_part.get_text(separator='\n').split('\n') 
                        if line.strip()]
        
        if header and 'Mailing Address' in header.get_text():
            addresses['mailing_address'] = address_lines
        else:
            addresses['property_address'] = address_lines
    
    if (addresses['mailing_address'] == None):
        addresses['mailing_address'] = addresses['property_address']
    return addresses

def get_member_address(member_id, headers, cookies, max_retries=3, initial_delay=1):
    """
    Get member address information from the website with retry logic
    Args:
        member_id: The member ID to fetch
        headers: Request headers
        cookies: Request cookies
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
    """
    base_url = "https://harborislandoa.com/Member/"
    url = base_url + member_id
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, cookies=cookies)
            response.raise_for_status()
            address_info = extract_address_info(response.text)
            time.sleep(1)  # Be nice to the server
            return address_info
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                delay = initial_delay * (2 ** attempt)  # Exponential backoff
                tqdm.write(f"Attempt {attempt + 1} failed for member {member_id}: {e}")
                tqdm.write(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                tqdm.write(f"All {max_retries} attempts failed for member {member_id}: {e}")
                return None

def process_hoa_records(all_data, headers, cookies):
    records = []
    
    # Main progress bar for all records
    for data in tqdm(all_data, desc="Processing HOA records", unit="record"):
        # Get member ID for the HTTP request
        member_id = f"{data['assn_id']}~{data['member_id']}"
        
        # Get address information from website
        address_info = get_member_address(member_id, headers, cookies)
        
        # Get both addresses
        property_address_lines = None
        mailing_address_lines = None
        if address_info:
            property_address_lines = address_info['property_address']
            mailing_address_lines = address_info['mailing_address']
        
        if data.get('contact'):
            # Process each contact
            for contact in data['contact']:
                contact_name = f"{contact.get('fname', '')} {contact.get('lname', '')}".strip()
                # Skip company contacts
                if is_likely_company(contact_name):
                    continue
                    
                first_name = contact.get('fname', '')
                last_name = contact.get('lname', '')
                
                # Find email if it exists
                email = ''
                if 'comm' in contact:
                    for comm in contact['comm']:
                        if comm.get('comm_type_id') == '1':
                            email = comm['comm_num']
                            break
                
                # Create record with both addresses
                record = {
                    'First Name': first_name,
                    'Last Name': last_name,
                    'Email': email,
                    'Property Street': property_address_lines[0] if property_address_lines else '',
                    'Property City': property_address_lines[1].split(',')[0].strip() if property_address_lines and len(property_address_lines) > 1 else '',
                    'Property StateZip': ' '.join(property_address_lines[1].split(',')[1].strip().split()) if property_address_lines and len(property_address_lines) > 1 else '',
                    'Full Property Address': '\n'.join(property_address_lines) if property_address_lines else '',
                    'Mailing Street': mailing_address_lines[0] if mailing_address_lines else '',
                    'Mailing City': mailing_address_lines[1].split(',')[0].strip() if mailing_address_lines and len(mailing_address_lines) > 1 else '',
                    'Mailing StateZip': ' '.join(mailing_address_lines[1].split(',')[1].strip().split()) if mailing_address_lines and len(mailing_address_lines) > 1 else '',
                    'Full Mailing Address': '\n'.join(mailing_address_lines) if mailing_address_lines else '',
                    'Is Company': False
                }
                records.append(record)
        else:
            # Add record for member_name, whether it's a company or not
            record = {
                'First Name': data.get('member_name', ''),
                'Last Name': '',
                'Email': '',
                'Property Street': property_address_lines[0] if property_address_lines else '',
                'Property City': property_address_lines[1].split(',')[0].strip() if property_address_lines and len(property_address_lines) > 1 else '',
                'Property StateZip': ' '.join(property_address_lines[1].split(',')[1].strip().split()) if property_address_lines and len(property_address_lines) > 1 else '',
                'Full Property Address': '\n'.join(property_address_lines) if property_address_lines else '',
                'Mailing Street': mailing_address_lines[0] if mailing_address_lines else '',
                'Mailing City': mailing_address_lines[1].split(',')[0].strip() if mailing_address_lines and len(mailing_address_lines) > 1 else '',
                'Mailing StateZip': ' '.join(mailing_address_lines[1].split(',')[1].strip().split()) if mailing_address_lines and len(mailing_address_lines) > 1 else '',
                'Full Mailing Address': '\n'.join(mailing_address_lines) if mailing_address_lines else '',
                'Is Company': is_likely_company(data.get('member_name', ''))
            }
            records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    return df

def sanitize_string(s):
    """
    Sanitize a string by removing special characters, extra spaces, and converting to lowercase.
    """
    if not isinstance(s, str):
        return ""
    # Convert to lowercase and remove special characters
    s = s.lower().strip()
    # Remove multiple spaces
    s = ' '.join(s.split())
    return s

def condense_records(df):
    """
    Condense records by combining people with same last name and property address.
    Returns a new DataFrame with combined records.
    """
    # Create a copy of the DataFrame
    df = df.copy()
    
    # Log records with missing addresses
    missing_property = df[df['Full Property Address'].isna() | (df['Full Property Address'] == '')]
    if len(missing_property) > 0:
        print(f"Warning: Found {len(missing_property)} records with missing property addresses")
        print(missing_property[['First Name', 'Last Name', 'Full Property Address']])
    
    # Create sanitized versions of last name and address for grouping
    df['sanitized_last_name'] = df['Last Name'].apply(sanitize_string)
    df['sanitized_property_address'] = df['Full Property Address'].apply(sanitize_string)
    
    # Create a grouping key based on sanitized last name and property address
    df['group_key'] = df['sanitized_last_name'] + '|' + df['sanitized_property_address']
    
    # Initialize lists to store condensed records
    condensed_records = []
    
    # Group by the key
    for _, group in df.groupby('group_key'):
        if len(group) > 1:  # Multiple people with same last name and address
            # Combine first names, removing duplicates and handling NaN
            first_names = group['First Name'].dropna().unique()
            combined_first_names = ' & '.join(first_names) if len(first_names) > 0 else ''
            
            # Combine emails into a list, removing any empty strings, NaN, and duplicates
            emails = list(set(email for email in group['Email'] if pd.notna(email) and email))
            
            # Handle NaN for last name
            last_name = group['Last Name'].iloc[0] if pd.notna(group['Last Name'].iloc[0]) else ''
            
            # Create condensed record
            condensed_record = {
                'First Name': combined_first_names,
                'Last Name': last_name,
                'Email': emails,  # This will be a list
                'Property Street': group['Property Street'].iloc[0],
                'Property City': group['Property City'].iloc[0],
                'Property StateZip': group['Property StateZip'].iloc[0],
                'Full Property Address': group['Full Property Address'].iloc[0],
                'Mailing Street': group['Mailing Street'].iloc[0],
                'Mailing City': group['Mailing City'].iloc[0],
                'Mailing StateZip': group['Mailing StateZip'].iloc[0],
                'Full Mailing Address': group['Full Mailing Address'].iloc[0],
                'Is Company': group['Is Company'].iloc[0],
                'Number of Unique People': len(first_names),
                'Number of Unique Emails': len(emails)
            }
            
        else:  # Single person
            record = group.iloc[0]
            # Handle NaN values for first name, last name, and email
            first_name = record['First Name'] if pd.notna(record['First Name']) else ''
            last_name = record['Last Name'] if pd.notna(record['Last Name']) else ''
            email = [record['Email']] if pd.notna(record['Email']) and record['Email'] else []
            
            condensed_record = {
                'First Name': first_name,
                'Last Name': last_name,
                'Email': email,  # This will be a list
                'Property Street': record['Property Street'],
                'Property City': record['Property City'],
                'Property StateZip': record['Property StateZip'],
                'Full Property Address': record['Full Property Address'],
                'Mailing Street': record['Mailing Street'],
                'Mailing City': record['Mailing City'],
                'Mailing StateZip': record['Mailing StateZip'],
                'Full Mailing Address': record['Full Mailing Address'],
                'Is Company': record['Is Company'],
                'Number of Unique People': 1,
                'Number of Unique Emails': len(email)
            }
            
        condensed_records.append(condensed_record)
    
    # Create new DataFrame from condensed records
    condensed_df = pd.DataFrame(condensed_records)
    
    # Drop the temporary columns used for grouping
    if 'sanitized_last_name' in condensed_df.columns:
        condensed_df = condensed_df.drop(['sanitized_last_name'], axis=1)
    if 'sanitized_property_address' in condensed_df.columns:
        condensed_df = condensed_df.drop(['sanitized_property_address'], axis=1)
    if 'group_key' in condensed_df.columns:
        condensed_df = condensed_df.drop(['group_key'], axis=1)
    
    return condensed_df

def get_initial_data():
    """Fetch initial data from the HOA website with specific cookies and headers"""
    url = "https://harborislandoa.com/Member/SearchData"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Host": "harborislandoa.com",
        "Origin": "https://harborislandoa.com",
        "Priority": "u=0",
        "Referer": "https://harborislandoa.com/member/search/41678/resident-directory",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Sec-GPC": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "X-Requested-With": "XMLHttpRequest"
    }
    cookies = {
        "avs": "vgpcxdibnomednmawwx2hulh",
        "av2": "u12kWSHFNtcwIYTgyshSebIG7k75aQs0Bowd6qVqZrY="
    }

    page_number = 1
    page_size = 614
    all_data = []
    total_records = 0
    
    with tqdm(desc="Fetching member data", unit=" page") as pbar:
        while page_number < 2:
            payload = {
                "g_search": "",
                "search_letter": "",
                "search_all": "0",
                "page": page_number,
                "pageSize": page_size
            }

            try:
                response = requests.post(url, headers=headers, data=json.dumps(payload), cookies=cookies)
                response.raise_for_status()

                page_data = response.json()
                print(page_data)
                if not isinstance(page_data['Directory']['member'], list):
                    tqdm.write("Error: Response is not a list.")
                    break

                if not page_data:
                    tqdm.write("No more data, breaking")
                    break

                all_data.extend(page_data['Directory']['member'])
                total_records += len(page_data['Directory']['member'])
                tqdm.write(f"Fetched page {page_number} with {len(page_data['Directory']['member'])} records")
                
                time.sleep(0.5)  # Be nice to the server
                page_number += 1
                pbar.update(1)

            except requests.exceptions.RequestException as e:
                tqdm.write(f"An error occurred: {e}")
                break
    
    return all_data, headers, cookies 

def match_records(excel_df, hoa_df):
    """
    Match records between Excel and HOA dataframes
    Returns a DataFrame with matched records, their scores, and flags.
    Unmatched records (score < 50) will be placed in separate rows with empty fields for the unmatched side.
    """
    try:
        # Create a copy of the Excel DataFrame to store results
        results = []
        matched_hoa_indices = set()  # Keep track of matched HOA records
        matched_excel_indices = set()  # Keep track of matched Excel records
        analyzer = MatchAnalyzer()  # Create a single analyzer instance
        
        # Sort HOA records by number of unique people (prefer single-person records)
        hoa_df = hoa_df.sort_values('Number of Unique People')
        
        # First pass: Find all good matches (score >= 50)
        for idx, excel_record in excel_df.iterrows():
            try:
                best_match = None
                best_score = 0
                best_match_details = None
                best_flags = None
                best_hoa_idx = None
                
                for hoa_idx, hoa_record in hoa_df.iterrows():
                    if hoa_idx in matched_hoa_indices:
                        continue
                    
                    try:
                        print(f"Comparing records:")
                        print(f"Excel: {excel_record['First Name']} {excel_record['Last Name']}")
                        print(f"HOA: {hoa_record['First Name']} {hoa_record['Last Name']}")
                        
                        score, match_details, flags = analyzer.calculate_match_score(dict(excel_record), dict(hoa_record))
                        
                        if score > best_score:
                            best_score = score
                            best_match = hoa_record
                            best_match_details = match_details
                            best_flags = flags
                            best_hoa_idx = hoa_idx
                            
                    except Exception as e:
                        print(f"Error comparing records:")
                        print(f"Excel Record: {excel_record['First Name']} {excel_record['Last Name']}")
                        print(f"HOA Record: {hoa_record['First Name']} {hoa_record['Last Name']}")
                        print(f"Error: {str(e)}")
                        continue
                
                # Only create a matched record if the score is >= 50
                if best_score >= 50 and best_match is not None:
                    result = {
                        'Excel_First_Name': excel_record['First Name'],
                        'Excel_Last_Name': excel_record['Last Name'],
                        'Excel_Email': excel_record['Email'],
                        'Excel_Street': excel_record.get('Street', ''),
                        'Excel_City': excel_record.get('City', ''),
                        'Excel_StateZip': excel_record.get('StateZip', ''),
                        'HOA_First_Name': best_match['First Name'],
                        'HOA_Last_Name': best_match['Last Name'],
                        'HOA_Email': best_match['Email'],
                        'HOA_Street': best_match['Mailing Street'],
                        'HOA_City': best_match['Mailing City'],
                        'HOA_StateZip': best_match['Mailing StateZip'],
                        'Match_Score': best_score,
                        'Email_Match': best_match_details['email_match'],
                        'Last_Name_Match': best_match_details['last_name_match'],
                        'Address_Match': best_match_details['address_match'],
                        'Match_Type': 'Exact' if best_score >= 100 else 'Fuzzy',
                        'Match_Flags': [flag.name for flag in best_flags]
                    }
                    results.append(result)
                    matched_excel_indices.add(idx)
                    matched_hoa_indices.add(best_hoa_idx)
                    print(f"Matched {excel_record['First Name']} {excel_record['Last Name']} with {best_match['First Name']} {best_match['Last Name']}")
                    
            except Exception as e:
                print(f"Error processing Excel record:")
                print(f"Record: {excel_record['First Name']} {excel_record['Last Name']}")
                print(f"Error: {str(e)}")
                continue
        
        # Add unmatched Excel records
        for idx, excel_record in excel_df.iterrows():
            if idx not in matched_excel_indices:
                result = {
                    'Excel_First_Name': excel_record['First Name'],
                    'Excel_Last_Name': excel_record['Last Name'],
                    'Excel_Email': excel_record['Email'],
                    'Excel_Street': excel_record.get('Street', ''),
                    'Excel_City': excel_record.get('City', ''),
                    'Excel_StateZip': excel_record.get('StateZip', ''),
                    'HOA_First_Name': None,
                    'HOA_Last_Name': None,
                    'HOA_Email': None,
                    'HOA_Street': None,
                    'HOA_City': None,
                    'HOA_StateZip': None,
                    'Match_Score': 0,
                    'Email_Match': False,
                    'Last_Name_Match': False,
                    'Address_Match': False,
                    'Match_Type': 'No Match',
                    'Match_Flags': ['RECORD_REMOVED']
                }
                results.append(result)
        
        # Add unmatched HOA records
        for idx, hoa_record in hoa_df.iterrows():
            if idx not in matched_hoa_indices:
                result = {
                    'Excel_First_Name': None,
                    'Excel_Last_Name': None,
                    'Excel_Email': None,
                    'Excel_Street': None,
                    'Excel_City': None,
                    'Excel_StateZip': None,
                    'HOA_First_Name': hoa_record['First Name'],
                    'HOA_Last_Name': hoa_record['Last Name'],
                    'HOA_Email': hoa_record['Email'],
                    'HOA_Street': hoa_record['Mailing Street'],
                    'HOA_City': hoa_record['Mailing City'],
                    'HOA_StateZip': hoa_record['Mailing StateZip'],
                    'Match_Score': 0,
                    'Email_Match': False,
                    'Last_Name_Match': False,
                    'Address_Match': False,
                    'Match_Type': 'No Match',
                    'Match_Flags': ['NEW_RECORD']
                }
                results.append(result)
        
        # Create DataFrame from results
        results_df = pd.DataFrame(results)
        
        # Sort by match score (descending) and match type
        results_df = results_df.sort_values(['Match_Score', 'Match_Type'], ascending=[False, True])
        
        return results_df
        
    except Exception as e:
        print("Error in match_records:")
        print(f"Error: {str(e)}")
        raise

def analyze_matches(match_df):
    """Analyze the matching results and return summary statistics"""
    total_records = len(match_df)
    if total_records == 0:
        return "No records to analyze." # Handle empty df case

    exact_matches = len(match_df[match_df['Match_Type'] == 'Exact'])
    fuzzy_matches = len(match_df[match_df['Match_Type'] == 'Fuzzy'])
    no_matches = len(match_df[match_df['Match_Type'] == 'No Match'])

    analysis_text = f"""
Matching Analysis:
Total Records: {total_records}
Exact Matches: {exact_matches} ({exact_matches/total_records*100:.1f}%)
Fuzzy Matches: {fuzzy_matches} ({fuzzy_matches/total_records*100:.1f}%)
No Matches: {no_matches} ({no_matches/total_records*100:.1f}%)
"""
    # Return the analysis text
    return analysis_text

def load_and_process_hoa_data(csv_path):
    """
    Load HOA data from CSV, condense records, clean, and sort.
    
    Args:
        csv_path (str): Path to the HOA CSV file
        
    Returns:
        pd.DataFrame: Processed HOA data
    """
    # Load the CSV
    df = pd.read_csv(csv_path)
    
    # Condense the records
    df = condense_records(df)
    
    # Clean string columns
    string_columns = ['First Name', 'Last Name', 
                     'Property Street', 'Property City', 'Property StateZip',
                     'Mailing Street', 'Mailing City', 'Mailing StateZip']
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    # Sort and reset index
    df = df.sort_values('First Name').reset_index(drop=True)
    
    return df

def load_and_process_excel_data(excel_path):
    """
    Load Excel data, clean, and sort.
    
    Args:
        excel_path (str): Path to the Excel file
        
    Returns:
        pd.DataFrame: Processed Excel data
    """
    # Load the Excel file
    df = pd.read_excel(excel_path)
    
    # Remove rows with no last name
    df = df.dropna(subset=['Last Name'])
    
    # Clean string columns - adjust these column names if they're different in your Excel file
    string_columns = ['First Name', 'Last Name', 'Email', 'Street', 'City', 'StateZip']
    
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    
    # Sort and reset index
    df = df.sort_values('First Name').reset_index(drop=True)
    
    return df 