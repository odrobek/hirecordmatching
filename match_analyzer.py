from enum import Enum, auto
from typing import Dict, List, Set, Tuple, Union
from rapidfuzz import fuzz
import pandas as pd

def normalize_address(address):
    """
    Normalize address string for better matching.
    Standardizes abbreviations, removes special characters, and converts to lowercase.
    
    Args:
        address: String address to normalize
        
    Returns:
        Normalized address string
    """
    if not isinstance(address, str):
        return ""
    
    # Convert to lowercase
    address = address.lower()
    
    # Standardize common abbreviations
    replacements = {
        'street': 'st',
        'avenue': 'ave',
        'boulevard': 'blvd',
        'drive': 'dr',
        'road': 'rd',
        'lane': 'ln',
        'court': 'ct',
        'circle': 'cir',
        'place': 'pl',
        'north': 'n',
        'south': 's',
        'east': 'e',
        'west': 'w',
        'northeast': 'ne',
        'northwest': 'nw',
        'southeast': 'se',
        'southwest': 'sw'
    }
    
    for full, abbrev in replacements.items():
        address = address.replace(full, abbrev)
    
    # Remove special characters and extra spaces
    address = ''.join(c for c in address if c.isalnum() or c.isspace())
    address = ' '.join(address.split())
    
    return address

class MatchFlags(Enum):
    # Address-Related Flags
    ADDRESS_MISMATCH = auto()
    MULTIPLE_PROPERTIES = auto()
    ADDRESS_FORMAT_DIFF = auto()
    
    # Name-Related Flags
    NAME_MISMATCH = auto()
    MULTIPLE_RESIDENTS = auto()
    NAME_ORDER_SWAP = auto()
    COMPANY_RECORD = auto()
    HOUSEHOLD_COMPOSITION_CHANGE = auto()
    PARTIAL_HOUSEHOLD_MATCH = auto()
    NAME_COUNT_CHANGE = auto()
    COMMON_MEMBER_PRESENT = auto()
    
    # Email-Related Flags
    EMAIL_CHECK_NEEDED = auto()
    EMAIL_PRESERVED = auto()
    MULTIPLE_EMAILS = auto()
    NO_EMAILS = auto()
    
    # Record Existence Flags
    NEW_RECORD = auto()
    RECORD_REMOVED = auto()
    
    # Confidence-Related Flags
    HIGH_CONFIDENCE_MATCH = auto()
    MEDIUM_CONFIDENCE_MATCH = auto()
    LOW_CONFIDENCE_MATCH = auto()
    POTENTIAL_DUPLICATE = auto()

class MatchAnalyzer:
    def __init__(self):
        # Thresholds for fuzzy matching
        self.name_match_threshold = 90
        self.address_match_threshold = 90
        
        # Score weights
        self.email_match_weight = 50
        self.name_match_weight = 35
        self.address_match_weight = 15
        
        # Flag descriptions and examples for UI tooltips
        self.flag_metadata = {
            # Address-Related Flags
            MatchFlags.ADDRESS_MISMATCH: {
                "description": "The addresses between the two sources differ even after normalization",
                "example": 'Excel: "123 Main St" vs. Site: "123 Main Street"'
            },
            MatchFlags.MULTIPLE_PROPERTIES: {
                "description": "The record indicates associations with multiple properties",
                "example": "A contact linked to two different property addresses"
            },
            MatchFlags.ADDRESS_FORMAT_DIFF: {
                "description": "The addresses are essentially the same but formatted differently (e.g., abbreviations)",
                "example": '"321 Elm Rd" vs. "321 Elm Road"'
            },
            
            # Name-Related Flags
            MatchFlags.NAME_MISMATCH: {
                "description": "Slight variations exist in the names (e.g., minor spelling differences)",
                "example": '"Jon Doe" vs. "John Doe"'
            },
            MatchFlags.MULTIPLE_RESIDENTS: {
                "description": "The site data shows multiple residents at a single address",
                "example": '"Alice & Bob Smith"'
            },
            MatchFlags.NAME_ORDER_SWAP: {
                "description": "The order of first and last names might be swapped between sources",
                "example": 'Excel: "Doe, John" vs. Site: "John Doe"'
            },
            MatchFlags.COMPANY_RECORD: {
                "description": "The record appears to represent a company rather than an individual",
                "example": '"Acme Corp"'
            },
            MatchFlags.HOUSEHOLD_COMPOSITION_CHANGE: {
                "description": "The overall household composition has changed",
                "example": 'Excel: "Rick & Laura Diedrich" vs. Site: "Bob & Laura Diedrich"'
            },
            MatchFlags.PARTIAL_HOUSEHOLD_MATCH: {
                "description": "Some household members match while others do not",
                "example": 'Excel: "Rick & Laura Diedrich" vs. Site: "Laura Diedrich"'
            },
            MatchFlags.NAME_COUNT_CHANGE: {
                "description": "The number of individuals in the household has changed",
                "example": 'Excel: "Laura Diedrich" vs. Site: "Bob & Laura Diedrich"'
            },
            MatchFlags.COMMON_MEMBER_PRESENT: {
                "description": "At least one name is common between the two records, indicating partial stability",
                "example": 'Both sources include "Laura Diedrich"'
            },
            
            # Email-Related Flags
            MatchFlags.EMAIL_CHECK_NEEDED: {
                "description": "The site holds a different email or a list that does not include the Excel email",
                "example": 'Excel: "john@example.com" vs. Site: "[jon@example.com]"'
            },
            MatchFlags.EMAIL_PRESERVED: {
                "description": "The site has no email while the Excel record has one, so the Excel email should be retained",
                "example": 'Excel: "mary@example.com", Site: None'
            },
            MatchFlags.MULTIPLE_EMAILS: {
                "description": "The site record contains multiple email addresses, which may require manual review",
                "example": 'Site: "[a@example.com, b@example.com]"'
            },
            MatchFlags.NO_EMAILS: {
                "description": "Neither the site data nor the Excel record provides an email address",
                "example": "Both sources have no email information"
            },
            
            # Record Existence Flags
            MatchFlags.NEW_RECORD: {
                "description": "The contact is present on the site but missing in the Excel document",
                "example": "A new contact found on the site that is not recorded in Excel"
            },
            MatchFlags.RECORD_REMOVED: {
                "description": "The contact exists in the Excel document but is no longer found on the site",
                "example": "A contact listed in Excel that does not appear on the site"
            },
            
            # Confidence-Related Flags
            MatchFlags.HIGH_CONFIDENCE_MATCH: {
                "description": "Multiple matching criteria (name, email, address) indicate a very likely match",
                "example": "All key fields align closely between the two sources"
            },
            MatchFlags.MEDIUM_CONFIDENCE_MATCH: {
                "description": "Two key criteria match while one is uncertain",
                "example": "Name and address match, but email is different or missing"
            },
            MatchFlags.LOW_CONFIDENCE_MATCH: {
                "description": "Only one matching criterion is satisfied or there are conflicting indicators",
                "example": "Only the name matches, with differences in both email and address"
            },
            MatchFlags.POTENTIAL_DUPLICATE: {
                "description": "There are indications of duplicate records requiring manual review",
                "example": "Two site records closely match a single Excel record"
            }
        }

    def compare_household_members(self, excel_names: str, hoa_names: str) -> Tuple[Set[str], Set[str], Set[str]]:
        """
        Compare household members between Excel and HOA records.
        Returns sets of common, excel_only, and hoa_only members.
        """
        # Split names and normalize
        excel_set = {name.strip().lower() for name in excel_names.split('&')}
        hoa_set = {name.strip().lower() for name in hoa_names.split('&')}
        
        # Find common and unique members
        common = excel_set & hoa_set
        excel_only = excel_set - hoa_set
        hoa_only = hoa_set - excel_set
        
        return common, excel_only, hoa_only

    def evaluate_email_flags(self, excel_email: str, hoa_emails: Union[List[str], str]) -> Set[MatchFlags]:
        """Evaluate email-related flags"""
        flags = set()
        
        # Convert HOA emails to list if it's not already
        if isinstance(hoa_emails, str):
            hoa_emails = [hoa_emails]
        
        # Handle empty/None cases
        if not excel_email and not hoa_emails:
            flags.add(MatchFlags.NO_EMAILS)
            return flags
            
        if excel_email and not hoa_emails:
            flags.add(MatchFlags.EMAIL_PRESERVED)
            return flags
            
        if len(hoa_emails) > 1:
            flags.add(MatchFlags.MULTIPLE_EMAILS)
            
        if excel_email:
            excel_email = excel_email.lower()
            if not any(email.lower() == excel_email for email in hoa_emails if email):
                flags.add(MatchFlags.EMAIL_CHECK_NEEDED)
                
        return flags

    def evaluate_name_flags(self, excel_record: Dict, hoa_record: Dict) -> Set[MatchFlags]:
        """Evaluate name-related flags"""
        flags = set()
        
        # Check for company records
        if hoa_record.get('Is Company', False):
            flags.add(MatchFlags.COMPANY_RECORD)
            return flags
            
        # Get full names
        excel_first = excel_record.get('First Name', '')
        excel_last = excel_record.get('Last Name', '')
        hoa_first = hoa_record.get('First Name', '')
        hoa_last = hoa_record.get('Last Name', '')
        
        excel_full = f"{excel_first} {excel_last}".strip()
        hoa_full = f"{hoa_first} {hoa_last}".strip()
        
        # Compare household members if multiple residents
        if '&' in excel_full or '&' in hoa_full:
            common, excel_only, hoa_only = self.compare_household_members(excel_full, hoa_full)
            
            if common:
                flags.add(MatchFlags.COMMON_MEMBER_PRESENT)
            
            if excel_only or hoa_only:
                flags.add(MatchFlags.PARTIAL_HOUSEHOLD_MATCH)
                
            if len(excel_full.split('&')) != len(hoa_full.split('&')):
                flags.add(MatchFlags.NAME_COUNT_CHANGE)
                
            if common and (excel_only or hoa_only):
                flags.add(MatchFlags.HOUSEHOLD_COMPOSITION_CHANGE)
        
        # Check for name mismatches and order swaps
        if excel_last and hoa_last:
            last_name_ratio = fuzz.ratio(excel_last.lower(), hoa_last.lower())
            if last_name_ratio < self.name_match_threshold:
                # Check if names might be swapped
                if fuzz.ratio(excel_last.lower(), hoa_first.lower()) > self.name_match_threshold:
                    flags.add(MatchFlags.NAME_ORDER_SWAP)
                else:
                    flags.add(MatchFlags.NAME_MISMATCH)
        
        return flags

    def evaluate_address_flags(self, excel_record: Dict, hoa_record: Dict) -> Set[MatchFlags]:
        """Evaluate address-related flags"""
        flags = set()
        
        # Construct full addresses
        excel_addr = f"{excel_record.get('Street', '')}\n{excel_record.get('City', '')}, {excel_record.get('StateZip', '')}"
        hoa_addr = hoa_record.get('Full Mailing Address', '')
        
        if not excel_addr.strip() or not hoa_addr.strip():
            return flags
            
        # Normalize addresses
        excel_norm = normalize_address(excel_addr)
        hoa_norm = normalize_address(hoa_addr)
        
        # Compare addresses
        if excel_norm != hoa_norm:
            # Check if it's just a formatting difference
            addr_ratio = fuzz.ratio(excel_norm, hoa_norm)
            if addr_ratio >= self.address_match_threshold:
                flags.add(MatchFlags.ADDRESS_FORMAT_DIFF)
            else:
                flags.add(MatchFlags.ADDRESS_MISMATCH)
        
        # Check for multiple properties if available in HOA record
        if hoa_record.get('Property Street') != hoa_record.get('Mailing Street'):
            flags.add(MatchFlags.MULTIPLE_PROPERTIES)
            
        return flags

    def calculate_match_score(self, excel_record: Dict, hoa_record: Dict) -> Tuple[int, Dict[str, bool], Set[MatchFlags]]:
        """
        Calculate match score and flags between two records.
        Returns:
            - score: int (0-100)
            - match_details: Dict[str, bool]
            - flags: Set[MatchFlags]
        """
        score = 0
        match_details = {
            'email_match': False,
            'last_name_match': False,
            'address_match': False
        }
        all_flags = set()
        
        # Collect flags from all evaluations
        email_flags = self.evaluate_email_flags(excel_record.get('Email', ''), hoa_record.get('Email', []))
        name_flags = self.evaluate_name_flags(excel_record, hoa_record)
        address_flags = self.evaluate_address_flags(excel_record, hoa_record)
        
        all_flags.update(email_flags)
        all_flags.update(name_flags)
        all_flags.update(address_flags)
        
        # Calculate score components
        # Email matching (case insensitive, exact match)
        if excel_record.get('Email') and hoa_record.get('Email'):
            excel_email = excel_record['Email'].lower()
            hoa_emails = hoa_record['Email'] if isinstance(hoa_record['Email'], list) else [hoa_record['Email']]
            
            if any(excel_email == hoa_email.lower() for hoa_email in hoa_emails if hoa_email):
                score += self.email_match_weight
                match_details['email_match'] = True
        
        # Last name matching (fuzzy)
        if excel_record.get('Last Name') and hoa_record.get('Last Name'):
            last_name_ratio = fuzz.ratio(
                excel_record['Last Name'].lower(),
                hoa_record['Last Name'].lower()
            )
            if last_name_ratio >= self.name_match_threshold:
                score += self.name_match_weight
                match_details['last_name_match'] = True
        
        # Address matching (fuzzy)
        excel_addr = f"{excel_record.get('Street', '')}\n{excel_record.get('City', '')}, {excel_record.get('StateZip', '')}"
        hoa_addr = hoa_record.get('Full Mailing Address', '')
        
        if excel_addr and hoa_addr:
            excel_norm = normalize_address(excel_addr)
            hoa_norm = normalize_address(hoa_addr)
            addr_ratio = fuzz.ratio(excel_norm, hoa_norm)
            
            if addr_ratio >= self.address_match_threshold:
                score += self.address_match_weight
                match_details['address_match'] = True
        
        # Add confidence flags based on score
        if score >= 95:
            all_flags.add(MatchFlags.HIGH_CONFIDENCE_MATCH)
        elif score >= 70:
            all_flags.add(MatchFlags.MEDIUM_CONFIDENCE_MATCH)
        elif score > 50:
            all_flags.add(MatchFlags.LOW_CONFIDENCE_MATCH)
            
        return score, match_details, all_flags 