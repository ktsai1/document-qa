"""
Error Seeding Streamlit App for Annotation Data

This app processes CSV files by:
1. Converting csv to excel (resolve unicode issues)
2. Removing rows with blank Document names
3. Calculating acceptance rates for each document based on annotation issues
4. Selecting the worst 10% (or min 5) documents by lowest acceptance rates for each language
5. Seeding errors into the medium 5% (top of the lowest quartile)
6. Creating output files with token_id column (random unique alphanumeric identifier)

Authors: @ktsai, @jburnsky
Date: Aug 13, 2025
Version: 4.0 (Streamlit)
"""

import streamlit as st
import pandas as pd
import chardet
import numpy as np
import os
import random
import string
import io
import base64
from typing import List, Tuple, Set, Dict
import tempfile

# Set page config
st.set_page_config(
    page_title="Error Seeding App",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS
st.markdown("""
<style>
    .main {
        padding: 2rem;
    }
    .stProgress > div > div > div > div {
        background-color: #4CAF50;
    }
    .success {
        color: #4CAF50;
        font-weight: bold;
    }
    .warning {
        color: #FFA500;
        font-weight: bold;
    }
    .error {
        color: #FF0000;
        font-weight: bold;
    }
    .info {
        color: #0000FF;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Helper function to create a download link
def get_download_link(file_path, link_text):
    with open(file_path, 'rb') as f:
        data = f.read()
    b64 = base64.b64encode(data).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{os.path.basename(file_path)}">{link_text}</a>'
    return href

def get_binary_file_downloader_html(file_path, link_text):
    with open(file_path, 'rb') as f:
        data = f.read()
    b64 = base64.b64encode(data).decode()
    ext = os.path.splitext(file_path)[1].lower()
    mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if ext == ".xlsx" else "text/csv"
    href = f'<a href="data:{mime_type};base64,{b64}" download="{os.path.basename(file_path)}">{link_text}</a>'
    return href

def generate_token_id(is_seeded):
    """
    Generate a random unique alphanumeric token ID.
    
    Args:
        is_seeded (bool): Whether this is a seeded error
    
    Returns:
        str: Random alphanumeric token ID
    """
    if is_seeded:
        # For seeded errors, ensure the token contains the letter "s"
        chars = string.ascii_letters + string.digits
        # Generate a random length between 8 and 12
        length = random.randint(8, 12)
        # Ensure at least one "s" is included
        token = ''.join(random.choice(chars) for _ in range(length-1)) + 's'
        # Shuffle to make the "s" position random
        token_list = list(token)
        random.shuffle(token_list)
        return ''.join(token_list)
    else:
        # For non-seeded errors, ensure no "s" is included
        chars = string.ascii_letters.replace('s', '').replace('S', '') + string.digits
        length = random.randint(8, 12)
        return ''.join(random.choice(chars) for _ in range(length))

def detect_encoding(file_content):
    """Detect file encoding."""
    result = chardet.detect(file_content)
    encoding = result['encoding']
    
    # If chardet couldn't detect encoding or detected ASCII, use UTF-8
    if encoding is None or encoding.lower() == 'ascii':
        encoding = 'utf-8'
    
    return encoding

def read_file(uploaded_file, progress_bar):
    """Read uploaded file (CSV or Excel) and detect encoding."""
    try:
        # Get file content
        file_content = uploaded_file.getvalue()
        file_ext = os.path.splitext(uploaded_file.name)[1].lower()
        
        progress_bar.progress(0.2)
        st.info("Detecting file encoding...")
        
        if file_ext in ['.xlsx', '.xls']:
            # Read Excel file
            df = pd.read_excel(io.BytesIO(file_content), header=None)
            encoding = 'utf-8' # Excel files are typically UTF-8 compatible
        else:
            # Detect encoding for CSV
            encoding = detect_encoding(file_content)
            st.info(f"Detected file encoding: {encoding}")
            
            # Read CSV file
            df = pd.read_csv(io.BytesIO(file_content), encoding=encoding, header=None)
        
        progress_bar.progress(0.4)
        
        # Get the headers from the second row (index 1)
        if len(df) > 1:
            headers = df.iloc[0].tolist()
            
            # Read the file again, now using the second row as headers and skipping the first two rows
            if file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(io.BytesIO(file_content), header=None, names=headers, skiprows=1)
            else:
                df = pd.read_csv(io.BytesIO(file_content), encoding=encoding, header=None, names=headers, skiprows=1)
            
            st.success("Successfully read file with headers from row 2")
        else:
            st.warning("File has fewer than 2 rows. Using default headers.")
            if file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(io.BytesIO(file_content))
            else:
                df = pd.read_csv(io.BytesIO(file_content), encoding=encoding)
        
        progress_bar.progress(0.6)
        
        return df, encoding
    
    except Exception as e:
        st.error(f"Error reading file: {str(e)}")
        # Try with UTF-8 encoding if detection fails
        try:
            st.info("Attempting to read file with UTF-8 encoding...")
            if file_ext in ['.xlsx', '.xls']:
                df = pd.read_excel(io.BytesIO(file_content))
            else:
                df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8')
            return df, 'utf-8'
        except Exception as e2:
            st.error(f"Error reading with UTF-8: {str(e2)}")
            raise

def clean_data(df, progress_bar):
    """
    Clean the data by removing rows with blank Document names.
    
    Args:
        df (pd.DataFrame): Original DataFrame
        progress_bar: Streamlit progress bar
    
    Returns:
        pd.DataFrame: Cleaned DataFrame with blank Document names removed
    """
    # Count total rows before cleaning
    total_rows = len(df)
    
    # Remove rows where Document name is blank (NaN, None, empty string)
    cleaned_df = df.dropna(subset=['Document name'])
    cleaned_df = cleaned_df[cleaned_df['Document name'].astype(str).str.strip() != '']
    
    # Count rows removed
    removed_rows = total_rows - len(cleaned_df)
    
    st.write("### Cleaning Data")
    st.write(f"- Total rows before cleaning: {total_rows}")
    st.write(f"- Rows with blank Document names removed: {removed_rows}")
    st.write(f"- Remaining rows: {len(cleaned_df)}")
    
    # Convert Document name to string type
    cleaned_df['Document name'] = cleaned_df['Document name'].astype(str)
    
    progress_bar.progress(0.7)
    
    return cleaned_df, removed_rows

def calculate_acceptance_rates(df, progress_bar):
    """
    Calculate acceptance rates for each document based on annotation issues.
    
    Args:
        df (pd.DataFrame): DataFrame containing annotation data
        progress_bar: Streamlit progress bar
    
    Returns:
        Dict[str, float]: Dictionary mapping document names to acceptance rates
    """
    # Check if required columns exist
    required_columns = ['Document name', 'Error description', 'Annotation Accepted/Rejected']
    if not all(col in df.columns for col in required_columns):
        st.warning("Missing columns for acceptance rate calculation. Using default method.")
        return calculate_default_acceptance_rates(df, progress_bar)
    
    # Initialize dictionaries to store counts
    rejected_counts = {}
    accepted_counts = {}
    
    # Group by document name
    for doc_name, group in df.groupby('Document name'):
        # Count annotation issues that were rejected
        rejected = len(group[(group['Error description'] == 'Annotation issue') & 
                            (group['Annotation Accepted/Rejected'] == 'Rejected')])
        
        # Count annotation issues that were accepted
        accepted = len(group[(group['Error description'] == 'Annotation issue') & 
                            (group['Annotation Accepted/Rejected'] == 'Accepted')])
        
        rejected_counts[doc_name] = rejected
        accepted_counts[doc_name] = accepted
    
    # Calculate acceptance rates
    acceptance_rates = {}
    for doc_name in set(list(rejected_counts.keys()) + list(accepted_counts.keys())):
        rejected = rejected_counts.get(doc_name, 0)
        accepted = accepted_counts.get(doc_name, 0)
        total = rejected + accepted
        
        # Avoid division by zero
        if total > 0:
            # Lower acceptance rate means more rejections
            acceptance_rates[doc_name] = accepted / total
        else:
            # If no annotation issues, set a default value
            acceptance_rates[doc_name] = 1.0 # Assume perfect if no issues
    
    # Print some statistics
    st.write("### Acceptance Rate Statistics")
    st.write(f"- Documents with annotation issues: {len(acceptance_rates)}")
    
    # Print some examples of acceptance rates
    sorted_rates = sorted(acceptance_rates.items(), key=lambda x: x[1])
    if sorted_rates:
        st.write("#### Sample acceptance rates (lowest first):")
        for doc, rate in sorted_rates[:5]:
            st.write(f"- {doc}: {rate:.2%} (Rejected: {rejected_counts.get(doc, 0)}, Accepted: {accepted_counts.get(doc, 0)})")
    
    progress_bar.progress(0.75)
    
    return acceptance_rates

def calculate_default_acceptance_rates(df, progress_bar):
    """
    Fallback method to calculate acceptance rates if the required columns don't exist.
    Uses Reported Problem for Rejection column instead.
    
    Args:
        df (pd.DataFrame): DataFrame containing annotation data
        progress_bar: Streamlit progress bar
    
    Returns:
        Dict[str, float]: Dictionary mapping document names to acceptance rates
    """
    st.info("Using fallback method for acceptance rate calculation.")
    
    # Filter for annotation tasks only
    annotation_df = df[df['Task'] == 'Annotation']
    
    # Initialize dictionaries to store counts
    accepted_counts = {}
    rejected_counts = {}
    
    # Count accepted and rejected issues for each document
    for doc_name, group in annotation_df.groupby('Document name'):
        # Count rows where Reported Problem is empty or NaN (accepted)
        accepted = group['Reported Problem for Rejection'].isna().sum() + \
                  (group['Reported Problem for Rejection'] == '').sum()
        
        # Count rows where Reported Problem is not empty (rejected)
        rejected = len(group) - accepted
        
        accepted_counts[doc_name] = accepted
        rejected_counts[doc_name] = rejected
    
    # Calculate acceptance rates
    acceptance_rates = {}
    for doc_name in accepted_counts:
        accepted = accepted_counts[doc_name]
        rejected = rejected_counts[doc_name]
        total = accepted + rejected
        
        # Avoid division by zero
        if total > 0:
            acceptance_rates[doc_name] = accepted / total
        else:
            acceptance_rates[doc_name] = 0.0
    
    progress_bar.progress(0.75)
    
    return acceptance_rates

def select_worst_documents(acceptance_rates, total_docs_count, min_docs=5, df=None, progress_bar=None):
    """
    Select documents for error seeding:
    1. Determine total sample size: 10% of total docs, or minimum 5 docs
    2. Determine lowest quartile by acceptance rate
    3. 5% of total docs (or min 5) consists of the lowest files by acceptance rate
    4. Another 5% (or remaining docs to reach 10%) from the top of the lowest quartile
    5. Seed errors into the top 5% of files from the combined 10% sample
    
    Args:
        acceptance_rates (Dict[str, float]): Dictionary mapping document names to acceptance rates
        total_docs_count (int): Total number of documents
        min_docs (int): Minimum number of documents to select per language
        df (pd.DataFrame): DataFrame containing all documents, used for language grouping
        progress_bar: Streamlit progress bar
    
    Returns:
        List[str]: List of all document names selected (worst 5% + top of lowest quartile 5%)
        Dict[str, List[str]]: Dictionary mapping languages to lists of document names for error seeding
    """
    # If no DataFrame or no Language column, fall back to non-language specific method
    if df is None or 'Language' not in df.columns:
        # Sort documents by acceptance rate (ascending)
        sorted_docs = sorted(acceptance_rates.items(), key=lambda x: x[1])
        sorted_doc_names = [doc for doc, _ in sorted_docs]
        
        # 1. Determine total sample size: 10% of total docs, or minimum 5 docs
        total_sample_size = max(min_docs, int(total_docs_count * 0.10))
        total_sample_size = min(total_sample_size, len(sorted_docs)) # Don't exceed available docs
        
        # 2. Determine lowest quartile by acceptance rate
        q1_boundary = int(len(sorted_docs) * 0.25) # 25% mark
        lowest_quartile = sorted_doc_names[:q1_boundary]
        
        # 3. 5% of total docs (or min 5) consists of the lowest files by acceptance rate
        worst_sample_size = max(5, int(total_docs_count * 0.05))
        worst_sample_size = min(worst_sample_size, len(sorted_docs)) # Don't exceed available docs
        worst_docs = sorted_doc_names[:worst_sample_size]
        
        # 4. Another 5% (or remaining docs to reach 10%) from the top of the lowest quartile
        remaining_sample_size = total_sample_size - worst_sample_size
        
        if remaining_sample_size > 0 and q1_boundary > worst_sample_size:
            # Get docs from the top of the lowest quartile (excluding those already in worst_docs)
            top_of_lowest_quartile = sorted_doc_names[worst_sample_size:q1_boundary]
            
            # Take from the top (best acceptance rates in lowest quartile)
            if len(top_of_lowest_quartile) > remaining_sample_size:
                quartile_top_selected = top_of_lowest_quartile[-remaining_sample_size:] # Take from the end
            else:
                quartile_top_selected = top_of_lowest_quartile
        else:
            quartile_top_selected = []
        
        # Combine selected documents
        all_selected_docs = worst_docs + quartile_top_selected
        
        # 5. Seed errors into the top 5% of files from the combined 10% sample
        # "Top" means the best acceptance rates (end of the list)
        seed_count = max(5, int(total_docs_count * 0.05))
        seed_count = min(seed_count, len(all_selected_docs)) # Don't exceed available selected docs
        
        # Sort the selected docs by acceptance rate (ascending)
        selected_docs_with_rates = [(doc, acceptance_rates.get(doc, 1.0)) for doc in all_selected_docs]
        selected_docs_sorted = [doc for doc, _ in sorted(selected_docs_with_rates, key=lambda x: x[1])]
        
        # Take the top (best acceptance rates) from the sorted selection
        seed_docs = selected_docs_sorted[-seed_count:]
        
        # Create error seed docs dictionary
        error_seed_docs = {
            'default': seed_docs
        }
        
        # Logging
        st.write("### Document Selection (no language grouping)")
        st.write(f"- Total documents: {len(sorted_docs)}")
        st.write(f"- Total sample size (10%): {total_sample_size} documents")
        st.write(f"- Worst docs sample (5%): {len(worst_docs)} documents")
        st.write(f"- Top of lowest quartile sample: {len(quartile_top_selected)} documents")
        st.write(f"- Documents selected for error seeding: {len(seed_docs)} documents")
        
        if progress_bar:
            progress_bar.progress(0.8)
        
        return all_selected_docs, error_seed_docs
    
    # Group documents by language
    docs_by_language = {}
    for doc_name, group in df.groupby('Document name'):
        # Get the most common language for this document
        if len(group) > 0:
            languages = group['Language'].dropna().unique()
            if len(languages) > 0:
                language = languages[0] # Take the first language if multiple exist
                if language not in docs_by_language:
                    docs_by_language[language] = []
                docs_by_language[language].append(doc_name)
    
    # Select documents per language
    all_selected_docs = []
    error_seed_docs = {}
    
    st.write("### Selecting Documents for Mutation by Language")
    
    for language, docs in docs_by_language.items():
        # Get acceptance rates for this language's documents
        language_acceptance_rates = {doc: acceptance_rates.get(doc, 1.0) for doc in docs}
        
        # Sort documents by acceptance rate (ascending)
        sorted_docs = sorted(language_acceptance_rates.items(), key=lambda x: x[1])
        sorted_doc_names = [doc for doc, _ in sorted_docs]
        
        language_doc_count = len(docs)
        
        # 1. Determine total sample size: 10% of language docs, or minimum 5 docs
        total_sample_size = max(min_docs, int(language_doc_count * 0.10))
        total_sample_size = min(total_sample_size, language_doc_count) # Don't exceed available docs
        
        # 2. Determine lowest quartile by acceptance rate
        q1_boundary = int(language_doc_count * 0.25) # 25% mark
        lowest_quartile = sorted_doc_names[:q1_boundary]
        
        # 3. 5% of language docs (or min 5) consists of the lowest files by acceptance rate
        worst_sample_size = max(5, int(language_doc_count * 0.05))
        worst_sample_size = min(worst_sample_size, language_doc_count) # Don't exceed available docs
        worst_docs = sorted_doc_names[:worst_sample_size]
        
        # 4. Another 5% (or remaining docs to reach 10%) from the top of the lowest quartile
        remaining_sample_size = total_sample_size - worst_sample_size
        
        if remaining_sample_size > 0 and q1_boundary > worst_sample_size:
            # Get docs from the top of the lowest quartile (excluding those already in worst_docs)
            top_of_lowest_quartile = sorted_doc_names[worst_sample_size:q1_boundary]
            
            # Take from the top (best acceptance rates in lowest quartile)
            if len(top_of_lowest_quartile) > remaining_sample_size:
                quartile_top_selected = top_of_lowest_quartile[-remaining_sample_size:] # Take from the end
            else:
                quartile_top_selected = top_of_lowest_quartile
        else:
            quartile_top_selected = []
        
        # Combine selected documents
        language_selected_docs = worst_docs + quartile_top_selected
        
        # 5. Seed errors into the top 5% of files from the combined 10% sample
        # "Top" means the best acceptance rates (end of the list)
        seed_count = max(5, int(language_doc_count * 0.05))
        seed_count = min(seed_count, len(language_selected_docs)) # Don't exceed available selected docs
        
        # Sort the selected docs by acceptance rate (ascending)
        selected_docs_with_rates = [(doc, language_acceptance_rates.get(doc, 1.0)) for doc in language_selected_docs]
        selected_docs_sorted = [doc for doc, _ in sorted(selected_docs_with_rates, key=lambda x: x[1])]
        
        # Take the top (best acceptance rates) from the sorted selection
        language_error_seed_docs = selected_docs_sorted[-seed_count:]
        
        # Logging
        st.write(f"#### {language}:")
        st.write(f"- Total documents: {language_doc_count}")
        st.write(f"- Total sample size (10%): {total_sample_size} documents")
        st.write(f"- Worst docs sample (5%): {len(worst_docs)} documents")
        st.write(f"- Top of lowest quartile sample: {len(quartile_top_selected)} documents")
        st.write(f"- Documents selected for error seeding: {len(language_error_seed_docs)} documents")
        
        # Add to overall lists
        all_selected_docs.extend(language_selected_docs)
        error_seed_docs[language] = language_error_seed_docs
    
    st.write(f"**Total documents selected across all languages: {len(all_selected_docs)}**")
    
    if progress_bar:
        progress_bar.progress(0.8)
    
    return all_selected_docs, error_seed_docs

def create_language_specific_error_banks(df: pd.DataFrame, worst_docs: List[str], progress_bar=None):
    """
    Create language-specific error banks for each document to be mutated.
    
    For each language:
    - If only 1 file, skip that language
    - If 2-8 files, use cross-file error banks (errors from other files in the same language)
    - If >8 files, use errors from files not selected for mutation
    
    Args:
        df (pd.DataFrame): Full DataFrame containing all documents
        worst_docs (List[str]): List of document names selected for mutation
        progress_bar: Streamlit progress bar
    
    Returns:
        Dict[str, Dict[str, List[str]]]: Dictionary mapping languages to dictionaries 
                                         mapping document names to lists of errors
    """
    # Check if Language column exists
    if 'Language' not in df.columns:
        st.warning("'Language' column not found. Using default language.")
        # Create a single error bank for all documents
        error_list = create_error_list(df[~df['Document name'].isin(worst_docs)])
        return {'default': {'default': error_list}}
    
    # Group documents by language
    docs_by_language = {}
    for doc_name, group in df.groupby('Document name'):
        # Get the most common language for this document
        if len(group) > 0:
            languages = group['Language'].dropna().unique()
            if len(languages) > 0:
                language = languages[0] # Take the first language if multiple exist
                if language not in docs_by_language:
                    docs_by_language[language] = []
                docs_by_language[language].append(doc_name)
    
    # Create error banks for each language and document
    language_error_banks = {}
    
    st.write("### Creating Error Banks")
    
    for language, docs in docs_by_language.items():
        # Skip languages with only 1 document
        if len(docs) < 2:
            st.warning(f"Language '{language}' has only {len(docs)} document. Skipping.")
            continue
        
        # Get documents selected for mutation in this language
        selected_docs = [doc for doc in docs if doc in worst_docs]
        
        # Skip if no documents in this language were selected
        if not selected_docs:
            continue
            
        # Create document-specific error banks
        language_error_banks[language] = {}
        
        if 2 <= len(docs) <= 8:
            st.info(f"Language '{language}' has {len(docs)} documents. Using cross-file error banks.")
            # For each selected document, create an error bank from other documents
            for doc in selected_docs:
                other_docs = [d for d in docs if d != doc]
                # Create error bank from other documents in the same language
                error_bank = []
                for other_doc in other_docs:
                    doc_errors = extract_errors_from_document(df, other_doc)
                    error_bank.extend(doc_errors)
                
                # Remove duplicates
                error_bank = list(set(error_bank))
                language_error_banks[language][doc] = error_bank
                st.write(f"- Document '{doc}': {len(error_bank)} errors available from {len(other_docs)} other documents")
        else: # More than 8 files
            st.info(f"Language '{language}' has {len(docs)} documents. Using non-selected documents for error banks.")
            # Use non-selected documents as error sources
            non_selected_docs = [doc for doc in docs if doc not in worst_docs]
            
            # Create error bank from non-selected documents
            common_error_bank = []
            for doc in non_selected_docs:
                doc_errors = extract_errors_from_document(df, doc)
                common_error_bank.extend(doc_errors)
            
            # Remove duplicates
            common_error_bank = list(set(common_error_bank))
            
            # Use the same error bank for all selected documents
            for doc in selected_docs:
                language_error_banks[language][doc] = common_error_bank.copy()
            
            st.write(f"- {len(common_error_bank)} errors available from {len(non_selected_docs)} non-selected documents")
    
    if progress_bar:
        progress_bar.progress(0.85)
    
    return language_error_banks

def extract_errors_from_document(df: pd.DataFrame, doc_name: str) -> List[str]:
    """
    Extract rejected annotation issues from a specific document.
    ONLY includes rows where:
    1. 'Annotation Accepted/Rejected' is 'Rejected' AND
    2. 'Error description' is 'Annotation issue'
    
    Args:
        df (pd.DataFrame): DataFrame containing all documents
        doc_name (str): Name of the document to extract errors from
    
    Returns:
        List[str]: List of rejected annotation issues
    """
    doc_df = df[df['Document name'] == doc_name]
    
    # Extract rejected annotation issues - strict filtering
    errors = doc_df[
        (doc_df['Reported Problem for Rejection'].notna()) & # not blank
        (doc_df['Reported Problem for Rejection'] != '') & # not empty string
        (doc_df['Error description'] == 'Annotation issue') & # ONLY 'Annotation issue'
        (doc_df['Annotation Accepted/Rejected'] == 'Rejected') # ONLY 'Rejected'
    ]['Reported Problem for Rejection'].unique().tolist()
    
    return errors

def create_error_list(df: pd.DataFrame) -> List[str]:
    """
    Create list of annotation issues for seeding, excluding blank entries.
    Only includes rows where 'Annotation Accepted/Rejected' has the value 'Rejected'.
    
    Args:
        df (pd.DataFrame): DataFrame containing the subset of data to extract errors from
    
    Returns:
        List[str]: List of unique errors for seeding
    """
    # Handle potential NaN values in Reported Problem for Rejection
    error_list = df[
        (df['Reported Problem for Rejection'].notna()) & # not blank
        (df['Reported Problem for Rejection'] != '') & # not empty string
        (df['Annotation Accepted/Rejected'] == 'Rejected') # only where status is 'Rejected'
    ]['Reported Problem for Rejection'].unique().tolist()
    
    st.write(f"Number of lures for error seeding: {len(error_list)}")
    
    if error_list:
        st.write("Sample of error list:")
        for error in error_list[:5]:
            st.write(f"- {error}")
    
    return error_list

def add_errors_from_bank(error_bank: List[str], doc_df: pd.DataFrame, error_count: int) -> pd.DataFrame:
    """
    Add seeded errors to a document DataFrame and randomly distribute them among existing rows.
    If there aren't enough errors in the bank, duplicate just enough errors to meet the required count,
    ensuring maximum variety by cycling through all errors before repeating any.
    
    All seeded errors will have:
    1. 'Annotation Accepted/Rejected' set to 'Rejected'
    2. 'Error description' set to 'Annotation issue'
    
    Args:
        error_bank (List[str]): Available errors to sample from
        doc_df (pd.DataFrame): DataFrame containing rows for a single document
        error_count (int): Number of errors to seed (max 15 or 20% of doc rows)
    
    Returns:
        pd.DataFrame: Original DataFrame with seeded error rows randomly distributed
    """
    # If error bank is empty, return the original DataFrame
    if not error_bank:
        return doc_df
    
    # Create copy of original DataFrame and mark all rows as not seeded
    new_df = doc_df.copy()
    new_df['is_seeded_error'] = False
    
    # If we need more errors than are available, duplicate with maximum variety
    if len(error_bank) < error_count:
        original_count = len(error_bank)
        
        # Calculate how many complete cycles of the error bank we need
        complete_cycles = error_count // len(error_bank)
        remainder = error_count % len(error_bank)
        
        # Create expanded error bank with complete cycles of the original bank
        expanded_error_bank = []
        for _ in range(complete_cycles):
            expanded_error_bank.extend(error_bank)
        
        # Add the remaining errors needed from the beginning of the bank
        if remainder > 0:
            expanded_error_bank.extend(error_bank[:remainder])
        
        # Log information about error duplication
        duplicated_errors = len(expanded_error_bank) - len(error_bank)
        st.warning(f"⚠️ DUPLICATING ERRORS: Added {duplicated_errors} duplicate errors to bank")
        st.write(f"   Original bank size: {original_count} errors")
        st.write(f"   Expanded bank size: {len(expanded_error_bank)} errors")
        st.write(f"   Method: {complete_cycles} complete cycles plus {remainder} additional errors")
        
        # Use the expanded error bank
        working_error_bank = expanded_error_bank
    else:
        # If we have enough errors, use the original error bank without duplication
        working_error_bank = error_bank.copy()
        st.success(f"✓ Using {len(working_error_bank)} errors from bank (no duplication needed)")
    
    # Select random errors without replacement
    errors = np.random.choice(working_error_bank, size=error_count, replace=False)
    
    # Create seeded rows
    seeded_rows = []
    for error in errors:
        # Sample a random row as template
        if len(doc_df) > 0:
            row = doc_df.sample(n=1).copy()
            
            # Set the required fields for seeded errors
            row['Reported Problem for Rejection'] = error
            row['is_seeded_error'] = True
            
            # CRITICAL: Always set required fields for seeded errors
            if 'Annotation Accepted/Rejected' in row.columns:
                row['Annotation Accepted/Rejected'] = 'Rejected'
            
            if 'Error description' in row.columns:
                row['Error description'] = 'Annotation issue'
            
            seeded_rows.append(row)
    
    # If no seeded rows were created, return the original DataFrame
    if not seeded_rows:
        return new_df
    
    # Combine all seeded rows
    seeded_df = pd.concat(seeded_rows)
    
    # Generate random positions to insert seeded rows
    total_rows = len(new_df) + len(seeded_rows)
    insert_positions = sorted(np.random.choice(
        range(total_rows), 
        size=len(seeded_rows), 
        replace=False
    ))
    
    # Insert seeded rows at random positions
    final_rows = []
    original_idx = 0
    seeded_idx = 0
    
    for i in range(total_rows):
        if i in insert_positions and seeded_idx < len(seeded_df):
            # Insert a seeded row
            final_rows.append(seeded_df.iloc[seeded_idx])
            seeded_idx += 1
        elif original_idx < len(new_df):
            # Insert an original row
            final_rows.append(new_df.iloc[original_idx])
            original_idx += 1
    
    # Combine all rows into a new DataFrame
    try:
        return pd.concat(final_rows, axis=1).T
    except:
        # Fallback if concat fails
        result = pd.concat([new_df] + seeded_rows)
        result['is_seeded_error'] = result['is_seeded_error'].fillna(False)
        return result

def generate_acceptance_rates_report(acceptance_rates):
    """Generate a report of document names and acceptance rates in ascending order."""
    report = []
    report.append("Document Acceptance Rates Report")
    report.append("=============================\n")
    report.append("Documents sorted by acceptance rate (lowest first):")
    report.append("------------------------------------------------")
    
    # Sort documents by acceptance rate (ascending)
    sorted_docs = sorted(acceptance_rates.items(), key=lambda x: x[1])
    
    for i, (doc, rate) in enumerate(sorted_docs):
        report.append(f"{i+1}. {doc}: {rate:.2%} acceptance rate")
    
    return "\n".join(report)

def generate_mutation_report(acceptance_rates, mutated_documents):
    """Generate a report of document names, acceptance rates, and mutation status."""
    report = []
    report.append("Document Mutation Report")
    report.append("======================\n")
    report.append("Documents sorted by acceptance rate (lowest first):")
    report.append("------------------------------------------------")
    
    # Sort documents by acceptance rate (ascending)
    sorted_docs = sorted(acceptance_rates.items(), key=lambda x: x[1])
    
    for i, (doc, rate) in enumerate(sorted_docs):
        mutation_status = "MUTATED" if doc in mutated_documents else "Not mutated"
        report.append(f"{i+1}. {doc}: {rate:.2%} acceptance rate - {mutation_status}")
    
    return "\n".join(report)

def generate_worst_documents_report(worst_docs, acceptance_rates):
    """Generate a report listing the document names for the worst 10% by acceptance rate for each language."""
    # Create the report content
    report = []
    report.append("Worst 10% Documents by Acceptance Rate for Each Language")
    report.append("====================================================\n")
    
    # Sort the worst documents by acceptance rate
    sorted_docs = sorted(
        [(doc, acceptance_rates.get(doc, 0.0)) for doc in worst_docs],
        key=lambda x: x[1]
    )
    
    for doc, rate in sorted_docs:
        report.append(f"{doc}: {rate:.2%} acceptance rate")
    
    return "\n".join(report)

def process_file(uploaded_file):
    """Process the uploaded file and return the results."""
    try:
        # Create a temporary directory for output files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create progress bar
            progress_bar = st.progress(0.1)
            st.write("### Processing File")
            
            # Read the uploaded file
            df, encoding = read_file(uploaded_file, progress_bar)
            
            # Print column names for debugging
            st.write("### Columns in the file")
            st.write(df.columns.tolist())
            
            # Check if required columns exist
            required_columns = ['Document name', 'Task', 'Reported Problem for Rejection']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                st.error(f"Missing required columns: {missing_columns}")
                return None
            
            # Check if Language column exists
            if 'Language' not in df.columns:
                st.warning("'Language' column not found. Will use default language for error seeding.")
            
            # Clean data by removing rows with blank Document names
            cleaned_df, blank_rows_removed = clean_data(df, progress_bar)
            
            # Get the total number of unique documents
            total_unique_documents = len(cleaned_df['Document name'].unique())
            st.write(f"### Total unique documents: {total_unique_documents}")
            
            # DIAGNOSTIC: Count documents by language
            if 'Language' in cleaned_df.columns:
                language_counts = cleaned_df.groupby('Language')['Document name'].nunique()
                st.write("### Documents by language")
                for language, count in language_counts.items():
                    st.write(f"- {language}: {count} documents")
            
            # DIAGNOSTIC: Count rejected annotation issues
            rejected_issues = cleaned_df[
                (cleaned_df['Reported Problem for Rejection'].notna()) &
                (cleaned_df['Reported Problem for Rejection'] != '') &
                (cleaned_df['Annotation Accepted/Rejected'] == 'Rejected')
            ]
            rejected_count = len(rejected_issues)
            rejected_docs = rejected_issues['Document name'].nunique()
            st.write(f"### Rejected annotation issues: {rejected_count} issues across {rejected_docs} documents")
            
            # Calculate acceptance rates for each document using the correct columns
            acceptance_rates = calculate_acceptance_rates(cleaned_df, progress_bar)
            
            # Select worst documents (lowest acceptance rates) - 10% or minimum 5
            # Also include all documents from languages with fewer than 5 documents
            worst_docs, error_seed_docs = select_worst_documents(
                acceptance_rates, total_unique_documents, min_docs=5, df=cleaned_df, progress_bar=progress_bar
            )
            
            # Create language-specific error banks for each document to be mutated
            language_error_banks = create_language_specific_error_banks(
                cleaned_df, worst_docs, progress_bar=progress_bar
            )
            
            # DIAGNOSTIC: Count documents with error banks
            docs_with_error_banks = 0
            docs_with_empty_error_banks = 0
            for language, doc_banks in language_error_banks.items():
                for doc, errors in doc_banks.items():
                    if errors:
                        docs_with_error_banks += 1
                    else:
                        docs_with_empty_error_banks += 1
            
            st.write("### Error bank statistics")
            st.write(f"- Documents with non-empty error banks: {docs_with_error_banks}")
            st.write(f"- Documents with empty error banks: {docs_with_empty_error_banks}")
            
            # DIAGNOSTIC: Count documents by language that will be skipped
            skipped_docs = []
            for doc in worst_docs:
                doc_df = cleaned_df[cleaned_df['Document name'] == doc]
                
                # Skip empty documents
                if len(doc_df) == 0:
                    skipped_docs.append((doc, "empty document"))
                    continue
                
                # Get document language
                if 'Language' in doc_df.columns and len(doc_df) > 0:
                    languages = doc_df['Language'].dropna().unique()
                    if len(languages) > 0:
                        language = languages[0]
                    else:
                        language = 'default'
                else:
                    language = 'default'
                
                # Check if error bank exists
                if language not in language_error_banks:
                    skipped_docs.append((doc, f"no error bank for language '{language}'"))
                    continue
                
                if doc not in language_error_banks[language] and 'default' not in language_error_banks[language]:
                    skipped_docs.append((doc, f"no document-specific error bank"))
                    continue
                
                # Get error bank
                if doc in language_error_banks[language]:
                    error_bank = language_error_banks[language][doc]
                else:
                    error_bank = language_error_banks[language].get('default', [])
                
                # Check if error bank is empty
                if not error_bank:
                    skipped_docs.append((doc, f"empty error bank"))
                    continue
            
            st.write(f"### Documents that will be skipped: {len(skipped_docs)} out of {len(worst_docs)}")
            if skipped_docs:
                st.write("Sample of skipped documents:")
                for i, (doc, reason) in enumerate(skipped_docs[:10]):
                    st.write(f"- {doc}: {reason}")
                if len(skipped_docs) > 10:
                    st.write(f"... and {len(skipped_docs) - 10} more")
            
            progress_bar.progress(0.9)
            
            # Check if we have any error banks
            if not language_error_banks:
                st.warning("No valid error banks created. No mutations will be performed.")
                # Create empty output files
                new_df = cleaned_df.copy()
                new_df['is_seeded_error'] = False
                
                # Generate token_ids for each row
                new_df['token_id'] = new_df['is_seeded_error'].apply(generate_token_id)
                
                # Reorder columns to have token_id as first column
                cols = ['token_id'] + [col for col in new_df.columns if col != 'token_id' and col != 'is_seeded_error']
                final_df = new_df[cols]
                
                # Generate output file paths
                base_name = os.path.splitext(uploaded_file.name)[0]
                output_csv_path = os.path.join(temp_dir, f"{base_name}-output.csv")
                output_xlsx_path = os.path.join(temp_dir, f"{base_name}-output.xlsx")
                
                # Save final version in both formats
                final_df.to_csv(output_csv_path, index=False, encoding=encoding)
                final_df.to_excel(output_xlsx_path, index=False, engine='openpyxl')
                
                st.success("Processing complete! No mutations were performed.")
                
                # Return paths to the output files
                return {
                    'csv_path': output_csv_path,
                    'xlsx_path': output_xlsx_path,
                    'final_df': final_df,
                    'mutated_docs_count': 0,
                    'seeded_count': 0
                }
            
            # Start with a copy of all processed data
            # and add is_seeded_error column set to False
            new_df = cleaned_df.copy()
            new_df['is_seeded_error'] = False
            
            # Track documents that have mutations
            mutated_documents = set()
            mutated_docs_count = 0
            
            st.write("### Adding Seeded Errors")
            
            # Process only the documents in error_seed_docs for mutation (medium 5%)
            for language, docs_to_seed in error_seed_docs.items():
                for doc in docs_to_seed:
                    doc_df = cleaned_df[cleaned_df['Document name'] == doc]
                    
                    # Skip empty documents
                    if len(doc_df) == 0:
                        st.write(f"Skipping empty document: {doc}")
                        continue
                    
                    # Skip if no error bank for this language or document
                    if language not in language_error_banks or (
                        language in language_error_banks and 
                        doc not in language_error_banks[language] and
                        'default' not in language_error_banks[language]
                    ):
                        st.write(f"Skipping document '{doc}': No error bank available for language '{language}'")
                        continue
                    
                    # Get error bank for this document
                    if doc in language_error_banks[language]:
                        error_bank = language_error_banks[language][doc]
                    else:
                        error_bank = language_error_banks[language].get('default', [])
                    
                    # Skip if error bank is empty
                    if not error_bank:
                        st.write(f"Skipping document '{doc}': Empty error bank for language '{language}'")
                        continue
                        
                    # Calculate number of errors to seed
                    errors_to_seed = max(5, min(15, int(len(doc_df) * 0.2)))
                    
                    # Make sure we don't try to seed more errors than we have in the bank
                    errors_to_seed = min(errors_to_seed, len(error_bank))
                    
                    if errors_to_seed > 0:
                        try:
                            # Add errors to the document
                            seeded_doc_df = add_errors_from_bank(error_bank, doc_df, errors_to_seed)
                            
                            # Replace this document's rows in the output DataFrame
                            new_df = new_df[new_df['Document name'] != doc] # Remove original rows
                            new_df = pd.concat([new_df, seeded_doc_df]) # Add rows with seeds
                            
                            # Add document to the mutated set
                            mutated_documents.add(doc)
                            mutated_docs_count += 1
                            st.success(f"Added {errors_to_seed} errors to document: {doc} (acceptance rate: {acceptance_rates.get(doc, 0.0):.2%})")
                        except Exception as e:
                            st.error(f"Error adding errors to document {doc}: {str(e)}")
                            # Continue with next document
                            continue
            
            progress_bar.progress(0.95)
            
            # Fill NaN values in is_seeded_error column
            new_df['is_seeded_error'] = new_df['is_seeded_error'].fillna(False)
            
            # Generate token_ids for each row based on is_seeded_error flag
            new_df['token_id'] = new_df['is_seeded_error'].apply(generate_token_id)
            
            # Reorder columns to have token_id as first column
            cols = ['token_id'] + [col for col in new_df.columns if col != 'token_id' and col != 'is_seeded_error']
            final_df = new_df[cols]
            
            # Generate output file paths
            base_name = os.path.splitext(uploaded_file.name)[0]
            output_csv_path = os.path.join(temp_dir, f"{base_name}-output.csv")
            output_xlsx_path = os.path.join(temp_dir, f"{base_name}-output.xlsx")
            
            # Save final version in both formats
            final_df.to_csv(output_csv_path, index=False, encoding=encoding)
            final_df.to_excel(output_xlsx_path, index=False, engine='openpyxl')
            
            # Generate and save reports
            acceptance_rates_report = generate_acceptance_rates_report(acceptance_rates)
            mutation_report = generate_mutation_report(acceptance_rates, mutated_documents)
            worst_docs_report = generate_worst_documents_report(worst_docs, acceptance_rates)
            
            # Save reports to temp files
            acceptance_rates_path = os.path.join(temp_dir, f"{base_name}-acceptance-rates.txt")
            mutation_report_path = os.path.join(temp_dir, f"{base_name}-mutation-details.txt")
            worst_docs_report_path = os.path.join(temp_dir, f"{base_name}-worst-documents.txt")
            
            with open(acceptance_rates_path, 'w', encoding=encoding) as f:
                f.write(acceptance_rates_report)
            
            with open(mutation_report_path, 'w', encoding=encoding) as f:
                f.write(mutation_report)
            
            with open(worst_docs_report_path, 'w', encoding=encoding) as f:
                f.write(worst_docs_report)
            
            progress_bar.progress(1.0)
            
            # Count seeded errors
            seeded_count = new_df['is_seeded_error'].sum()
            
            st.success("Processing complete!")
            
            # Return paths to the output files and reports
            return {
                'csv_path': output_csv_path,
                'xlsx_path': output_xlsx_path,
                'acceptance_rates_path': acceptance_rates_path,
                'mutation_report_path': mutation_report_path,
                'worst_docs_report_path': worst_docs_report_path,
                'final_df': final_df,
                'mutated_docs_count': mutated_docs_count,
                'seeded_count': seeded_count,
                'blank_rows_removed': blank_rows_removed,
                'total_unique_documents': total_unique_documents,
                'worst_docs_count': len(worst_docs)
            }
        
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None

def main():
    st.title("Error Seeding App for Annotation Data")
    
    st.markdown("""
    This app processes CSV/Excel files containing annotation data and seeds errors into selected documents.
    
    ### How it works:
    1. Upload a CSV or Excel file with annotation data
    2. The app calculates acceptance rates for each document
    3. The worst 10% of documents by acceptance rate are selected
    4. Errors are seeded into the medium 5% (not the worst 5%)
    5. Output files are generated with token_ids that indicate seeded errors
    
    ### Output:
    - CSV and Excel files with token_id column
    - Seeded error rows have token_ids containing the letter "s"
    - Non-seeded error rows have token_ids WITHOUT the letter "s"
    """)
    
    # File uploader
    uploaded_file = st.file_uploader("Upload a CSV or Excel file", type=["csv", "xlsx", "xls"])
    
    if uploaded_file is not None:
        # Process button
        if st.button("Process File"):
            with st.spinner("Processing file..."):
                results = process_file(uploaded_file)
                
                if results:
                    # Display summary
                    st.write("## Processing Summary")
                    st.write(f"- Rows with blank Document names removed: {results.get('blank_rows_removed', 0)}")
                    st.write(f"- Total unique documents: {results.get('total_unique_documents', 0)}")
                    st.write(f"- Documents selected for mutation: {results.get('worst_docs_count', 0)}")
                    st.write(f"- Documents with mutations: {results.get('mutated_docs_count', 0)}")
                    st.write(f"- Documents skipped: {results.get('worst_docs_count', 0) - results.get('mutated_docs_count', 0)}")
                    st.write(f"- Total seeded errors: {results.get('seeded_count', 0)}")
                    
                    # Display download links
                    st.write("## Download Results")
                    
                    # Create columns for download buttons
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(get_binary_file_downloader_html(
                            results['csv_path'], "Download CSV Output"), unsafe_allow_html=True)
                    
                    with col2:
                        st.markdown(get_binary_file_downloader_html(
                            results['xlsx_path'], "Download Excel Output"), unsafe_allow_html=True)
                    
                    # Display report download links if available
                    if 'acceptance_rates_path' in results:
                        st.write("## Download Reports")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.markdown(get_binary_file_downloader_html(
                                results['acceptance_rates_path'], "Acceptance Rates Report"), unsafe_allow_html=True)
                        
                        with col2:
                            st.markdown(get_binary_file_downloader_html(
                                results['mutation_report_path'], "Mutation Details Report"), unsafe_allow_html=True)
                        
                        with col3:
                            st.markdown(get_binary_file_downloader_html(
                                results['worst_docs_report_path'], "Worst Documents Report"), unsafe_allow_html=True)
                    
                    # Display preview of the output data
                    st.write("## Output Data Preview")
                    st.dataframe(results['final_df'].head(10))

if __name__ == "__main__":
    main()
