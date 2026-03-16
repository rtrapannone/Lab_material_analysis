#!/usr/bin/env python
# coding: utf-8

# Script for analysis purchase from excel file and extracting the plastic data. Everything that has been assigned to the subgroup "plastic" will be extracted and moved into a new file wheere items are ranked by the expenses. Items with the same name have been summed up. Small spelling mistakes are also considered.

# In[ ]:


import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import re

def similarity(a, b):
    """Calculate similarity between two strings (0-1, where 1 is identical)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_plastic_categories(categories, threshold=0.7):
    """
    Find category values that likely refer to 'plastic' using fuzzy matching.
    
    Parameters:
    categories (list): List of unique category values
    threshold (float): Similarity threshold (0.7 = 70% similar)
    
    Returns:
    list: Categories that likely mean 'plastic'
    """
    plastic_variations = []
    target = "plastic"
    
    for category in categories:
        if pd.isna(category):
            continue
            
        category_clean = str(category).strip().lower()
        
        # Direct substring match
        if target in category_clean or category_clean in target:
            plastic_variations.append(category)
            continue
            
        # Fuzzy matching for typos
        if similarity(category_clean, target) >= threshold:
            plastic_variations.append(category)
            continue
            
        # Check for common misspellings
        common_misspellings = [
            "platic", "plastik", "plasitc", "plastc", "plstic", 
            "plasctic", "plaastic", "plastique", "plastci"
        ]
        
        for misspelling in common_misspellings:
            if similarity(category_clean, misspelling) >= 0.8:
                plastic_variations.append(category)
                break
    
    return plastic_variations

def normalize_item_names(items):
    """
    Normalize item names for better grouping (handles minor variations).
    
    Parameters:
    items (pandas.Series): Series of item names
    
    Returns:
    pandas.Series: Normalized item names
    """
    # Convert to string and basic cleaning
    normalized = items.astype(str).str.strip()
    
    # Remove extra whitespace
    normalized = normalized.str.replace(r'\s+', ' ', regex=True)
    
    # Convert to lowercase for comparison
    normalized = normalized.str.lower()
    
    # Remove common variations in punctuation
    normalized = normalized.str.replace(r'[^\w\s]', '', regex=True)
    
    return normalized

def group_similar_items(df, item_col, cost_col, similarity_threshold=0.85):
    """
    Group items with similar names and sum their costs.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with items and costs
    item_col (str): Name of the item column
    cost_col (str): Name of the cost column
    similarity_threshold (float): Threshold for considering items similar
    
    Returns:
    pandas.DataFrame: Grouped DataFrame with summed costs
    """
    if df.empty:
        return df
    
    # Create a copy to work with
    df_work = df.copy()
    
    # Normalize item names for better grouping
    df_work['normalized_name'] = normalize_item_names(df_work[item_col])
    
    # Simple grouping by exact normalized names first
    grouped_simple = df_work.groupby('normalized_name').agg({
        item_col: 'first',  # Keep the first occurrence of the original name
        cost_col: 'sum',    # Sum all costs
        'normalized_name': 'count'  # Count occurrences
    }).rename(columns={'normalized_name': 'count'})
    
    # For more sophisticated fuzzy grouping (optional - can be slow with many items)
    # This part groups items that are very similar but not exactly the same
    unique_normalized = grouped_simple.index.tolist()
    
    # Create groups of similar items
    groups = []
    used_items = set()
    
    for i, item1 in enumerate(unique_normalized):
        if item1 in used_items:
            continue
            
        current_group = [item1]
        used_items.add(item1)
        
        for j, item2 in enumerate(unique_normalized[i+1:], i+1):
            if item2 in used_items:
                continue
                
            if similarity(item1, item2) >= similarity_threshold:
                current_group.append(item2)
                used_items.add(item2)
        
        groups.append(current_group)
    
    # Merge similar groups and sum costs
    final_grouped = []
    
    for group in groups:
        if len(group) == 1:
            # Single item, use as is
            item_name = grouped_simple.loc[group[0], item_col]
            total_cost = grouped_simple.loc[group[0], cost_col]
            total_count = grouped_simple.loc[group[0], 'count']
        else:
            # Multiple similar items, merge them
            item_name = grouped_simple.loc[group[0], item_col]  # Use first item's original name
            total_cost = sum(grouped_simple.loc[item, cost_col] for item in group)
            total_count = sum(grouped_simple.loc[item, 'count'] for item in group)
        
        final_grouped.append({
            item_col: item_name,
            cost_col: total_cost,
            'count': total_count,
            'grouped_items': len(group)
        })
    
    return pd.DataFrame(final_grouped)

def extract_and_rank_plastic_items(excel_file_path, sheet_name=None, plastic_threshold=0.7, item_similarity_threshold=0.85):
    """
    Extract items classified as 'plastic' (with fuzzy matching) and rank them by total cost.
    Groups items with the same/similar names and sums their costs.
    
    Parameters:
    excel_file_path (str): Path to the Excel file
    sheet_name (str): Name of the sheet to read (None for first sheet)
    plastic_threshold (float): Similarity threshold for finding 'plastic' categories
    item_similarity_threshold (float): Similarity threshold for grouping similar item names
    
    Returns:
    pandas.DataFrame: Filtered, grouped, and sorted DataFrame with plastic items
    """
    
    try:
        # First, let's inspect the Excel file structure
        print("Inspecting Excel file structure...")
        
        # Get all sheet names
        excel_file = pd.ExcelFile(excel_file_path)
        sheet_names = excel_file.sheet_names
        print(f"Available sheets: {sheet_names}")
        
        # If no sheet_name specified, use the first sheet
        if sheet_name is None:
            sheet_name = sheet_names[0]
            print(f"Using sheet: '{sheet_name}'")
        
        # Read the Excel file with specific parameters to handle formatting issues
        # Skip the first row since it contains the headers we want in row 1 (0-indexed)
        df = pd.read_excel(
            excel_file_path, 
            sheet_name=sheet_name,
            header=0,  # Use first row as header (which contains "Kostenstelle", "Segmenttext", etc.)
            skiprows=2,  # Skip the first 3 rows (0,1,2) to get to actual data
            engine='openpyxl'  # Specify engine for better compatibility
        )
        
        # Set proper column names based on your data structure
        expected_columns = ['Cost_Center', 'Item_Description', 'Cost_EUR', 'Main_Category', 'Second_Level_Category']
        if len(df.columns) >= 5:
            df.columns = expected_columns[:len(df.columns)]
        
        # Check if we actually got a DataFrame
        if not isinstance(df, pd.DataFrame):
            print(f"Error: Expected DataFrame, got {type(df)}")
            print("This might be due to multiple sheets or formatting issues.")
            return pd.DataFrame()
        
        # Display basic info about the dataset
        print(f"\nDataFrame successfully loaded!")
        print(f"Total rows in dataset: {len(df)}")
        print(f"Total columns in dataset: {len(df.columns)}")
        print(f"Column names: {list(df.columns)}")
        print(f"DataFrame shape: {df.shape}")
        
        # Show first few rows
        print("\nFirst few rows:")
        print(df.head())
        
        # Show data types
        print("\nColumn data types:")
        print(df.dtypes)
        
        # Check if we have enough columns
        if len(df.columns) < 5:
            print(f"\nError: Expected at least 5 columns (A, B, C, D, E), but found only {len(df.columns)}")
            print("Your Excel file might have a different structure than expected.")
            print("Please check that your data has:")
            print("- Column B: Items")
            print("- Column C: Cost") 
            print("- Column E: Category")
            return pd.DataFrame()
        
        # Based on your file structure, map to the correct columns:
        # Column 1 (Unnamed: 1) = Item descriptions  
        # Column 2 (Ist 1-16/2023) = Costs in EUR
        # Column 4 (Unnamed: 4) = 2nd level Product Category
        try:
            item_col = 'Item_Description'  # Was "Segmenttext" 
            cost_col = 'Cost_EUR'  # Was "Ist 1-16/2023"
            category_col = 'Second_Level_Category'  # Was "2nd level Product Category"
        except KeyError as e:
            print(f"Error: Cannot find expected columns. Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        print(f"\nUsing columns:")
        print(f"Items: {item_col}")
        print(f"Cost: {cost_col}")
        print(f"Category: {category_col}")
        
        # Find all unique categories (handle mixed data types)
        unique_categories = df[category_col].dropna().astype(str).unique()
        print(f"\nAll unique categories found:")
        for cat in sorted(unique_categories):
            print(f"  - '{cat}'")
        
        # Find categories that likely mean 'plastic'
        plastic_categories = find_plastic_categories(unique_categories, plastic_threshold)
        
        print(f"\nCategories identified as 'plastic' (threshold: {plastic_threshold}):")
        for cat in plastic_categories:
            print(f"  - '{cat}'")
        
        if not plastic_categories:
            print("\nNo categories identified as 'plastic'. You may need to:")
            print("1. Lower the similarity threshold")
            print("2. Check the category spellings manually")
            return pd.DataFrame()
        
        # Filter for plastic items
        plastic_items = df[df[category_col].astype(str).str.lower().isin([cat.lower() for cat in plastic_categories])].copy()
        
        print(f"\nFound {len(plastic_items)} plastic item entries (before grouping)")
        
        if len(plastic_items) == 0:
            return pd.DataFrame()
        
        # Clean and convert cost column to numeric (handle mixed strings/numbers)
        # Remove any non-numeric characters and convert to float
        plastic_items[cost_col] = pd.to_numeric(plastic_items[cost_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
        
        # Remove rows where cost couldn't be converted to numeric
        plastic_items = plastic_items.dropna(subset=[cost_col])
        
        print(f"After removing invalid costs: {len(plastic_items)} entries")
        
        # Group similar items and sum their costs
        print(f"\nGrouping similar items (similarity threshold: {item_similarity_threshold})...")
        grouped_items = group_similar_items(plastic_items, item_col, cost_col, item_similarity_threshold)
        
        if grouped_items.empty:
            return pd.DataFrame()
        
        # Sort by total cost (highest to lowest)
        grouped_items = grouped_items.sort_values(by=cost_col, ascending=False).reset_index(drop=True)
        
        # Add rank column
        grouped_items['Rank'] = range(1, len(grouped_items) + 1)
        
        # Reorder columns
        result_columns = ['Rank', item_col, cost_col, 'count', 'grouped_items']
        grouped_items = grouped_items[result_columns]
        
        # Rename columns for clarity
        grouped_items = grouped_items.rename(columns={
            'count': 'Total_Purchases',
            'grouped_items': 'Similar_Items_Grouped'
        })
        
        print(f"\nAfter grouping: {len(grouped_items)} unique items")
        
        return grouped_items
        
    except FileNotFoundError:
        print(f"Error: File '{excel_file_path}' not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return pd.DataFrame()

def save_results(df, output_file='plastic_items_ranked_grouped.xlsx'):
    """
    Save the results to a new Excel file.
    """
    if not df.empty:
        df.to_excel(output_file, index=False)
        print(f"\nResults saved to '{output_file}'")
    else:
        print("No data to save.")

# Main execution - with better error handling
if __name__ == "__main__":
    # Replace 'your_file.xlsx' with the actual path to your Excel file
    excel_file = '/Users/riccardo/Desktop/File for analysis.xlsx'
    
    # First, let's try to understand your file structure
    try:
        # Quick file inspection
        print("=== EXCEL FILE INSPECTION ===")
        excel_info = pd.ExcelFile(excel_file)
        print(f"File: {excel_file}")
        print(f"Available sheets: {excel_info.sheet_names}")
        
        # Try reading each sheet to see which one has your data
        for sheet in excel_info.sheet_names:
            try:
                temp_df = pd.read_excel(excel_file, sheet_name=sheet, nrows=5)  # Read only first 5 rows
                print(f"\nSheet '{sheet}':")
                print(f"  - Shape: {temp_df.shape}")
                print(f"  - Columns: {list(temp_df.columns)}")
                if not temp_df.empty:
                    print("  - Sample data:")
                    print(temp_df.head(2).to_string(index=False))
            except Exception as e:
                print(f"  - Error reading sheet '{sheet}': {str(e)}")
        
        print("\n" + "="*50)
        
    except Exception as e:
        print(f"Error inspecting file: {str(e)}")
        print("Please check:")
        print("1. File path is correct")
        print("2. File exists and is not corrupted")
        print("3. File is not currently open in Excel")
    
    # Now run the main extraction
    print("\n=== RUNNING MAIN EXTRACTION ===")
    plastic_items_df = extract_and_rank_plastic_items(
        excel_file, 
        sheet_name=None,  # Will use first sheet by default
        plastic_threshold=0.7,
        item_similarity_threshold=0.85
    )
    
    if not plastic_items_df.empty:
        print("\n" + "="*80)
        print("PLASTIC ITEMS RANKED BY TOTAL COST (Highest to Lowest)")
        print("Grouped by similar item names with summed costs")
        print("="*80)
        print(plastic_items_df.to_string(index=False))
        
        # Calculate summary statistics
        total_cost = plastic_items_df.iloc[:, 2].sum()  # Cost column
        avg_cost = plastic_items_df.iloc[:, 2].mean()
        max_cost = plastic_items_df.iloc[:, 2].max()
        min_cost = plastic_items_df.iloc[:, 2].min()
        total_purchases = plastic_items_df['Total_Purchases'].sum()
        
        print(f"\n" + "="*80)
        print("SUMMARY STATISTICS")
        print("="*80)
        print(f"Unique plastic items: {len(plastic_items_df)}")
        print(f"Total purchases: {total_purchases}")
        print(f"Total cost: ${total_cost:.2f}")
        print(f"Average cost per item: ${avg_cost:.2f}")
        print(f"Highest cost item: ${max_cost:.2f}")
        print(f"Lowest cost item: ${min_cost:.2f}")
        print(f"Average cost per purchase: ${total_cost/total_purchases:.2f}")
        
        # Show top 5 most expensive items
        print(f"\nTOP 5 MOST EXPENSIVE PLASTIC ITEMS:")
        top_5 = plastic_items_df.head(5)
        for _, row in top_5.iterrows():
            item_col = plastic_items_df.columns[1]
            cost_col = plastic_items_df.columns[2]
            print(f"  {row['Rank']}. {row[item_col]}: ${row[cost_col]:.2f} ({row['Total_Purchases']} purchases)")
        
        # Save results
        save_results(plastic_items_df, 'plastic_items_ranked_grouped.xlsx')
    else:
        print("No plastic items found to rank.")


# Script for extracting data ralated to chemicals and solvents. Everything that has been classified as "chemical" or "solvent" will be extracted and moved into a new file where items are ranked by the expenses. Items with the same name have been summed up. Small spelling mistakes are also considered.

# In[2]:


import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import re

def similarity(a, b):
    """Calculate similarity between two strings (0-1, where 1 is identical)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_keyword_categories(categories, keywords, threshold=0.7):
    """
    Find category values that likely refer to any of the specified keywords using fuzzy matching.
    
    Parameters:
    categories (list): List of unique category values
    keywords (list): List of keywords to search for (e.g., ['chemicals', 'solvents'])
    threshold (float): Similarity threshold (0.7 = 70% similar)
    
    Returns:
    list: Categories that likely match any of the keywords
    """
    matching_variations = []
    
    for category in categories:
        if pd.isna(category):
            continue
            
        category_clean = str(category).strip().lower()
        
        for target_keyword in keywords:
            target = target_keyword.lower()
            
            # Direct substring match
            if target in category_clean or category_clean in target:
                matching_variations.append(category)
                break  # Found a match, no need to check other keywords for this category
                
            # Fuzzy matching for typos
            if similarity(category_clean, target) >= threshold:
                matching_variations.append(category)
                break
                
            # Check for common variations based on keyword type
            variations = generate_keyword_variations(target)
            
            for variation in variations:
                if similarity(category_clean, variation) >= 0.8:
                    matching_variations.append(category)
                    break
            
            if category in matching_variations:
                break  # Already added, move to next category
    
    return matching_variations

def generate_keyword_variations(keyword):
    """
    Generate common variations/misspellings for different types of keywords.
    
    Parameters:
    keyword (str): The base keyword
    
    Returns:
    list: List of common variations
    """
    variations = []
    
    # Common variations for chemicals
    if keyword in ['chemical', 'chemicals']:
        variations = [
            'chemcial', 'chemial', 'chemic', 'chem', 'chemical',
            'chemicals', 'chemcials', 'chemica'
        ]
    
    # Common variations for solvents
    elif keyword in ['solvent', 'solvents']:
        variations = [
            'solvent', 'solvents', 'slovent', 'solv', 'solve',
            'solvnt', 'solvet'
        ]
    
    # Common variations for plastic (keeping original functionality)
    elif keyword in ['plastic', 'plastics']:
        variations = [
            'platic', 'plastik', 'plasitc', 'plastc', 'plstic',
            'plasctic', 'plaastic', 'plastique', 'plastci'
        ]
    
    # Add generic variations (partial matching)
    else:
        # For any other keyword, create some basic variations
        variations = [
            keyword,
            keyword + 's',  # plural
            keyword[:-1] if keyword.endswith('s') else keyword,  # singular
        ]
    
    return variations

def normalize_item_names(items):
    """
    Normalize item names for better grouping (handles minor variations).
    
    Parameters:
    items (pandas.Series): Series of item names
    
    Returns:
    pandas.Series: Normalized item names
    """
    # Convert to string and basic cleaning
    normalized = items.astype(str).str.strip()
    
    # Remove extra whitespace
    normalized = normalized.str.replace(r'\s+', ' ', regex=True)
    
    # Convert to lowercase for comparison
    normalized = normalized.str.lower()
    
    # Remove common variations in punctuation
    normalized = normalized.str.replace(r'[^\w\s]', '', regex=True)
    
    return normalized

def group_similar_items(df, item_col, cost_col, similarity_threshold=0.85):
    """
    Group items with similar names and sum their costs.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with items and costs
    item_col (str): Name of the item column
    cost_col (str): Name of the cost column
    similarity_threshold (float): Threshold for considering items similar
    
    Returns:
    pandas.DataFrame: Grouped DataFrame with summed costs
    """
    if df.empty:
        return df
    
    # Create a copy to work with
    df_work = df.copy()
    
    # Normalize item names for better grouping
    df_work['normalized_name'] = normalize_item_names(df_work[item_col])
    
    # Simple grouping by exact normalized names first
    grouped_simple = df_work.groupby('normalized_name').agg({
        item_col: 'first',  # Keep the first occurrence of the original name
        cost_col: 'sum',    # Sum all costs
        'normalized_name': 'count'  # Count occurrences
    }).rename(columns={'normalized_name': 'count'})
    
    # For more sophisticated fuzzy grouping (optional - can be slow with many items)
    # This part groups items that are very similar but not exactly the same
    unique_normalized = grouped_simple.index.tolist()
    
    # Create groups of similar items
    groups = []
    used_items = set()
    
    for i, item1 in enumerate(unique_normalized):
        if item1 in used_items:
            continue
            
        current_group = [item1]
        used_items.add(item1)
        
        for j, item2 in enumerate(unique_normalized[i+1:], i+1):
            if item2 in used_items:
                continue
                
            if similarity(item1, item2) >= similarity_threshold:
                current_group.append(item2)
                used_items.add(item2)
        
        groups.append(current_group)
    
    # Merge similar groups and sum costs
    final_grouped = []
    
    for group in groups:
        if len(group) == 1:
            # Single item, use as is
            item_name = grouped_simple.loc[group[0], item_col]
            total_cost = grouped_simple.loc[group[0], cost_col]
            total_count = grouped_simple.loc[group[0], 'count']
        else:
            # Multiple similar items, merge them
            item_name = grouped_simple.loc[group[0], item_col]  # Use first item's original name
            total_cost = sum(grouped_simple.loc[item, cost_col] for item in group)
            total_count = sum(grouped_simple.loc[item, 'count'] for item in group)
        
        final_grouped.append({
            item_col: item_name,
            cost_col: total_cost,
            'count': total_count,
            'grouped_items': len(group)
        })
    
    return pd.DataFrame(final_grouped)

def extract_and_rank_items_by_keywords(excel_file_path, keywords, sheet_name=None, keyword_threshold=0.7, item_similarity_threshold=0.85, output_file=None):
    """
    Extract items classified with any of the specified keywords (with fuzzy matching) and rank them by total cost.
    Groups items with the same/similar names and sums their costs.
    
    Parameters:
    excel_file_path (str): Path to the Excel file
    keywords (list): List of keywords to search for (e.g., ['chemicals', 'solvents'])
    sheet_name (str): Name of the sheet to read (None for first sheet)
    keyword_threshold (float): Similarity threshold for finding keyword categories
    item_similarity_threshold (float): Similarity threshold for grouping similar item names
    output_file (str): Optional output filename
    
    Returns:
    pandas.DataFrame: Filtered, grouped, and sorted DataFrame with matching items
    """
    
    try:
        # First, let's inspect the Excel file structure
        print("Inspecting Excel file structure...")
        
        # Get all sheet names
        excel_file = pd.ExcelFile(excel_file_path)
        sheet_names = excel_file.sheet_names
        print(f"Available sheets: {sheet_names}")
        
        # If no sheet_name specified, use the first sheet
        if sheet_name is None:
            sheet_name = sheet_names[0]
            print(f"Using sheet: '{sheet_name}'")
        
        # Read the Excel file with specific parameters to handle formatting issues
        # Skip the first row since it contains the headers we want in row 1 (0-indexed)
        df = pd.read_excel(
            excel_file_path, 
            sheet_name=sheet_name,
            header=0,  # Use first row as header (which contains "Kostenstelle", "Segmenttext", etc.)
            skiprows=2,  # Skip the first 3 rows (0,1,2) to get to actual data
            engine='openpyxl'  # Specify engine for better compatibility
        )
        
        # Set proper column names based on your data structure
        expected_columns = ['Cost_Center', 'Item_Description', 'Cost_EUR', 'Main_Category', 'Second_Level_Category']
        if len(df.columns) >= 5:
            df.columns = expected_columns[:len(df.columns)]
        
        # Check if we actually got a DataFrame
        if not isinstance(df, pd.DataFrame):
            print(f"Error: Expected DataFrame, got {type(df)}")
            print("This might be due to multiple sheets or formatting issues.")
            return pd.DataFrame()
        
        # Display basic info about the dataset
        print(f"\nDataFrame successfully loaded!")
        print(f"Total rows in dataset: {len(df)}")
        print(f"Total columns in dataset: {len(df.columns)}")
        print(f"Column names: {list(df.columns)}")
        print(f"DataFrame shape: {df.shape}")
        
        # Show first few rows
        print("\nFirst few rows:")
        print(df.head())
        
        # Check if we have enough columns
        if len(df.columns) < 5:
            print(f"\nError: Expected at least 5 columns (A, B, C, D, E), but found only {len(df.columns)}")
            print("Your Excel file might have a different structure than expected.")
            print("Please check that your data has:")
            print("- Column B: Items")
            print("- Column C: Cost") 
            print("- Column E: Category")
            return pd.DataFrame()
        
        # Based on your file structure, map to the correct columns:
        try:
            item_col = 'Item_Description'  # Column B equivalent
            cost_col = 'Cost_EUR'  # Column C equivalent
            category_col = 'Second_Level_Category'  # Column E equivalent
        except KeyError as e:
            print(f"Error: Cannot find expected columns. Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        print(f"\nUsing columns:")
        print(f"Items: {item_col}")
        print(f"Cost: {cost_col}")
        print(f"Category: {category_col}")
        print(f"Searching for keywords: {keywords}")
        
        # Find all unique categories (handle mixed data types)
        unique_categories = df[category_col].dropna().astype(str).unique()
        print(f"\nAll unique categories found:")
        for cat in sorted(unique_categories):
            print(f"  - '{cat}'")
        
        # Find categories that likely match any of the keywords
        matching_categories = find_keyword_categories(unique_categories, keywords, keyword_threshold)
        
        print(f"\nCategories identified as matching keywords {keywords} (threshold: {keyword_threshold}):")
        for cat in matching_categories:
            print(f"  - '{cat}'")
        
        if not matching_categories:
            print(f"\nNo categories identified as matching keywords {keywords}. You may need to:")
            print("1. Lower the similarity threshold")
            print("2. Check the category spellings manually")
            print("3. Try different keywords")
            return pd.DataFrame()
        
        # Filter for matching items
        matching_items = df[df[category_col].astype(str).str.lower().isin([cat.lower() for cat in matching_categories])].copy()
        
        print(f"\nFound {len(matching_items)} item entries matching keywords (before grouping)")
        
        if len(matching_items) == 0:
            return pd.DataFrame()
        
        # Clean and convert cost column to numeric (handle mixed strings/numbers)
        matching_items[cost_col] = pd.to_numeric(matching_items[cost_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
        
        # Remove rows where cost couldn't be converted to numeric
        matching_items = matching_items.dropna(subset=[cost_col])
        
        print(f"After removing invalid costs: {len(matching_items)} entries")
        
        # Group similar items and sum their costs
        print(f"\nGrouping similar items (similarity threshold: {item_similarity_threshold})...")
        grouped_items = group_similar_items(matching_items, item_col, cost_col, item_similarity_threshold)
        
        if grouped_items.empty:
            return pd.DataFrame()
        
        # Sort by total cost (highest to lowest)
        grouped_items = grouped_items.sort_values(by=cost_col, ascending=False).reset_index(drop=True)
        
        # Add rank column
        grouped_items['Rank'] = range(1, len(grouped_items) + 1)
        
        # Reorder columns
        result_columns = ['Rank', item_col, cost_col, 'count', 'grouped_items']
        grouped_items = grouped_items[result_columns]
        
        # Rename columns for clarity
        grouped_items = grouped_items.rename(columns={
            'count': 'Total_Purchases',
            'grouped_items': 'Similar_Items_Grouped'
        })
        
        print(f"\nAfter grouping: {len(grouped_items)} unique items")
        
        # Save results if output_file is specified
        if output_file:
            save_results(grouped_items, output_file)
        
        return grouped_items
        
    except FileNotFoundError:
        print(f"Error: File '{excel_file_path}' not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return pd.DataFrame()

def save_results(df, output_file='ranked_items_grouped.xlsx'):
    """
    Save the results to a new Excel file.
    """
    if not df.empty:
        df.to_excel(output_file, index=False)
        print(f"\nResults saved to '{output_file}'")
    else:
        print("No data to save.")

def analyze_multiple_keyword_sets(excel_file, keyword_sets):
    """
    Analyze multiple sets of keywords and save separate reports.
    
    Parameters:
    excel_file (str): Path to Excel file
    keyword_sets (dict): Dictionary with analysis names as keys and keyword lists as values
    """
    for analysis_name, keywords in keyword_sets.items():
        print(f"\n{'='*80}")
        print(f"ANALYZING: {analysis_name.upper()}")
        print(f"Keywords: {keywords}")
        print(f"{'='*80}")
        
        output_file = f"{analysis_name.lower().replace(' ', '_')}_ranked.xlsx"
        
        result_df = extract_and_rank_items_by_keywords(
            excel_file, 
            keywords,
            output_file=output_file
        )
        
        if not result_df.empty:
            print(f"\nTOP 10 {analysis_name.upper()} ITEMS:")
            print(result_df.head(10).to_string(index=False))
            
            # Summary stats
            total_cost = result_df.iloc[:, 2].sum()
            print(f"\nSummary for {analysis_name}:")
            print(f"- Unique items: {len(result_df)}")
            print(f"- Total cost: ${total_cost:.2f}")
            print(f"- Average cost per item: ${total_cost/len(result_df):.2f}")
        else:
            print(f"No items found for {analysis_name}")

# Main execution function for easy use
def main():
    """
    Main function - modify the variables below for your analysis
    """
    # CONFIGURATION - MODIFY THESE VARIABLES
    excel_file_path = '/Users/riccardo/Desktop/File for analysis.xlsx'  # Your Excel file path
    
    # Define keyword sets you want to analyze
    keyword_sets = {
        'chemicals_and_solvents': ['chemicals', 'solvents'],
        'office_supplies': ['paper', 'office', 'supplies'],
        'electronics': ['electronic', 'computer', 'tech'],
        'safety_equipment': ['safety', 'protective', 'ppe']
    }
    
    # Single analysis example
    print("=== SINGLE KEYWORD ANALYSIS ===")
    keywords_to_search = ['chemicals', 'solvents']  # Modify this for your needs
    
    result = extract_and_rank_items_by_keywords(
        excel_file_path, 
        keywords_to_search,
        output_file='chemicals_solvents_ranking.xlsx'
    )
    
    if not result.empty:
        print(f"\nTOP 10 ITEMS FOR KEYWORDS {keywords_to_search}:")
        print(result.head(10).to_string(index=False))
    
    # Multiple analyses example
    print(f"\n{'='*80}")
    print("=== MULTIPLE KEYWORD SET ANALYSIS ===")
    analyze_multiple_keyword_sets(excel_file_path, keyword_sets)

# For Jupyter notebook usage
def analyze_keywords(excel_file, keywords, output_file=None):
    """
    Simple function for Jupyter notebook usage.
    
    Usage:
    result = analyze_keywords('/path/to/file.xlsx', ['chemicals', 'solvents'])
    """
    return extract_and_rank_items_by_keywords(excel_file, keywords, output_file=output_file)

if __name__ == "__main__":
    main()


# Script for extracting data ralated to kits. Everything that has been classified as "kit" will be extracted and moved into a new file where items are ranked by the expenses. Items with the same name have been summed up. Small spelling mistakes are also considered.

# In[2]:


import pandas as pd
import numpy as np
from difflib import SequenceMatcher
import re

def similarity(a, b):
    """Calculate similarity between two strings (0-1, where 1 is identical)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def find_kits_categories(categories, threshold=0.7):
    """
    Find category values that likely refer to 'kits' using fuzzy matching.
    
    Parameters:
    categories (list): List of unique category values
    threshold (float): Similarity threshold (0.7 = 70% similar)
    
    Returns:
    list: Categories that likely contain 'kits'
    """
    kits_variations = []
    target = "kits"
    
    for category in categories:
        if pd.isna(category):
            continue
            
        category_clean = str(category).strip().lower()
        
        # Direct substring match - contains "kit" or "kits"
        if "kit" in category_clean or target in category_clean:
            kits_variations.append(category)
            continue
            
        # Fuzzy matching for typos
        if similarity(category_clean, target) >= threshold:
            kits_variations.append(category)
            continue
            
        # Check for common variations and misspellings
        kit_variations = [
            "kit", "kits", "kitss", "kitt", "kiits", 
            "test kit", "testing kit", "starter kit", 
            "diagnostic kit", "assay kit", "reagent kit"
        ]
        
        for variation in kit_variations:
            if variation in category_clean or similarity(category_clean, variation) >= 0.8:
                kits_variations.append(category)
                break
    
    return kits_variations

def normalize_item_names(items):
    """
    Normalize item names for better grouping (handles minor variations).
    
    Parameters:
    items (pandas.Series): Series of item names
    
    Returns:
    pandas.Series: Normalized item names
    """
    # Convert to string and basic cleaning
    normalized = items.astype(str).str.strip()
    
    # Remove extra whitespace
    normalized = normalized.str.replace(r'\s+', ' ', regex=True)
    
    # Convert to lowercase for comparison
    normalized = normalized.str.lower()
    
    # Remove common variations in punctuation
    normalized = normalized.str.replace(r'[^\w\s]', '', regex=True)
    
    return normalized

def group_similar_items(df, item_col, cost_col, similarity_threshold=0.85):
    """
    Group items with similar names and sum their costs.
    
    Parameters:
    df (pandas.DataFrame): DataFrame with items and costs
    item_col (str): Name of the item column
    cost_col (str): Name of the cost column
    similarity_threshold (float): Threshold for considering items similar
    
    Returns:
    pandas.DataFrame: Grouped DataFrame with summed costs
    """
    if df.empty:
        return df
    
    # Create a copy to work with
    df_work = df.copy()
    
    # Normalize item names for better grouping
    df_work['normalized_name'] = normalize_item_names(df_work[item_col])
    
    # Simple grouping by exact normalized names first
    grouped_simple = df_work.groupby('normalized_name').agg({
        item_col: 'first',  # Keep the first occurrence of the original name
        cost_col: 'sum',    # Sum all costs
        'normalized_name': 'count'  # Count occurrences
    }).rename(columns={'normalized_name': 'count'})
    
    # For more sophisticated fuzzy grouping (optional - can be slow with many items)
    unique_normalized = grouped_simple.index.tolist()
    
    # Create groups of similar items
    groups = []
    used_items = set()
    
    for i, item1 in enumerate(unique_normalized):
        if item1 in used_items:
            continue
            
        current_group = [item1]
        used_items.add(item1)
        
        for j, item2 in enumerate(unique_normalized[i+1:], i+1):
            if item2 in used_items:
                continue
                
            if similarity(item1, item2) >= similarity_threshold:
                current_group.append(item2)
                used_items.add(item2)
        
        groups.append(current_group)
    
    # Merge similar groups and sum costs
    final_grouped = []
    
    for group in groups:
        if len(group) == 1:
            # Single item, use as is
            item_name = grouped_simple.loc[group[0], item_col]
            total_cost = grouped_simple.loc[group[0], cost_col]
            total_count = grouped_simple.loc[group[0], 'count']
        else:
            # Multiple similar items, merge them
            item_name = grouped_simple.loc[group[0], item_col]  # Use first item's original name
            total_cost = sum(grouped_simple.loc[item, cost_col] for item in group)
            total_count = sum(grouped_simple.loc[item, 'count'] for item in group)
        
        final_grouped.append({
            item_col: item_name,
            cost_col: total_cost,
            'count': total_count,
            'grouped_items': len(group)
        })
    
    return pd.DataFrame(final_grouped)

def extract_and_rank_kits_items(excel_file_path, sheet_name=None, kits_threshold=0.7, item_similarity_threshold=0.85):
    """
    Extract items containing 'kits' keyword (with fuzzy matching) and rank them by total cost.
    Groups items with the same/similar names and sums their costs.
    
    Parameters:
    excel_file_path (str): Path to the Excel file
    sheet_name (str): Name of the sheet to read (None for first sheet)
    kits_threshold (float): Similarity threshold for finding 'kits' categories
    item_similarity_threshold (float): Similarity threshold for grouping similar item names
    
    Returns:
    pandas.DataFrame: Filtered, grouped, and sorted DataFrame with kits items
    """
    
    try:
        # First, let's inspect the Excel file structure
        print("Inspecting Excel file structure...")
        
        # Get all sheet names
        excel_file = pd.ExcelFile(excel_file_path)
        sheet_names = excel_file.sheet_names
        print(f"Available sheets: {sheet_names}")
        
        # If no sheet_name specified, use the first sheet
        if sheet_name is None:
            sheet_name = sheet_names[0]
            print(f"Using sheet: '{sheet_name}'")
        
        # Read the Excel file with specific parameters to handle formatting issues
        # Skip the first row since it contains the headers we want in row 1 (0-indexed)
        df = pd.read_excel(
            excel_file_path, 
            sheet_name=sheet_name,
            header=0,  # Use first row as header
            skiprows=2,  # Skip the first 3 rows (0,1,2) to get to actual data
            engine='openpyxl'  # Specify engine for better compatibility
        )
        
        # Set proper column names based on your data structure
        expected_columns = ['Cost_Center', 'Item_Description', 'Cost_EUR', 'Main_Category', 'Second_Level_Category']
        if len(df.columns) >= 5:
            df.columns = expected_columns[:len(df.columns)]
        
        # Check if we actually got a DataFrame
        if not isinstance(df, pd.DataFrame):
            print(f"Error: Expected DataFrame, got {type(df)}")
            print("This might be due to multiple sheets or formatting issues.")
            return pd.DataFrame()
        
        # Display basic info about the dataset
        print(f"\nDataFrame successfully loaded!")
        print(f"Total rows in dataset: {len(df)}")
        print(f"Total columns in dataset: {len(df.columns)}")
        print(f"Column names: {list(df.columns)}")
        print(f"DataFrame shape: {df.shape}")
        
        # Show first few rows
        print("\nFirst few rows:")
        print(df.head())
        
        # Show data types
        print("\nColumn data types:")
        print(df.dtypes)
        
        # Check if we have enough columns
        if len(df.columns) < 5:
            print(f"\nError: Expected at least 5 columns (A, B, C, D, E), but found only {len(df.columns)}")
            print("Your Excel file might have a different structure than expected.")
            print("Please check that your data has:")
            print("- Column B: Items")
            print("- Column C: Cost") 
            print("- Column E: Category")
            return pd.DataFrame()
        
        # Based on your file structure, map to the correct columns:
        try:
            item_col = 'Item_Description'  # Column B
            cost_col = 'Cost_EUR'  # Column C
            category_col = 'Second_Level_Category'  # Column E
        except KeyError as e:
            print(f"Error: Cannot find expected columns. Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        print(f"\nUsing columns:")
        print(f"Items: {item_col}")
        print(f"Cost: {cost_col}")
        print(f"Category: {category_col}")
        
        # Find all unique categories (handle mixed data types)
        unique_categories = df[category_col].dropna().astype(str).unique()
        print(f"\nAll unique categories found:")
        for cat in sorted(unique_categories):
            print(f"  - '{cat}'")
        
        # Find categories that likely contain 'kits'
        kits_categories = find_kits_categories(unique_categories, kits_threshold)
        
        print(f"\nCategories identified as containing 'kits' (threshold: {kits_threshold}):")
        for cat in kits_categories:
            print(f"  - '{cat}'")
        
        if not kits_categories:
            print("\nNo categories identified as containing 'kits'. You may need to:")
            print("1. Lower the similarity threshold")
            print("2. Check the category spellings manually")
            return pd.DataFrame()
        
        # Filter for kits items
        kits_items = df[df[category_col].astype(str).str.lower().isin([cat.lower() for cat in kits_categories])].copy()
        
        print(f"\nFound {len(kits_items)} kits item entries (before grouping)")
        
        if len(kits_items) == 0:
            return pd.DataFrame()
        
        # Clean and convert cost column to numeric (handle mixed strings/numbers)
        # Remove any non-numeric characters and convert to float
        kits_items[cost_col] = pd.to_numeric(kits_items[cost_col].astype(str).str.replace(r'[^\d.-]', '', regex=True), errors='coerce')
        
        # Remove rows where cost couldn't be converted to numeric
        kits_items = kits_items.dropna(subset=[cost_col])
        
        print(f"After removing invalid costs: {len(kits_items)} entries")
        
        # Group similar items and sum their costs
        print(f"\nGrouping similar items (similarity threshold: {item_similarity_threshold})...")
        grouped_items = group_similar_items(kits_items, item_col, cost_col, item_similarity_threshold)
        
        if grouped_items.empty:
            return pd.DataFrame()
        
        # Sort by total cost (highest to lowest)
        grouped_items = grouped_items.sort_values(by=cost_col, ascending=False).reset_index(drop=True)
        
        # Add rank column
        grouped_items['Rank'] = range(1, len(grouped_items) + 1)
        
        # Reorder columns
        result_columns = ['Rank', item_col, cost_col, 'count', 'grouped_items']
        grouped_items = grouped_items[result_columns]
        
        # Rename columns for clarity
        grouped_items = grouped_items.rename(columns={
            'count': 'Total_Purchases',
            'grouped_items': 'Similar_Items_Grouped'
        })
        
        print(f"\nAfter grouping: {len(grouped_items)} unique items")
        
        return grouped_items
        
    except FileNotFoundError:
        print(f"Error: File '{excel_file_path}' not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading Excel file: {str(e)}")
        return pd.DataFrame()

def save_results(df, output_file='kits_items_ranked_grouped.xlsx'):
    """
    Save the results to a new Excel file.
    """
    if not df.empty:
        df.to_excel(output_file, index=False)
        print(f"\nResults saved to '{output_file}'")
    else:
        print("No data to save.")

# Main execution - with better error handling
if __name__ == "__main__":
    # Replace 'your_file.xlsx' with the actual path to your Excel file
    excel_file = '/Users/riccardo/Desktop/File for analysis.xlsx'  # Update this path
    
    # First, let's try to understand your file structure
    try:
        # Quick file inspection
        print("=== EXCEL FILE INSPECTION ===")
        excel_info = pd.ExcelFile(excel_file)
        print(f"File: {excel_file}")
        print(f"Available sheets: {excel_info.sheet_names}")
        
        # Try reading each sheet to see which one has your data
        for sheet in excel_info.sheet_names:
            try:
                temp_df = pd.read_excel(excel_file, sheet_name=sheet, nrows=5)  # Read only first 5 rows
                print(f"\nSheet '{sheet}':")
                print(f"  - Shape: {temp_df.shape}")
                print(f"  - Columns: {list(temp_df.columns)}")
                if not temp_df.empty:
                    print("  - Sample data:")
                    print(temp_df.head(2).to_string(index=False))
            except Exception as e:
                print(f"  - Error reading sheet '{sheet}': {str(e)}")
        
        print("\n" + "="*50)
        
    except Exception as e:
        print(f"Error inspecting file: {str(e)}")
        print("Please check:")
        print("1. File path is correct")
        print("2. File exists and is not corrupted")
        print("3. File is not currently open in Excel")
    
    # Now run the main extraction
    print("\n=== RUNNING MAIN EXTRACTION ===")
    kits_items_df = extract_and_rank_kits_items(
        excel_file, 
        sheet_name=None,  # Will use first sheet by default
        kits_threshold=0.7,
        item_similarity_threshold=0.85
    )
    
    if not kits_items_df.empty:
        print("\n" + "="*80)
        print("KITS ITEMS RANKED BY TOTAL COST (Highest to Lowest)")
        print("Grouped by similar item names with summed costs")
        print("="*80)
        print(kits_items_df.to_string(index=False))
        
        # Calculate summary statistics
        total_cost = kits_items_df.iloc[:, 2].sum()  # Cost column
        avg_cost = kits_items_df.iloc[:, 2].mean()
        max_cost = kits_items_df.iloc[:, 2].max()
        min_cost = kits_items_df.iloc[:, 2].min()
        total_purchases = kits_items_df['Total_Purchases'].sum()
        
        print(f"\n" + "="*80)
        print("SUMMARY STATISTICS")
        print("="*80)
        print(f"Unique kits items: {len(kits_items_df)}")
        print(f"Total purchases: {total_purchases}")
        print(f"Total cost: ${total_cost:.2f}")
        print(f"Average cost per item: ${avg_cost:.2f}")
        print(f"Highest cost item: ${max_cost:.2f}")
        print(f"Lowest cost item: ${min_cost:.2f}")
        print(f"Average cost per purchase: ${total_cost/total_purchases:.2f}")
        
        # Show top 5 most expensive items
        print(f"\nTOP 5 MOST EXPENSIVE KITS ITEMS:")
        top_5 = kits_items_df.head(5)
        for _, row in top_5.iterrows():
            item_col = kits_items_df.columns[1]
            cost_col = kits_items_df.columns[2]
            print(f"  {row['Rank']}. {row[item_col]}: ${row[cost_col]:.2f} ({row['Total_Purchases']} purchases)")
        
        # Save results
        save_results(kits_items_df, 'kits_items_ranked_grouped.xlsx')
    else:
        print("No kits items found to rank.")


# In[ ]:




