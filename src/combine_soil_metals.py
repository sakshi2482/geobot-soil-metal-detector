import pandas as pd
import os
import re # Import the regular expression module for safer column matching

# ====== PATHS – EDIT ONLY THE FILE NAMES IF DIFFERENT ======
# NOTE: The Vladimir file is an Excel, so we need to inspect the sheets and header row.
vladimir_path = r"C:\Soil_Metal_Project\data_raw\Soils and plants_Vladimir region, Russia_v2 new.xlsx"
# *** PATH UPDATED: Correcting extension to CSV and using Attachment 5 ***
global_path   = r"C:\Soil_Metal_Project\data_raw\Attachment 5 Global dataset of predicted toxic metals exceedance under AT.csv" 
out_path      = r"C:\Soil_Metal_Project\data_processed\soil_metals_combined.csv"
# ===========================================================

os.makedirs(os.path.dirname(out_path), exist_ok=True)

# Helper function to ensure column names are unique (to prevent ValueError: cannot reindex on an axis with duplicate labels)
def make_unique_labels(labels):
    # This is a robust way to handle duplicate labels across pandas versions.
    seen = {}
    new_labels = []
    for item in labels:
        original = item
        count = seen.get(item, 0)
        
        # If the item has been seen, append a unique suffix
        if count > 0:
            item = f"{original}_{count}"
        
        # Update counters and append the new label
        seen[original] = count + 1
        new_labels.append(item)
    return pd.Index(new_labels)

# 1) Load raw files
try:
    # --- ROBUST VLADIMIR FILE LOADING FIX: Manually setting header and data start ---
    # 1. Load data without any header, treating all rows as data initially.
    vlad_soil = pd.read_excel(vladimir_path, header=None) 
    
    # 2. REVISED FIX: Row 4 (index 3) is now assumed to contain the main element/location names.
    # We capture these, converting all to string to prevent 'float' object errors and strip whitespace.
    new_header = vlad_soil.iloc[3].astype(str).str.strip()
    
    # 3. Assign this cleaned row as the new column headers.
    vlad_soil.columns = new_header
    
    # 4. The actual data is assumed to start at Row 6 (index 5). Drop all preceding metadata rows (0 through 4).
    # This keeps Row 6 (index 5) as the *first data row*.
    vlad_soil = vlad_soil.iloc[5:].reset_index(drop=True)
    
    # --- END ROBUST LOADING ---
    
    # 5. Drop columns that contain 'nan', 'Unnamed', or the GPS metadata, which result from empty or merged cells in Row 4.
    cols_to_drop_by_name = [
        col for col in vlad_soil.columns if 
        str(col).lower() in ['nan', 'unnamed: 10', 'unnamed: 22', 'unnamed: 1'] or # Common Excel empty/placeholder headers
        str(col).startswith('nan_') or
        str(col).startswith('Unnamed:') or
        col in ['GPS coordinates of the experimental field', '56°25\'02" N 40°25\'25" E'] # Known stray columns
    ]
    
    vlad_soil = vlad_soil.drop(columns=cols_to_drop_by_name, errors='ignore')
    
    # 6. CRITICAL: Strip all whitespace (leading/trailing) from column names (though mostly done in step 2)
    vlad_soil.columns = vlad_soil.columns.str.strip()
    
    # 7. CRITICAL FIX: Ensure column labels are unique AFTER cleaning. 
    vlad_soil.columns = make_unique_labels(vlad_soil.columns)

    # Note: We rely entirely on the robust find_col regex function to match the complex column names.
    
    # 8. Rename 'Depth of soil sampling, cm' to 'Depth' if it exists
    # We now search the fully cleaned and stringified headers.
    depth_col = next((col for col in vlad_soil.columns if 'depth' in str(col).lower() and 'cm' in str(col).lower()), None)
    if depth_col:
        vlad_soil = vlad_soil.rename(columns={depth_col: 'Depth'})
    
    # 9. Drop 'Sample' column if it exists
    if 'Sample' in vlad_soil.columns:
        vlad_soil = vlad_soil.drop(columns=['Sample'])
    
except Exception as e:
    print(f"FATAL ERROR: Failed to load and clean Vladimir file. Error: {e}")
    raise

if global_path.lower().endswith(".xlsx"):
    global_soil = pd.read_excel(global_path)
else:
    # Use 'low_memory=False' when reading large CSVs
    global_soil = pd.read_csv(global_path, low_memory=False) 

print("Vladimir columns (after final fix):", list(vlad_soil.columns))
print("Global columns:", list(global_soil.columns))

# 2) Try to automatically find columns for each metal
def find_col(df, keys):
    # Search for columns that contain any of the keys (case-insensitive)
    
    # We must use regex to ensure we are matching a whole word/symbol, not just a substring
    # Create a regex pattern to match the key as a word boundary, or followed by a delimiter (like comma or space)
    
    patterns = []
    # 1. Match the key exactly (case insensitive)
    patterns.extend([r'^\s*' + re.escape(k) + r'\s*$' for k in keys]) # Exact match, trimmed
    # 2. Match key followed by unit/delimiter (e.g., 'As, mg/kg')
    patterns.extend([re.escape(k) + r'[\s,].*$' for k in keys])
    # 3. Match key as a word boundary
    patterns.extend([r'\b' + re.escape(k) + r'\b' for k in keys])
    
    # Combine patterns into a single regex for matching
    combined_pattern = '|'.join(patterns)
    
    # Use re.IGNORECASE for case-insensitive matching
    # Note: str(c) conversion is already here, making it safe.
    cols = [c for c in df.columns if re.search(combined_pattern, str(c), re.IGNORECASE)]
    
    # Return the first matching column name
    return cols[0] if cols else None

# Element list
ALL_ELEMENTS = ['Latitude', 'Longitude', 'As', 'Cd', 'Co', 'Cr', 'Cu', 'Fe', 'K', 'Mg', 'Ni', 'Pb', 'Zn', 'pH', 'Ca', 'Mn', 'Ba', 'Sr', 'Na', 'P']

# *** CRITICAL FIX: Mapping uses find_col on the raw, descriptive column names ***
vlad_map = {
    # Still need find_col for GPS and Treatment as those names might be complex
    'Latitude': find_col(vlad_soil, ['lat', 'широта', 'latitude', 'coord', 'gps']),
    'Longitude': find_col(vlad_soil, ['lon', 'долгота', 'longitude', 'coord', 'gps']),
    'Treatment': find_col(vlad_soil, ['Control', 'Background', 'Experimental group', 'treatment', 'group', 'Контроль']),

    # Use find_col for elements, searching for the clean symbols (e.g., 'As') that resulted from our cleanup above.
    'As': find_col(vlad_soil, ['As', 'Arsenic']),
    'Cd': find_col(vlad_soil, ['Cd', 'Cadmium']),
    'Co': find_col(vlad_soil, ['Co', 'Cobalt']),
    'Cr': find_col(vlad_soil, ['Cr', 'Chromium']),
    'Cu': find_col(vlad_soil, ['Cu', 'Copper']),
    'Fe': find_col(vlad_soil, ['Fe', 'Iron']),
    'K' : find_col(vlad_soil, ['K', 'Potassium', 'Kalium']), 
    'Mg': find_col(vlad_soil, ['Mg', 'Magnesium']),
    'Ni': find_col(vlad_soil, ['Ni', 'Nickel']),
    'Pb': find_col(vlad_soil, ['Pb', 'Lead']),
    'Zn': find_col(vlad_soil, ['Zn', 'Zinc']),
    'pH': find_col(vlad_soil, ['pH']),
    'Ca': find_col(vlad_soil, ['Ca', 'Calcium']),
    'Mn': find_col(vlad_soil, ['Mn', 'Manganese']),
    'Ba': find_col(vlad_soil, ['Ba', 'Barium']),
    'Sr': find_col(vlad_soil, ['Sr', 'Strontium']),
    'Na': find_col(vlad_soil, ['Na', 'Sodium']),
    'P': find_col(vlad_soil, ['P', 'Phosphorus']),
}

# Global mapping (reverts to the reliable find_col method for the CSV)
glob_map = {
    'Latitude': find_col(global_soil, ['lat', 'широта', 'latitude']),
    'Longitude': find_col(global_soil, ['lon', 'долгота', 'longitude']),
    'As': find_col(global_soil, ['as', 'arsenic']),
    'Cd': find_col(global_soil, ['cd', 'cadmium']),
    'Co': find_col(global_soil, ['co', 'cobalt']),
    'Cr': find_col(global_soil, ['cr', 'chromium']),
    'Cu': find_col(global_soil, ['cu', 'copper']),
    'Fe': find_col(global_soil, ['fe', 'iron']),
    'K' : find_col(global_soil, ['k', 'pota', 'kalium', 'potassium']),
    'Mg': find_col(global_soil, ['mg', 'magnesium']),
    'Ni': find_col(global_soil, ['ni', 'nickel']),
    'Pb': find_col(global_soil, ['pb', 'lead']),
    'Zn': find_col(global_soil, ['zn', 'zinc']),
    'pH': find_col(global_soil, ['ph']),
    'Ca': find_col(global_soil, ['ca', 'calcium']),
    'Mn': find_col(global_soil, ['mn', 'manganese']),
    'Ba': find_col(global_soil, ['ba', 'barium']),
    'Sr': find_col(global_soil, ['sr', 'strontium']),
    'Na': find_col(global_soil, ['na', 'sodium']),
    'P': find_col(global_soil, ['p', 'phosphorus']),
}

# Add 'Treatment' to the list of expected elements so it is included in the final DF
ALL_ELEMENTS.append('Treatment')


print("Vladimir mapping:", {k: v for k, v in vlad_map.items() if v is not None})
print("Global mapping:", {k: v for k, v in glob_map.items() if v is not None})

# 3) Build unified dataframes (use only found columns)
vlad_cols_used = {}
for new, old in vlad_map.items():
    # Only map if the old column name exists AND hasn't been used yet
    if old is not None and old in vlad_soil.columns and old not in vlad_cols_used.values():
        vlad_cols_used[new] = old

glob_cols_used = {}
for new, old in glob_map.items():
    if old is not None and old in global_soil.columns and old not in glob_cols_used.values():
        glob_cols_used[new] = old

# Now rename the columns in the respective DataFrames
vlad = vlad_soil[list(vlad_cols_used.values())].rename(columns={old: new for new, old in vlad_cols_used.items()})
global_df = global_soil[list(glob_cols_used.values())].rename(columns={old: new for new, old in glob_cols_used.items()})

vlad['source'] = 'vladimir'
global_df['source'] = 'global'

# 4) Align columns and combine
all_cols = sorted(set(vlad.columns) | set(global_df.columns))
vlad = vlad.reindex(columns=all_cols)
global_df = global_df.reindex(columns=all_cols)

combined = pd.concat([vlad, global_df], ignore_index=True)

# 5) Drop rows where all elements are missing
element_cols = [c for c in ALL_ELEMENTS if c in combined.columns and c != 'Treatment'] # Exclude Treatment from dropna check
combined = combined.dropna(subset=element_cols, how='all')

print("Combined shape:", combined.shape)
print(combined.head())

# 6) Save single CSV
combined.to_csv(out_path, index=False)
print("Saved combined file to:", out_path)