import pandas as pd
from collections import defaultdict
import math

# pivot table column, which comes from match.py
P_CONCURUSERS = "Concurrent Users"
P_CONCURDURATION = "Concurrent Duration"
P_TOTAL = "_total"

# ---------- CONFIG ----------
SHEET = "Current Provisioning"

# Candidate column name keywords (case-insensitive, partial matches allowed)
PROV_PROJECT = "Project"
PROV_PERFORMER = "Performer"
PROV_VENDOR = "Vendor"
PROV_PRODUCT = "Product Feature"
PROV_CURRENT_PROV = "Current Provision"   # value column to accumulate
PROV_CONCURRENT_USERS = P_CONCURUSERS
PROV_CONCURRENT_DURATION = P_CONCURDURATION
PROV_OVER = "Over Provision"
PROV_UNDER = "Under Provision"
PROV_EVEN = "Adequate Provision"
PROV_TOTAL = "Usage time (hours)"	# from P_TOTAL

# How many top rows may contain selection controls (not data headers)
MAX_SELECTION_ROWS = 9
# How far to search for header row (some tolerance)
MAX_HEADER_SEARCH_ROWS = 20
# ----------------------------


def find_header_row(raw_df, required_keywords, max_search=20):
    """
    Scan the first `max_search` rows to find a row that contains all required keywords
    (partial, case-insensitive match). Returns header_row_index (0-based) or None.
    """
    for i in range(min(max_search, len(raw_df))):
        row_vals = ["" if pd.isna(v) else str(v).strip().lower() for v in raw_df.iloc[i].tolist()]
        found_all = True
        for kw in required_keywords:
            # check if any cell contains the keyword substring
            if not any(kw.lower() in cell for cell in row_vals if cell):
                found_all = False
                break
        if found_all:
            return i
    return None

def normalize_col_name(col):
    return "" if pd.isna(col) else str(col).strip().lower()

def match_column_name(cols, keyword):
    """
    Return the actual column name from `cols` that best matches `keyword` (partial, case-insensitive).
    If none found, return None.
    """
    keyword = keyword.lower()
    for c in cols:
        if keyword in normalize_col_name(c):
            return c
    # fallback: exact match ignoring case/whitespace
    for c in cols:
        if normalize_col_name(c) == keyword:
            return c
    return None

# Helper to detect NaN
def is_nan(x):
    return isinstance(x, float) and math.isnan(x)

# Depth-first traversal of pivot_data
def traverse_pivot(pivot_data, callback, path=None):
    if path is None:
        path = []

    if not isinstance(pivot_data, dict):
        # Reached a leaf value
        callback(path, pivot_data)
        return

    for key, value in pivot_data.items():
        if key is None or is_nan(key):
            continue

        traverse_pivot(value, callback, path + [key])

def update_df_provision_with_pivot(df_provision, pivot_data):

    """
    df_provision must contain columns:
    [PROV_PROJECT, PROV_PERFORMER, PROV_VENDOR, PROV_PRODUCT_FEATURE, PROV_CURRENT_PROV]
#    ['Project', 'Performer', 'Vendor', 'Product Feature', 'Current Provision']

    This function:
    - Adds 'concurrency' column if missing
    - Fills/updates concurrency values
    - Appends new rows if not found
    """

    df_provision = df_provision.copy()

    # translating perfomer name to standardized one
    # these are used in the provisioned data
    trans_performer = {
        "USC-ISI, The MOSIS Services": "MOSIS 2.0",
        "UCR, The MOSIS Services": "UCR",
        "University of California, San Diego" : "UCSD",
        "University of California, Santa Barbara": "UCSB",
        "University of California, San Diego": "UCSD",
        "USC - Beerel": "USC-Beerel",
        "USC - Lim": "USC-Lim",
        "USC - Kapadia": "USC-Kapadia",
        "USC - Chen - Wu - Yang": "USC-Chen-Wu-Yang",
        "USC - Hossein": "USC-Hossein",
        "UCLA - Wang": "UCLA-Wang"
    }

    for i, row in df_provision.iterrows():
        if (row[PROV_PERFORMER] in trans_performer):
            row[PROV_PERFORMER] = trans_performer[row[PROV_PERFORMER]]

    # sort df_provision first
    df_provision.sort_values(by=df_provision.columns[:4].tolist(), inplace=True)

    if PROV_CONCURRENT_USERS not in df_provision.columns:
        df_provision.loc[:,PROV_CONCURRENT_USERS] = 0
        df_provision.loc[:,PROV_CONCURRENT_DURATION] = 0.0
        df_provision.loc[:,PROV_TOTAL] = 0
        df_provision.loc[:,PROV_OVER] = 0	# red
        df_provision.loc[:,PROV_UNDER] = 0	# red
        df_provision.loc[:,PROV_EVEN] = "No"    # blue
 
    # This callback is executed for each leaf of pivot_data
    def handle_leaf(path, value):

        # path = [proj, perf, vend, prod, P_CONCURUSERS]
        if (len(path) != 5):
           return

        proj, perf, vend, prod, field = path

        # Only process the P_CONCURUSERS leaves
        if field != P_CONCURUSERS and field != P_TOTAL and field != P_CONCURDURATION:
            return

        # Build filter
        match = (
            (df_provision[PROV_PROJECT] == proj) &
            (df_provision[PROV_PERFORMER] == perf) &
            (df_provision[PROV_VENDOR] == vend) &
            (df_provision[PROV_PRODUCT] == prod) 
        )

        if (field == P_TOTAL):
            column_name = PROV_TOTAL
        elif (field == P_CONCURDURATION):
            column_name = PROV_CONCURRENT_DURATION
        elif (field == P_CONCURUSERS):
            column_name = PROV_CONCURRENT_USERS
        if match.any():
            # Update existing row
            df_provision.loc[match, column_name] = value
        else:
            # Append a new row
            #df_provision.loc[match, "concurrency"] = value
            new_row = {
                PROV_PROJECT: proj,
                PROV_PERFORMER: perf,
                PROV_VENDOR: vend,
                PROV_PRODUCT: prod,
                PROV_CURRENT_PROV: 0,
            }
            new_row[column_name] = value
            df_provision.loc[len(df_provision)] = new_row

    # Traverse the tree
    traverse_pivot(pivot_data, handle_leaf)

    # post-processing
    # Add 'diff' field
    for i, row in df_provision.iterrows():
        if (pd.isna(row[PROV_PROJECT]) or row[PROV_PROJECT] == ""):
            df_provision.loc[i, PROV_EVEN] = ""
            df_provision.loc[i, PROV_OVER] = ""
            df_provision.loc[i, PROV_UNDER] = ""
            continue 
        diff = row[PROV_CURRENT_PROV] - row[PROV_CONCURRENT_USERS]
            
        if diff == 0: # adequate
            df_provision.loc[i, PROV_EVEN] = "Yes"
            df_provision.loc[i, PROV_OVER] = ""
            df_provision.loc[i, PROV_UNDER] = ""
        elif diff > 0: # over
            df_provision.loc[i, PROV_EVEN] = ""
            df_provision.loc[i, PROV_OVER] = diff
            df_provision.loc[i, PROV_UNDER] = ""
        else : # under
            df_provision.loc[i, PROV_EVEN] = ""
            df_provision.loc[i, PROV_OVER] = ""
            df_provision.loc[i, PROV_UNDER] = diff

    # remove rows whose current_provision == 0 and concurrent_users == 0
    mask = df_provision.apply(
        lambda row: row[PROV_CURRENT_PROV] == 0 and row[PROV_EVEN] == "Yes",
        axis=1
    )
    df_provision = df_provision[~mask] 

    return df_provision

def build_current_provision_usage(file_prov, pivot_data):
    # --- Step 0: read sheet with no header so we can detect header row ---
    raw = pd.read_excel(file_prov, sheet_name=SHEET, header=None)
    
    # --- Step 1: detect header row within first MAX_HEADER_SEARCH_ROWS rows ---
    required_keywords = [PROV_PROJECT, PROV_PERFORMER, PROV_VENDOR, PROV_PRODUCT, PROV_CURRENT_PROV]
    header_idx = find_header_row(raw, required_keywords, max_search=MAX_HEADER_SEARCH_ROWS)
    
    if header_idx is None:
        raise RuntimeError(f"Failed to detect header row within first {MAX_HEADER_SEARCH_ROWS} rows. "
                           "Check the sheet and column names.")
    
    #print(f"Detected header row (1-based): {header_idx + 1}")
    
    # --- Step 2: read again using detected header row ---
    df_provision = pd.read_excel(file_prov, sheet_name=SHEET, header=header_idx, dtype=object)
    
    # --- Step 3: Map columns robustly (partial matches) ---
    cols = list(df_provision.columns)
    project_col = match_column_name(cols, PROV_PROJECT)
    performer_col = match_column_name(cols, PROV_PERFORMER)
    vendor_col = match_column_name(cols, PROV_VENDOR)
    product_col = match_column_name(cols, PROV_PRODUCT)
    value_col = match_column_name(cols, PROV_CURRENT_PROV)
    
    missing = [name for name, actual in [
        (PROV_PROJECT, project_col),
        (PROV_PERFORMER, performer_col),
        (PROV_VENDOR, vendor_col),
        (PROV_PRODUCT, product_col),
        (PROV_CURRENT_PROV, value_col)
    ] if actual is None]
    if missing:
        raise RuntimeError(f"Could not find these required columns (partial match): {missing}. "
                           f"Available columns: {cols}")
   
    keep_cols = [ project_col, performer_col, vendor_col, product_col, value_col]
    df_provision_filtered = df_provision[keep_cols]

    df_provision_final = update_df_provision_with_pivot(df_provision_filtered, pivot_data)
    return df_provision_final

def set_color_column(writer, df_prov, sheet_name):
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        red_bold_format = workbook.add_format({
        'font_color': 'red',
        'bold': True
        })
        blue_bold_format = workbook.add_format({
        'font_color': 'blue',
        'bold': True
        })
        green_bold_format = workbook.add_format({
        'font_color': 'green',
        'bold': True
        })
        col_index = df_prov.columns.get_loc(PROV_OVER)
        worksheet.set_column(col_index, col_index, None, red_bold_format)
        col_index = df_prov.columns.get_loc(PROV_UNDER)
        worksheet.set_column(col_index, col_index, None, blue_bold_format)
        col_index = df_prov.columns.get_loc(PROV_EVEN)
        worksheet.set_column(col_index, col_index, None, green_bold_format)
