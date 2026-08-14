import pandas as pd
from collections import defaultdict
import math
import constants

# ---------- CONFIG ----------
SHEET = "Current Provisioning"

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
    [constants.PROV_PROJECT, constants.PROV_PERFORMER, constants.PROV_VENDOR, constants.PROV_PRODUCT_FEATURE, constants.PROV_CURRENT_PROV]
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
        "USC-ISI, MOSIS 2.0": "MOSIS 2.0",
        "MOSIS 2.0": "MOSIS 2.0",
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

    df_provision[constants.PROV_PERFORMER] = (
        df_provision[constants.PROV_PERFORMER]
        .replace(trans_performer)
    )

    # sort df_provision first
    df_provision.sort_values(by=df_provision.columns[:4].tolist(), inplace=True)

    if constants.PROV_CONCURRENT_USERS not in df_provision.columns:
        df_provision.loc[:,constants.PROV_CONCURRENT_USERS] = 0
        df_provision.loc[:,constants.PROV_CONCURRENT_DURATION] = 0.0
        df_provision.loc[:,constants.PROV_TOTAL] = 0.0
        df_provision.loc[:,constants.PROV_OVER] = ""	# red
        df_provision.loc[:,constants.PROV_UNDER] = ""	# red
        df_provision.loc[:,constants.PROV_EVEN] = "No"    # blue
 
    # This callback is executed for each leaf of pivot_data
    def handle_leaf(path, value):

        # path = [proj, perf, vend, prod, constants.P_CONCURUSERS]
        if (len(path) != 5):
           return

        proj, perf, vend, prod, field = path

        # Only process the constants.P_CONCURUSERS leaves
        if field != constants.P_CONCURUSERS and field != constants.P_TOTAL and field != constants.P_CONCURDURATION:
            return

        # Build filter
        match = (
            (df_provision[constants.PROV_PROJECT] == proj) &
            (df_provision[constants.PROV_PERFORMER] == perf) &
            (df_provision[constants.PROV_VENDOR] == vend) &
            (df_provision[constants.PROV_PRODUCT] == prod) 
        )

        if (field == constants.P_TOTAL):
            column_name = constants.PROV_TOTAL
        elif (field == constants.P_CONCURDURATION):
            column_name = constants.PROV_CONCURRENT_DURATION
        elif (field == constants.P_CONCURUSERS):
            column_name = constants.PROV_CONCURRENT_USERS
        if match.any():
            # Update existing row
            df_provision.loc[match, column_name] = value
        else:
            # Append a new row
            #df_provision.loc[match, "concurrency"] = value
            new_row = {
                constants.PROV_PROJECT: proj,
                constants.PROV_PERFORMER: perf,
                constants.PROV_VENDOR: vend,
                constants.PROV_PRODUCT: prod,
                constants.PROV_CURRENT_PROV: 0,
            }
            new_row[column_name] = value
            df_provision.loc[len(df_provision)] = new_row

    # Traverse the tree
    traverse_pivot(pivot_data, handle_leaf)

    # post-processing
    # Add 'diff' field
    for i, row in df_provision.iterrows():
        if (pd.isna(row[constants.PROV_PROJECT]) or row[constants.PROV_PROJECT] == ""):
            df_provision.loc[i, constants.PROV_EVEN] = ""
            df_provision.loc[i, constants.PROV_OVER] = ""
            df_provision.loc[i, constants.PROV_UNDER] = ""
            continue 
        diff = row[constants.PROV_CURRENT_PROV] - row[constants.PROV_CONCURRENT_USERS]
            
        if diff == 0: # adequate
            df_provision.loc[i, constants.PROV_EVEN] = "Yes"
            df_provision.loc[i, constants.PROV_OVER] = ""
            df_provision.loc[i, constants.PROV_UNDER] = ""
        elif diff > 0: # over
            df_provision.loc[i, constants.PROV_EVEN] = ""
            df_provision.loc[i, constants.PROV_OVER] = diff
            df_provision.loc[i, constants.PROV_UNDER] = ""
        else : # under
            df_provision.loc[i, constants.PROV_EVEN] = ""
            df_provision.loc[i, constants.PROV_OVER] = ""
            df_provision.loc[i, constants.PROV_UNDER] = diff

    # remove rows whose current_provision == 0 and concurrent_users == 0
    mask = df_provision.apply(
        lambda row: ((row[constants.PROV_CURRENT_PROV] == 0 and row[constants.PROV_EVEN] == "Yes") or (not isinstance(row[constants.PROV_PROJECT], str))),
        axis=1
    )
    df_provision = df_provision[~mask] 

    return df_provision

def build_current_provision_usage(file_prov, pivot_data):
    # --- Step 0: read sheet with no header so we can detect header row ---
    raw = pd.read_excel(file_prov, sheet_name=SHEET, header=None)
    
    # --- Step 1: detect header row within first MAX_HEADER_SEARCH_ROWS rows ---
    required_keywords = [constants.PROV_PROJECT, constants.PROV_PERFORMER, constants.PROV_VENDOR, constants.PROV_PRODUCT, constants.PROV_CURRENT_PROV]
    header_idx = find_header_row(raw, required_keywords, max_search=MAX_HEADER_SEARCH_ROWS)
    
    if header_idx is None:
        raise RuntimeError(f"Failed to detect header row within first {MAX_HEADER_SEARCH_ROWS} rows. "
                           "Check the sheet and column names.")
    
    #print(f"Detected header row (1-based): {header_idx + 1}")
    
    # --- Step 2: read again using detected header row ---
    df_provision = pd.read_excel(file_prov, sheet_name=SHEET, header=header_idx, dtype=object)
    
    # --- Step 3: Map columns robustly (partial matches) ---
    cols = list(df_provision.columns)
    project_col = match_column_name(cols, constants.PROV_PROJECT)
    performer_col = match_column_name(cols, constants.PROV_PERFORMER)
    vendor_col = match_column_name(cols, constants.PROV_VENDOR)
    product_col = match_column_name(cols, constants.PROV_PRODUCT)
    value_col = match_column_name(cols, constants.PROV_CURRENT_PROV)
    
    missing = [name for name, actual in [
        (constants.PROV_PROJECT, project_col),
        (constants.PROV_PERFORMER, performer_col),
        (constants.PROV_VENDOR, vendor_col),
        (constants.PROV_PRODUCT, product_col),
        (constants.PROV_CURRENT_PROV, value_col)
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
        col_index = df_prov.columns.get_loc(constants.PROV_OVER)
        worksheet.set_column(col_index, col_index, None, red_bold_format)
        col_index = df_prov.columns.get_loc(constants.PROV_UNDER)
        worksheet.set_column(col_index, col_index, None, blue_bold_format)
        col_index = df_prov.columns.get_loc(constants.PROV_EVEN)
        worksheet.set_column(col_index, col_index, None, green_bold_format)

def set_color_by_value(writer, df_prov, sheet_name, column_name):
    workbook  = writer.book
    worksheet = writer.sheets[sheet_name]

    # Formats
    red_bold = workbook.add_format({
        'font_color': 'red',
        'bold': True
    })

    blue_bold = workbook.add_format({
        'font_color': 'blue',
        'bold': True
    })

    # Column index
    col_index = df_prov.columns.get_loc(column_name)

    # Data range (skip header row)
    start_row = 1
    end_row   = len(df_prov)

    # < 0.5 → red bold
    worksheet.conditional_format(
        start_row, col_index, end_row, col_index,
        {
            'type': 'cell',
            'criteria': '<',
            'value': 0.5,
            'format': red_bold
        }
    )

    # > 0.5 → blue bold
    worksheet.conditional_format(
        start_row, col_index, end_row, col_index,
        {
            'type': 'cell',
            'criteria': '>',
            'value': 0.5,
            'format': blue_bold
        }
    )
