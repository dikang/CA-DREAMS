import sys
import os
import pandas as pd
import json
from openpyxl import load_workbook
from collections import defaultdict
import provision as pr
import math

# -- pivot table keys
# 1. fields from usage Excel file
USG_USERNAME = "User Name"
USG_FEATURE = "Product"
USG_TIME = "Total usage time (hours)"

# 2. newly added columns to usage table
VENDOR = "Vendor Name"
PRODUCT = "Product Name"
ORG = "Organization"
PROJECT = "Project Name"

# 3. calculated columns
P_NUMUSERS = "Number of Users"
P_CONCURUSERS = "Concurrent Users"
P_INSTANCES = "_instances"
P_TOTAL = "_total"

# total number of columns output of pivot table in Excel file
NUMCOLUMNS = 9	

# -- end of pivot table keys

# columns of feature file
F_PRODUCT="Product"
F_FEATURE="Feature"

# Sheet name for Comparison between provisioned and used 
SHEET_NAME_STAT = "Actual Usage"

def setup_feature_dictionary(file_b):
    mapping = []
    xls_b = pd.ExcelFile(file_b)
    
    for sheet in xls_b.sheet_names:
        df_raw = xls_b.parse(sheet, header=None)  # read without assuming header row
    
        # Find the header row: the one that contains both "Product" and "Feature"
        header_row = None
        for i, row in df_raw.iterrows():
            if F_PRODUCT in row.values and F_FEATURE in row.values:
                header_row = i
                break
    
        if header_row is not None:
            # Re-read the sheet using that row as the header
            df = xls_b.parse(sheet, header=header_row)
    
            # Only proceed if both columns exist after header detection
            if F_PRODUCT in df.columns and F_FEATURE in df.columns:
                for _, row in df.iterrows():
                    mapping.append({
                        "vendor" : sheet,
                        "product" : row[F_PRODUCT],
                        "feature" : row[F_FEATURE]
                    })
#        else:
#            print(f"⚠️ Warning: 'Product'/'Feature' columns not found in sheet '{sheet}'")
    # Build lookup dictionary
    lookup = {
        row["feature"]: (row["vendor"], row["product"])
        for row in mapping if pd.notna(row["feature"])
    }

    return lookup

USER_LAST_NAME="LAST NAME"
USER_FIRST_NAME="FIRST NAME"
USER_ORGANIZATION="ORGANIZATION"
USER_PROJECT_NAME="PROJECT NAME"
USER_EMAIL="microelectornics.us E-MAIL"

def setup_user_dictionary(file_c):
    mapping = []
    xls_c = pd.ExcelFile(file_c)
    
    for sheet in xls_c.sheet_names:
        if str(sheet) != "Admin-User List": continue
        df_raw = xls_c.parse(sheet, header=None)  # read without assuming header row
    
        # Find the header row: the one that contains both "LAST NAME" and "ORGANIZATION"
        header_row = None
        for i, row in df_raw.iterrows():
            if "LAST NAME" in row.values and "ORGANIZATION" in row.values:
                header_row = i
                break
    
        if header_row is not None:
            # Re-read the sheet using that row as the header
            df = xls_c.parse(sheet, header=header_row)
    
            # Only proceed if both columns exist after header detection
            if "LAST NAME" in df.columns and "ORGANIZATION" in df.columns:
                for _, row in df.iterrows():
                    if (str(row["NOTES"]).lower() == "remove"): continue
                    mapping.append({
                        "last": row[USER_LAST_NAME],
                        "first": row[USER_FIRST_NAME],
                        "org": row[USER_ORGANIZATION],
                        "pname": row[USER_PROJECT_NAME],
                        "email": row[USER_EMAIL]
                    })
        else:
            print(f"⚠️ Warning: 'LAST NAME'/'ORGANIZATION' columns not found in sheet '{sheet}'")
    
    # Build user_lookup dictionary
    user_lookup = {
        row["email"]: (row["last"], row["first"], row["pname"], row["org"])
        for row in mapping if pd.notna(row["org"])
    }
    return user_lookup

def add_extra_fields(file_a, feature_lookup, user_lookup):
    xls_a = pd.ExcelFile(file_a)
    target_sheet = None
    header_row = 0

    # Find the header row
    for sheet in xls_a.sheet_names:
        guessed_header_row = detect_header_row_usagefile(file_a, sheet)
        df_temp = pd.read_excel(file_a, sheet_name=sheet, header=guessed_header_row)
        if USG_USERNAME in df_temp.columns:
            target_sheet = sheet
            header_row = guessed_header_row
            break

    if not target_sheet:
        print("❌ Could not find a sheet with 'User Name' in first or second row.")
        sys.exit(1)

    # Load the target sheet with correct header row ---
    df = pd.read_excel(file_a, sheet_name=target_sheet, header=header_row)

    if "Product" not in df.columns:
        print("❌ Column 'Product' not found in target sheet.")
        sys.exit(1)

    # Add Vendor and Product Name columns
    df["Vendor Name"] = ""
    df["Product Name"] = ""

    for idx, prod in df["Feature"].items():
        if pd.notna(prod) and prod in feature_lookup:
            vendor, pname = feature_lookup[prod]
            df.at[idx, "Vendor Name"] = vendor
            df.at[idx, "Product Name"] = pname

    # Add Organization Name
    df["Organization"] = ""

    for idx, email in df["Email"].items():
        if pd.notna(email) and email in user_lookup:
            last, first, pname, org = user_lookup[email]
            df.at[idx, "Organization"] = org
            df.at[idx, "Project Name"] = pname 
    return (xls_a, target_sheet, df)

def calculate_concurrency(A):
    events = []

    # Build event points: +1 for start, -1 for end
    for start_time, end_time in A:
        if (start_time == end_time): 
            continue;
        events.append((start_time, 1))     # start event
        events.append((end_time, -1))      # end event

    # Sort events: if same time, process end (-1) before start (+1)
    events.sort(key=lambda x: (x[0], x[1]))

    max_concurrency = 0
    current = 0

    # Sweep line: accumulate current usage
    for _, change in events:
        current += change
        max_concurrency = max(max_concurrency, current)

    if (max_concurrency < 1): max_concurrency = 1
    return max_concurrency

# Sort keys: put "_total" first, put NUMUSERS and P_CONCURUSERS last
def sort_key(k):
    if k == "_total":
        return (0, k)
    elif k == P_NUMUSERS:
        return (98, k)
    elif k == P_CONCURUSERS:
        return (99, k)
    else:
        return (1, k)

def flatten_defaultdict(d, parent_keys=None, show_blank=True):
    """
    Recursively flatten nested defaultdicts into a list of key paths + value,
    with these features:
      1. Key name appears only once per group (other rows under it are blank)
      2. Keys ending with '_total' appear first at each level
    """
    if parent_keys is None:
        parent_keys = []
    rows = []
    keys = sorted(d.keys(), key=sort_key)

    max_concurrency = 0
    for i, k in enumerate(keys):
        if k == P_INSTANCES:	# list of [start_time, end_time]
            _max_concurrency = calculate_concurrency(d[k])
            if (_max_concurrency > max_concurrency):
                max_concurrency = _max_concurrency
            continue
        v = d[k]
        if isinstance(v, dict):
            (child_rows, _max_concurrency)  = flatten_defaultdict(v, parent_keys + [k], show_blank)
            if (_max_concurrency > max_concurrency):
                max_concurrency = _max_concurrency
        elif (k == P_CONCURUSERS): 
            d[P_CONCURUSERS] = max_concurrency
            if ("_total" in keys):
                rows[0][-1] = max_concurrency
            continue
        elif (k == P_NUMUSERS):
            if ("_total" in keys):
                rows[0][-2] = d[P_NUMUSERS]
            continue
        else:	# value is added at the end of the child_rows, which becomes first row of rows
            if (k == "_total"):
                t = [v]
            else:
                t = [k, v]
            # insert "" to make the columns aligned
            diff = NUMCOLUMNS - 2 - len(parent_keys) - len(t)
            child_row = list(parent_keys)
            child_row.extend([""]*diff)
            child_rows = [child_row + t]
            child_rows[0].append("")
            child_rows[0].append("")
        # If we’re not at the topmost level, blank out repeated parent keys
        if show_blank and rows:
            for r in child_rows[0:]:
                # Blank all parent columns for visual grouping
                for j in range(len(parent_keys)):
                    r[j] = ""

        rows.extend(child_rows)

    return (rows, max_concurrency)

def write_new_file(file_a, xls_a, target_sheet, df, df_pivot_data, df_pivot_data_tool, df_prov):
    processed_filename = os.path.splitext(file_a)[0] + "-processed.xlsx"
    print("[5] Write all to file: %s" % processed_filename)
    with pd.ExcelWriter(processed_filename, engine="xlsxwriter") as writer:
        for sheet in xls_a.sheet_names:
            if sheet == target_sheet:
                df.to_excel(writer, sheet_name=sheet, index=False)
            else:
                # Detect if header in row 2 for other sheets as well
                guessed_header_row = detect_header_row(file_a, sheet)
                df_other = pd.read_excel(file_a, sheet_name=sheet, header=guessed_header_row)
                df_other.to_excel(writer, sheet_name=sheet, index=False)
        df_pivot_data.to_excel(writer, sheet_name="Performer Summary", index=False)
        df_pivot_data_tool.to_excel(writer, sheet_name="Tool Summary", index=False)
        df_prov.to_excel(writer, sheet_name=SHEET_NAME_STAT, index=False)
        pr.set_color_column(writer, df_prov, SHEET_NAME_STAT)

    # Set writable permission 
    os.chmod(processed_filename, 0o666)

def tree():
    return defaultdict(tree)

# build pivot_table
# calculate # of users per tool
# estimate # of concurrent users
def build_pivot_table(df, nested_data, per_team=True):
    # Fill nested structure
    prod_user = tree()
    for _, row in df.iterrows():
        project = row[PROJECT]
        org = row[ORG]

        if isinstance(project, float) and math.isnan(project):
            # no such user exists in AdminUser list.
            print("No such user exists: Exit")
            sys.exit(1)

        vendor = row[VENDOR]
        product = row[PRODUCT]
        feature = row[USG_FEATURE]
        username = row[USG_USERNAME]
        time = float(row[USG_TIME])
        start_t = str(row["Start Time"])
        end_t = str(row["End Time"])

        if (per_team):  
            # project, org, vendor, product, feature
            proj_node = nested_data[project]
            org_node = proj_node[org]
            vend_node = org_node[vendor]
            prod_node = vend_node[product]
            prod_node[P_CONCURUSERS] = 0
            feature_node = prod_node[feature]
        else:
            # project, vendor, product, feature, org
            proj_node = nested_data[project]
            vend_node = proj_node[vendor]
            prod_node = vend_node[product]
            prod_node[P_CONCURUSERS] = 0
            feature_node = prod_node[feature]
            org_node = feature_node[org]

        t_prod = prod_user[product] 
    
        # Store total per username at feature-level
        if (feature_node.get(username, 0) == 0 and time > 0):
            feature_node[P_NUMUSERS]= feature_node.get(P_NUMUSERS, 0) + 1
        feature_node[username] = feature_node.get(username, 0) + time
        feature_node[P_CONCURUSERS]= 1	# initial value

        # Store username per product
        if (t_prod.get(username, 0) == 0 and time > 0):
            # prod_node[P_NUMUSERS] = prod_node[P_NUMUSERS] + 1
            t_prod[username] = t_prod.get(username, 0) + time	# not used, but set for value for the key "username"

#        if (prod_node.get(username, 0) == 0 and time > 0):
#            prod_node[P_NUMUSERS]= prod_node.get(P_NUMUSERS, 0) + 1
    
        if (per_team):  
            # Update accumulated totals at each level
            proj_node["_total"] = proj_node.get("_total", 0) + time
            org_node["_total"] = org_node.get("_total", 0) + time
            vend_node["_total"] = vend_node.get("_total", 0) + time
            prod_node["_total"] = prod_node.get("_total", 0) + time
            feature_node["_total"] = feature_node.get("_total", 0) + time
        else:
            # Update accumulated totals at each level
            proj_node["_total"] = proj_node.get("_total", 0) + time
            vend_node["_total"] = vend_node.get("_total", 0) + time
            prod_node["_total"] = prod_node.get("_total", 0) + time
            feature_node["_total"] = feature_node.get("_total", 0) + time
            org_node["_total"] = org_node.get("_total", 0) + time
 
        # get concurrency (TODO)
        # concurrency of tool per performer
        #                     across performers
        # build a list of [[start1, end1], [star2, end2], ...]
        if (time > 0):
            feature_node[P_INSTANCES] = feature_node.get(P_INSTANCES, [])
            feature_node[P_INSTANCES].append([start_t, end_t])
  
    # remove prod_node[username]    

    # Print nicely formatted result

def detect_header_row_usagefile(file_path, sheet_name):
    """Detect whether headers are in row 1 or 2 based on 'User Name' or 'Product'."""
    df_preview = pd.read_excel(file_path, sheet_name=sheet_name, nrows=2, header=None)
    for row_idx in [0, 1]:  # check first and second row
        row_values = df_preview.iloc[row_idx].astype(str).str.strip().tolist()
        if any(x.lower() in [USG_USERNAME.lower(), PRODUCT.lower()] for x in row_values):
            return row_idx  # return 0 for first row, 1 for second row
    return 0  # default to first row if not found

def detect_header_row_featurefile(file_path, sheet_name):
    """Detect whether headers are in row 1 or 2 based on 'User Name' or 'Product'."""
    df_preview = pd.read_excel(file_path, sheet_name=sheet_name, nrows=2, header=None)
    for row_idx in [0, 20]:  # check first and second row
        row_values = df_preview.iloc[row_idx].astype(str).str.strip().tolist()
        if any(x.lower() in [F_PRODUCT.lower(), F_FEATURE.lower()] for x in row_values):
            return row_idx  # return 0 for first row, 1 for second row
    return 0  # default to first row if not found

def main():
    if len(sys.argv) < 5:
        print("Usage: python match.py <Usage log.xlsx> <EDA feature.xlsx> <User list.xlsx> <Current provisioning.xlsx")
        sys.exit(1)

    file_a = sys.argv[1]
    file_b = sys.argv[2]
    file_c = sys.argv[3]
    file_d = sys.argv[4]

    # --- Step 1: Build mapping dictionary from EDA feature.xlsx ---
    print("[1] Build mapping dictionary from EDA feature file: %s" % file_b)
    feature_lookup = setup_feature_dictionary(file_b)
    
    # --- Step 2: Build mapping dictionary from User list.xlsx ---
    print("[2] Build mapping dictionary from User list file: %s" % file_c)
    user_lookup = setup_user_dictionary(file_c)

    # --- Step 2.1: Read A.xlsx and add extra fields ---
    (xls_a, target_sheet, df) = add_extra_fields(file_a, feature_lookup, user_lookup)

    print("[3] Make Pivot table from usage file: %s" % file_a)
    # -- Step 3: Collect data for a pivot table
    pivot_data = tree()
    build_pivot_table(df, pivot_data)
    (rows, max_concurrency) = flatten_defaultdict(pivot_data)
    
    df_pivot_data = pd.DataFrame(rows) 
    df_pivot_data.columns = ["Project", "Performer", "Vendor", "Product", "Product Feature", "User", "Total Usage Time", "Number of Users", "Concurrency (Estimated)"]

    # -- Step 3.1: Collect data for a pivot table
    pivot_data_tool = tree()
    build_pivot_table(df, pivot_data_tool, False)
    (rows_tool, max_concurrency) = flatten_defaultdict(pivot_data_tool)
    
    df_pivot_data_tool = pd.DataFrame(rows_tool) 
    df_pivot_data_tool.columns = ["Project", "Vendor", "Product", "Product Feature", "Performer", "User", "Total Usage Time", "Number of Users", "Concurrency (Estimated)"]

    # -- Step 4: read current_provisiong Exel file and create usage table
    print("[4] Build table comparing current provisions (from file: %s) and actual usage" % file_d)
    (df_prov) = pr.build_current_provision_usage(file_d, pivot_data)

    # --- Step 5: Write all sheets into A-processed.xlsx ---
    write_new_file(file_a, xls_a, target_sheet, df, df_pivot_data, df_pivot_data_tool, df_prov)

if __name__ == "__main__":
    main()

