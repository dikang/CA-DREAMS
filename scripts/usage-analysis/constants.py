
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
P_CONCURDURATION= "Concurrent Duration"
P_INSTANCES = "_instances"
P_TOTAL = "_total"

# provisioning Excel sheet
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

# Summay Excel sheet
S_NUM_TOOLS_USED = "Number of Tools Used"	# number of tool used
S_NUM_TOOLS_PROVISIONED = "Number of Tools Provisioned"	# number of tool provisioned
S_TOOL_USAGE_RATIO = "Ratio of Tools Used" # ration of tools used over tools provisioned
