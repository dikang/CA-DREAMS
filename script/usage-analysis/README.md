# Scripts to Analyze Usage Data

A collection of scripts to analyze monthly usage data for EDA tooling across the performers in a hub. These scripts process hub-specific input files and generate summary outputs for easier usage monitoring and provisioning decisions.

---

## 📁 Inputs (Required Files)

The script expects these input files:

- **Monthly usage Excel file** (hub-specific)  
- **Feature mapping Excel file** (shared across all hubs)  
- **User list Excel file** (hub-specific)  
- **Current EDA tool provisioning Excel file** (hub-specific)  

---

## 📊 Outputs (Generated Results)

Running the script produces the following sheets/tabs:

- **Usage** — the original usage data  
- **Performer Summary** — pivot-table–style summary grouped by performing organization  
- **Tool Summary** — pivot-table–style summary grouped by EDA tool  
- **Tool Usage** — comparison between what is provisioned and what was actually used  

---

## ⚠️ Notes & Assumptions

- The script builds several internal mapping tables using the input files. In particular:  
  - It maps usernames to **\<Organization, Project name>** using the monthly usage data + user list file.  
  - It maps “features” to **\<Product Name>** using the feature-mapping Excel file.  
- The code is sensitive to the exact column names in the input Excel files. If column names change, the script may fail.  
- The user list file is assumed to match exactly what the provisioning system (Nimbis) uses when accounts are created.  

---

## ▶️ Usage

```bash
python analyze.py <usage_excel_file> \
                  <feature_mapping_excel_file> \
                  <user_list_excel_file> \
                  <current_provisioning_excel_file>
