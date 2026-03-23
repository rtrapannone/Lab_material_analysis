#!/usr/bin/env python
# coding: utf-8

# In[6]:


# =============================================================================
# Environmental Survey Data Analysis
# Compatible with Jupyter Notebook on macOS
# =============================================================================

import pandas as pd

# -----------------------------------------------------------------------------
# 1. Load CSV into a DataFrame
# -----------------------------------------------------------------------------
df = pd.read_csv("/Users/riccardo/Desktop/Survey data filtered.csv", sep=";", encoding="utf-8-sig",
                 on_bad_lines="warn", engine="python")

# Drop the trailing empty column if present
df = df.loc[:, ~df.columns.str.startswith("Unnamed")]

# Strip any invisible whitespace or BOM remnants from column names
df.columns = df.columns.str.strip()

# Rename columns for convenience
col_prevention       = "Prevention_important"           # col 1 – agree/disagree
col_change_habit     = "Change_habit_willingness"       # col 2 – agree/disagree
col_travel           = "Travels_Europe_year"            # col 3 – flight categories
col_env_travel       = "Environmentally_friendly_travel"# col 4 – agree/disagree
col_turn_off         = "Turn_off_machine"               # col 5 – frequency
col_plastic          = "Plastic_recycling"              # col 6 – frequency
col_recycle          = "Willing_to_recycle"             # col 7 – yes/no/I don't know

print("=" * 65)
print("ENVIRONMENTAL SURVEY ANALYSIS")
print("=" * 65)
print(f"\nTotal rows loaded : {len(df)}")
print(f"Columns           : {list(df.columns)}\n")

# =============================================================================
# 2. Proportion who responded "Fully agree" OR "Somewhat agree" in col 2
#    (Change_habit_willingness)
# =============================================================================
agree_labels = ["Fully agree", "Somewhat agree"]

col2_valid  = df[col_change_habit].dropna()
col2_agree  = col2_valid[col2_valid.isin(agree_labels)]

n_total_col2  = len(col2_valid)
n_agree_col2  = len(col2_agree)
pct_agree_col2 = round(n_agree_col2 / n_total_col2 * 100, 1) if n_total_col2 else 0

print("-" * 65)
print("2. Agree with willingness to change habits (col 2)")
print("-" * 65)
print(f"   Respondents (non-empty) : {n_total_col2}")
print(f"   'Fully agree'           : {(col2_valid == 'Fully agree').sum()}")
print(f"   'Somewhat agree'        : {(col2_valid == 'Somewhat agree').sum()}")
print(f"   Combined agree          : {n_agree_col2}  ({pct_agree_col2} %)\n")

# =============================================================================
# 3. Count the categories in col 3 (Travels_Europe_year)
# =============================================================================
travel_order = ["0", "1", "2-5", "6-10", "11-20", ">20"]

travel_counts = (
    df[col_travel]
    .dropna()
    .astype(str)
    .value_counts()
    .reindex(travel_order)
    .dropna()
    .astype(int)
)

print("-" * 65)
print("3. Flights within Europe per year (col 3)")
print("-" * 65)
print(travel_counts.to_string())
print()

# =============================================================================
# 4. Among those who fly >1 time/year (categories 2-5, 6-10, 11-20, >20),
#    how many agreed in col 4 (Environmentally_friendly_travel)?
#    Results kept separate per travel category.
# =============================================================================
more_than_one = ["2-5", "6-10", "11-20", ">20"]

df_travel_str = df.copy()
df_travel_str[col_travel] = df_travel_str[col_travel].astype(str)

subset = df_travel_str[df_travel_str[col_travel].isin(more_than_one)].copy()

# Count agree per travel group
records_q4 = []
for grp in more_than_one:
    grp_df   = subset[subset[col_travel] == grp]
    n_valid  = grp_df[col_env_travel].notna().sum()
    n_fully  = (grp_df[col_env_travel] == "Fully agree").sum()
    n_some   = (grp_df[col_env_travel] == "Somewhat agree").sum()
    n_agree  = n_fully + n_some
    pct      = round(n_agree / n_valid * 100, 1) if n_valid else 0
    records_q4.append({
        "Travel_category"    : grp,
        "n_respondents"      : n_valid,
        "Fully_agree"        : n_fully,
        "Somewhat_agree"     : n_some,
        "Total_agree"        : n_agree,
        "Pct_agree_%"        : pct,
    })

df_q4 = pd.DataFrame(records_q4)

print("-" * 65)
print("4. Agree with environmentally friendly travel (col 4),")
print("   for those who fly >1 time/year (col 3)")
print("-" * 65)
print(df_q4.to_string(index=False))
print()

# =============================================================================
# 5. Count categories in col 5 (Turn_off_machine) and col 6 (Plastic_recycling)
# =============================================================================
freq_order = ["Always", "Regularly", "Rarely", "Never", "I can't say"]

turn_off_counts = (
    df[col_turn_off]
    .dropna()
    .value_counts()
    .reindex(freq_order)
    .dropna()
    .astype(int)
)

plastic_counts = (
    df[col_plastic]
    .dropna()
    .value_counts()
    .reindex(freq_order)
    .dropna()
    .astype(int)
)

print("-" * 65)
print("5a. Frequency of turning off machines (col 5)")
print("-" * 65)
print(turn_off_counts.to_string())
print()

print("-" * 65)
print("5b. Frequency of plastic recycling (col 6)")
print("-" * 65)
print(plastic_counts.to_string())
print()

# =============================================================================
# 6. Among those who responded "Rarely" or "Never" in col 6 (Plastic_recycling),
#    how many said "Yes" in col 7 (Willing_to_recycle)?
# =============================================================================
rare_never_labels = ["Rarely", "Never"]

df_rare_never = df[df[col_plastic].isin(rare_never_labels)].copy()

records_q6 = []
for cat in rare_never_labels:
    grp_df  = df_rare_never[df_rare_never[col_plastic] == cat]
    n_valid = grp_df[col_recycle].notna().sum()
    n_yes   = (grp_df[col_recycle] == "Yes").sum()
    pct     = round(n_yes / n_valid * 100, 1) if n_valid else 0
    records_q6.append({
        "Plastic_recycling_category" : cat,
        "n_respondents"              : n_valid,
        "Willing_Yes"                : n_yes,
        "Pct_Yes_%"                  : pct,
    })

df_q6 = pd.DataFrame(records_q6)

print("-" * 65)
print("6. 'Willing to recycle' (col 7) among rare/never plastic recyclers (col 6)")
print("-" * 65)
print(df_q6.to_string(index=False))
print()

# =============================================================================
# Export all results to a single CSV
# =============================================================================

output_rows = []

# Section 2
output_rows.append({"Section": "2 – Agree to change habits (col 2)",
                     "Category": "Fully agree",       "Count": int((col2_valid == "Fully agree").sum()),   "Pct_%": ""})
output_rows.append({"Section": "2 – Agree to change habits (col 2)",
                     "Category": "Somewhat agree",    "Count": int((col2_valid == "Somewhat agree").sum()), "Pct_%": ""})
output_rows.append({"Section": "2 – Agree to change habits (col 2)",
                     "Category": "TOTAL agree",       "Count": n_agree_col2,                                "Pct_%": pct_agree_col2})

# Section 3
for cat, cnt in travel_counts.items():
    output_rows.append({"Section": "3 – Flights per year (col 3)",
                         "Category": cat, "Count": cnt, "Pct_%": ""})

# Section 4
for _, row in df_q4.iterrows():
    output_rows.append({"Section": "4 – Env-friendly travel agreement by flight group (col 4 × col 3)",
                         "Category": f"{row['Travel_category']} | Fully agree",
                         "Count": row["Fully_agree"], "Pct_%": ""})
    output_rows.append({"Section": "4 – Env-friendly travel agreement by flight group (col 4 × col 3)",
                         "Category": f"{row['Travel_category']} | Somewhat agree",
                         "Count": row["Somewhat_agree"], "Pct_%": ""})
    output_rows.append({"Section": "4 – Env-friendly travel agreement by flight group (col 4 × col 3)",
                         "Category": f"{row['Travel_category']} | TOTAL agree",
                         "Count": row["Total_agree"], "Pct_%": row["Pct_agree_%"]})

# Section 5a
for cat, cnt in turn_off_counts.items():
    output_rows.append({"Section": "5a – Turn off machines frequency (col 5)",
                         "Category": cat, "Count": cnt, "Pct_%": ""})

# Section 5b
for cat, cnt in plastic_counts.items():
    output_rows.append({"Section": "5b – Plastic recycling frequency (col 6)",
                         "Category": cat, "Count": cnt, "Pct_%": ""})

# Section 6
for _, row in df_q6.iterrows():
    output_rows.append({"Section": "6 – Willing to recycle (col 7) among rare/never recyclers (col 6)",
                         "Category": f"{row['Plastic_recycling_category']} | Yes",
                         "Count": row["Willing_Yes"], "Pct_%": row["Pct_Yes_%"]})

df_output = pd.DataFrame(output_rows, columns=["Section", "Category", "Count", "Pct_%"])
output_path = "survey_results.csv"   # saved next to this notebook / script
df_output.to_csv(output_path, index=False)

print("=" * 65)
print(f"Results exported to  →  {output_path}")
print("=" * 65)


# In[ ]:




