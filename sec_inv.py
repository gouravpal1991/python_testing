import pandas as pd

# Sample data initialization (replace with actual data loading)
data = {
    'WA Security ID/Config': ['Config', 'Config'],
    'NBIN Type ID': ['', ''],
    'NBIN Class ID': ['', ''],
    'NBIN Type Name': ['TypeName1', 'TypeName2'],
    'NBIN Class Name': ['ClassName1', 'ClassName2'],
    'Final WA Base Style': ['Style1', 'Style2'],
    'Action': ['Update', 'Update'],
    'MS CIFSC Category': ['Cat1', 'Cat1'],
    'MS Category Type': ['TypeA', 'TypeB'],
    'MS Market Cap': ['Large', 'Rest'],
    'MS Fund Region Focus': ['Test', 'Region1'],
    'MS Fund Asset Class Focus': ['Asset1', 'Asset2']
}

# Create DataFrame from sample data
df = pd.DataFrame(data)

# Initialize error_df to collect error rows
error_df = pd.DataFrame(columns=df.columns.tolist() + ['Warning'])

# Step 1: Extract rows where WA Security ID/Config is empty or not as expected
invalid_wa = df[(df['WA Security ID/Config'].str.strip().eq('') |
                 ~df['WA Security ID/Config'].str.startswith('WA') &
                 ~df['WA Security ID/Config'].eq('Config'))]
invalid_wa['Warning'] = 'Entry without a WS Security Id'
error_df = pd.concat([error_df, invalid_wa])

# Remove invalid WA rows from main df
df = df[~df.index.isin(invalid_wa.index)]

# Step 2: Extract rows with Action value other than Update or Expire
invalid_action = df[~df['Action'].isin(['Update', 'Expire'])]
invalid_action['Warning'] = 'Entry with action value other than Update or Expire'
error_df = pd.concat([error_df, invalid_action])

# Remove invalid Action rows from main df
df = df[~df.index.isin(invalid_action.index)]

# Step 3: Create three separate DataFrames based on criteria
df1 = df[df['WA Security ID/Config'].str.startswith('WA')]

df2 = df[(df['WA Security ID/Config'] == 'Config') &
         ((df['NBIN Type ID'] != '') | (df['NBIN Class ID'] != ''))]

df3 = df[(df['WA Security ID/Config'] == 'Config') &
         (df[['MS CIFSC Category', 'MS Category Type', 'MS Market Cap', 'MS Fund Region Focus', 'MS Fund Asset Class Focus']].notna().any(axis=1))]

# Step 3.1: Validation logic for df1
duplicate_wa = df1[df1.duplicated(subset=['WA Security ID/Config'], keep=False)]
duplicate_wa['Warning'] = 'Unclassified NBIN security type'
error_df = pd.concat([error_df, duplicate_wa])

# Step 3.2: Validation logic for df2
df2_missing_ids = df2[(df2['NBIN Type ID'] == '') | (df2['NBIN Class ID'] == '')]
df2_missing_ids['Warning'] = 'Unclassified NBIN Config'
error_df = pd.concat([error_df, df2_missing_ids])

duplicate_nbin_keys = df2[df2.duplicated(subset=['NBIN Type ID', 'NBIN Class ID'], keep=False)]
duplicate_nbin_keys['Warning'] = 'Duplicate NBIN Key Config values'
error_df = pd.concat([error_df, duplicate_nbin_keys])

df2['CATEGORY TYPE'] = 'NBIN ROWS'

# Step 3.3: Validation logic for df3
df3_missing_ms_values = df3[df3[['MS CIFSC Category', 'MS Category Type', 'MS Market Cap', 'MS Fund Region Focus', 'MS Fund Asset Class Focus']].isna().any(axis=1)]
df3_missing_ms_values['Warning'] = 'Unclassified MS Values'
error_df = pd.concat([error_df, df3_missing_ms_values])

duplicate_ms_keys = df3[df3.duplicated(subset=['MS CIFSC Category', 'MS Category Type', 'MS Market Cap', 'MS Fund Region Focus', 'MS Fund Asset Class Focus'], keep=False)]
duplicate_ms_keys['Warning'] = 'Duplicate MS Key Config values'
error_df = pd.concat([error_df, duplicate_ms_keys])

# Corrected Step 3.3.3: Check for MS CIFSC Category with more than one MS Category Type
ms_multiple_combinations = df3[df3.groupby('MS CIFSC Category')['MS Category Type'].transform('nunique') > 1]

# Filter out rows where MS CIFSC Category is NaN
# ms_multiple_combinations = ms_multiple_combinations[ms_multiple_combinations['MS CIFSC Category'].notna()]

ms_multiple_combinations['Warning'] = 'Multiple combinations of MS CIFSC Category and MS Category Type found'
error_df = pd.concat([error_df, ms_multiple_combinations])

# Displaying error_df (you would typically log or further process this)
print("\nError DataFrame:")
print(error_df)
