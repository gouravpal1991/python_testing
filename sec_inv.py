import sys
# from awsglue.utils import getResolvedOptions
import pandas as pd

# Initialize AWS Glue job arguments
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Assume 'main_df' is your main DataFrame obtained from a Glue DynamicFrame or other source
# Replace this with your actual data loading method
# For example, if reading from S3:
# main_df = pd.read_parquet("s3://your-bucket/path/to/main_df.parquet")

# Example data (replace this with loading your actual data)
data = {
    'WA Security ID/Config': ['WA123', '', 'Config', 'WA456', 'WA789', 'Config', 'WA123', 'WA123', 'Config'],
    'NBIN Type ID': [101, 102, None, 103, 104, None, 101, 105, None],
    'NBIN Class ID': [201, None, 202, None, 203, 204, 201, 201, None],
    'NBIN Type Name': ['Type A', 'Type B', 'Type C', 'Type D', 'Type E', 'Type F', 'Type A', 'Type B', 'Type C'],
    'NBIN Class Name': ['Class X', 'Class Y', 'Class Z', 'Class P', 'Class Q', 'Class R', 'Class X', 'Class X', 'Class Z'],
    'Final WA Base Style': ['Style 1', 'Style 2', 'Style 3', 'Style 4', 'Style 5', 'Style 6', 'Style 1', 'Style 7', 'Style 8'],
    'Action': ['Update', 'Expire', 'Hold', 'Update', 'Buy', 'Expire', 'Update', 'Update', 'Hold'],
    'MS CIFSC Category': ['Cat1', 'Cat2', 'Cat3', 'Cat4', None, 'Cat6', 'Cat1', 'Cat2', 'Cat3'],
    'MS Category Type': ['Type1', 'Type2', 'Type3', None, 'Type5', 'Type6', 'Type1', 'Type2', 'Type3'],
    'MS Market Cap': ['Large', 'Mid', None, 'Large', 'Mid', 'Small', 'Large', 'Mid', 'Small'],
    'MS Fund Region Focus': ['Region1', 'Region2', 'Region3', 'Region4', 'Region5', 'Region6', 'Region1', 'Region2', 'Region3'],
    'MS Fund Asset Class Focus': ['AssetClass1', 'AssetClass2', 'AssetClass3', 'AssetClass4', 'AssetClass5', 'AssetClass6', 'AssetClass1', 'AssetClass2', 'AssetClass3']
}

# Create main DataFrame
main_df = pd.DataFrame(data)

# 1. Extract rows into error_df where WA Security ID/Config is empty or not valid
error_df = main_df[(main_df['WA Security ID/Config'].isnull()) |
                   (~main_df['WA Security ID/Config'].str.startswith('WA')) &
                   (main_df['WA Security ID/Config'] != 'Config')]
error_df['Warning'] = 'Entry without a WS Security Id'

# 2. Extract rows into error_df where Action is not 'Update' or 'Expire'
action_error_df = main_df[~main_df['Action'].isin(['Update', 'Expire'])]
error_df = pd.concat([error_df, action_error_df])
error_df.loc[action_error_df.index, 'Warning'] = 'Entry with action value other than Update or Expire'

# Remove rows in error_df from main_df to get remaining_df
remaining_df = main_df.drop(index=error_df.index, errors='ignore')

# 3. Create three additional DataFrames based on specified criteria

# DataFrame 1: WA Security ID/Config starts with 'WA'
df1 = remaining_df[remaining_df['WA Security ID/Config'].str.startswith('WA')]

# Apply validation logic for df1
duplicate_wa_ids = df1[df1.duplicated(subset=['WA Security ID/Config'], keep=False)]
error_df = pd.concat([error_df, duplicate_wa_ids])
error_df.loc[duplicate_wa_ids.index, 'Warning'] = 'Unclassified NBIN security type'

# DataFrame 2: WA Security ID/Config is 'Config' and either of NBIN Type ID or NBIN Class ID value is present
df2 = remaining_df[(remaining_df['WA Security ID/Config'] == 'Config') &
                   (remaining_df[['NBIN Type ID', 'NBIN Class ID']].notnull().any(axis=1))]

# Apply validation logic for df2
df2_no_ids = df2[(df2['NBIN Type ID'].isnull()) & (df2['NBIN Class ID'].isnull())]
error_df = pd.concat([error_df, df2_no_ids])
error_df.loc[df2_no_ids.index, 'Warning'] = 'Unclassified NBIN Config'

duplicate_nbin_keys = df2[df2.duplicated(subset=['NBIN Type ID', 'NBIN Class ID'], keep=False)]
error_df = pd.concat([error_df, duplicate_nbin_keys])
error_df.loc[duplicate_nbin_keys.index, 'Warning'] = 'Duplicate NBIN Key Config values'

df2['CATEGORY TYPE'] = 'NBIN ROWS'  # Add new column

# DataFrame 3: WA Security ID/Config is 'Config' and at least one of the specified MS fields is populated
df3 = remaining_df[(remaining_df['WA Security ID/Config'] == 'Config') &
                   (remaining_df[['MS CIFSC Category', 'MS Category Type', 'MS Market Cap',
                                  'MS Fund Region Focus', 'MS Fund Asset Class Focus']].notnull().all(axis=1))]

# Apply validation logic for df3
df3_no_values = df3[df3[['MS CIFSC Category', 'MS Category Type', 'MS Market Cap',
                         'MS Fund Region Focus', 'MS Fund Asset Class Focus']].isnull().any(axis=1)]
error_df = pd.concat([error_df, df3_no_values])
error_df.loc[df3_no_values.index, 'Warning'] = 'Unclassified MS Values'

duplicate_ms_keys = df3[df3.duplicated(subset=['MS CIFSC Category', 'MS Category Type', 'MS Market Cap',
                                               'MS Fund Region Focus', 'MS Fund Asset Class Focus'], keep=False)]
error_df = pd.concat([error_df, duplicate_ms_keys])
error_df.loc[duplicate_ms_keys.index, 'Warning'] = 'Duplicate MS Key Config values'

df3['CATEGORY TYPE'] = 'MS ROWS'  # Add new column

# Display the results (for demonstration purposes)
print("Error DataFrame:")
print(error_df)
print("\nDataFrame 1 (WA Security ID/Config starts with 'WA'):")
print(df1)
print("\nDataFrame 2 (WA Security ID/Config is 'Config' and either of NBIN Type ID or NBIN Class ID is present):")
print(df2)
print("\nDataFrame 3 (WA Security ID/Config is 'Config' and at least one of MS fields is populated):")
print(df3)