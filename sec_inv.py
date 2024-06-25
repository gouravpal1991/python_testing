import pandas as pd

# Create a stub DataFrame for testing
def create_stub_df():
    data = {
        'WA Security ID/Config': ['config', 'config', None, 'config', 'config', 'config', 'config', '123', '124'],
        'NBIN Type ID': [1, 1, 1, 1, 2, 2, None, 1, 1],
        'NBIN Class ID': [10, 10, 20, None, 20, 21, 21, 10, 20],
        'NBIN Type Name': ['Type1', 'Type1', 'Type2', 'Type1', 'Type2', 'Type3', 'Type3', 'Type1', 'Type2'],
        'NBIN Class Name': ['Class1', 'Class1', 'Class2', 'Class1', 'Class2', 'Class3', 'Class3', 'Class1', 'Class2'],
        'Final WA Base Style': ['Style1', 'Style1', 'Style2', 'Style1', 'Style2', 'Style3', 'Style3', 'Style1', 'Style2'],
        'Action': ['Update', 'Expire', 'Update', 'Update', 'Update', 'Expire', 'Update', 'Update', 'Create'],
        'MS CIFSC Category': ['Category1', 'Category1', 'Category2', 'Category1', 'Category2', 'Category3', None, 'Category1', 'Category2'],
        'MS Category Type': ['TypeA', 'TypeA', 'TypeB', 'TypeA', 'TypeB', 'TypeC', None, 'TypeA', 'TypeB'],
        'MS Market Cap': ['Large', 'Large', 'Mid', 'Large', 'Mid', 'Small', None, 'Large', 'Mid'],
        'MS Fund Region Focus': ['Region1', 'Region1', 'Region2', 'Region1', 'Region2', 'Region3', None, 'Region1', 'Region2'],
        'MS Fund Asset Class Focus': ['Equity', 'Equity', 'Bond', 'Equity', 'Bond', 'RealEstate', None, 'Equity', 'Bond'],
        'Final WA Base Style': ['Style1', 'Style1', 'Style2', 'Style1', 'Style2', 'Style3', 'Style3', 'Style1', 'Style2']
    }
    return pd.DataFrame(data)

# Main function to process the DataFrame
def process_df(df):
    # Initialize error DataFrame
    error_df = pd.DataFrame(columns=df.columns.tolist() + ['Error Message'])
    
    # Initialize valid DataFrame
    valid_df = df.copy()

    # Check if WA Security ID/Config is empty
    empty_security_id_df = df[df['WA Security ID/Config'].isna()].copy()
    if not empty_security_id_df.empty:
        empty_security_id_df['Error Message'] = 'Entry without a WS Security Id'
        error_df = pd.concat([error_df, empty_security_id_df], ignore_index=True)
        valid_df = valid_df.drop(empty_security_id_df.index)

    # Check for Action values other than 'Update' or 'Expire'
    invalid_action_df = df[~df['Action'].isin(['Update', 'Expire'])].copy()
    if not invalid_action_df.empty:
        invalid_action_df['Error Message'] = 'Entry with action value other than Update or Expire'
        error_df = pd.concat([error_df, invalid_action_df], ignore_index=True)
        valid_df = valid_df.drop(invalid_action_df.index)

    # Check for duplicate WA Security ID/Config
    duplicate_security_id_df = df[df.duplicated(subset=['WA Security ID/Config'], keep=False)].copy()
    if not duplicate_security_id_df.empty:
        duplicate_security_id_df['Error Message'] = 'No duplicate security key'
        error_df = pd.concat([error_df, duplicate_security_id_df], ignore_index=True)
        valid_df = valid_df.drop(duplicate_security_id_df.index)

    # Process rows where WA Security ID/Config value is 'config'
    config_df = valid_df[valid_df['WA Security ID/Config'] == 'config'].copy()

    # Check for NBIN Type ID and NBIN Class ID pairing
    unclassified_nbin_df = config_df[
        config_df[['NBIN Type ID', 'NBIN Class ID']].apply(lambda x: x.isnull().sum() == 1, axis=1)
    ].copy()
    if not unclassified_nbin_df.empty:
        unclassified_nbin_df['Error Message'] = 'Unclassified NBIN security type'
        error_df = pd.concat([error_df, unclassified_nbin_df], ignore_index=True)
        valid_df = valid_df.drop(unclassified_nbin_df.index)

    # Check for MS CIFSC Category, MS Category Type, MS Market Cap, MS Fund Region Focus, MS Fund Asset Class Focus combination
    ms_category_columns = ['MS CIFSC Category', 'MS Category Type', 'MS Market Cap', 'MS Fund Region Focus', 'MS Fund Asset Class Focus']
    unclassified_ms_df = config_df[
        config_df[ms_category_columns].apply(lambda x: x.notnull().sum() not in [0, len(ms_category_columns)], axis=1)
    ].copy()
    if not unclassified_ms_df.empty:
        unclassified_ms_df['Error Message'] = 'Unclassified MS CIFSC Category Exceptions'
        error_df = pd.concat([error_df, unclassified_ms_df], ignore_index=True)
        valid_df = valid_df.drop(unclassified_ms_df.index)

    # Check for duplicate combination of NBIN Type ID and NBIN Class ID
    duplicate_nbin_df = config_df[config_df.duplicated(subset=['NBIN Type ID', 'NBIN Class ID'], keep=False)].copy()
    if not duplicate_nbin_df.empty:
        duplicate_nbin_df['Error Message'] = 'Duplicate NBIN Type ID and NBIN Class ID combination'
        error_df = pd.concat([error_df, duplicate_nbin_df], ignore_index=True)
        valid_df = valid_df.drop(duplicate_nbin_df.index)

    # Check for duplicate combination of MS CIFSC Category, MS Category Type, MS Market Cap, MS Fund Region Focus, MS Fund Asset Class Focus
    duplicate_ms_combination_df = config_df[
        config_df.duplicated(subset=ms_category_columns, keep=False)
    ].copy()
    if not duplicate_ms_combination_df.empty:
        duplicate_ms_combination_df['Error Message'] = 'Duplicate MS CIFSC Category, MS Category Type, MS Market Cap, MS Fund Region Focus, MS Fund Asset Class Focus combination'
        error_df = pd.concat([error_df, duplicate_ms_combination_df], ignore_index=True)
        valid_df = valid_df.drop(duplicate_ms_combination_df.index)

    # Remove duplicates to keep unique errors
    error_df = error_df.drop_duplicates().reset_index(drop=True)

    return valid_df.reset_index(drop=True), error_df

# Create stub DataFrame
stub_df = create_stub_df()

# Process the stub DataFrame
valid_df, error_df = process_df(stub_df)

# Display results
print("Valid DataFrame:")
print(valid_df)
print("\nError DataFrame:")
print(error_df)
