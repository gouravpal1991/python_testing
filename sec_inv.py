import pandas as pd

# Create a stub DataFrame for testing
def create_stub_df():
    data = {
        'WA Security ID/Config': ['config', 'config', None, 'config', 'config', 'config', 'config', '123', '124', '123'],
        'NBIN Type ID': [1, 1, 1, 1, 2, 2, None, 1, 1, 1],
        'NBIN Class ID': [10, 10, 20, None, 20, 21, 21, 10, 20, 10],
        'NBIN Type Name': ['Type1', 'Type1', 'Type2', 'Type1', 'Type2', 'Type3', 'Type3', 'Type1', 'Type2', 'Type1'],
        'NBIN Class Name': ['Class1', 'Class1', 'Class2', 'Class1', 'Class2', 'Class3', 'Class3', 'Class1', 'Class2', 'Class1'],
        'Final WA Base Style': ['Style1', 'Style1', 'Style2', 'Style1', 'Style2', 'Style3', 'Style3', 'Style1', 'Style2', 'Style1'],
        'Action': ['Update', 'Expire', 'Update', 'Update', 'Update', 'Expire', 'Update', 'Update', 'Create', 'Update'],
        'MS CIFSC Category': ['Category1', 'Category1', 'Category2', 'Category1', 'Category2', 'Category3', None, 'Category1', 'Category2', 'Category1'],
        'MS Category Type': ['TypeA', 'TypeA', 'TypeB', 'TypeA', 'TypeB', 'TypeC', None, 'TypeA', 'TypeB', 'TypeA'],
        'MS Market Cap': ['Large', 'Large', 'Mid', 'Large', 'Mid', 'Small', None, 'Large', 'Mid', 'Large'],
        'MS Fund Region Focus': ['Region1', 'Region1', 'Region2', 'Region1', 'Region2', 'Region3', None, 'Region1', 'Region2', 'Region1'],
        'MS Fund Asset Class Focus': ['Equity', 'Equity', 'Bond', 'Equity', 'Bond', 'RealEstate', None, 'Equity', 'Bond', 'Equity'],
        'Final WA Base Style': ['Style1', 'Style1', 'Style2', 'Style1', 'Style2', 'Style3', 'Style3', 'Style1', 'Style2', 'Style1']
    }
    return pd.DataFrame(data)

# Main function to process the DataFrame
def process_df(df):
    error_df = pd.DataFrame(columns=df.columns.tolist() + ['Error Message'])
    valid_mask = pd.Series(True, index=df.index)

    def add_error(df, mask, error_message):
        error_part = df[mask].copy()
        error_part['Error Message'] = error_message
        return error_part
    
    # Check for empty WA Security ID/Config
    empty_security_id_mask = df['WA Security ID/Config'].isna()
    error_df = pd.concat([error_df, add_error(df, empty_security_id_mask, 'Entry without a WS Security Id')])
    valid_mask &= ~empty_security_id_mask

    # Check for invalid Action values
    invalid_action_mask = ~df['Action'].isin(['Update', 'Expire'])
    error_df = pd.concat([error_df, add_error(df, invalid_action_mask, 'Entry with action value other than Update or Expire')])
    valid_mask &= ~invalid_action_mask

    # Check for duplicate WA Security ID/Config (non-config)
    non_config_mask = df['WA Security ID/Config'] != 'config'
    duplicate_security_id_mask = df.duplicated(subset=['WA Security ID/Config'], keep=False) & non_config_mask
    error_df = pd.concat([error_df, add_error(df, duplicate_security_id_mask, 'No duplicate security key')])
    valid_mask &= ~duplicate_security_id_mask

    # Processing config rows
    config_mask = df['WA Security ID/Config'] == 'config'
    config_df = df[config_mask]

    # Check for unclassified NBIN
    unclassified_nbin_mask = config_df['NBIN Type ID'].isna() ^ config_df['NBIN Class ID'].isna()
    error_df = pd.concat([error_df, add_error(config_df, unclassified_nbin_mask, 'Unclassified NBIN security type')])
    valid_mask &= ~pd.Series(unclassified_nbin_mask, index=config_df.index).reindex(df.index, fill_value=False)

    # Check for unclassified MS CIFSC Category
    ms_category_columns = ['MS CIFSC Category', 'MS Category Type', 'MS Market Cap', 'MS Fund Region Focus', 'MS Fund Asset Class Focus']
    ms_any_null_mask = config_df[ms_category_columns].isnull().any(axis=1)
    ms_all_null_mask = config_df[ms_category_columns].isnull().all(axis=1)
    unclassified_ms_mask = ms_any_null_mask & ~ms_all_null_mask
    error_df = pd.concat([error_df, add_error(config_df, unclassified_ms_mask, 'Unclassified MS CIFSC Category Exceptions')])
    valid_mask &= ~pd.Series(unclassified_ms_mask, index=config_df.index).reindex(df.index, fill_value=False)

    # Check for duplicate NBIN Type ID and NBIN Class ID
    nbin_group = config_df.groupby(['NBIN Type ID', 'NBIN Class ID']).size()
    duplicate_nbin_keys = nbin_group[nbin_group > 1].index
    duplicate_nbin_mask = config_df.set_index(['NBIN Type ID', 'NBIN Class ID']).index.isin(duplicate_nbin_keys)
    error_df = pd.concat([error_df, add_error(config_df, duplicate_nbin_mask, 'Duplicate NBIN Type ID and NBIN Class ID combination')])
    valid_mask &= ~pd.Series(duplicate_nbin_mask, index=config_df.index).reindex(df.index, fill_value=False)

    # Check for duplicate MS CIFSC Category combination
    ms_group = config_df.groupby(ms_category_columns).size()
    duplicate_ms_keys = ms_group[ms_group > 1].index
    duplicate_ms_combination_mask = config_df.set_index(ms_category_columns).index.isin(duplicate_ms_keys)
    error_df = pd.concat([error_df, add_error(config_df, duplicate_ms_combination_mask, 'Duplicate MS CIFSC Category, MS Category Type, MS Market Cap, MS Fund Region Focus, MS Fund Asset Class Focus combination')])
    valid_mask &= ~pd.Series(duplicate_ms_combination_mask, index=config_df.index).reindex(df.index, fill_value=False)

    # Create valid_df using the valid_mask
    valid_df = df[valid_mask].reset_index(drop=True)
    error_df = error_df.drop_duplicates().reset_index(drop=True)

    return valid_df, error_df

# Create stub DataFrame
stub_df = create_stub_df()

# Process the stub DataFrame
valid_df, error_df = process_df(stub_df)

# Display results
print("Valid DataFrame:")
print(valid_df)
print("\nError DataFrame:")
print(error_df)
