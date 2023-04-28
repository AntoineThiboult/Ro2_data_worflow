# -*- coding: utf-8 -*-
import os
import pandas as pd

def rename_trim_vars(stationName,varNameExcelTab,df,tab):
    """Rename variables according to an Excel spreadsheet. Trim the DataFrame
    in order to keep only the variable specified in the spreadsheet

    Parameters
    ----------
    stationName: name of the station
    varNameExcelTab: path and name of the Excel file that contains the
        variable description and their names
    df: pandas DataFrame that contains the variables
    tab: Excel spreadsheet table suffix that refers to the source of the data
        (either 'cs' for Campbell Scientific files, or 'eddypro' for EddyPro
         output files)

    Returns
    -------
    df: a nice and tidy pandas DataFrame
    """

    print(f'Start renaming variables for {stationName}...')

    # Open error log file
    logf = open(os.path.join('.','Logs','rename_and_trim_variables.log'), "a")

    # Column names
    if tab == 'cs':
        col_names = ['db_name', 'original_name', 'var_description','units',
                     'instrument','instrument_descrip','remarks']
    elif tab == 'eddypro':
        col_names = ['db_name', 'original_name', 'var_description','units',
                     'remarks']

    # Import Excel documentation file
    xlsFile = pd.ExcelFile(varNameExcelTab)
    column_dic = pd.read_excel(
        xlsFile, stationName + '_' + tab, dtype=str,
        names = col_names)

    # Remove rows that should not be included in the database
    id_rm = ~(column_dic['db_name'].isna() |
              (column_dic['db_name'] == 'Database variable name') |
              (column_dic['db_name'] == 'NA - Only stored as binary'))
    column_dic = column_dic[id_rm].reset_index(drop=True)

    # Check that all variables in dictionary are present in df
    is_var_in_df = column_dic['original_name'].isin(df.columns)
    missing_var = column_dic.loc[~is_var_in_df,'original_name'].values
    if any(missing_var):
        # Remove absent variables from dictionary and log the error
        logf.write(f"Variable {missing_var} absent from dataframe in {stationName}\n")
        column_dic = column_dic[is_var_in_df]

    # Select the variables to be kept
    df = df[column_dic['original_name']]

    # Make name translation from Campbell Scientific / EddyPro to DB names
    df.columns = column_dic['db_name'].values

    # Merge duplicated columns and keep the non-NaN value
    dup_col_f = df.columns.duplicated('first')
    dup_col_l = df.columns.duplicated('last')
    df.iloc[:,dup_col_f] = df.iloc[:,dup_col_f].combine_first(
        df.iloc[:,dup_col_l])
    df = df.iloc[:,~dup_col_l]

    # Close error log file
    logf.close()

    print('Done!')

    return df