# -*- coding: utf-8 -*-
import os
import pandas as pd

def rename_trim(stationName,df,db_name_map):
    """
    Rename and select a subset of the df variables according to the
    db_name_map. Writes a log file if variables are missing.

    Parameters
    ----------
    stationName : String
        Statione name
    df : Pandas dataframe
        Dataframe that contains columns that should be selected and renamed
    db_name_map : Pandas dataframe
        Dataframe that include columns db_name and 'original_name' to perform
        the mapping of the names.

    Returns
    -------
    df : Pandas dataframe
        Nice and tidy dataframe

    """

    # Open error log file
    logf = open(os.path.join('.','Logs','rename_and_trim_variables.log'), "a")

    # Check that all variables in dictionary are present in df
    is_var_in_df = db_name_map['original_name'].isin(df.columns)
    missing_var = db_name_map.loc[~is_var_in_df,'original_name'].values
    if any(missing_var):
        # Remove absent variables from dictionary and log the error
        logf.write(f"Variable {missing_var} absent from dataframe in {stationName}\n")
        db_name_map = db_name_map[is_var_in_df]

    # Select the variables to be kept
    df = df[db_name_map['original_name']]

    # Make name translation from Campbell Scientific / EddyPro to DB names
    df.columns = db_name_map['db_name'].values

    # Remove duplicated columns by merging them and keep non-nan values
    df = merge_duplicate_columns(df)

    # Close error log file
    logf.close()

    return df


def map_db_names(stationName,excelFile,tab):
    """
    Read the Excel file that contains the translation from the datalogger (cs)
    and EddyPro (eddypro) to the final database names (db)

    Parameters
    ----------
    stationName : String
        Name of the station
    excelFile : Excel file that includes its path
        Excel file that contains the correspondance between cs, EddyPro and db
        names
    tab : String
        Either 'cs' or 'eddypro'. Indicates what tab should be used

    Returns
    -------
    db_name_map : Pandas dataframe
        Dataframe that include columns db_name and 'original_name' to perform
        the mapping of the names.
    """

    if tab == 'cs':
        col_names = ['db_name', 'original_name', 'var_description','units',
                     'instrument','instrument_descrip','remarks']
    elif tab == 'eddypro':
        col_names = ['db_name', 'original_name', 'var_description','units',
                     'remarks']

    # Import Excel documentation file
    xlsFile = pd.ExcelFile(excelFile)
    db_name_map = pd.read_excel(
        xlsFile, f'{stationName}_{tab}', dtype=str,
        names = col_names)

    # Remove rows that should not be included in the database
    id_rm = ~(db_name_map['db_name'].isna() |
              (db_name_map['db_name'] == 'Database variable name') |
              (db_name_map['db_name'] == 'NA - Only stored as binary'))
    db_name_map = db_name_map[id_rm].reset_index(drop=True)

    return db_name_map


def merge_duplicate_columns(df):
    """
    Merge duplicated columns, keep value over NaN if available, and return
    a dataframe without duplicated columns

    Parameters
    ----------
    df : Pandas dataframe
        Pandas dataframe with duplicated columns

    Returns
    -------
    df : Pandas dataframe
        Pandas dataframe without duplicated columns
    """
    return (
        df.T
          .groupby(level=0)           # group by column names
          .apply(lambda g: g.bfill().ffill().iloc[0])  # or g.max(), etc.
          .T
    )