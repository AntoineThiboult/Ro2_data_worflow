# -*- coding: utf-8 -*-
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

    print('Start renaming variables for station:', stationName, '...', end='\r')
    # Import Excel documentation file
    xlsFile = pd.ExcelFile(varNameExcelTab)
    column_dic = pd.read_excel(xlsFile,stationName+'_'+tab)

    # Make translation dictionary from CS vars to DB vars
    lines_to_include = column_dic.iloc[:,0].str.contains(
        'NA - Only stored as binary|Database variable name', regex=True)
    column_dic = column_dic[lines_to_include == False]
    column_dic = column_dic.iloc[:,[0,1]]
    column_dic.columns = ['db_name','cs_name']

    # Trim dataframe and rename columns
    idColumnsIntersect = column_dic.cs_name.isin(df.columns)
    df = df[column_dic.cs_name[idColumnsIntersect]]
    df.columns = column_dic.db_name[idColumnsIntersect]

    # Merge columns that have similar column name
    if df.keys().shape != df.keys().unique().shape:
        df = df.groupby(df.columns, axis=1).mean()
    print('Done!')

    return df