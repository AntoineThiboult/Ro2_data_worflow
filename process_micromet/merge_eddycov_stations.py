# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def merge_eddycov_stations(stationName, mergedCsvOutDir, varNameExcelTab):
    """Merge :
        - the Berge and Reservoir stations together. The station reservoir has
          "priority", meaning that if some data is available on both stations,
          the data from reservoir is kept, while the one from Berge is discarded.
          However, the following variables from Berge have priority:
              - rad_longwave_down_CNR4
              - rad_longwave_up_CNR4
              - rad_net_CNR4
              - rad_shortwave_down_CNR4
              - rad_shortwave_up_CNR4'
        - the Foret_ouest and Foret_est stations together. Data from both stations
          are mutually exclusive in approx 99.85% of the cases. In the case data
          is available from both stations, the average of the value is kept,
          except for the following variables where foret_ouest data is kept:
              - wind_dir_sonic

    Parameters
    ----------
    stationName: name of the station where modifications should be applied
    mergedCsvOutDir: path to the directory that contains the final .csv files
    varNameExcelTab: path and name of the Excel file that contains the
        variable description and their names

    Returns
    -------
    df: pandas DataFrame"""

    if stationName == 'Water_stations':

        ##############################################################
        ### Merge all eddypro variable between berge and reservoir ###
        ##############################################################

        # Import Reservoir data
        df = pd.read_csv(mergedCsvOutDir+'Berge'+'.csv', low_memory=False)
        df_res = pd.read_csv(mergedCsvOutDir+'Reservoir'+'.csv', low_memory=False)

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_dic = pd.read_excel(xlsFile,'Reservoir_eddypro')

        # Lines of tab that should be averaged
        lines_to_include = column_dic.iloc[:,0].str.contains('NA - Only stored as binary|Database variable name', regex=True)
        column_dic = column_dic[lines_to_include == False]
        column_dic = column_dic.iloc[:,0]
        column_dic = column_dic[~(column_dic == 'timestamp')]

        # Merge Berge and Reservoir DataFrames
        for iVar in column_dic:
            if iVar not in ['rad_longwave_down_CNR4', 'rad_longwave_up_CNR4',
                            'rad_net_CNR4', 'rad_shortwave_down_CNR4', 'rad_shortwave_up_CNR4']:
                # Substitutes Berge values with reservoir when available
                id_substitute = ~df_res[iVar].isna()
                df.loc[id_substitute, iVar] = df_res.loc[id_substitute, iVar]

    elif stationName == 'Forest_stations':

        ##############################################################
        ### Merge all eddypro variable between foret est and ouest ###
        ##############################################################

        # Import Foret data
        df = pd.read_csv(mergedCsvOutDir+'Foret_ouest'+'.csv', low_memory=False)
        df_foret_est = pd.read_csv(mergedCsvOutDir+'Foret_est'+'.csv', low_memory=False)

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_dic = pd.read_excel(xlsFile,'Foret_est_eddypro')

        # Lines of tab that should be averaged
        lines_to_include = column_dic.iloc[:,0].str.contains('NA - Only stored as binary|Database variable name', regex=True)
        column_dic = column_dic[lines_to_include == False]
        column_dic = column_dic.iloc[:,0]
        column_dic = column_dic[~(column_dic == 'timestamp')]

        # Merge foret ouest and est DataFrames
        for iVar in column_dic:
            if iVar == 'wind_dir_sonic':
                # Substitutes ouest NaN values by est values
                df.loc[df[iVar].isna(), iVar] = df_foret_est.loc[df[iVar].isna(), iVar]
            else:
                # Average variable
                df[iVar] = np.nanmean(
                    pd.concat( [df[iVar], df_foret_est[iVar]], axis=1), axis=1)


    else:
        df = pd.read_csv(mergedCsvOutDir+stationName+'.csv', low_memory=False)

    return df
