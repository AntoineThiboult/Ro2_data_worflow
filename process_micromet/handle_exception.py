# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from process_micromet import detect_spikes
import warnings

def handle_exception(stationName, df, mergedCsvOutDir, varNameExcelTab):
    """Handle exception in data processing. This function contains instructions
    for special cases, such as replacements of measurements data from one
    instrument to another.

    Parameters
    ----------
    stationName: name of the station where modifications should be applied
    df: pandas DataFrame
    mergedCsvOutDir: path to the directory that contains the final .csv files
    varNameExcelTab: path and name of the Excel file that contains the
        variable description and their names

    Returns
    -------
    df: pandas DataFrame"""

    # Ignore warnings caused by averaging nan
    warnings.filterwarnings("ignore")

    if stationName == 'Berge':

        ######################################################################
        # Handle the RMY 05103 counter clockwise wind direction reference frame
        ######################################################################

        df['wind_dir_05103'] = 360 - df['wind_dir_05103']


        ######################################################################
        # Merge all eddypro variable between berge and reservoir
        ######################################################################

        # Import Reservoir data
        df_res = pd.read_csv(mergedCsvOutDir+'Reservoir'+'.csv', low_memory=False)

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_dic = pd.read_excel(xlsFile,'Berge_eddypro')

        # Lines of tab that should be averaged
        lines_to_include = column_dic.iloc[:,0].str.contains('NA - Only stored as binary|Database variable name', regex=True)
        column_dic = column_dic[lines_to_include == False]
        column_dic = column_dic.iloc[:,0]
        column_dic = column_dic[~(column_dic == 'timestamp')]

        # Merge Berge and Reservoir DataFrames
        for iVar in column_dic:
            if iVar == 'wind_dir_sonic':
                # Substitutes ouest NaN values by est values
                df.loc[df[iVar].isna(), iVar] = df_res.loc[df[iVar].isna(), iVar]
            else:
                # Average variable
                df[iVar] = np.nanmean(
                    pd.concat( [df[iVar], df_res[iVar]], axis=1), axis=1)

    if stationName == 'Foret_ouest':

        ######################################################################
        # Merge all eddypro variable between foret est and ouest
        ######################################################################

        # Import Foret_est data
        df_est = pd.read_csv(mergedCsvOutDir+'Foret_est'+'.csv', low_memory=False)

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_dic = pd.read_excel(xlsFile,'Foret_ouest_eddypro')

        # Lines of tab that should be averaged
        lines_to_include = column_dic.iloc[:,0].str.contains('NA - Only stored as binary|Database variable name', regex=True)
        column_dic = column_dic[lines_to_include == False]
        column_dic = column_dic.iloc[:,0]
        column_dic = column_dic[~(column_dic == 'timestamp')]

        # Merge foret ouest and est DataFrames
        for iVar in column_dic:
            if iVar == 'wind_dir_sonic':
                # Substitutes ouest NaN values by est values
                df.loc[df[iVar].isna(), iVar] = df_est.loc[df[iVar].isna(), iVar]
            else:
                # Average variable
                df[iVar] = np.nanmean(
                    pd.concat( [df[iVar], df_est[iVar]], axis=1), axis=1)


        ######################################################################
        # Handle the issue with the faulty CNR1 temperature probe
        # Take the CSAT temperature as proxy
        ######################################################################

        # Despike IRGASON temperature time series
        id_spikes = detect_spikes(df, 'air_temp_IRGASON')
        T_proxy = df['air_temp_IRGASON'].copy()
        T_proxy[(T_proxy<-35) | (T_proxy>35)] = np.nan
        T_proxy[id_spikes] = np.nan
        T_proxy = T_proxy.interpolate() + 273.15

        # Correct longwave radiation for blackbody radiation
        df['rad_longwave_down_CNR4'] = df['rad_longwave_down_CNR4'] + (5.67e-8*T_proxy**4)
        df['rad_longwave_up_CNR4'] = df['rad_longwave_up_CNR4'] + (5.67e-8*T_proxy**4)


        ######################################################################
        # Handle the RMY 05103 counter clockwise wind direction reference
        # frame that was introduced the 2019-06-08 10:30:00
        ######################################################################

        id_change_progr = df[df['timestamp'].str.contains('2019-06-08 10:30:00')].index.values[0]
        df.loc[0:id_change_progr,'wind_dir_05103'] = 360 - df.loc[0:id_change_progr,'wind_dir_05103']

    return df