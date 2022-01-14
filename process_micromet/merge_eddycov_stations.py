# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import warnings

def merge_eddycov_stations(stationName, rawFileDir,
                           mergedCsvOutDir, varNameExcelTab):
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
        df = pd.read_csv(mergedCsvOutDir+'Berge'+'.csv',
                         low_memory=False)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df_res = pd.read_csv(mergedCsvOutDir+'Reservoir'+'.csv',
                             low_memory=False)
        df_therm = pd.read_csv(mergedCsvOutDir+'Thermistors'+'.csv',
                               low_memory=False)
        df_freeze = pd.read_csv(rawFileDir+'External_data_and_misc/'+
                                'Reservoir_freezup_and_melt'+'.csv',
                                low_memory=False, delimiter=';',header=1)

        # Add resservoir freeze up / ice melt information
        df['frozen_sfc'] = np.zeros((df.shape[0]))
        for index_df in df_freeze.index:
            s = pd.to_datetime(df_freeze.loc[index_df,'Freezeup'])
            index_s = df.index[df['timestamp'] == s][0]
            e = pd.to_datetime(df_freeze.loc[index_df,'Icemelt'])
            index_e = df.index[df['timestamp'] == e][0]
            df.loc[index_s:index_e,'frozen_sfc'] = 1

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_dic_res_ep = pd.read_excel(
            xlsFile,'Reservoir_eddypro',
            usecols=(0,1),names=['db','raw_name'],header=None)
        column_dic_res_cs = pd.read_excel(
            xlsFile,'Reservoir_cs',
            usecols=(0,1),names=['db','raw_name'],header=None)
        column_dic = pd.concat((column_dic_res_ep, column_dic_res_cs))

        # Lines of tab that should be averaged / substituted
        lines_to_include = column_dic.iloc[:,0].str.contains(
            'NA - Only stored as binary|Database variable name', regex=True)
        column_dic = column_dic[lines_to_include == False]
        column_dic = column_dic.iloc[:,0]
        column_dic = column_dic[~(column_dic == 'timestamp')]

        # Move rad_net in last position to be sure that all components
        # have been processed beforehand
        old_cols = df.columns
        new_cols = [col for col in df.columns if col != 'rad_net_CNR4'] + ['rad_net_CNR4']
        df = df[new_cols]

        # Merge Berge and Reservoir DataFrames
        for iVar in column_dic:
            if iVar not in ['rad_longwave_up_CNR4',
                            'rad_shortwave_up_CNR4',
                            'rad_net_CNR4']:
                # Substitutes Berge values with reservoir when available
                id_substitute = ~df_res[iVar].isna()
                df.loc[id_substitute, iVar] = df_res.loc[id_substitute, iVar]

            elif iVar == 'rad_longwave_up_CNR4':
                # Substitutes Berge values with reservoir when available or
                # with theoretical black body radiation calculated with water
                # surface temperature when reservoir not frozen
                id_res_avail = ~df_res[iVar].isna()
                id_frozen_res = df['frozen_sfc'] == 1
                id_snow_gnd = df['albedo_CNR4'].rolling(
                    window=48*4,min_periods=20).mean() > 0.6

                # Theoretical black body radiation
                rad_longwave_up_BB = 0.995*5.67e-8* \
                    (df_therm['sfc_temp'].interpolate()+273.15)**4

                rad_longwave_up_merged = rad_longwave_up_BB.copy()

                rad_longwave_up_merged[id_frozen_res & id_snow_gnd] = df.loc[
                    id_frozen_res, 'rad_longwave_up_CNR4']
                rad_longwave_up_merged[id_res_avail] = df_res.loc[
                    id_res_avail,'rad_longwave_up_CNR4']

                id_malfunc = (rad_longwave_up_merged - rad_longwave_up_BB) > 35
                rad_longwave_up_merged[id_malfunc] = \
                    rad_longwave_up_BB[id_malfunc]

                df['rad_longwave_up_CNR4'] = rad_longwave_up_merged


            elif iVar == 'rad_shortwave_up_CNR4':
                # Substitutes Berge values with reservoir when available or
                # with radiation computed from Berge incoming rad and
                # reservoir mean albedo when not frozen. When frozen, take
                # radiation as Berge.
                id_res_avail = ~df_res[iVar].isna()
                id_frozen_res = df['frozen_sfc'] == 1
                id_snow_gnd = df['albedo_CNR4'].rolling(
                    window=48*4,min_periods=20).mean() > 0.6

                rad_shortwave_up_merged = np.zeros((df.shape[0]))

                rad_shortwave_up_merged[id_frozen_res & id_snow_gnd] = df.loc[
                    id_frozen_res & id_snow_gnd, 'rad_shortwave_up_CNR4']
                rad_shortwave_up_merged[id_res_avail] = df_res.loc[
                    id_res_avail, 'rad_shortwave_up_CNR4']
                rad_shortwave_up_merged[
                    ~id_res_avail & id_frozen_res & ~id_snow_gnd] = \
                    0.76 * df.loc[~id_res_avail & id_frozen_res & ~id_snow_gnd,
                                  'rad_shortwave_down_CNR4']
                rad_shortwave_up_merged[
                    ~id_res_avail & ~id_frozen_res] = \
                    0.05 * df.loc[~id_res_avail & ~id_frozen_res,
                                  'rad_shortwave_down_CNR4']

            else:
                warnings.warn(f'Unknown variable: {iVar}')

        # Move columns back to normal position
        df = df[old_cols]

    elif stationName == 'Forest_stations':

        ###################################################################
        ### Merge all eddypro variable between foret est, ouest and sol ###
        ###################################################################

        # Import Foret data
        df = pd.read_csv(mergedCsvOutDir+'Foret_ouest'+'.csv', low_memory=False)

        # Foret Est
        df_foret_est = pd.read_csv(mergedCsvOutDir+'Foret_est'+'.csv', low_memory=False)

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_for_est_ep = pd.read_excel(xlsFile,'Foret_est_eddypro')
        column_for_est_cs = pd.read_excel(xlsFile,'Foret_est_cs')
        column_dic = pd.concat((column_for_est_ep, column_for_est_cs))

        # Lines of tab that should be averaged
        lines_to_include = column_dic.iloc[:,0].str.contains(
            'NA - Only stored as binary|Database variable name', regex=True)
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

        # Import Foret sol
        df_foret_sol = pd.read_csv(mergedCsvOutDir+'Foret_sol'+'.csv', low_memory=False)

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_dic = pd.read_excel(xlsFile,'Foret_sol')

        # Lines of tab that should be averaged
        lines_to_include = column_dic.iloc[:,0].str.contains(
            'NA - Only stored as binary|Database variable name', regex=True)
        column_dic = column_dic[lines_to_include == False]
        column_dic = column_dic.iloc[:,0]
        column_dic = column_dic[~(column_dic == 'timestamp')]

        # Merge foret ouest and est DataFrames
        df[column_dic] = df_foret_sol[column_dic]

    else:
        print('{}: Unknown station name'.format(stationName))

    return df
