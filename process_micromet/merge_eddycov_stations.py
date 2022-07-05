# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import warnings

def merge_eddycov_stations(stationName, rawFileDir,
                           intermediateOutDir, varNameExcelTab):
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
    intermediateOutDir: path to the directory that contains the final .csv files
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
        df = pd.read_csv(intermediateOutDir+'Berge'+'.csv',
                         low_memory=False)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        df_res = pd.read_csv(intermediateOutDir+'Reservoir'+'.csv',
                             low_memory=False)
        df_therm = pd.read_csv(intermediateOutDir+'Thermistors'+'.csv',
                               low_memory=False)
        df_freeze = pd.read_csv(rawFileDir+'External_data_and_misc/'+
                                'Reservoir_freezup_and_melt'+'.csv',
                                low_memory=False, delimiter=';',header=1)

        # Add resservoir freeze up / ice melt information
        df['water_frozen_sfc'] = np.zeros((df.shape[0]))
        for index_df in df_freeze.index:
            s = pd.to_datetime(df_freeze.loc[index_df,'Freezeup'])
            index_s = df.index[df['timestamp'] == s][0]
            e = pd.to_datetime(df_freeze.loc[index_df,'Icemelt'])
            index_e = df.index[df['timestamp'] == e][0]
            df.loc[index_s:index_e,'water_frozen_sfc'] = 1

        # Add thermistors informations to dataframe
        for iVar in df_therm.columns:
            if iVar not in df.columns:
                df[iVar] = df_therm[iVar]

        # Add variable for gap filling
        df['delta_temp_air_eau'] = df['air_temp_HMP45C'] - df['water_temp_sfc']

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


        # Merge radiations
        rad_vars = ['rad_shortwave_up_CNR4', 'rad_longwave_up_CNR4',
                    'rad_shortwave_down_CNR4', 'rad_longwave_down_CNR4',
                    'albedo_CNR4','rad_net_CNR4']

        for iVar in rad_vars:

            if iVar == 'rad_longwave_up_CNR4':
                # Substitutes Berge values with reservoir when available or
                # with theoretical black body radiation calculated with water
                # surface temperature when reservoir not frozen
                id_res_avail = ~df_res[iVar].isna()
                id_frozen_res = df['water_frozen_sfc'] == 1
                id_snow_gnd = df['albedo_CNR4'].rolling(
                    window=48*4,min_periods=20).mean() > 0.6

                # Theoretical black body radiation
                rad_longwave_up_BB = 0.995*5.67e-8* \
                    (df['water_temp_sfc'].interpolate()+273.15)**4

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
                id_frozen_res = df['water_frozen_sfc'] == 1
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

            elif iVar == 'albedo_CNR4':
                # Recompute albedo
                df['albedo_CNR4'] = df['rad_shortwave_down_CNR4'] \
                    / df['rad_shortwave_down_CNR4']
                    #TODO vérifier que pas albedo < 50 W/m²

            elif iVar == 'rad_net_CNR4':
                df['rad_net_CNR4'] = df['rad_shortwave_down_CNR4'] \
                    + df['rad_longwave_down_CNR4'] \
                        - df['rad_shortwave_up_CNR4'] \
                            - df['rad_longwave_up_CNR4']


        # Merge fluxes
        flux_vars = ['LE','H','CO2_flux']

        for iVar in flux_vars:
            # Replace fluxes when reservoir quality is better
            id_sub = df_res[iVar+'_qf'] < df[iVar+'_qf']
            df.loc[id_sub,iVar] = df_res.loc[id_sub,iVar]
            df.loc[id_sub,iVar+'_qf'] = df_res.loc[id_sub,iVar+'_qf']

            # Average flux when similar quality
            id_avg = df_res[iVar+'_qf'] == df[iVar+'_qf']
            df.loc[id_avg,iVar] = np.mean(
                [df_res.loc[id_avg,iVar], df.loc[id_avg,iVar]], axis=0)

            # Replace remaining fluxes
            id_sub = df[iVar].isna()
            df.loc[id_sub,iVar] = df_res.loc[id_sub,iVar]


        # Merge Berge and Reservoir DataFrames for remaining variables
        for iVar in column_dic:
            if not ( (iVar in rad_vars) or (iVar in flux_vars) ):
                try:
                    # Substitutes Berge values with reservoir when available
                    id_substitute = ~df_res[iVar].isna()
                    df.loc[id_substitute, iVar] = df_res.loc[
                        id_substitute, iVar]
                except:
                    warnings.warn(f'Unknown variable: {iVar}')


    elif stationName == 'Forest_stations':

        ###################################################################
        ### Merge all eddypro variable between foret est, ouest and sol ###
        ###################################################################

        # Import Foret data
        df = pd.read_csv(intermediateOutDir+'Foret_ouest'+'.csv', low_memory=False)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Foret Est
        df_foret_est = pd.read_csv(intermediateOutDir+'Foret_est'+'.csv', low_memory=False)
        df_foret_est['timestamp'] = pd.to_datetime(df_foret_est['timestamp'])

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
            elif iVar in df.columns:
                # Average variable
                df[iVar] = np.nanmean(
                    pd.concat( [df[iVar], df_foret_est[iVar]], axis=1), axis=1)
            else:
                # Add variable from foret est to foret ouest
                df[iVar] = df_foret_est[iVar]

        # Import Foret sol
        df_foret_sol = pd.read_csv(intermediateOutDir+'Foret_sol'+'.csv', low_memory=False)

        # Import EddyPro variable names that should be merged
        xlsFile = pd.ExcelFile(varNameExcelTab)
        column_dic = pd.read_excel(xlsFile,'Foret_sol_cs')

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
