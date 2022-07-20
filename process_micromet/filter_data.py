# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 17:59:16 2021

@author: ANTHI182
"""

import pandas as pd
import numpy as np
import pysolar # conda install -c conda-forge pysolar
from process_micromet import bandpass_filter, detect_spikes, find_friction_vel_threshold

def filter_data(stationName,df,finalOutDir=None):
    """Perform tests on fluxes and radiations data and remove suspicious data
    Radiations:
        - computes maximal theoretical incomming shortwave radiations
        - removes suspicious upwelling shortwave radiations
        - recomputes albedo
    Fluxes:
        - Applies a passband filter
        - Removes rainy events
        - Removes low quality data according to Mauder et Folken (2004)
        - Removes data that lagerly violates engergy balance for forested
          stations (i.e., H+λE > 5Rn)
        - Remove carbon flux below the friction velocity threshold
        - Remove spikes

    Parameters
    ----------
    stationName : string
        Name of the station currently processed.
    df : Pandas dataframe
        Contains the slow and eddy covariance data.
    finalOutDir : string
        Path to the folder that contains final results.

    Returns
    -------
    df : Pandas dataframe
        Contains filtered slow and eddy covariance data.

    """
    print(f'Start filtering data for {stationName}')

    station_infos = {'Water_stations':  {
                         'lon':-63.2494011,
                         'lat':50.6889992,
                         'proxy':[],
                         'flux_vars':
                             ['LE_corr','H_corr']},
                     'Forest_stations':  {
                         'lon':-63.4051018,
                         'lat':50.9020996,
                         'proxy':[],
                         'flux_vars':
                             ['LE_corr','H_corr']},
                     'Berge':           {
                         'lon':-63.2594986,
                         'lat':50.6538010,
                         'proxy':['Reservoir','Foret_ouest','Foret_est'],
                         'flux_vars':
                             ['LE','H','CO2_flux','CH4_flux',
                              'LE_strg','H_strg','CO2_strg','CH4_strg']},
                     'Reservoir':       {
                         'lon':-63.2494011,
                         'lat':50.6889992,
                         'proxy':['Berge','Foret_ouest','Foret_est'],
                         'flux_vars':
                             ['LE','H','CO2_flux',
                              'LE_strg','H_strg','CO2_strg']},
                     'Foret_ouest':     {
                         'lon':-63.4051018,
                         'lat':50.9020996,
                         'proxy':['Foret_est','Berge','Reservoir'],
                         'flux_vars':
                             ['LE','H','CO2_flux','CH4_flux',
                              'LE_strg','H_strg','CO2_strg','CH4_strg']},
                     'Foret_est':       {
                         'lon':-63.4051018,
                         'lat':50.9020996,
                         'proxy':['Foret_ouest','Berge','Reservoir'],
                         'flux_vars':
                             ['LE','H','CO2_flux',
                              'LE_strg','H_strg','CO2_strg']},
                     'Foret_sol':       {
                         'lon':-63.4051018,
                         'lat':50.9020996,
                         'proxy':['Foret_ouest','Berge','Reservoir'],
                         'flux_vars': []}
                     }

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    ##################
    ### Radiations ###
    ##################

    if stationName in ['Berge', 'Reservoir', 'Foret_ouest']:
        # Computes sun angle and max theorethical downward shortwave values
        df['solar_angle'] = np.nan
        rad_short_down_max = np.zeros((df.shape[0],))
        dates = df['timestamp'].dt.tz_localize('Etc/GMT+5').dt.to_pydatetime()
        for counter, iDate in enumerate(dates):
            altitude_deg = pysolar.solar.get_altitude(
                station_infos[stationName]['lat'],
                station_infos[stationName]['lon'],
                iDate)
            altitude_deg = max(0, altitude_deg)
            df.loc[counter,'solar_angle'] = altitude_deg
            max_rad = 1370 * np.sin(np.deg2rad(altitude_deg))
            rad_short_down_max[counter] = max_rad

        # Filter unplausible downward short wave solar radiations
        id_sub = df['rad_shortwave_down_CNR4'] > rad_short_down_max
        df.loc[id_sub,'rad_shortwave_down_CNR4'] = rad_short_down_max[id_sub]
        id_sub = df['rad_shortwave_down_CNR4'] < 0
        df.loc[id_sub,'rad_shortwave_down_CNR4'] = 0

        # Filter upward short wave solar radiations
        id_sub = df['rad_shortwave_up_CNR4'] < 0
        df.loc[id_sub,'rad_shortwave_up_CNR4'] = 0
        id_sub = df['rad_shortwave_down_CNR4'] == 0
        df.loc[id_sub,'rad_shortwave_up_CNR4'] = 0

        # Filter downward radiation for snow obstruction during daytime
        id_sub = (df['rad_shortwave_up_CNR4'] >
                  (0.85 * df['rad_shortwave_down_CNR4'])) \
                  & (df['rad_shortwave_up_CNR4'] > 25*0.85)
        df.loc[id_sub,'rad_shortwave_down_CNR4'] = np.nan
        df.loc[id_sub,'rad_longwave_down_CNR4'] = np.nan

        # Cap upward shortwave outliers according to a median rolling albedo
        id_albedo = (df['rad_shortwave_down_CNR4'] > 25) & \
            (df['rad_shortwave_down_CNR4'] > df['rad_shortwave_up_CNR4'])
        rolling_albedo = (
            df.loc[id_albedo,'rad_shortwave_up_CNR4'].rolling(
                window=48*2,min_periods=12).median() \
                / df.loc[id_albedo,'rad_shortwave_down_CNR4'].rolling(
                    window=48*2,min_periods=12).median()
                ).interpolate()
        id_sub = (df['rad_shortwave_up_CNR4'] >
                  (0.85 * df['rad_shortwave_down_CNR4']))
        df.loc[id_sub,'rad_shortwave_up_CNR4'] = \
            df.loc[id_sub,'rad_shortwave_down_CNR4'] * \
                rolling_albedo[id_sub]

        # Recompute albedo
        id_daylight = df['rad_shortwave_down_CNR4'] > 25
        df['albedo_CNR4'] = np.nan
        df.loc[id_daylight, 'albedo_CNR4'] = \
            df.loc[id_daylight,'rad_shortwave_up_CNR4'] \
                / df.loc[id_daylight,'rad_shortwave_down_CNR4']

        # Recompute net radiation
        df['rad_net_CNR4'] = \
            df['rad_shortwave_down_CNR4'] + df['rad_longwave_down_CNR4'] \
                - df['rad_shortwave_up_CNR4'] - df['rad_longwave_up_CNR4']

    #####################
    ### Ground fluxes ###
    #####################

    if stationName == 'Foret_sol':
        for iVar in df.columns:
            if iVar != 'timestamp':
                # Very permissive dispiking
                id_spikes = detect_spikes(df, iVar, 48, 250, False)
                df.loc[id_spikes,iVar] = np.nan

    #######################
    ### Air temperature ###
    #######################

    if stationName in ['Berge','Foret_ouest','Foret_est','Reservoir']:

        temperature_vars = [var for var in df.columns if 'air_temp' in var]
        temperature_logger_vars = [var for var in df.columns if 'air_temp_CR' in var]
        no_filtering_vars = ['air_temp_spikes', 'air_temp_dewPoint'] + temperature_logger_vars
        kelvin_vars = [var for var in temperature_vars if df[var].median() > 100]

        median_temperature = np.nanmedian(
            np.concatenate(
                [np.expand_dims(df['air_temp'].values-273.15, axis=1),
                df[temperature_logger_vars].values], axis=1), axis=1)

        for i_temp_var in temperature_vars:
            if i_temp_var in no_filtering_vars:
                continue
            else:
                if i_temp_var in kelvin_vars:
                    id_outlier = np.abs(
                        median_temperature - df[i_temp_var] + 273.15)  > 10
                else:
                    id_outlier = np.abs(
                        median_temperature - df[i_temp_var])  > 10
                df.loc[id_outlier,i_temp_var] = np.nan

    ########################
    ### Turbulent Fluxes ###
    ########################

    for iVar in station_infos[stationName]['flux_vars']:

        # Remove unplausible values
        id_band = bandpass_filter(df, iVar)
        df.loc[id_band,iVar] = np.nan

        if stationName in ['Berge','Foret_ouest','Foret_est','Reservoir']:

            # Remove timestep where rain is measured
            if 'precip_TB4' in df.columns:
                id_rain = df['precip_TB4'] > 0
            else:
                # Load proxy files until it contains rain information
                for iProx in station_infos[stationName]['proxy']:
                    df_proxy = pd.read_csv(
                        finalOutDir + iProx + '.csv', low_memory=False)
                    if 'precip_TB4' in df_proxy.columns:
                        break
                id_rain = df_proxy['precip_TB4'] > 0
            df.loc[id_rain,iVar] = np.nan

            if 'strg' not in iVar:
                # Remove low quality time step (Mauder et Folken 2004)
                id_low_quality = df[iVar+'_qf'] == 2
                df.loc[id_low_quality,iVar] = np.nan
                strg_var = iVar.split('_')[0]
                df.loc[id_low_quality,strg_var+'_strg'] = np.nan

            # Energy balance violation (i.e., H+λE > 5Rn). Only for forested stations
            if stationName in ['Foret_ouest','Foret_est']:
                if 'rad_net_CNR4' in df.columns:
                    id_balance = \
                        (np.abs(df['H'] + df['LE']) > np.abs(5 * df['rad_net_CNR4'])) \
                            & (df['rad_net_CNR4'] > 50)
                else:
                    # Load proxy files until it contains radiation information
                    for iProx in station_infos[stationName]['proxy']:
                        df_proxy = pd.read_csv(
                            finalOutDir + iProx + '.csv', low_memory=False)
                        if 'rad_net_CNR4' in df_proxy.columns:
                            break
                    id_balance = \
                        (np.abs(df['H'] + df['LE']) > np.abs(5 * df_proxy['rad_net_CNR4'])) \
                            & (df_proxy['rad_net_CNR4'] > 50)
                df.loc[id_balance,iVar] = np.nan

            # Remove flux below the friction velocity threshold for carbon
            if iVar in ['CO2_flux', 'CH4_flux']:
                id_fric_vel = find_friction_vel_threshold(
                    df, iVar, 'air_temp_IRGASON')
                df.loc[id_fric_vel[0],iVar] = np.nan
                strg_var = iVar.split('_')[0]
                df.loc[id_fric_vel[0],strg_var+'_strg'] = np.nan

        # Identify spikes that should be discarded
        if (iVar in ['CO2_flux', 'CH4_flux']) | (stationName in ['Foret_ouest','Foret_est']):
            id_spikes = detect_spikes(df, iVar, 624, 7, True)
        else:
            id_spikes = detect_spikes(df, iVar, 624, 5, False)
        df.loc[id_spikes,iVar] = np.nan

    print('Done!\n')

    return df
