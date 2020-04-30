# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from glob import glob
import os

def merge_natashquan(dates, extDataDir, mergedCsvOutDir):
    """ Merge and format the data retrieved from Environment Canada and the
    one collected manually on the Hipou Pourvoirie (over the years 2018 and
    2019 by Médéric Girard)

    To retrieve data from Env Canada servers, check this link
    https://drive.google.com/drive/folders/1WJCDEU34c60IfOnG4rv5EPZ4IhhW9vZH

    The directory should be organized as follow
    extDataDir
    |-- Environnement_canada_Natashquan/
    |-- Hipou_Natashquan/

    Parameters
    ----------
    dates: dictionnary that contains a 'start' and 'end' key.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    rawFileDir: path to the directory that contains the .xlsx files
    mergedCsvOutDir: path to the directory that contains final .csv files

    Returns
    -------
    None.

    """

    ######################################
    ### Handle Environment Canada data ###
    ######################################

    print('Start merging Natashquan data...')

    # List files to be merged
    listFile = glob(extDataDir + "Environnement_canada_Natashquan/" + "*.csv")

    # Initialize DataFrame
    df = pd.DataFrame( index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min') )

    # Name correspondance
    var_names = pd.DataFrame(
        {'env_can_var_names':['Temp (°C)','Dew Point Temp (°C)',
                              'Rel Hum (%)','Wind Dir (10s deg)',
                              'Wind Spd (km/h)','Stn Press (kPa)'],
         'db_var_names':['air_temp_EC','air_temp_dewPoint_EC',
                         'air_relativeHumidity_EC','wind_dir_EC',
                         'wind_speed_EC','air_pressure_EC']})

    # Fill DataFrame with records
    for iFile in listFile:

        # Load files
        df_tmp = pd.read_csv(iFile)
        df_tmp.index = pd.to_datetime(df_tmp['Date/Time'], yearfirst=True)

        # Find common indices
        idDates_RecInRef = df_tmp.index.isin(df.index)
        idDates_RefInRec = df.index.isin(df_tmp.index)

        # Merge
        for iVar in range(len(var_names)):
            df.loc[idDates_RefInRec,var_names.loc[iVar,'db_var_names']] = df_tmp.loc[idDates_RecInRef,var_names.loc[iVar,'env_can_var_names']]

    # Format data to database standards
    df['timestamp'] = df.index
    df['wind_dir_EC'] = df['wind_dir_EC']*10

    # Linear interpolation
    df = df.interpolate(method='linear', axis=0)


    ####################################
    ### Handle evaporation pans data ###
    ####################################

    # Load data
    df_pan = pd.read_csv(extDataDir + "Hipou_Natashquan/" + "bacs.csv",sep=";")

    # Prepare data
    df_pan['Date'] = pd.to_datetime(df_pan['Date'], format="%d/%m/%Y %H:%M")
    df_pan.index = df_pan['Date']
    df_pan['delta_m'] = df_pan[['delta_m1','delta_m2','delta_m3']].mean(axis=1)

    # Name correspondance
    var_names = pd.DataFrame(
        {'pan_var_names':['Date','delta_m','delta_m1','delta_m2','delta_m3'],
          'db_var_names':['timestamp','delta_m','delta_m1','delta_m2','delta_m3']})

    # Find common indices
    idDates_RecInRef = df_pan.index.isin(df.index)
    idDates_RefInRec = df.index.isin(df_pan.index)

    # Merge
    for iVar in range(len(var_names)):
        df.loc[idDates_RefInRec,var_names.loc[iVar,'db_var_names']] = df_pan.loc[idDates_RecInRef,var_names.loc[iVar,'pan_var_names']]



    ############################
    ### Handle meteo station ###
    ############################

    # Load data
    df_station = pd.read_csv(extDataDir + "Hipou_Natashquan/" +  "station.csv",sep=";")

    # Prepare data
    df_station['TIMESTAMP'] = pd.to_datetime(df_station['TIMESTAMP'], format="%d/%m/%Y %H:%M")
    df_station.index = df_station['TIMESTAMP']

    # Name correspondance
    var_names = pd.DataFrame(
        {'station_var_names':['TIMESTAMP', 'RECORD', 'BattV_Min', 'PTemp_C_Avg',
                              'SWUpper', 'SWLower','LWUpper', 'LWLower',
                              'CNR4TC_Avg', 'CNR4TK_Avg', 'RsNet_Avg', 'RlNet_Avg',
                              'Albedo_Avg', 'Rn', 'LWUpper_Avg', 'LWLower_Avg', 'Ta',
                              'RH','WS', 'WindDir', 'Tw'],
          'db_var_names':['timestamp', 'unused', 'batt_volt_min', 'air_temp_CR5000',
                        'rad_shortwave_down_CNR4', 'rad_shortwave_up_CNR4',
                        'rad_longwave_down_CNR4', 'rad_longwave_up_CNR4',
                        'air_temp_CNR4', 'unused', 'unused', 'unused','unused',
                        'unused', 'unused', 'unused', 'air_temp_HMP45C',
                        'air_relhum_HMP45C','wind_speed_05103', 'wind_dir_05103',
                        'water_temp_T107C']})

    # Resample 1min data to 30min
    df_station = df_station.resample(rule='30min', label='right').median()
    df_station['TIMESTAMP'] = df_station.index

    # Find common indices
    idDates_RecInRef = df_station.index.isin(df.index)
    idDates_RefInRec = df.index.isin(df_station.index)

    # Merge
    for iVar in range(len(var_names)):
        df.loc[idDates_RefInRec,var_names.loc[iVar,'db_var_names']] = df_station.loc[idDates_RecInRef,var_names.loc[iVar,'station_var_names']]

    # Add commonly used calculated variables
    df['es'] = 610.78 * np.exp(17.27*df['water_temp_T107C'] / (237.3 + df['water_temp_T107C']))
    df['ea'] = df['air_relhum_HMP45C'] / 100*610.78 * np.exp(17.27*df['air_temp_HMP45C'] / (237.3 + df['air_temp_HMP45C']))

    # Drop unnecessary columns
    df = df.drop('unused', axis=1)

    # Save file
    df.to_csv(os.path.join(mergedCsvOutDir,'Riviere.csv'), index=False)
    print('Done!')