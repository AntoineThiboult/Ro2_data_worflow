# -*- coding: utf-8 -*-

import pandas as pd
import os

def merge_hq_meteo_station(dates, extDataDir, mergedCsvOutDir):
    """ Extract and merge the Hydro Quebec weather station ROMA0967.

    The directory should be organized as follow
    extDataDir
    |-- HQ_station_météo.xlsx

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

    print('Start extracting Hydro-Quebec weather station data...')

    # Initialize reference dataframe
    df = pd.DataFrame( index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min') )

    # Load HQ meteorological station file
    xlsFile = pd.ExcelFile(extDataDir + 'HQ_station_météo.xlsx')

    # Excel tabs of interest and corresponding database name
    sheet_list = ['Humidité_relative', 'Pression atm._077', 'Pre_cum_br_611', 'Neige_617',
                  'Temp. Air_078', 'DirVent.Moy60min2m_190', 'DirVent.Moy60min10m_182',
                  'VitVent.Moy60min2m_193', 'VitVent.Moy60min10m_185']

    db_var_list = ['air_relativeHumidity', 'air_press', 'precip', 'snow_cover',
                   'air_temp', 'wind_dir_2m', 'wind_dir_10m',
                   'wind_speed_2m', 'wind_speed_10m']

    # Extract data
    for iSheet in range(len(sheet_list)):

        # Load file and clean data
        df_tmp = pd.read_excel(xlsFile, sheet_name=sheet_list[iSheet], skiprows=20, usecols=[0,1])
        df_tmp.columns = ['timestamp',db_var_list[iSheet]]
        df_tmp.index = df_tmp['timestamp']

        # Handle raw precipitation data from the OTT1350 precipitometer
        if db_var_list[iSheet]=='precip':
            df_tmp['precip'] = df_tmp['precip'].diff()
            df_tmp.loc[ df_tmp['precip']<-10, 'precip'] = 0
            df_tmp['precip'] = df_tmp['precip'].cumsum().cummax().diff()

        # Fill DataFrame
        idDates_RecInRef = df_tmp.index.isin(df.index)
        idDates_RefInRec = df.index.isin(df_tmp.index)
        df.loc[idDates_RefInRec,db_var_list[iSheet]] = df_tmp.loc[idDates_RecInRef,db_var_list[iSheet]]

        if db_var_list[iSheet]=='precip':
            # Fill DataFrame
            df['precip'] = df['precip'].fillna(method='ffill', limit=1) # NaN is replaced with previous value
            df['precip'] = df['precip']/2
        else:
            df[db_var_list[iSheet]] = df[db_var_list[iSheet]].interpolate(limit=1)

    # Add the timestamp column
    df['timestamp'] = df.index

    df.to_csv(os.path.join(mergedCsvOutDir,'HQ_meteo_station.csv'), index=False)
    print('Done!')