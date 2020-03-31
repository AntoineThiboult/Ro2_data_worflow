# -*- coding: utf-8 -*-

import pandas as pd
from glob import glob
import os

def merge_natashquan(dates, rawFileDir, mergedCsvOutDir):
    """ Merge and format the data retrieved from Environment Canada.
    To retrieve data from Env Canada servers, check this link
    https://drive.google.com/drive/folders/1WJCDEU34c60IfOnG4rv5EPZ4IhhW9vZH

    Parameters
    ----------
    dates: dictionnary that contains a 'start' and 'end' key to indicates the
        period range to EddyPro.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    rawFileDir: path to the directory that contains the .xlsx files
    mergedCsvOutDir: path to the directory that contains final .csv files

    Returns
    -------
    None.

    """

    print('Start merging Environment Canada Natashquan data...')

    # List files to be merged
    listFile = glob(extDataDir + os.path.sep +
                    "Environnement_canada_natashquan" + "*.csv")

    # Initialize DataFrame
    df = pd.DataFrame( index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min') )

    # Name correspondance
    var_names = pd.DataFrame(
        {'env_can_var_names':['Temp (°C)','Dew Point Temp (°C)','Rel Hum (%)',
                              'Wind Dir (10s deg)','Wind Spd (km/h)',
                              'Stn Press (kPa)'],
         'db_var_names':['air_temp','air_temp_dewPoint','air_relativeHumidity',
                           'wind_dir','wind_speed','air_pressure']})

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
    df['wind_dir'] = df['wind_dir']*10

    # Linear interpolation when less than two days are missing
    df = df.interpolate(method='linear', axis=0)

    # Save file
    df.to_csv(os.path.join(mergedCsvOutDir,'Natashquan.csv'), index=False)
    print('Done!')