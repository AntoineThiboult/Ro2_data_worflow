# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from process_micromet import detect_spikes
import warnings

def handle_exception(stationName, df, mergedCsvOutDir):
    """Handle exception in data processing. This function contains instructions
    for special cases, such as replacements of measurements data from one
    instrument to another.

    Parameters
    ----------
    df: pandas DataFrame
    stationName: name of the station where modifications should be applied

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

    if stationName == 'Foret_ouest':
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

        # Import foret_est data
        df_est = pd.read_csv(mergedCsvOutDir+'Foret_est'+'.csv', low_memory=False)
        for iVar in ['LE','H','CO2_flux']:
            df[iVar] = np.nanmean(pd.concat( [df[iVar], df_est[iVar]], axis=1), axis=1)

        ######################################################################
        # Handle the RMY 05103 counter clockwise wind direction reference
        # frame that was introduced the 2019-06-08 10:30:00
        ######################################################################

        id_change_progr = df[df['timestamp'].str.contains('2019-06-08 10:30:00')].index.values[0]
        df.loc[id_change_progr:,'wind_dir_05103'] = 360 - df.loc[id_change_progr:,'wind_dir_05103']
    return df