# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from process_micromet import detect_spikes

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


    if stationName == 'Foret_ouest':
        # Handle the issue with the faulty CNR1 temperature probe
        # Take the CSAT temperature as proxy

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

    return df