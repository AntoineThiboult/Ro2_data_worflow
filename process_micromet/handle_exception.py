# -*- coding: utf-8 -*-
import warnings
import numpy as np
import pandas as pd
from process_micromet import detect_spikes


def handle_exception(stationName, df):
    """Handle exception in data processing. This function contains instructions
    for special cases, such as replacements of measurements data from one
    instrument to another.

    Parameters
    ----------
    stationName: name of the station where modifications should be applied
    df: pandas DataFrame

    Returns
    -------
    df: pandas DataFrame"""

    # Ignore warnings caused by averaging nan
    warnings.filterwarnings("ignore")

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    if stationName in ['Berge']:

        #############################################################################
        ### Handle the RMY 05103 counter clockwise wind direction reference frame ###
        #############################################################################

        df['wind_dir_05103'] = 360 - df['wind_dir_05103']


    if stationName in ['Foret_ouest']:

        ######################################################################
        # Handle the RMY 05103 counter clockwise wind direction reference    #
        # frame that was introduced the 2019-06-08 10:30:00                  #
        ######################################################################

        try:
            id_change_progr = df[df['timestamp']==pd.to_datetime(
                '2019-06-08 10:30:00')].index[0]
        except IndexError:
            id_change_progr = df.shape[0]
        df.loc[0:id_change_progr,'wind_dir_05103'] = 360 - df.loc[0:id_change_progr,'wind_dir_05103']


    if stationName in ['Foret_ouest']:

        ######################################################################
        # Handle the issue with the faulty CNR1 temperature probe that was   #
        # replaced the 2021-06-11 14:30:00 by working CNR4                   #
        # Take the CSAT temperature as proxy                                 #
        ######################################################################

        # Despike IRGASON temperature time series
        id_dissim = np.abs(df['air_temp_IRGASON'] - df['air_temp_CR3000']) > 10
        df.loc[id_dissim, 'air_temp_IRGASON'] = np.nan
        id_spikes = detect_spikes(df, 'air_temp_IRGASON')
        df.loc[id_spikes,'air_temp_IRGASON'] = np.nan
        T_proxy = df['air_temp_IRGASON'].interpolate() + 273.15

        # Correct longwave radiation for blackbody radiation
        try:
            id_change_CNR4 = df[df['timestamp']==pd.to_datetime(
                '2021-06-11 14:00:00')].index[0]
        except IndexError:
            id_change_CNR4 = df.shape[0]

        df.loc[0:id_change_CNR4,'rad_longwave_down_CNR4'] = \
            df.loc[0:id_change_CNR4,'rad_longwave_down_CNR4'] \
                + (5.67e-8*T_proxy[0:id_change_CNR4]**4)
        df.loc[0:id_change_CNR4,'rad_longwave_up_CNR4'] = \
            df.loc[0:id_change_CNR4,'rad_longwave_up_CNR4'] \
                + (5.67e-8*T_proxy[0:id_change_CNR4]**4)
        df.loc[0:id_change_CNR4,'air_temp_CNR4'] = T_proxy[0:id_change_CNR4]

    return df