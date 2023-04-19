# -*- coding: utf-8 -*-
import warnings
import numpy as np
import pandas as pd
from process_micromet.filters import detect_spikes


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


        ###############################################################
        # Handle the faulty HMP45C replaced the 2018-12-19 10:00:00 ###
        ###############################################################

        try:
            id_change_HMP = df[df['timestamp']==pd.to_datetime(
                '2018-12-19 10:00:00')].index[0]
        except IndexError:
            id_change_HMP = df.shape[0]
        df.loc[0:id_change_HMP,
               ['air_temp_HMP45C',
                'air_temp_max_HMP45C',
                'air_temp_min_HMP45C',
                'air_relhum_HMP45C']] = np.nan



    if stationName in ['Foret_ouest']:

        ###############################################################
        # Handle the faulty HMP45C replaced the 2019-08-30 15:00:00 ###
        ###############################################################

        try:
            id_change_HMP = df[df['timestamp']==pd.to_datetime(
                '2019-08-30 15:00:00')].index[0]
        except IndexError:
            id_change_HMP = df.shape[0]
        df.loc[0:id_change_HMP,
               ['air_temp_HMP45C',
                'air_temp_max_HMP45C',
                'air_temp_min_HMP45C',
                'air_relhum_HMP45C']] = np.nan


        ###############################################################
        # Handle the faulty CS106 between 2020-10-23 and 2021-04-01 ###
        ###############################################################

        try:
            id_change_CS106_start = df[df['timestamp']==pd.to_datetime(
                '2020-10-23 00:00:00')].index[0]
            id_change_CS106_end = df[df['timestamp']==pd.to_datetime(
                '2021-04-01 00:00:00')].index[0]
            id_change_CS106 = np.arange(
                id_change_CS106_start,id_change_CS106_end)
        except IndexError:
            id_change_CS106 = []
        df.loc[id_change_CS106,'air_press_CS106'] = np.nan


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

    if stationName in ['Reservoir']:

        ######################################################
        # Remove first and last days of each yearly campaign #
        ######################################################

        years=df['timestamp'].dt.year.unique()
        col_rm = df.columns.drop('timestamp')
        for iyear in years:
            id_year = df['timestamp'].dt.year == iyear
            id_start = df.loc[id_year,'air_temp_CR6'].first_valid_index()
            id_end = df.loc[id_year,'air_temp_CR6'].last_valid_index()
            if (id_start is not None) & (id_end is not None):
                if iyear == 2019: # raft stayed longer out of water
                    df.loc[id_start:id_start+48*3,col_rm] = np.nan
                else:
                    df.loc[id_start:id_start+48,col_rm] = np.nan
                df.loc[id_end-48:id_end,col_rm] = np.nan

    return df