# -*- coding: utf-8 -*-
import warnings
import numpy as np
import pandas as pd
from process_micromet.filters import spikes


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

    if stationName in ['Berge']:

        #############################################################################
        ### Handle the RMY 05103 counter clockwise wind direction reference frame ###
        #############################################################################

        df['wind_dir_05103'] = 360 - df['wind_dir_05103']


        ######################
        # Correct CO2 fluxes #
        ######################

        # Correct the CO2 fluxes bias that is related to the slow-response air
        # temperature probe used in the calculation of the CO2 density
        # Following :
        #
        # M. Helbig, K. Wischnewski, G.H. Gosselin, S.C. Biraud, I. Bogoev,
        # W.S. Chan, E.S. Euskirchen, A.J. Glenn, P.M. Marsh, W.L. Quinton,
        # O. Sonnentag, Addressing a systematic bias in carbon dioxide flux
        # measurements with the EC150 and the IRGASON open-path gas analyzers,
        # Agricultural and Forest Meteorology, Volumes 228–229, 2016
        #
        # Eric S. Russell, Victor Dziekan, Jinshu Chi, Sarah Waldo,
        # Shelley N. Pressley, Patrick O’Keeffe, Brian K. Lamb, Adjustment of
        # CO2 flux measurements due to the bias in the EC150 infrared gas
        # analyzer, Agricultural and Forest Meteorology, Volumes 276–277, 2019

        wT = df['H'] / (df['air_heat_capacity'] * df['air_density'])

        # First Irgason (SN:1061)
        date_irga_1_start = '2018-01-01 00:00:00' # installation date of Irga SN:1061
        date_irga_1_end   = '2021-10-20 09:00:00' # replacement date of Irga SN:1061
        id_irga_1 = (
            ( pd.to_datetime(date_irga_1_start) <= df.index ) &
            ( df.index < pd.to_datetime(date_irga_1_end) )
            )
        df.loc[id_irga_1, 'CO2_flux'] = df.loc[id_irga_1, 'CO2_flux'] \
            - (-6.69604501 * wT[id_irga_1] + -0.006874538127916616)

        # Second Irgason (SN:1061)
        date_irga_2_start = '2021-10-20 09:00:00' # installation date of Irga SN:1061
        date_irga_2_end   = '2022-06-13 15:30:00' # replacement date of Irga SN:1061
        id_irga_2 = (
            ( pd.to_datetime(date_irga_2_start) <= df.index ) &
            ( df.index < pd.to_datetime(date_irga_2_end) )
            )
        df.loc[id_irga_2, 'CO2_flux'] = df.loc[id_irga_2, 'CO2_flux'] \
            - (-13.74654197 * wT[id_irga_2] + -0.0004747748983443545)


    if stationName in ['Foret_ouest']:


        ######################################################################
        # Handle the RMY 05103 counter clockwise wind direction reference    #
        # frame that was introduced the 2019-06-08 10:30:00                  #
        ######################################################################

        date_rmy = '2019-06-08 10:30:00'
        id_rmy = df.index < pd.to_datetime(date_rmy)
        df.loc[id_rmy,'wind_dir_05103'] = 360 - df.loc[id_rmy,'wind_dir_05103']


        ######################################################################
        # Handle the issue with the faulty CNR1 temperature probe that was   #
        # replaced the 2021-06-11 14:30:00 by working CNR4                   #
        # Take the CSAT temperature as proxy                                 #
        ######################################################################

        date_cnr1 = '2021-06-11 14:00:00'
        id_cnr1 = df.index < pd.to_datetime(date_cnr1)

        # Despike IRGASON temperature time series
        id_dissim = np.abs(df['air_temp_IRGASON'] - df['air_temp_CR3000']) > 10
        df.loc[id_dissim, 'air_temp_IRGASON'] = np.nan
        id_spikes = spikes(df, 'air_temp_IRGASON')
        df.loc[id_spikes,'air_temp_IRGASON'] = np.nan
        T_proxy = df['air_temp_IRGASON'].interpolate() + 273.15

        # Correct longwave radiation for blackbody radiation
        df.loc[id_cnr1,'rad_longwave_down_CNR4'] = \
            df.loc[id_cnr1,'rad_longwave_down_CNR4'] \
                + (5.67e-8*T_proxy[id_cnr1]**4)
        df.loc[id_cnr1,'rad_longwave_up_CNR4'] = \
            df.loc[id_cnr1,'rad_longwave_up_CNR4'] \
                + (5.67e-8*T_proxy[id_cnr1]**4)
        df.loc[id_cnr1,'air_temp_CNR4'] = T_proxy[id_cnr1]


        ############################################
        # Handle the CO2 flux artifact related to  #
        # the use of the Li7500 during cold period #
        ############################################

        # See:
        # Addressing the influence of instrument surface heat exchange on the
        # measurements of CO2 flux from open-path gas analyzers. Burba et al. 2008

        date_li75 = '2022-10-22 12:00:00'
        id_li75 = df.index < pd.to_datetime(date_li75)

        correc_coeff = {
            'Ta_cutoff_ouest': 21.117778284570775,
            'Ta_slope_ouest': 0.11644845032658153,
            'Ta_intercept_ouest': -0.8221929625175574
            }

        # Air temperature based correction
        id_corr_temperature = id_li75 & ( df['air_temp'] <= 273.15 + correc_coeff['Ta_cutoff_ouest'] )
        df.loc[id_corr_temperature,'CO2_flux'] = df.loc[id_corr_temperature,'CO2_flux'] \
            - (correc_coeff['Ta_slope_ouest'] * (df.loc[id_corr_temperature,'air_temp']-273.15) + correc_coeff['Ta_intercept_ouest'])


    if stationName in ['Foret_est']:

        ############################################
        # Handle the CO2 flux artifact related to  #
        # the use of the Li7500 during cold period #
        ############################################

        # See:
        # Addressing the influence of instrument surface heat exchange on the
        # measurements of CO2 flux from open-path gas analyzers. Burba et al. 2008

        date_li75 = '2022-10-22 12:00:00'
        id_li75 = df.index < pd.to_datetime(date_li75)

        correc_coeff = {
            'Ta_cutoff_est': 18.802976760121066,
            'Ta_slope_est': 0.1607430897209991,
            'Ta_intercept_est': -1.5983324462698427
            }

        # Air temperature based correction
        id_corr_temperature = id_li75 & (df['air_temp'] <= 273.15 + correc_coeff['Ta_cutoff_est'])
        df.loc[id_corr_temperature,'CO2_flux'] = df.loc[id_corr_temperature,'CO2_flux'] \
            - (correc_coeff['Ta_slope_est'] * (df.loc[id_corr_temperature,'air_temp']-273.15) + correc_coeff['Ta_intercept_est'])


    if stationName in ['Reservoir']:

        ######################################################
        # Remove first and last days of each yearly campaign #
        ######################################################

        years=df.index.year.unique()
        for iyear in years:
            id_year = df.index.year == iyear
            id_start = df.loc[id_year,'air_temp_CR6'].first_valid_index()
            id_end = df.loc[id_year,'air_temp_CR6'].last_valid_index()
            if (id_start is not None) and (id_end is not None):
                if iyear == 2019: # raft stayed longer out of water
                    df.loc[id_start:id_start+pd.Timedelta(days=3),:] = np.nan
                else:
                    df.loc[id_start:id_start+pd.Timedelta(days=1),:] = np.nan
                df.loc[id_end-+pd.Timedelta(days=1):id_end,:] = np.nan

    if stationName in ['Bernard_lake']:

        #############################################################################
        ### Handle the RMY 05103 counter clockwise wind direction reference frame ###
        #############################################################################

        df['wind_dir_05103'] = 360 - df['wind_dir_05103']

    return df