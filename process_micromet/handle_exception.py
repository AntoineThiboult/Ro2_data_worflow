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


        ###############################################################
        # Handle the faulty HMP45C replaced the 2018-12-19 10:00:00 ###
        ###############################################################

        date_hmp = '2018-10-19 15:00:00'
        id_change_HMP = df.index <= pd.to_datetime(date_hmp)
        df.loc[id_change_HMP,
               ['air_temp_HMP45C',
                'air_temp_max_HMP45C',
                'air_temp_min_HMP45C',
                'air_relhum_HMP45C']] = np.nan

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

        ###############################################################
        # Handle the faulty HMP45C replaced the 2019-08-30 15:00:00 ###
        ###############################################################

        date_hmp = '2019-08-30 15:00:00'
        id_change_HMP = df.index <= pd.to_datetime(date_hmp)
        df.loc[id_change_HMP,
               ['air_temp_HMP45C',
                'air_temp_max_HMP45C',
                'air_temp_min_HMP45C',
                'air_relhum_HMP45C']] = np.nan


        ###############################################################
        # Handle the faulty CS106 between 2020-10-23 and 2021-04-01 ###
        ###############################################################

        date_CS106_start = '2020-10-23 00:00:00'
        date_CS106_end   = '2021-04-01 00:00:00'
        id_CS106 = (
            ( pd.to_datetime(date_CS106_start) <= df.index ) &
            ( df.index < pd.to_datetime(date_CS106_end) )
            )
        df.loc[id_CS106,'air_press_CS106'] = np.nan


        ######################################
        # Handle erroneous Li7500 pressure ###
        ######################################
        date_air_press_IRGA = '2022-06-19 13:00:00'
        id_air_press_IRGA = df.index <= pd.to_datetime(date_air_press_IRGA)
        df.loc[id_air_press_IRGA, 'air_press_IRGASON'] = np.nan

        date_air_press = '2021-10-21 17:00:00'
        id_air_press = df.index <= pd.to_datetime(date_air_press)
        df.loc[id_air_press, 'air_press'] = np.nan


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


        #########################################################
        # Handle the faulty Li7500 that gave nonsensical values #
        #########################################################

        date_li75_start = '2022-03-26 02:00:00'
        date_li75_end   = '2022-06-19 14:00:00'
        id_li75 = (
            ( pd.to_datetime(date_li75_start) <= df.index ) &
            ( df.index < pd.to_datetime(date_li75_end) )
            )

        Li7500_vars = [
            # Campbell variables
            'air_density_IRGASON', 'air_temp_IRGASON', 'CO2_conc_mean_IRGASON',
            'CO2_conc_stdev_IRGASON', 'CO2_flux_H_wpl_IRGASON', 'CO2_flux_IRGASON',
            'CO2_flux_LE_wpl_IRGASON', 'CO2_flux_wpl_IRGASON', 'error_flag_IRGASON',
            'H_IRGASON', 'H2O_conc_mean_IRGASON', 'H2O_conc_stdev_IRGASON',
            'LE_IRGASON','CO2_density_IRGASON',
            # EddyPro variables
            'CO2_mixing_ratio', 'CO2_molar_density', 'CO2_mole_fraction',
            'air_heat_capacity', 'H2O_mixing_ratio', 'H2O_molar_density',
            'H2O_mole_fraction'
            ]
        df.loc[id_li75,Li7500_vars] = np.nan


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
            'Ta_cutoff_ouest': 20.460649803900587,
            'Ta_slope_ouest': 0.12034168706972999,
            'Ta_intercept_ouest': -1.837221855313137,
            'U_A_ouest': -2.4905471374599237,
            'U_B_ouest': 0.156252236457155,
            'U_C_ouest': 3.7158489232698457
            }

        # Air temperature based correction
        id_corr_temperature = id_li75 & ( df['air_temp'] <= 273.15 + correc_coeff['Ta_cutoff_ouest'] )
        df.loc[id_corr_temperature,'CO2_flux'] = df.loc[id_corr_temperature,'CO2_flux'] \
            - (correc_coeff['Ta_slope_ouest'] * (df.loc[id_corr_temperature,'air_temp']-273.15) + correc_coeff['Ta_intercept_ouest'])
        # Wind speed based correction
        df.loc[id_li75, 'CO2_flux'] = df.loc[id_li75, 'CO2_flux'] \
            - (correc_coeff['U_A_ouest'] * np.exp(correc_coeff['U_B_ouest'] * df.loc[id_li75,'wind_speed_sonic']) + correc_coeff['U_C_ouest'])


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
            'Ta_cutoff_est': 21.046866259070256,
            'Ta_slope_est': 0.12621863504666053,
            'Ta_intercept_est': -1.1281302401119822,
            'U_A_est': -0.2433512952999454,
            'U_B_est': 0.40040063387223357,
            'U_C_est': 0.8040098979405979
            }

        # Air temperature based correction
        id_corr_temperature = id_li75 & (df['air_temp'] <= 273.15 + correc_coeff['Ta_cutoff_est'])
        df.loc[id_corr_temperature,'CO2_flux'] = df.loc[id_corr_temperature,'CO2_flux'] \
            - (correc_coeff['Ta_slope_est'] * (df.loc[id_corr_temperature,'air_temp']-273.15) + correc_coeff['Ta_intercept_est'])
        # Wind speed based correction
        df.loc[id_li75, 'CO2_flux'] = df.loc[id_li75, 'CO2_flux'] \
            - (correc_coeff['U_A_est'] * np.exp(correc_coeff['U_B_est'] * df.loc[id_li75,'wind_speed_sonic']) + correc_coeff['U_C_est'])


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

        #####################################
        ### Handle non connected RMY05103 ###
        #####################################

        id_rmy = df.index < pd.to_datetime('2022-08-16 13:00:00')
        df.loc[id_rmy,'wind_speed_05103'] = np.nan

    return df