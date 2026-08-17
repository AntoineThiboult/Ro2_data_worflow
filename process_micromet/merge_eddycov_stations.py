# -*- coding: utf-8 -*-
import numpy as np
from utils import data_loader as dl, dataframe_manager as dfm

def compute_water_albedo(solar_angle):
    """
    Compute theorectical albedo based on solar angle

    Parameters
    ----------
    solar_angle: incident solar angle in degrees from horizon

    Returns
    -------
    water albedo
    """

    # Indices of refraction
    n_air = 1
    n_water = 4/3

    # Angles
    inc_angle = np.deg2rad(90-solar_angle)
    trans_angle = np.arcsin(n_air/n_water*np.sin(inc_angle))

    r_s = (np.sin(inc_angle-trans_angle) / np.sin(inc_angle+trans_angle))**2
    r_p = (np.tan(inc_angle-trans_angle) / np.tan(inc_angle+trans_angle))**2
    albedo = (r_s+r_p)/2

    return albedo

def merge_eddycov_stations(stationName, rawFileDir,
                           finalOutDir, miscDir, varNameExcelTab):
    """Merge :
        - the Romaine-2_reservoir_shore and Romaine-2_reservoir_raft stations
        together. The station raft has "priority", meaning that if some data
        is available on both stations, the data from raft is kept, while the
        one from shore is discarded.
        - the Bernard_spruce_moss_west and Bernard_spruce_moss_east stations
        together. Data from both stations are mutually exclusive in approx
        99.85% of the cases. In the case data is available from both stations,
        the average of the value is kept, except for the following variables
        where west is kept:
              - wind_dir_sonic

    Parameters
    ----------
    stationName: name of the station where modifications should be applied
    finalOutDir: path to the directory that contains the final .csv files
    varNameExcelTab: path and name of the Excel file that contains the
        variable description and their names

    Returns
    -------
    df: pandas DataFrame"""

    if stationName == 'Romaine-2_reservoir':

        # Import station data
        df = dl.csv(finalOutDir.joinpath('Romaine-2_reservoir_shore'))

        # Import and merge thermistors and precipitation
        df_therm = dl.csv(finalOutDir.joinpath('Romaine-2_reservoir_thermistor_chain'))
        df = dfm.merge(df,df_therm)
        df_precip = dl.csv(finalOutDir.joinpath('Romaine-2_reservoir_precip'))
        df = dfm.merge(df,df_precip)

        # Import raft station
        df_res = dl.csv(finalOutDir.joinpath('Romaine-2_reservoir_raft'))


        ###############################################################
        # Merge radiations
        ###############################################################

        rad_vars = ['rad_shortwave_down_CNR4','rad_longwave_down_CNR4',
                    'rad_shortwave_up_CNR4', 'rad_longwave_up_CNR4',
                    'albedo_CNR4','rad_net_CNR4']

        for iVar in rad_vars:

            if iVar == 'rad_shortwave_up_CNR4':
                # Substitutes shore values with raft when available or
                # with radiation computed from shore incoming rad and
                # raft computed albedo when not frozen. When frozen, take
                # radiation as shore.
                id_res_avail = ~df_res[iVar].isna()
                id_frozen_res = df['water_frozen_sfc'] == 1
                id_snow_gnd = df['albedo_CNR4'].rolling(
                    window=48*10,min_periods=6,center=True).mean() > 0.4

                # Compute theretical water albedo based on solar angle
                albedo_computed = compute_water_albedo(df['solar_angle'])

                # Replace with raft data
                df.loc[id_res_avail,iVar] = df_res.loc[id_res_avail,iVar]

                # Replace with computed shortwave up from water albedo and shore shortwave down
                # when raft no available, and raft not frozen
                id_replace = ~id_res_avail & ~id_frozen_res
                df.loc[id_replace,iVar] = df.loc[id_replace,'rad_shortwave_down_CNR4'] * \
                    albedo_computed[id_replace]

                # Replace with computed shortwave up from default melting snow albedo and
                # shore shortwave down when raft no available, frozen reservoir but no snow on shore
                id_replace = ~id_res_avail & id_frozen_res & ~id_snow_gnd
                df.loc[id_replace,iVar] = df.loc[id_replace,'rad_shortwave_down_CNR4'] * 0.4

                # Cap shortwave up with shortwave down
                id_replace =  (df[iVar]-df['rad_shortwave_down_CNR4']) > 0
                df.loc[id_replace,iVar] = df.loc[id_replace,'rad_shortwave_down_CNR4'] * \
                    albedo_computed[id_replace]

            elif iVar == 'rad_longwave_up_CNR4':
                # Substitutes shore values with raft when available or
                # with theoretical black body radiation calculated with water
                # surface temperature when reservoir not frozen
                id_res_avail = ~df_res[iVar].isna()
                id_frozen_res = df['water_frozen_sfc'] == 1
                id_snow_gnd = df['albedo_CNR4'].rolling(
                    window=48*10,min_periods=6,center=True).mean() > 0.4

                # Theoretical black body radiation
                rad_longwave_up_BB = 0.995*5.67e-8* \
                    (df['water_temp_0m0']+273.15)**4

                # Replace shore data with blackbody rad when reservoir not frozen
                df.loc[~id_frozen_res,iVar] = \
                    rad_longwave_up_BB[~id_frozen_res]

                # Filter abnormal values related to shore partial melt in spring
                df.loc[id_frozen_res,iVar] = np.min([
                    rad_longwave_up_BB[id_frozen_res],
                    df.loc[id_frozen_res,iVar]], axis=0)

                # Replace blackbody/shore radiation with raft CNR4 when available
                df.loc[id_res_avail,iVar] = df_res.loc[
                    id_res_avail,iVar]

            elif iVar == 'albedo_CNR4':
                # Recompute albedo
                id_daylight = df['rad_shortwave_down_CNR4'] > 25
                df['albedo_CNR4'] = np.nan
                df.loc[id_daylight, 'albedo_CNR4'] = \
                    df.loc[id_daylight,'rad_shortwave_up_CNR4'] \
                        / df.loc[id_daylight,'rad_shortwave_down_CNR4']

            elif iVar == 'rad_net_CNR4':
                # Recompute net radiation
                df['rad_net_CNR4'] = df['rad_shortwave_down_CNR4'] \
                    + df['rad_longwave_down_CNR4'] \
                        - df['rad_shortwave_up_CNR4'] \
                            - df['rad_longwave_up_CNR4']


        ###############################################################
        # Merge fluxes
        ###############################################################

        flux_vars = ['LE','H','CO2_flux']

        for iVar in flux_vars:
            # Replace fluxes when raft quality is better
            id_sub = df_res[iVar+'_qf'] < df[iVar+'_qf']
            df.loc[id_sub,iVar] = df_res.loc[id_sub,iVar]
            df.loc[id_sub,iVar+'_qf'] = df_res.loc[id_sub,iVar+'_qf']

            # Average flux when similar quality
            id_avg = df_res[iVar+'_qf'] == df[iVar+'_qf']
            df.loc[id_avg,iVar] = np.mean(
                [df_res.loc[id_avg,iVar], df.loc[id_avg,iVar]], axis=0)

            # Replace remaining fluxes
            id_sub = df[iVar].isna()
            df.loc[id_sub,iVar] = df_res.loc[id_sub,iVar]

        ###############################################################
        # Other variables
        ###############################################################

        # Merge shore and raft DataFrames for remaining variables giving
        # prioriy to raft data, but exclude flux and radiation variables
        excluded_vars = rad_vars + flux_vars
        df = dfm.merge(df_res.drop(excluded_vars, axis=1), df)


    elif stationName == 'Bernard_spruce_moss':

        # Import station data
        df = dl.csv(finalOutDir.joinpath('Bernard_spruce_moss_west'))

        # Import and merge Bernard spruce moss ground and precip
        df_bsm_ground = dl.csv(finalOutDir.joinpath('Bernard_spruce_moss_ground'))
        df = dfm.merge(df,df_bsm_ground)
        df_precip = dl.csv(finalOutDir.joinpath('Bernard_spruce_moss_precip'))
        df = dfm.merge(df,df_precip)

        # Import and merge Bernard spruce moss east
        df_bsm_east = dl.csv(finalOutDir.joinpath('Bernard_spruce_moss_east'))

        ###############################################################
        # Merge fluxes
        ###############################################################

        flux_vars = ['LE','H','CO2_flux','CH4_flux']
        strg_vars = {'LE':'LE_strg', 'H':'H_strg', 'CO2_flux':'CO2_strg', 'CH4_flux':'CH4_strg'}

        for iVar in flux_vars:
            # Index for substitution when quality flag of east is better
            id_sub = df_bsm_east[iVar+'_qf'] < df[iVar+'_qf']
            # Index for averaging when quality flag of east and west are the same
            id_avg = df_bsm_east[iVar+'_qf'] == df[iVar+'_qf']

            # Replace fluxes and storage when east quality is better
            df.loc[id_sub,iVar] = df_bsm_east.loc[id_sub,iVar]
            df.loc[id_sub,strg_vars[iVar]] = df_bsm_east.loc[id_sub,strg_vars[iVar]]
            df.loc[id_sub,iVar+'_qf'] = df_bsm_east.loc[id_sub,iVar+'_qf']

            # Average flux and storage when similar quality
            df.loc[id_avg,iVar] = np.mean(
                [df_bsm_east.loc[id_avg,iVar], df.loc[id_avg,iVar]], axis=0)

        ###############################################################
        # Other variables
        ###############################################################

        # Merge Bernard spruce moss west and east DataFrames for remaining variables
        # giving prioriy to west data, but exclude flux variables
        df = dfm.merge(df,df_bsm_east.drop(flux_vars,axis=1))


    elif stationName == 'Bernard_lake':

        ###############################################################
        # Other variables
        ###############################################################

        # Import Bernard lake outcrop data
        df = dl.csv(finalOutDir.joinpath('Bernard_lake_outcrop'))

        # Import and merge thermistors and precipitation
        df_therm = dl.csv(finalOutDir.joinpath('Bernard_lake_thermistor_chain'))
        df = dfm.merge(df,df_therm)
        df_precip = dl.csv(finalOutDir.joinpath('Bernard_spruce_moss_precip'))
        df = dfm.merge(df,df_precip)

    return df
