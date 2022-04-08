# -*- coding: utf-8 -*-
"""
Created on Tue Dec 21 09:35:26 2021

@author: ANTHI182
"""
import numpy as np
import pandas as pd


def compute_storage_flux(stationName,df):

    #############################
    ### Begin handling fluxes ###
    #############################

    if stationName == 'Forest_stations':
        # Ground storage
        df['G'] = compute_ground_heat_flux(df)

        # Air column storage
        LE_strg, H_strg = compute_storage_below_instrument(df)
        df.loc[~np.isnan(LE_strg), 'LE_strg'] = LE_strg[~np.isnan(LE_strg)]
        df.loc[~np.isnan(H_strg), 'H_strg'] = H_strg[~np.isnan(H_strg)]

    if stationName == 'Water_stations':
        # Water column storage
        df['G'] = compute_water_column_heat_flux(df)

    return df

def compute_ground_heat_flux(df):
    """
    Script to compute ground storage and heat flux.
    Adapted from a Matlab script written by P.E. Isabelle

    Parameters
    ----------
    df : Pandas dataframe

    Returns
    -------
    G : Pandas series
        Ground heat flux

    References
    ----------
    T. R. Oke, Boundary Layer Climates
    Volumetric Water Content heat capacity of dry soil comes from Oke,
    Boundary Layer Climates, Table 2.1
    """

    # Variable subscript
    # _1 : -9cm
    # _2 : -3cm

    # Field constants
    z = [0.03, 0.09] # depth of sensor [m]
    dz = np.diff([0]+z)
    dt = 30*60 # duration of timestep [s]

    # Soil heat capacity is calculated by weighting dry soil and water obtained
    # from volumetric water content
    Cp_dry = 0.58e6 # J m-3 K-1 (peat soil)
    Cp_water = 4.184e6 # J m-3 K-1
    Cp = Cp_dry + Cp_water * df[['soil_watercontent_CS650_2',
                                 'soil_watercontent_CS650_1']].values

    # Calculate the temperature variations
    dtemp = df[['soil_temp_CS650_2','soil_temp_CS650_1']].diff().values

    # Calculate heat storage above soil heat flux plates.
    G_strg = dz*Cp*(dtemp/dt)

    # Calculate totale soil heat flux
    G = G_strg.sum(axis=1) + df['soil_heatflux_HFP01SC_1']

    return G



def compute_storage_below_instrument(df):
    """
    Script to compute ground heat flux.
    Adapted from a Matlab script written by P.E. Isabelle

    Parameters
    ----------
    df : Pandas dataframe
        Frame that contains a .

    Returns
    -------
    LE_strg: Pandas series
        Energy storage as latent heat
    H_strg: Pandas series
        Energy storage as latent heat

    References
    ----------
    """

    ################################
    ### Definitions of constants ###
    ################################

    # Duration of timestep [s]
    dt = 30*60

    # Specific gaz constant of dry air [J kg-1 K-1]
    Rd = 287.04

    # Specific heat of dry air at constant pressure [J kg-1 K-1]
    cpd = 1004.67


    ##########################
    ### Layers description ###
    ##########################

    # Elevation of pressure sensor
    z0 = 25

    # Measurement height [m]
    height_meas = [5, 10, 15, 20]

    # Thickness of each layer
    dz = np.diff(
        np.concatenate(
            ([0], height_meas[:-1] + np.diff(height_meas)/2, [height_meas[-1]])
        ))

    df_tmp = pd.DataFrame()

    #######################
    ### Compute storage ###
    #######################

    for iHeight in height_meas:


        air_temp = df['air_temp_'+str(iHeight)+'m'+'_HMP155']
        air_relhum = df['air_relhum_'+str(iHeight)+'m'+'_HMP155']

        # Correct air pressure for altitude
        air_press = df['air_press_CS106'] * 100 \
            * np.exp( -9.80665 * 0.0289644 * (iHeight-z0)
                     / ( 8.3144598 * (air_temp+273.15) ) )

        # Saturation vapour pressure [Pa]
        es = np.exp(
            23.3265 - 3802.7 / (air_temp + 273.15 ) - (472.68 / (air_temp + 273.15))**2
            )
        df_tmp['es_'+str(iHeight)] = es

        # Vapour pressure [Pa]
        e = air_relhum/100 * es
        df_tmp['e_'+str(iHeight)] = e

        # Specific humidity [kg/kg]
        q = (0.622 * e) / (101325 - e)
        df_tmp['q_'+str(iHeight)] = q

        # Humid air density matrix [kg m-3]
        rho_air = ( air_press/ (Rd* (air_temp+273.15)) ) * (1 - (0.378*e) / air_press)
        df_tmp['rho_air_'+str(iHeight)] = rho_air

        # Specific heat of the air [J kg-1 K-1]
        Cp = cpd*(1 + 0.84*q)
        df_tmp['Cp_'+str(iHeight)] = Cp

        # Latent heat of vaporization (Stull, 1988) [J kg-1]
        Lv = (2.501 - 0.00237 * air_temp) * 1e6
        df_tmp['Lv_'+str(iHeight)] = Lv

    # Create list of variables to treat them as matrix
    all_rho = ['rho_air_'+str(f) for f in height_meas]
    all_Cp = ['Cp_'+str(f) for f in height_meas]
    all_air_temp = ['air_temp_'+str(f)+'m'+'_HMP155' for f in height_meas]
    all_q = ['q_'+str(f) for f in height_meas]
    all_Lv = ['Lv_'+str(f) for f in height_meas]

    #######################
    ### Compute storage ###
    #######################

    H_strg = np.sum(df_tmp[all_rho].values * df_tmp[all_Cp].values *
        df[all_air_temp].diff().values * dz / dt, axis=1)

    LE_strg = np.sum(df_tmp[all_rho].values * df_tmp[all_Lv].values *
        df_tmp[all_q].diff().values * dz / dt, axis=1)

    return LE_strg, H_strg

def compute_water_column_heat_flux(df):
    """
    Script to compute reservoir storage and heat flux.
    Adapted from a Matlab script written by P.E. Isabelle

    Parameters
    ----------
    df : Pandas dataframe
        Frame that contains a .

    Returns
    -------
    None.

    References
    ----------
    T. R. Oke, Boundary Layer Climates

    """

    therm_depths = np.array(
        [0, 0.2, 0.4, 0.6, 0.8, 1, 1.4, 1.8, 2.2,
        2.6, 3, 4, 5, 6, 7, 8, 9, 10, 12.5, 15])

    # Duration of timestep
    dt = 30*60

    # Water specific heat capacity [J m-3 K-1]
    Cp_water = 4.184e6

    # Rolling window for noise reduction
    roll_window = 48*30

    # Names of temperature variables
    therm_depths_names_T1 = ['water_temp_{:d}m{:d}_Therm1'.format(
        int(f), int(np.round((f-np.fix(f))*10))) for f in therm_depths ]

    therm_depths_names_T2 = ['water_temp_{:d}m{:d}_Therm2'.format(
        int(f), int(np.round((f-np.fix(f))*10))) for f in therm_depths ]

    therm_depths_names = ['water_temp_{:d}m{:d}_Therm_avg'.format(
        int(f), int(np.round((f-np.fix(f))*10))) for f in therm_depths ]

    # Construct mean temperature dataset
    for count, iVar in enumerate(therm_depths_names):
        df[iVar] = df[[
            therm_depths_names_T1[count],therm_depths_names_T2[count]
            ]].mean(axis=1)

    # Rolling mean
    df[therm_depths_names] = \
        df[therm_depths_names].rolling(
            window=roll_window,center=True,min_periods=1).mean()

    # Thickness of layers [m]
    dz = np.diff(
        np.concatenate(
            ([0],
             therm_depths[:-1] + np.diff(therm_depths)/2,
             [therm_depths[-1]])
        ))

    G = np.nansum( Cp_water * df[therm_depths_names].diff()
                   * dz / dt, axis=1)

    return G