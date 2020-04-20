# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from process_micromet import bandpass_filter, detect_spikes, find_friction_vel_threshold, gapfill_mds

def gap_fill(stationName,df,mergedCsvOutDir,gapfillConfig):
    """Load gap filling config file, load additional data from other station
    if necessary, prepare data for gap filling, and call the specified gap
    filling algorithm

    Parameters
    ----------
    merged_df: pandas DataFrame that contains all variables -- slow and eddy
        covariance data -- for the entire measurement period
    mergedCsvOutDir: path to the directory that contains the final .csv files
    gapfillConfig: path to the directory that contains the gap filling
        configuration files

    Returns
    -------
    """
    # load gap filling config file
    xlsFile = pd.ExcelFile('./Config/GapFillingConfig/gapfilling_configuration.xlsx')
    df_config = pd.read_excel(xlsFile,stationName+'_MDS')

    # Check that all proxy vars are available for gap filling
    if 'Alternative_station' in df_config.columns:
        nAlternative_station = sum(~df_config['Alternative_station'].isna())
        for iAlternative_station in range(nAlternative_station):
            # Load alternative station data
            df_altStation = pd.read_csv(mergedCsvOutDir+df_config.loc[iAlternative_station,'Alternative_station']+'.csv')
            df_altStation = df_altStation[ ['timestamp',df_config.loc[iAlternative_station,'Proxy_vars_alternative_station']] ]
            df = df.merge(df_altStation, on='timestamp', how='left')

    # Handle special case of berge and reservoir
    if stationName in ['Berge', 'Reservoir']:
        df['water_temp_surface'] = df[['water_temp_0m0_Therm1','water_temp_0m0_Therm2',
                                       'water_temp_0m4_Therm1','water_temp_0m4_Therm2']].mean(axis=1)
        df['delta_Tair_Teau'] = abs(df['water_temp_surface'] - df['air_temp_IRGASON107probe'])

    # Loop over variable that will be gapfilled
    for iVar_to_fill in df_config.loc[~df_config['Vars_to_fill'].isna(),'Vars_to_fill']:

        # Create a duplicated column of the variable
        iVar_to_fill_trim = iVar_to_fill+'_trim'
        df[iVar_to_fill_trim] = df[iVar_to_fill]

        # Identify doubtful/missing flux that should be discarded
        id_band = bandpass_filter(df, iVar_to_fill)
        id_rain = df['precip_TB4'] > 0
        id_missing_nee = df.isna()[iVar_to_fill]
        id_low_quality = df[iVar_to_fill+'_qf'] == 2
        id_missing_flux = id_band | id_rain | id_missing_nee | id_low_quality
        df.loc[id_missing_flux,iVar_to_fill_trim] = np.nan

        # Remove flux below the friction velocity threshold for carbon
        if iVar_to_fill in ['CO2_flux', 'CH4_flux']:
            id_fric_vel = find_friction_vel_threshold(df, iVar_to_fill_trim, 'air_temp_IRGASON')
            df.loc[id_fric_vel[0],iVar_to_fill_trim] = np.nan

        # Identify spikes that should be discarded
        if (iVar_to_fill in ['CO2_flux', 'CH4_flux']) | (stationName in ['Foret_ouest']):
            id_spikes = detect_spikes(df, iVar_to_fill_trim, 624, 5, True)
        else:
            id_spikes = detect_spikes(df, iVar_to_fill_trim, 624, 5, False)
        df.loc[id_spikes,iVar_to_fill_trim] = np.nan

        # Perform gap filling
        print('\nStart gap filling for variable {:s} and station {:s}'.format(iVar_to_fill, stationName))
        df = gapfill_mds(df,iVar_to_fill,df_config,mergedCsvOutDir)

    return df

