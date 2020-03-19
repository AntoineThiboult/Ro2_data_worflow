# -*- coding: utf-8 -*-
import pandas as pd
from process_micromet import gapfill_mds

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

    for iVar_to_fill in df_config.loc[~df_config['Vars_to_fill'].isna(),'Vars_to_fill']:
        print('\nStart gap filling for variable {:s} and station {:s}'.format(iVar_to_fill, stationName))
        df = gapfill_mds(df,iVar_to_fill,df_config,mergedCsvOutDir)

    return df

