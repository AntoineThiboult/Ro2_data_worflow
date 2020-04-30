# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import os

def merge_hq_reservoir(dates, extDataDir, mergedCsvOutDir):
    """ Merge and format streamflow and level data provided by Hydro-Québec

    The external data directory should contain the following two files
    extDataDir
    |-- HQ_débits.xlsx
    |-- HQ_niveau_réservoir.xlsx"

    Parameters
    ----------
    dates: dictionnary that contains a 'start' and 'end' key.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    rawFileDir: path to the directory that contains the .xlsx files
    mergedCsvOutDir: path to the directory that contains final .csv files

    Returns
    -------
    None.

    """


    print('Start merging Hydro-Quebec reservoir data...')

    # Initialize DataFrame
    df = pd.DataFrame( index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min') )


    ##############################
    ### Handle streamflow data ###
    ##############################

    # Load gap filling config file
    xlsFile = pd.ExcelFile(extDataDir + 'HQ_débits.xlsx')
    df_flow = pd.read_excel(xlsFile,'2020-022_Débits',usecols="A,D,E",skiprows=16,header=None)

    # Clean data
    df_flow.columns = ['timestamp', 'turbined_flow', 'released_flow']
    df_flow.index = df_flow['timestamp']

    # Drop duplicated indices
    df_flow = df_flow.loc[~df_flow.index.duplicated(keep='first')]

    # Find common indices
    idDates_RecInRef = df_flow.index.isin(df.index)
    idDates_RefInRec = df.index.isin(df_flow.index)

    # Fill data
    for iVar in df_flow.columns:
        df.loc[idDates_RefInRec,iVar] = df_flow.loc[idDates_RecInRef,iVar]


    ###################################
    ### Handle reservoir level data ###
    ###################################

    # Load gap filling config file
    xlsFile = pd.ExcelFile(extDataDir + 'HQ_niveau_réservoir.xlsx')
    df_level = pd.read_excel(xlsFile,'horaire',skiprows=16,header=None)

    # Clean data
    df_level.columns = ['timestamp', 'level']
    df_level.index = df_level['timestamp']

    # Drop duplicated indices
    df_level = df_level.loc[~df_level.index.duplicated(keep='first')]

    # Find common indices
    idDates_RecInRef = df_level.index.isin(df.index)
    idDates_RefInRec = df.index.isin(df_level.index)

    # Fill data
    df.loc[idDates_RefInRec,'level'] = df_level.loc[idDates_RecInRef,'level']

    # Fill missing steps
    df = df.interpolate(method='linear')
    df['timestamp'] = df.index

    # Save file
    df.to_csv(os.path.join(mergedCsvOutDir,'HQ_reservoir.csv'), index=False)
    print('Done!')
