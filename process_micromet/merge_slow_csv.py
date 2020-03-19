# -*- coding: utf-8 -*-
import os
import pandas as pd
import re

def merge_slow_csv(stationName,asciiOutDir):
    """Merge slow csv data. Create a new column if the variable does not exist
    yet, or append if it does.

    Parameters
    ----------
    stationName: name of the station
    asciiOutDir: path to the directory that contains the .csv files

    Returns
    -------
    slow_df: pandas DataFrame that contains all slow variables for the entire
        measurement period
    """

    print('Start merging slow data for station:', stationName, '...', end='\r')
    # Module to merge same type of slow data together
    def merge_slow_data(slow_df, slowList):

        for iSlow in slowList:
            tmp_df = pd.read_csv(os.path.join(asciiOutDir,stationName,iSlow), sep=',',skiprows=[0,2,3], low_memory=False)
            slow_df = slow_df.append(tmp_df)
        slow_df = slow_df.drop_duplicates(subset='TIMESTAMP', keep='last')

        return slow_df

    # List all slow csv files and merge them together
    fileInDir = os.listdir(os.path.join(asciiOutDir,stationName))
    fileInDir.sort()

    # Slow data
    slowList = [s for s in fileInDir if re.match('.*slow\.csv', s)]
    slow_df = pd.DataFrame()
    slow_df = merge_slow_data(slow_df, slowList)

    # Slow data 2
    slowList2 = [s for s in fileInDir if re.match('.*slow2\.csv', s)]
    if slowList2:
        slow_df2 = pd.DataFrame()
        slow_df2 = merge_slow_data(slow_df2, slowList2)
        nonDuplicatedColumns = slow_df2.columns[~slow_df2.columns.isin(slow_df.columns)].append(pd.Index(['TIMESTAMP']))
        slow_df = slow_df.merge(slow_df2[nonDuplicatedColumns], how='left', on='TIMESTAMP')

    # Create the TIMESTAMP column that will be used for merging with other df
    slow_df['TIMESTAMP'] = pd.to_datetime(slow_df['TIMESTAMP'])

    # Converts columns to float when possible
    for iColumn in slow_df.columns:
        try:
            slow_df.loc[:,iColumn] = slow_df.loc[:,iColumn].astype(float)
        except:
            print('Could not convert {} to float'.format(iColumn))


    print('Done!')

    return slow_df