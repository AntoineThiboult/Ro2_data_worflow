# -*- coding: utf-8 -*-
import os
import pandas as pd
import re

#TODO: check error message
# C:\Users\anthi182\Documents\GitHub\Ro2_data_worflow\process_micromet\merge_slow_csv.py:35:
# PerformanceWarning: DataFrame is highly fragmented.  This is usually the result of calling `frame.insert` many times, which has poor performance.  Consider joining all columns at once using pd.concat(axis=1) instead. To get a de-fragmented frame, use `newframe = frame.copy()`
# slow_df.loc[id_slow_df_in_tmp_df,iVar] = \

def merge_slow_csv(dates,stationName,asciiOutDir):
    """Merge slow csv data. Create a new column if the variable does not exist
    yet, or append if it does.

    Parameters
    ----------
    dates : dictionnary that contains a 'start' and 'end' dates
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    stationName: name of the station
    asciiOutDir: path to the directory that contains the .csv files

    Returns
    -------
    slow_df: pandas DataFrame that contains all slow variables for the entire
        measurement period
    """

    print('Start merging slow data for station:', stationName, '...', end='\r')
    # Module to merge same type of slow data together
    def merge_slow_data(slow_df, slow_files):

        for iSlow in slow_files:
            tmp_df = pd.read_csv(os.path.join(asciiOutDir,stationName,iSlow), sep=',',skiprows=[0,2,3], low_memory=False)
            tmp_df['TIMESTAMP'] = pd.to_datetime(tmp_df['TIMESTAMP'])
            tmp_df = tmp_df.drop_duplicates(subset='TIMESTAMP', keep='last')
            id_tmp_in_slow_df = tmp_df['TIMESTAMP'].isin(slow_df['TIMESTAMP'])
            id_slow_df_in_tmp_df = slow_df['TIMESTAMP'].isin(tmp_df['TIMESTAMP'])
            col_names = tmp_df.columns[tmp_df.columns!='TIMESTAMP']
            for iVar in col_names:
                slow_df.loc[id_slow_df_in_tmp_df,iVar] = \
                    tmp_df.loc[id_tmp_in_slow_df,iVar].values
        return slow_df


    # List all slow csv files and merge them together
    fileInDir = os.listdir(os.path.join(asciiOutDir,stationName))
    fileInDir.sort()

    # Slow data
    slow_files = [s for s in fileInDir if re.match('.*slow\.csv', s)]
    slow_df = pd.DataFrame(
        {'TIMESTAMP': pd.date_range(
            start=dates['start'],end=dates['end'],freq='30min')})
    slow_df = merge_slow_data(slow_df, slow_files)

    # Slow data 2
    slow_files2 = [s for s in fileInDir if re.match('.*slow2\.csv', s)]
    if slow_files2:
        slow_df2 = pd.DataFrame(
            {'TIMESTAMP': pd.date_range(
                start=dates['start'],end=dates['end'],freq='30min')})
        slow_df2 = merge_slow_data(slow_df2, slow_files2)
        nonDuplicatedColumns = slow_df2.columns[~slow_df2.columns.isin(slow_df.columns)]
        slow_df[nonDuplicatedColumns] = slow_df2[nonDuplicatedColumns]

    # Converts columns to float when possible
    for iColumn in slow_df.columns:
        try:
            slow_df.loc[:,iColumn] = slow_df.loc[:,iColumn].astype(float)
        except:
            print('Could not convert {} to float'.format(iColumn))

    print('Done!')

    return slow_df