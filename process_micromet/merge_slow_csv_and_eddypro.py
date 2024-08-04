# -*- coding: utf-8 -*-

def merge_slow_csv_and_eddypro(stationName,slow_df,eddy_df):
    """Merge slow data and EddyPro data DataFrames

    Parameters
    ----------
    stationName: name of the station
    slow_df: pandas DataFrame that contains all slow variables for the entire
        measurement period
    eddy_df: pandas DataFrame that contains all eddy covariance variables
        processed by EddyPro for the entire measurement period

    Returns
    -------
    merged_df: pandas DataFrame that contains all variables -- slow and eddy
        covariance data -- for the entire measurement period
    """
    print('Start merging slow and Eddy Pro data for station:', stationName, '...', end='\r')

    # Merge and save
    merged_df = slow_df.combine_first(eddy_df)

    print('Done!')

    return merged_df


