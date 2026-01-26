# -*- coding: utf-8 -*-
"""
Created on Sat Dec 30 17:47:54 2023

@author: ANTHI182
"""
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import warnings
from . import data_loader as dl


warnings.simplefilter('ignore', UserWarning)


def create(dates,freq='30min',index_name='timestamp'):
    """
    Create a DataFrame with datetime index as specified by inputs.

    Parameters
    ----------
    dates : Dictionnary that contains a 'start' and 'end' dates
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    freq : String (or Timedelta, datetime.timedelta), optional
        Frequency strings can have multiples, e.g. ‘5h’, default '30min'
    index_name: String, optional
        Name of the index, default 'timestamp'

    Returns
    -------
    df : Pandas DataFrame with datetime index as specified by dates and freq
    """
    df = pd.DataFrame(
        index = pd.date_range(start=dates['start'], end=dates['end'], freq=freq)
        )
    df.index.name = index_name
    return df


def list_files(station_name,pattern,data_dir):
    """
    List files according to pattern

    Parameters
    ----------
    station_name : String
        Station name
    pattern : String
        Pattern to match
    data_dir : TYPE
        DESCRIPTION.

    Returns
    -------
    files : List
        List of files that matches patten in directory

    """
    src_dir = Path(data_dir).joinpath(station_name)
    files = list(src_dir.glob(pattern))
    return files


def merge_files(df, file_list, file_type, merge_col='timestamp',
                     preserve_index=True, verbose=True):
    """
    Merge list of csv files onto a reference DataFrame. Add the columns
    to df if they are present in the files to merge.
    May preserve index (preserve_index=True) or add extra index to df
    (preserve_index=False) if contained in files.

    Parameters
    ----------
    df : Reference Pandas Dataframe
    file_list : List of files path to merge
    merge_col: String, optional.
        Name of the column used to merge Dataframes. Will be used as index
    preserve_index : Bool, optional
        If true, the index of df is preserved.
        If false, additional index will be added if they are contained in the
        files to merge.
    verbose: Bool, optional
        Use tqdm to display progress

    Returns
    -------
    df : Pandas Datagrame
        Contains merged files
    """

    for i_file in tqdm(file_list, disable=not verbose, desc='Merging files'):
        try:
            # Load dataframe
            if file_type.lower() == 'toa5':
                tmp_df = dl.toa5_file(i_file)
            elif file_type.lower() == 'eddypro':
                tmp_df = dl.eddypro_fulloutput_file(i_file)

            # Merge
            if preserve_index:
                tmp_df = tmp_df[tmp_df.index.isin(df.index)]
            if preserve_index & (len(tmp_df.index) == 0):
                warnings.warn(f'File {i_file} has no matching index')
                continue
            df = df.combine_first(tmp_df)
        except Exception as e:
            warnings.warn(f'An unexpected error occurred while processing file {i_file}: {e}')

    return df


def merge(slow_df,eddy_df):
    """Merge slow data and EddyPro data DataFrames

    Parameters
    ----------
    slow_df: pandas DataFrame that contains all slow variables for the entire
        measurement period
    eddy_df: pandas DataFrame that contains all eddy covariance variables
        processed by EddyPro for the entire measurement period

    Returns
    -------
    merged_df: pandas DataFrame that contains all variables -- slow and eddy
        covariance data -- for the entire measurement period
    """

    # Merge and save
    merged_df = slow_df.combine_first(eddy_df)

    return merged_df


def save(df, dest_dir, file_name, index=True):
    """
    Save DataFrame to dest_dir with filename

    Parameters
    ----------
    df : Pandas DataFrame
    dest_dir : String or Path
    file_name : String
        File name with or without csv extension
    index : Bool, optional
        Saves the index or not. The default is True.

    Returns
    -------
    None.
    """
    if file_name.split('.')[-1] != 'csv':
        file_name = file_name + '.csv'
    df.to_csv(Path(dest_dir).joinpath(file_name),index=index)
