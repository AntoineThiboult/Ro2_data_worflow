# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import warnings
warnings.simplefilter('ignore', UserWarning)

def merge_slow_csv(dates,station_name,ascii_dir):
    """Merge slow csv data. Create a new column if the variable does not exist
    yet, or append if it does.

    Parameters
    ----------
    dates : dictionnary that contains a 'start' and 'end' dates
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    station_name: name of the station
    ascii_dir: path to the directory that contains the .csv files

    Returns
    -------
    slow_df: pandas DataFrame that contains all slow variables for the entire
        measurement period
    """

    print(f'Start merging slow data for station "{station_name}"...\n')

    src_dir = Path(ascii_dir).joinpath(station_name)
    slow_df = pd.DataFrame(index = pd.date_range(
        start=dates['start'], end=dates['end'], freq='30min') )

    # Slow data
    slow_files = list(src_dir.glob('*slow.csv'))
    slow_df = merge_dataframes(slow_df, slow_files)

    # Slow data 2
    slow_files2 = list(src_dir.glob('*slow2.csv'))
    slow_df = merge_dataframes(slow_df, slow_files2)

    # Converts columns to float when possible
    for iColumn in slow_df.columns.difference(['TIMESTAMP']):
        try:
            slow_df.loc[:,iColumn] = slow_df.loc[:,iColumn].astype(float)
        except:
            warnings.warn(f'Could not convert {iColumn} to float')

    print('Done!')

    return slow_df


def merge_dataframes(df_ref, file_list, merge_col='TIMESTAMP',
                     preserve_index=True, verbose=True):
    """
    Combine dataframes. Add the columns to df_ref if they are present in the
    files to merge.
    May preserve index (preserve_index=True) or add extra index to df_ref
    (preserve_index=False) if contained in files.

    Parameters
    ----------
    df_ref : Reference Pandas Dataframe
    file_list : List of files path to merge
    merge_col: String, optional.
        Name of the column used to merge Dataframes
    preserve_index : Bool, optional
        If true, the index of df_ref is preserved.
        If false, additional index will be added if they are contained in the
        files to merge.
    verbose: Bool, optional
        Use tqdm to display progress

    Returns
    -------
    df_ref : Pandas Datagrame
        Contains merged files
    """

    for i_slow_file in tqdm(file_list, disable=not verbose):
        try:
            tmp_df = pd.read_csv(i_slow_file, sep=',',skiprows=[0,2,3], low_memory=False)
            tmp_df = tmp_df.drop_duplicates(subset='TIMESTAMP', keep='last')
            tmp_df.index = pd.to_datetime(tmp_df['TIMESTAMP'])

            if preserve_index:
                tmp_df = tmp_df[tmp_df.index.isin(df_ref.index)]
            if preserve_index & (len(tmp_df.index) == 0):
                warnings.warn(f'File {i_slow_file} has no matching index')
                continue

            df_ref = df_ref.combine_first(tmp_df)
        except Exception as e:
            warnings.warn(f'An unexpected error occurred while processing file {i_slow_file}: {e}')

    return df_ref