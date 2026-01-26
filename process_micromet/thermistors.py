# -*- coding: utf-8 -*-
import warnings
import re
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn import linear_model
from process_micromet import ml_utils as ml
from utils import data_loader as dl



def list_data(station_name, raw_file_dir):
    """
    Search *.csv thermistors data for a given station.

    Parameters
    ----------
    station_name : String
        Name of the station
    raw_file_dir : String or Pathlib path
        Path to the root directory that contains thermistor data.

    Returns
    -------
    csv_files : List
        List of Pathlib path toward of the thermistors for a given station
    """

    root_path = Path(raw_file_dir)
    csv_files = []

    for subfolder in root_path.rglob(f'*{station_name}*'):
        for file in subfolder.glob('*.csv'):
            csv_files.append(file)

    return csv_files


def merge(dates, csv_files):
    """ Merge data collected by the thermistors (TidBit and U20 sensors).
    The depth of the sensor should be indicated in the name of the file
    following the first underscore. Ex: mystation_12.5_YYYYMMDD.csv

    Parameters
    ----------
    dates: dictionnary that contains a 'start' and 'end' key to indicates the
        period range to EddyPro.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    csv_files : List
        List of Pathlib path toward of the thermistors for a given station

    Returns
    -------
    df: Pandas dataframe
        Thermistor data. One column per variable and depth with datetime index
    retrieval_dates: Dictionary
        Dictionary that contains tuples where the first date indicate begining
        of the dataset, second date the end, for each data collection. The keys
        refer to the df columns.
    """

    df = pd.DataFrame( index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min') )

    # Routine to save the dates of thermistor data collection
    retrieval_dates = dict()

    def store_retrieval_dates(variable_name):
        if variable_name in retrieval_dates:
            # Append the new dates to the existing list
            retrieval_dates[variable_name].append(
                (df_tmp.index[0], df_tmp.index[-1])
                )
        else:
            # Create a new entry with the current dates
            retrieval_dates[variable_name] = [
                (df_tmp.index[0], df_tmp.index[-1])
                ]
        return retrieval_dates


    for counter, file in enumerate(tqdm(csv_files, desc='Merging thermistors')):

        if 'pro_oceanus' in file.stem:
            print('Pro oceanus sensors not implemented yet. DCO2 data skipped')
            continue

        # Get depth of the sensor
        match = re.search(r'_(\d+(\.\d+)?)', file.stem) # Get first number following first underscore
        if match:
            if '.' in match.group(1):
                depth_string = match.group(1).replace('.','m')
            else:
                depth_string = f'{match.group(1)}m0'
        else:
            warnings.warn(f'No depth found for {file.stem}. '
                          'Could not process the file.')
            continue

        # Get the number of rows to skip and check if there is a record number column
        with open(file, 'r', encoding='utf-8') as f:
            for l, line in enumerate(f, start=1):
                if any(keyword in line.lower() for keyword in ['date', 'day']):
                    skiprows = range(0,l-1)

                    # Split line into columns
                    parts = line.strip().split(',')

                    # Find index of the first column that contains 'date' or 'day'
                    for i, col in enumerate(parts):
                        if 'date' in col.lower() or 'day' in col.lower():
                            date_col = i
                            break
                    break

        # Load data file
        df_tmp = pd.read_csv(file, skiprows=skiprows)

        # Remove record number column if any
        df_tmp = df_tmp.iloc[:,date_col:]

        # Convert first col to datetime format and the rest to float
        df_tmp.index = pd.to_datetime(df_tmp.iloc[:,0])
        variables = ['temp', 'intensity', 'pres']
        data_cols = [element for element in df_tmp.columns if any(variables in element.lower() for variables in variables)]
        df_tmp[data_cols] = df_tmp[data_cols].apply(pd.to_numeric, errors='coerce')

        # Handle exceptions where loggers fail to increment time
        df_tmp = df_tmp.loc[ ~df_tmp.index.duplicated() ]

        # Find matchin dates
        idDates_RecInRef = df_tmp.index.isin(df.index)
        idDates_RefInRec = df.index.isin(df_tmp.index)

        for col in df_tmp.columns:
            if 'temp' in col.lower():
                df.loc[idDates_RefInRec, f'water_temp_{depth_string}'] = \
                    df_tmp.loc[idDates_RecInRef, col]
                store_retrieval_dates(f'water_temp_{depth_string}')

            if ('intensity' in col.lower()) or ('light' in col.lower()):
                df.loc[idDates_RefInRec, f'light_intensity_{depth_string}'] = \
                    df_tmp.loc[idDates_RecInRef, col]
                store_retrieval_dates(f'light_intensity_{depth_string}')

            if 'pres' in col.lower():
                df.loc[idDates_RefInRec, f'pressure_{depth_string}'] = \
                    df_tmp.loc[idDates_RecInRef, col]
                store_retrieval_dates(f'pressure_{depth_string}')

    return df, retrieval_dates


def filters(df, retrieval_dates=[]):
    """
    Filters thermistor data. Set data to nan 6 hours before and 12 hours after
    data collection to avoid air contamination. Set to nan when the chain is
    not straight enough (difference greater than 5 kPa from a two day rolling
    median) or when a temperature jump is observed (difference greater than
    2°C from a 6 hour rolling median)

    Parameters
    ----------
    df: Pandas dataframe
        Thermistor data. One column per variable and depth with datetime index
    retrieval_dates: Dictionary
        Dictionary that contains tuples where the first date indicate begining
        of the dataset, second date the end, for each data collection. The keys
        refer to the df columns. If not specified, no data will be removed
        around dates of data collection

    Returns
    -------
    df : Pandas dataframe
        Thermistor data filtered. One column per variable and depth with
        datetime index
    """

    # Remove 6 hours before and 12 hours after data collection
    # to avoid air contamination
    for variable in retrieval_dates:
        for date in retrieval_dates[variable]:

            buffer_before = pd.date_range(
                start=date[0].floor(freq='30min') - pd.Timedelta(hours=6),
                end=date[0].ceil(freq='30min'),
                freq='30min')
            for i in buffer_before:
                if i in df.index:
                    df.loc[i,variable] = np.nan

            buffer_after = pd.date_range(
                start=date[1].floor(freq='30min'),
                end=date[1].floor(freq='30min') + pd.Timedelta(hours=12),
                freq='30min')
            for i in buffer_after:
                if i in df.index:
                    df.loc[i,variable] = np.nan

    # Filter cases where the chain is not straight
    pressure_var = [var for var in df.columns if 'press' in var.lower()]
    rolling_press = df[pressure_var].rolling(
        window=96,center=True,min_periods=1).median()
    uplifts_index = df.index[
        (( rolling_press - df[pressure_var] ).abs() > 5).any(axis=1)
        ]
    df.loc[uplifts_index] = np.nan

    # Spiky temperture
    temp_var = [var for var in df.columns if 'temp' in var.lower()]
    for i_var in temp_var:
        rolling_temp = df[i_var].rolling(
            window=24,center=True,min_periods=1).median()
        spike_index = df.index[
            ( rolling_temp - df[i_var] ).abs() > 2 ]
        df.loc[spike_index,i_var] = np.nan

    return df


def average(df1, df2):
    """
    Average two DataFrame that contains thermistor data. Values in the result
    DataFrame is the average of the two DataFrames for the same date, column by
    column, unless there is no data available on one chain, in which case it
    is simply the data from the other chain.

    Parameters
    ----------
    df1, df2 : Pandas dataframe
        Thermistor data. One column per variable and depth with datetime index

    Returns
    -------
    df : Pandas dataframe
        Thermistor averaged data. One column per variable and depth with
        datetime index
    """

    # Concatenate the dataframes along the index
    combined_df = pd.concat([df1, df2], axis=0)
    # Calculate the mean of the columns
    df = combined_df.groupby(level=0).mean()
    return df


def gap_fill(df):
    """
    Gaps are filled with several technics applied in the following order
    1) Water surface temperature is derived from multidimensional linear
        regression performed on first meter temperatures
    2) Water temperature are derived from a linear regression with
        temperature above and below target.
    3) Remaining missing data are filled with yearly averaged temperature
        to which a linear detrending is applied to ensure reconnection with
        measurements at both ends of the gap.
    4) Remaining missing data are filled with linear interpolation

    Parameters
    ----------
    df: Pandas dataframe
        Thermistor data. One column per variable and depth with datetime index

    Returns
    -------
    df : Pandas DataFrame
        Dataframe that contains gapfilled temperature variable with the
        following format: water_temp_<d>m<d>_avg

    """
    def natural_sort(l):
        convert = lambda text: int(text) if text.isdigit() else text.lower()
        alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
        return sorted(l, key=alphanum_key)


    def pairwise(iterable):
        """
        Routine to iter over pairs
        s -> (s0, s1), (s2, s3), (s4, s5), ...
        """
        a = iter(iterable)
        return zip(a, a)


    def vertical_extrapolation(df, depths):
        # Perform a linear interpolation with the sensors located
        # above and below target, except for bottom and top sensors.
        for d, _ in enumerate(depths):
            if d == 0: # Surface interpolation
                target = depths[0]
                feature = depths[2:6]
            elif d == len(depths)-1: # Bottom interpolation
                target = depths[-1]
                feature = depths[-4:-2]
            else: # Mid water interpolation
                target = depths[d]
                feature = depths[d-1:d+2:2] # Takes depth above and below target

            regr = ml.train_lm(df[target].values, df[feature].values)
            if regr:
                pred = ml.predict_lm(regr, df[feature].values)
                mask = df[target].isna()
                df.loc[mask, target] = pred[mask]
        return df


    def distant_extrapolation(df, depths):
        # Perform a extrapolation with a random forest model. The input
        # features are chosen based on their availibility.

        for d, _ in enumerate(depths):
            target = depths[d]
            feature = depths[:d] + depths[d+1:]

            selec_features = []
            for f in feature:
                # Get the fraction of data available for each feature
                # when the target is NaN
                notna_frac = sum( df[f].notna() & df[target].isna() ) \
                    / df[target].isna().sum()
                if notna_frac > 0.8: selec_features.append(f)

            if len(selec_features) <= 3:
                continue

            target_values = df[target].values
            feature_values = np.column_stack(
                (df[selec_features].values,
                 df[target].shift(1).values))

            scalerX, scalery, regr = ml.train_rf(target_values, feature_values)

            for i, index in enumerate(df.index[1:]):
                if ~np.isnan(df.loc[index,target]):
                    continue
                feature_values = np.append(
                    df.loc[df.index[i],selec_features].values,
                     df.loc[df.index[i-1],target]).reshape(1,-1)
                if np.isnan(feature_values).any():
                    continue
                pred = ml.predict_rf(scalerX, scalery, regr, feature_values)
                df.loc[index, target] = pred

        return df


    def temporal_extrapolation(df, depths):
        # Perform a linear interpolation based on the same sensor to fill the
        # gaps. Maximum of 2 days of missing data.
        df[depths] = df[depths].interpolate(method='linear', limit=96)
        return df


    def yearly_avg_extrapolation(df, depths):
        # Replacement with yearly averaged data corrected for shift
        for iVar in depths:

            if df[iVar].isna().sum() == 0:
                continue

            yearly_avg = df[iVar].groupby([
                df.index.month,
                df.index.day,
                df.index.hour,
                df.index.minute]).transform('mean')

            # Get starting and ending index of NaN chunks
            id_chunks = df.index[
                df[iVar].isna().astype(int).diff().abs() > 0 ]

            # if there is a NaN block at the begining of the time series
            if np.isnan(df.loc[df.index[0], iVar]):
                id_chunks = pd.Index(
                    [ df.index[0] + pd.Timedelta(minutes=30) ]
                    ).append(id_chunks)
            # if there is a NaN block at the end of the time series
            elif np.isnan(df.loc[df.index[-1], iVar]):
                id_chunks = id_chunks.append( pd.Index(
                    [ df.index[-1] ] ))

            # Loop of NaN chunks for yearly averaged filling
            for id_start, id_end in pairwise(id_chunks):

                # Move to last value before NaN
                id_start = id_start - pd.Timedelta(minutes=30)

                start_offset = df.loc[ id_start ,iVar ] - yearly_avg[id_start]
                end_offset = df.loc[id_end,iVar] - yearly_avg[id_end]

                if np.isnan(start_offset):
                    start_offset = 0
                if np.isnan(end_offset):
                    end_offset = 0

                replace_chunk = \
                    np.linspace(start_offset, end_offset,
                                (id_end - id_start) // pd.Timedelta(minutes=30) +1 ) \
                        + yearly_avg[id_start:id_end]

                df.loc[id_start:id_end,iVar] = replace_chunk

        return df


    # Performs gap filling
    for var_type in ['water_temp', 'light_intensity']:

        depths = [var for var in df.columns if var_type in var]
        depths = natural_sort(depths)

        df = vertical_extrapolation(df, depths)
        if var_type == 'water_temp': df = temporal_extrapolation(df, depths)
        # df = distant_extrapolation(df, depths) # To be replaced by some smarter algorithm
        df = yearly_avg_extrapolation(df, depths)

        # Last hope interpolation
        df[depths] = df[depths].interpolate(method='linear')

    return df

def add_ice_phenology(df, phenology_file):
    """
    Add ice phenology dates to dataframe as a new column 'water_frozen_sfc'
    where 1 indicates frozen surface, 0 open water

    Parameters
    ----------
    df : Pandas dataframe
        Dataframe to which ice phenology dates are added
    phenology_file : String or pathlib.Path
        Path to a ice phenology csv file

    Returns
    -------
    df : Pandas dataframe
        Dataframe that include ice phenology dates
    """

    df_icepheno = dl.ice_phenology(phenology_file)
    df['water_frozen_sfc'] = np.zeros((df.shape[0]))
    for index_df in df_icepheno.index:
        s = pd.to_datetime(df_icepheno.loc[index_df,'Freezeup'])
        e = pd.to_datetime(df_icepheno.loc[index_df,'Icemelt'])
        df.loc[s:e,'water_frozen_sfc'] = 1
    return df


def compute_energy_storage(df):
    """
    Compute energy storage in the water column in J/m2, between the surface up to the
    deepest temperature sensor and add it to the dataframe (Hw).

    Parameters
    ----------
    df : Pandas dataframe
        Pandas dataframe that contain water temperature formated as
        water_temp_XmY where XmY is the depth

    Returns
    -------
    df : Pandas dataframe
        Pandas dataframe to which the column Hw is added.

    """

    # Get the measurement depths
    depth_string = np.array([d for d in df.columns if 'water_temp' in d])
    pattern = re.compile(r"(\d+)m(\d)")
    depths = np.array([
        float(f"{match.group(1)}.{match.group(2)}")
        for col in depth_string
        if (match := pattern.search(col))
        ])

    # Sort the depths increasingly
    sorting_index = np.argsort(depths)
    depth_string = depth_string[sorting_index]
    depths = depths[sorting_index]

    # Compute layer properties
    layer_thickness = np.diff(depths)

    # Compute temperatures at mid layer depths
    mid_layer_temp = (df[depth_string[:-1]].values + df[depth_string[1:]].values) / 2

    # Water specific heat capacity (J kg-1 K-1)
    Cp_water = 4184
    # Water density (kg m-3)
    rho = 1000

    # Compute energy contained in the water column
    df['Hw'] = np.sum(rho * Cp_water * mid_layer_temp * layer_thickness, axis=1)

    return df


def list_merge_filter(station, dates, raw_file_dir):
    """
    Higer level function that include the listing of the files to be merged,
    the merging of the files, and the filtering.

    Parameters
    ----------
    station_name : String
        Name of the station
    dates: dictionnary that contains a 'start' and 'end' key to indicates the
        period range to EddyPro.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    raw_file_dir : String or Pathlib path
        Path to the root directory that contains thermistor data.

    Returns
    -------
    df: Pandas dataframe
        Thermistor data. One column per variable and depth with datetime index

    """

    print(f'Start merging thermistors data for {station}')

    csv_files = list_data(station, raw_file_dir)
    df, retrieval_dates = merge(dates, csv_files)
    df = filters(df, retrieval_dates)
    return df

def save(df, station, destination_dir):

    df.to_csv(Path.joinpath(Path(destination_dir),f'{station}.csv'), index_label='timestamp')