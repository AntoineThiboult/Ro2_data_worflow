# -*- coding: utf-8 -*-
"""
Created on Sat Dec 30 17:47:54 2023

@author: ANTHI182
"""
from pathlib import Path
import yaml
import pandas as pd
import struct
import datetime as dt


def yaml_file(path, file):
    """
    Load a YAML file

    Parameters
    ----------
    path : String
        Path to the file
    file : file name
        file name with or without extension

    Returns
    -------
    data : Dictionary
    """

    file = Path(file)
    if not file.suffix:
        file = file.with_suffix('.yml')

    data = yaml.safe_load(
        open(Path(path).joinpath(file)))

    return data


def toa5_file(file, sep=',', skiprows=[0,2,3], index_col='TIMESTAMP',
              drop_duplicates=True, datetime_format='mixed'):
    """
    Load Campbell Scientific TOA5 files, set the index as the time and rename
    it 'timestamp', convert data to float if possible, and remove duplicated
    timestamps.

    Parameters
    ----------
    file : String or pathlib.Path
        Path to the TOA5 file
    sep : String, optional
        Separator. The default is ','.
    skiprows : Array, optional
        Rows to skip when reading file. The default is [0,2,3].
    index_col : String or float, optional
        Column to use as index The default is 'TIMESTAMP'.
    drop_duplicates : Bool, optional
        Drop duplicated time index. The default is True

    Returns
    -------
    df : Pandas DataFrame
    """

    df = pd.read_csv(
        file,
        sep=sep,
        skiprows=skiprows,
        index_col=index_col,
        low_memory=False,
        na_values="NAN")
    df.index.name = df.index.name.lower()
    df.index = pd.to_datetime(df.index, format=datetime_format)
    if drop_duplicates:
        df = df[~df.index.duplicated(keep='last')]
    return df


def eddypro_fulloutput_file(file, sep=',', skiprows=[0,2], index_col=None, drop_duplicates=True):
    """
    Load Eddypro csv full output into a Pandas Dataframe, set the index as the
    time and rename it 'timestamp', convert data to float if possible, and
    remove duplicated timestamps.

    Parameters
    ----------
    file : String or pathlib.Path
        Path to the EddyPro full output file
    sep : String, optional
        Separator. The default is ','.
    skiprows : Array, optional
        Rows to skip when reading file. The default is [0,2,3].
    index_col : String or float, optional
        Column to use as index The default is None.
    drop_duplicates : Bool, optional
        Drop duplicated time index. The default is True

    Returns
    -------
    df : Pandas DataFrame

    """

    df = pd.read_csv(
        file,
        sep=sep,
        skiprows=skiprows,
        index_col=index_col,
        low_memory=False,
        na_values="NaN")

    df.index = pd.to_datetime(
        df['date']+ " " + df['time'],
        yearfirst=True)
    df.index.name = 'timestamp'
    if drop_duplicates:
        df = df[~df.index.duplicated(keep='last')]
    return df


def csv(file, index_col='timestamp'):
    """
    Load pipeline csv

    Parameters
    ----------
    file : String or pathlib.Path
        Path to a time series csv file
    index_col : String or float, optional
        Column to use as index The default is 'timestamp'.

    Returns
    -------
    df : Pandas DataFrame
    """

    file = Path(file)
    if not file.suffix:
        file = file.with_suffix('.csv')

    df = pd.read_csv(file, index_col=index_col)
    if index_col.lower() == 'timestamp':
        df.index = pd.to_datetime(df.index)
        df.index.name = 'timestamp'
    return df


def ice_phenology(file, make_time_series=True):
    """
    Load ice phenology files (pipeline non standard csv)

    Parameters
    ----------
    file : String or pathlib.Path
        Path to a ice phenology csv file

    Returns
    -------
    df : Pandas DataFrame
    """

    file = Path(file)
    if not file.suffix:
        file = file.with_suffix('.csv')
    df = pd.read_csv(file, low_memory=False)
    return df


def tob3_first_timestamp(file):
    """
    Extract the first data record timestamp from a TOB3 file.

    This function reads the 6-line ASCII header of a TOB3 file, then
    parses the first binary frame header to extract the initial
    SecNano timestamp. The timestamp is converted to a Python
    ``datetime.datetime`` using the Campbell Scientific epoch
    (1990-01-01). Returns None if the file is empty or truncated.

    Parameters
    ----------
    file : str or pathlib.Path
        Path to a TOB3 file.

    Returns
    -------
    first_timestamp : datetime.datetime
        Timestamp of the first data record in the file.
    """
    file = Path(file)

    # Check if file is empty
    try:
        if file.stat().st_size == 0:
            return None
    except FileNotFoundError:
        raise

    with open(file, "rb") as f:

        # Skip ASCII header lines
        for _ in range(6):
            line = f.readline()
            if not line:
                # File ends before end of header. The file is either
                # corrupted or do not match expected format
                return None

        # Skip the TOB3 12-byte header
        frame_header = f.read(12)

        # Ensure we have at least 8 bits
        # (4 bits for seconds, 4 bits for nano seconds)
        if len(frame_header) < 8:
            # Not enough data, file is probably corrupted or empty
            return None

        try:
            sec = struct.unpack('<L', frame_header[0:4])[0]
            nano = struct.unpack('<L', frame_header[4:8])[0]
        except struct.error:
            # Other
            return None

        epoch = dt.datetime(1990, 1, 1)
        first_timestamp = epoch + dt.timedelta(seconds=sec, microseconds=nano/1000)
    return first_timestamp


def tob3_header(file,clean=True):
    """
    Extract the 6-line header of a TOB3 file.

    Parameters
    ----------
    file : String or pathlib.Path
        Path to a TOB3 file
    clean : bool, optional (default=True)
        If True, return a cleaned list of lists:
        - split by commas
        - remove surrounding quotes and newline chars

    Returns
    -------
    header : list
        Raw or cleaned header

    """
    file = Path(file)

    # Check if file is empty
    try:
        if file.stat().st_size == 0:
            return None
    except FileNotFoundError:
        raise

    with open(file, "r", encoding="latin1") as f:
        header = []
        for _ in range(6):
            line = f.readline()
            if not line:
                # File ends before end of header. The file is either
                # corrupted or do not match expected format
                return None
            header.append(line)

    if not clean:
        return header

    # Clean each line
    cleaned_header = []
    for line in header:
        # Strip newline, split by commas
        parts = line.strip().split(",")
        # Remove surrounding quotes from each element
        parts = [p.strip().strip('"') for p in parts]
        cleaned_header.append(parts)

    return cleaned_header