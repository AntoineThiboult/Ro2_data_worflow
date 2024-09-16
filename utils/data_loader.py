# -*- coding: utf-8 -*-
"""
Created on Sat Dec 30 17:47:54 2023

@author: ANTHI182
"""
from pathlib import Path
import yaml
import pandas as pd


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


def toa5_file(file, sep=',', skiprows=[0,2,3], index_col='TIMESTAMP', drop_duplicates=True):
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
    df.index = pd.to_datetime(df.index)
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
        Path to the EddyPro full output file
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