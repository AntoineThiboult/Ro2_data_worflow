# -*- coding: utf-8 -*-
import os
import re
import subprocess
import pandas as pd
from pathlib import Path
from utils import data_loader as dl
from tqdm import tqdm


def find_unconverted_files(station_name_raw, station_name_ascii,
                           bin_file_dir, csv_file_dir,):
    """
    Find Campbell Scientific binary TOB3 files (*.dat) located in `bin_file_dir`
    and determine which ones still need to be converted to CSV files in
    `csv_file_dir`.
    Once a TOB3 file is read, it will be stored in cache minimize running
    time for the next execution.

    The source directory containing the .dat files should be organized as
    follows:

        bin_file_dir/
        ├── YYYYMMDD/
        │   └── station_name_raw_YYYYMMDD/
        │       ├── foo.dat
        │       └── bar.dat
        ├── YYYYMMDD/
        │   └── station_name_raw_YYYYMMDD/
        │       ├── foo.dat
        │       └── bar.dat

    Issues are logged in the `convert_CSbinary_to_csv.log` file.

    Parameters
    ----------
    station_name_raw :Path or str
        Name of the station as stored in the raw data.
    station_name_ascii : Path or str
        Name of the station as stored in the ASCII (converted) data.
    bin_file_dir : Path or str
        Path to the directory that contains the Campbell Scientific binary
        files (.dat).
    csv_file_dir : Path or str
        Path to the directory that contains the converted CSV files.

        If False, only the folder date is checked: if the date present in the
        folder name already exists among the converted files, the entire folder
        is skipped without checking individual .dat files.

    Returns
    -------
    unconverted_files : list of pathlib.Path
        Paths to binary files that have not yet been converted.
    """

    # Open error log file
    logf = open(Path('.','Logs','csbinary_to_csv.log'), "a")

    # Cache file to store already processed files
    tob3_cache_file = Path(".", "Logs", f"{station_name_raw}_tob3_timestamps_cache.csv")
    ts_cache = load_cache(tob3_cache_file)
    new_cache_rows = []

    # Paths
    bin_file_dir = Path(bin_file_dir)
    csv_file_dir = Path(csv_file_dir).joinpath(station_name_ascii)

    # List files that are already converted
    csv_files = list_csv_files(csv_file_dir)
    csv_files_stems = {p.name for p in csv_files}
    unconverted_files = []

    # List all Campbell binary files located in subdirectories and
    # matching station name
    csbin_files = list_csbinary_files(bin_file_dir, station_name_raw)

    for csbin_file in tqdm(csbin_files, miniters=1, desc=f'{station_name_ascii}: Listing unconverted files'):

        # Get the type of file (eddy covariance, regular met data, etc)
        extension, split_interval = type_of_file(csbin_file)
        if not extension:
            # Type of file not to be converted
            continue

        # Check if the file has already been checked for conversion in the cache
        path = str(csbin_file)
        cached = ts_cache.get(path)

        if cached:
            first_ts, last_ts = cached
        else:
            file_header = dl.tob3_header(csbin_file)
            if not file_header:
                logf.write(f'{csbin_file}: Cannot read header\n')
                continue

            first_ts, last_ts = dl.tob3_first_last_timestamp(csbin_file)
            if not (first_ts and last_ts):
                logf.write(f'{csbin_file}: Cannot read first timestamp\n')
                continue

        if extension == '_eddy.csv':
            start_ts = pd.Timestamp(first_ts).ceil(split_interval)
            date_array = pd.DatetimeIndex([first_ts]).append(
                pd.date_range(
                    start=start_ts,
                    end=last_ts,
                    inclusive='left',
                    freq=split_interval)
                ).unique()
        elif extension == '_slow.csv':
            start_ts = pd.Timestamp(first_ts).ceil(split_interval) + pd.Timedelta(minutes=30)
            date_array = pd.DatetimeIndex([first_ts]).append(
                pd.date_range(
                    start=start_ts,
                    end=last_ts,
                    inclusive='left',
                    freq=split_interval)
                ).unique()

        if not cached:
            ts_cache[path] = (start_ts, last_ts)
            new_cache_rows.append((path, start_ts, last_ts))

        # Check if a expected dates are already in csv files
        needs_conversion = False
        for d in date_array:
            expected = f"{pd.Timestamp(d).strftime('%Y%m%d_%H%M')}{extension}"
            if expected not in csv_files_stems:
                needs_conversion = True
                break

        if needs_conversion:
            unconverted_files.append(str(csbin_file))

    if new_cache_rows:
        append_to_cache(tob3_cache_file, new_cache_rows)

    # Close error log file
    logf.close()

    return unconverted_files


def convert(station_name,
            csv_file_dir,
            unconverted_files):
    """
    Convert Campbell Scientific binary TOB3 files to TOA5 CSV blocks and write
    them to disk per station.

    For each binary file in ``unconverted_files``, this function:
    1. Invokes the external converter ``csidft_convert.exe`` to produce a
       temporary TOA5 CSV file.
    2. Determines the file type (eddy-covariance vs. slow/regular met) and the
       splitting interval via :func:`type_of_file`.
    3. Loads the converted CSV using ``dl.toa5_file``.
    4. Optionally resamples to 30‑minute intervals when needed (for 1‑minute
       slow data) via :func:`resample`.
    5. Splits the data into 30‑minute or daily blocks and writes each block to a
       CSV file with a preserved 4-line TOA5 header.

    Files are written under ``csv_file_dir / station_name`` with names of the
    form ``YYYYMMDD_HHMM_<suffix>.csv``, where ``<suffix>`` is determined by
    :func:`type_of_file` (e.g., ``_eddy.csv`` or ``_slow.csv``).

    Parameters
    ----------
    station_name : str
        Station identifier used to build the output directory and filenames.
    csv_file_dir : str or pathlib.Path
        Base directory where converted CSV files will be written. Files are
        placed under ``csv_file_dir / station_name``.
    unconverted_files : iterable of (str or pathlib.Path)
        Campbell Scientific binary files (e.g., ``.dat``) to convert.

    Returns
    -------
    None
        This function performs file I/O and does not return a value.

    Side Effects
    ------------
    - Creates or overwrites a log file at ``./Logs/csbinary_to_csv.log``.
    - Creates a temporary file ``tmp.csv`` under ``csv_file_dir / station_name``.
      The temporary file is deleted after each iteration.
    - Writes one or more CSV files per input binary file, each with a 4-line
      TOA5 header preserved from the converter output.
    """

    # Paths
    log_file = Path('.','Logs','csbinary_to_csv.log')
    csv_file_dir = Path(csv_file_dir)
    csidft_exe = Path("./Bin/raw2ascii/csidft_convert.exe")

    # Open error log file
    logf = open(log_file, "a")

    for csbin_file in tqdm(unconverted_files, miniters=1, desc=f'{station_name}: Converting CS binaries to csv'):

        try:
            # Conversion from the Campbell binary file to csv format
            tmp_file = csv_file_dir.joinpath(station_name, 'tmp.csv')
            subprocess.call(
                [csidft_exe, csbin_file, tmp_file , 'ToA5'],
                stdout=subprocess.DEVNULL)

            # Get the type of file (eddy covariance, regular met data, etc)
            extension, split_interval = type_of_file(csbin_file)

            # Save the header to respect TOA5 format
            with open(tmp_file) as f:
                header = [next(f) for x in range(4)]

            # Load file
            df = dl.toa5_file(tmp_file)

            if extension == "_eddy.csv":
                # Slice df into 30min blocks
                blocks = slice_30min_blocks(df)

            elif extension == "_slow.csv":
                # Get time step duration (1min files need to be resampled at 30min)
                tob3_file_header = dl.tob3_header(csbin_file)
                if tob3_file_header[1][1] == '1 MIN':
                    df = resample(df)
                # Slice df into daily blocks
                blocks = slice_day_blocks(df)

            # Write splitted files
            for block in blocks:
                file_name = csv_file_dir.joinpath(station_name,
                    block.index[0].strftime('%Y%m%d_%H%M') + extension)

                # Write header
                with open(file_name,'w') as f:
                    for h in header:
                        f.write(h)
                block.to_csv(file_name, mode='a', header=False, index=True)

            os.remove(tmp_file)

        except:
            logf.write(f'{csbin_file}: Cannot convert\n')
            continue

    logf.close()


def list_csbinary_files(root_dir,station):
    """
    Recursively list Campbell Scientific binary files (.dat) under ``root_dir``
    for a given station.

    This function walks the directory tree rooted at ``root_dir`` and collects
    all ``.dat`` files located in directories whose path contains a segment
    matching the pattern ``{station}_YYYYMMDD`` (8-digit date). It is designed
    for repositories organized by acquisition date and station name.

    The function will not look into folders named _quarantine.

    Parameters
    ----------
    root_dir : str or Pathlib path
        Path to the root directory to search recursively.
    station : str or Pathlib path
        Station name prefix used in directory names. Directories are considered
        a match if their path contains ``f"{station}_\\d{{8}}"`` (e.g.,
        ``Romaine-2_reservoir_shore_20180710``).

    Returns
    -------
    list of str
        Absolute or relative file paths (depending on ``root_dir`` input) to
        all matching ``.dat`` files found under ``root_dir``.

    Notes
    -----
    The expected directory structure is:

        bin_file_dir/
        ├── YYYYMMDD/
        │   └── station_name_raw_YYYYMMDD/
        │       ├── foo.dat
        │       └── bar.dat
        ├── YYYYMMDD/
        │   └── station_name_raw_YYYYMMDD/
        │       ├── foo.dat
        │       └── bar.dat

    """
    # List Campbell binary files located in the root dir, that contain
    pattern = re.compile(re.escape(station) + r"_\d{8}")
    csbin_files = []

    for root, dirs, files in os.walk(root_dir):
        # Prevent os.walk from searching in "_quarantine" folders
        dirs[:] = [d for d in dirs if d != "_quarantine"]
        # Check if the directory matches the pattern
        if pattern.search(root):
            for file in files:
                # Check if the file ends with .dat
                if file.endswith(".dat"):
                    csbin_files.append(os.path.join(root, file))
    return csbin_files


def list_csv_files(csv_dir):
    """
    List all CSV files located directly in the given directory.

    This function searches the directory specified by ``csv_dir`` and returns
    all files with a ``.csv`` extension. Only files in the top-level directory
    are considered; subdirectories are not searched.

    Parameters
    ----------
    csv_dir : pathlib.Path
        Path to the directory containing CSV files.

    Returns
    -------
    list of pathlib.Path
        A list of paths to all ``.csv`` files found in ``csv_dir``.
    """
    return list(csv_dir.glob("*.csv"))


def type_of_file(file_name):
    """
    Determine the type of a data file based on its name and return both the
    expected output extension and the appropriate time-splitting interval.

    This function identifies several categories of Campbell Scientific files,
    including:
    - Eddy covariance time-series files
    - Slow (regular meteorological / flux) files
    - Miscellaneous system or configuration files

    The classification is based on substring patterns in ``file_name``. The
    function returns a tuple specifying:
    1. The output file extension that should be used after conversion.
    2. The temporal split interval for processing (e.g., 30 minutes or daily).

    Parameters
    ----------
    file_name : str or pathlib.Path
        Name or path of the file to classify.

    Returns
    -------
    tuple of (str or None, str or None)
        A tuple ``(extension, split_interval)`` where:

        - ``extension`` is either ``"_eddy.csv"``, ``"_slow.csv"``, or ``None``
        - ``split_interval`` is either ``"30min"``, ``"1D"``, or ``None``

        ``None`` indicates that the file does not correspond to a known
        data-processing type (e.g., sys_log, code, notes, alerts).
    """
    file_name = str(file_name)
    if 'ts_data_' in file_name or '_Time_Series_' in file_name:
        extension='_eddy.csv'
        split_interval = '30min'
    elif '_Flux_CSIFormat_' in file_name or 'flux' in file_name or 'data_' in file_name:
        extension='_slow.csv'
        split_interval = '1D'
    else:
        # .cr1 / .cr3 / sys_log files / radiation / met30min / '_Flux_Notes_'
        # alert / Config_Setting_Notes / Flux_AmeriFluxFormat
        extension = None
        split_interval = None
    return extension, split_interval


def slice_30min_blocks(df):
    """
    Slice a time-indexed DataFrame into contiguous 30-minute blocks
    aligned to half-hour boundaries.

    Blocks are defined on half-hour boundaries (HH:00 and HH:30). Each
    block nominally starts at HH:00:00.1 or HH:30:00.1 and ends at the
    next HH:30:00.0 or HH+1:00:00.0. The first and last blocks are clipped
    to the actual data range if the DataFrame does not start or end
    exactly on these boundaries.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with a monotonically increasing
        ``pandas.DatetimeIndex``.

    Returns
    -------
    blocks : list of pandas.DataFrame
        List of DataFrame slices, one per 30-minute block. Each slice
        preserves the original columns and index and contains all rows
        whose timestamps fall within the corresponding block interval.
    """

    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("DataFrame index must be a DatetimeIndex")

    df = df.sort_index()

    start = df.index[0]
    end = df.index[-1]

    # Floor to 30-minute boundary
    aligned_start = start.floor("30min")

    # Build theoretical boundaries
    boundaries = pd.date_range(
        start=aligned_start,
        end=end.ceil("30min"),
        freq="30min"
    )

    blocks = []

    for b_start, b_end in zip(boundaries[:-1], boundaries[1:]):
        # Apply the 0.1s / 0.0s rule
        block_start = b_start + pd.Timedelta(milliseconds=100)
        block_end = b_end

        # Clip to actual data range
        block_start = max(block_start, start)
        block_end = min(block_end, end)

        if block_start <= block_end:
            block = df.loc[block_start:block_end]
            if not block.empty:
                blocks.append(block)
    return blocks


def slice_day_blocks(df):
    """
    Slice a time-indexed DataFrame into contiguous 24h blocks
    aligned to half-hour boundaries.

    Blocks are defined on day boundaries (00:30 and 00:00 next day). The first
    and last blocks are clipped to the actual data range if the DataFrame does
    not start or end exactly on these boundaries.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with a monotonically increasing
        ``pandas.DatetimeIndex``.

    Returns
    -------
    blocks : list of pandas.DataFrame
        List of DataFrame slices, one per day block. Each slice
        preserves the original columns and index and contains all rows
        whose timestamps fall within the corresponding block interval.
    """

    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("DataFrame index must be a DatetimeIndex")

    df = df.sort_index()

    start = df.index[0]
    end = df.index[-1]

    # Floor to 30-minute boundary
    aligned_start = start.floor("1D")

    # Build theoretical boundaries
    boundaries = pd.date_range(
        start=aligned_start,
        end=end.ceil("1D"),
        freq="1D"
    )

    blocks = []

    for b_start, b_end in zip(boundaries[:-1], boundaries[1:]):
        # Apply the 0.1s / 0.0s rule
        block_start = b_start + pd.Timedelta(minutes=30)
        block_end = b_end

        # Clip to actual data range
        block_start = max(block_start, start)
        block_end = min(block_end, end)

        if block_start <= block_end:
            block = df.loc[block_start:block_end]
            if not block.empty:
                blocks.append(block)
    return blocks


def resample(df):
    """
    Resample a time-indexed DataFrame to 30‑minute intervals using column‑specific
    aggregation rules.

    This function identifies two groups of columns:
    1. **Sum columns**: columns whose names contain ``"_Tot"`` or ``"_aggregate"``,
       which are aggregated by summation.
    2. **Average columns**: all remaining columns, which are aggregated by mean.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame with a ``DatetimeIndex``. Values represent higher
        frequency measurements (typically 1‑minute resolution).

    Returns
    -------
    pandas.DataFrame
        A DataFrame resampled to 30‑minute intervals. Columns matching
        ``"_Tot"`` or ``"_aggregate"`` are summed, and all other columns are
        averaged.

    """
    # Identify columns that are summed and columns that are averaged.
    col_to_sum = df.filter(regex=('_Tot|_aggregate')).columns
    col_to_average = df.columns.difference(col_to_sum, sort=False)
    func_all = {**{col_to_sum[i]: lambda x: x.sum() if len(x) >= 1 else None for i in range(len(col_to_sum))},
                **{col_to_average[i]: lambda x: x.mean() if len(x) >= 1 else None for i in range(len(col_to_average))}}

    # Resample the minute columns into 30-min using function defined for each column
    return df.resample('30min').agg(func_all)


def load_cache(tob3_cache_file):
    if tob3_cache_file.exists():
        df = pd.read_csv(tob3_cache_file, parse_dates=["first_ts", "last_ts"])
        return dict(zip(df["path"], zip(df["first_ts"], df["last_ts"])))
    return {}


def append_to_cache(tob3_cache_file, rows):
    df = pd.DataFrame(rows, columns=["path", "first_ts", "last_ts"])
    header = not tob3_cache_file.exists()
    df.to_csv(tob3_cache_file, mode="a", index=False, header=header)