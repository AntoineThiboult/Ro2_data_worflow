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
import re
import csv as csv_lib


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

def _parse_resolution(res: str):
    """
    TOB2/TOB3 frame timestamp 'sub-seconds' resolution parser.
    Examples: Sec100Usec, Sec10Usec, Sec1MSec, Sec50NSec
    """
    res = res.strip()
    m = re.match(r"Sec(\d+)(U|N|M)sec", res, flags=re.IGNORECASE)
    if not m:
        raise ValueError(f"Unsupported Frame Time Resolution: {res}")
    factor = int(m.group(1))
    unit_code = m.group(2).upper()
    unit = {"U": "usec", "N": "nsec", "M": "msec"}[unit_code]
    return unit, factor


def _ticks_to_timedelta(ticks: int, unit: str, factor: int) -> dt.timedelta:
    if unit == "usec":
        return dt.timedelta(microseconds=ticks * factor)
    if unit == "msec":
        return dt.timedelta(milliseconds=ticks * factor)
    if unit == "nsec":
        # datetime supports microseconds; keep floor microseconds
        return dt.timedelta(microseconds=(ticks * factor) // 1000)
    raise ValueError(unit)


def _parse_interval(s: str) -> dt.timedelta:
    s = s.strip()
    if s == "0":
        return dt.timedelta(0)
    parts = s.split()
    num = float(parts[0])
    unit = parts[1].upper() if len(parts) > 1 else "NSEC"
    mult = {"NSEC": 1e-9, "USEC": 1e-6, "MSEC": 1e-3, "SEC": 1,
            "MIN": 60, "HR": 3600, "DAY": 86400}[unit]
    return dt.timedelta(seconds=num * mult)


def _dtype_size(dt: str) -> int:
    dt = dt.strip()
    if dt in ("IEEE4", "IEEE4L", "IEEE4B", "UINT4", "INT4", "ULONG", "LONG", "BOOL4"):
        return 4
    if dt in ("FP2", "SHORT", "USHORT", "INT2", "UINT2", "BOOL2"):
        return 2
    if dt in ("BOOL", "BOOL8"):
        return 1
    if dt in ("SecNano", "NSec"):
        return 8
    m = re.match(r"ASCII\((\d+)\)", dt)
    if m:
        return int(m.group(1))
    if dt == "ASCII":
        return 1
    raise ValueError(dt)


def _iter_minor_segments(major_frame: bytes):
    """
    Yield (segment_bytes, footer_int) in chronological order.

    If major footer has M-bit set, walk backwards by minor-frame sizes
    stored in the footer 'offset/size' field (low 12 bits).
    """
    frame_size = len(major_frame)
    major_footer = struct.unpack("<I", major_frame[-4:])[0]
    major_is_minor = (major_footer >> 15) & 1  # M bit
    if not major_is_minor:
        yield major_frame, major_footer
        return

    end = frame_size
    segments = []
    safety = 0
    while end >= 4 and safety < 1000:
        safety += 1
        foot = struct.unpack("<I", major_frame[end - 4:end])[0]
        size = foot & 0x0FFF
        if size <= 0 or size > end:
            break
        start = end - size
        segments.append((major_frame[start:end], foot))
        end = start
        if end == 0:
            break

    for seg, foot in reversed(segments):
        yield seg, foot


def tob3_first_last_timestamp(file: str,
                              future_guard_days: int = 1,
                              add_window_days: int = 1):
    """
    Returns (first_ts, last_ts) from TOB3 file frames:
      - Reads 6 ASCII header lines
      - Uses Data Frame Size (line 2) to step through major frames
      - Uses footer validation stamp and empty-bit to decide if a frame contains a record
      - Reads TOB3 12-byte frame header: seconds, sub-seconds, record_number (all little-endian)
    """

    # Campbell Scientific reference time
    epoch = dt.datetime(1990, 1, 1)

    file = Path(file)

    # Check if file is empty
    try:
        if file.stat().st_size == 0:
            return None
    except FileNotFoundError:
        raise

    with open(file, "rb") as f:
        header_lines = []
        # Try reading ASCII header lines
        for _ in range(6):
            line = f.readline().decode("ascii", errors="replace").strip()
            if not line:
                # File ends before end of header. The file is either
                # corrupted or do not match expected format
                return None
            header_lines.append(line)
        # Save position of end of header
        binary_start = f.tell()

    # Line 2: "Table","Interval","FrameSize","Intended","Validation","Resolution",...
    line2 = next(csv_lib.reader([header_lines[1]]))
    interval = _parse_interval(line2[1])
    frame_size = int(line2[2])
    intended = int(line2[3])
    validation = int(line2[4])
    res_unit, res_factor = _parse_resolution(line2[5])

    dtypes = next(csv_lib.reader([header_lines[5]]))
    record_size = sum(_dtype_size(dt) for dt in dtypes)

    now_utc = dt.datetime.now()

    # --- PASS 1: find first record timestamp (anchor) ---
    first_dt = None

    with open(file, "rb") as f:
        f.seek(binary_start)
        while True:
            major = f.read(frame_size)
            if len(major) < frame_size:
                break

            major_footer = struct.unpack("<I", major[-4:])[0]
            vstamp = (major_footer >> 16) & 0xFFFF
            if vstamp not in (validation, validation ^ 0xFFFF):
                continue

            for seg, foot in _iter_minor_segments(major):
                empty_flag = (foot >> 14) & 1
                if empty_flag or len(seg) < 16:
                    continue

                sec, sub, _recno0 = struct.unpack("<III", seg[:12])
                dt0 = epoch + dt.timedelta(seconds=sec) + _ticks_to_timedelta(sub, res_unit, res_factor)

                # basic sanity: ignore frames “from the far future”
                if dt0 > now_utc + dt.timedelta(days=future_guard_days):
                    continue

                if first_dt is None or dt0 < first_dt:
                    first_dt = dt0

    if first_dt is None:
        return None, None

    # --- plausible window for last timestamp ---
    # Use intended*interval for interval-driven tables; otherwise keep a loose bound.
    if interval.total_seconds() > 0 and intended > 0:
        max_span = interval * intended
    else:
        max_span = dt.timedelta(days=365)

    earliest_allowed = first_dt - dt.timedelta(days=add_window_days)
    latest_allowed = min(first_dt + max_span + dt.timedelta(days=add_window_days),
                         now_utc + dt.timedelta(days=future_guard_days))

    # --- PASS 2: find the last *record* timestamp ---
    last_dt = None

    with open(file, "rb") as f:
        f.seek(binary_start)
        while True:
            major = f.read(frame_size)
            if len(major) < frame_size:
                break

            major_footer = struct.unpack("<I", major[-4:])[0]
            vstamp = (major_footer >> 16) & 0xFFFF
            if vstamp not in (validation, validation ^ 0xFFFF):
                continue

            for seg, foot in _iter_minor_segments(major):
                empty_flag = (foot >> 14) & 1
                if empty_flag or len(seg) < 16:
                    continue

                sec, sub, _recno0 = struct.unpack("<III", seg[:12])
                dt0 = epoch + dt.timedelta(seconds=sec) + _ticks_to_timedelta(sub, res_unit, res_factor)

                data_len = len(seg) - 12 - 4
                nrec = data_len // record_size if record_size > 0 else 0
                if nrec <= 0:
                    continue

                # IMPORTANT FIX: last record time in this segment/frame
                dt_last = dt0 if interval.total_seconds() == 0 else dt0 + (nrec - 1) * interval

                # reject spurious timestamps
                if not (earliest_allowed <= dt_last <= latest_allowed):
                    continue

                if last_dt is None or dt_last > last_dt:
                    last_dt = dt_last

    return first_dt, last_dt


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