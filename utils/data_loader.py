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

    return data or {}


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

def toa5_header(file, clean=True):
    """
    Extract the 4-line header of a TOA5 file.

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
        for _ in range(4):
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


def tob3_first_last_timestamp(
    file: str,
    future_guard_days: int = 1,
    add_window_days: int = 1,
):
    """
    Returns (first_ts, last_ts) from TOB3 file frames.

    Two-pass strategy:
      1) Find a robust *anchor* (first_dt) using the earliest plausible frame
         timestamp close to the table's header start datetime.
         - Option A: The anchor is restricted to a window centered on the
           header start time: [header_start - add_window_days, header_start + add_window_days].
           This avoids picking stray, older frames that may appear at the
           beginning of the binary region (e.g., recorder wrap leftovers).
         - Option B: If no frame falls in that window, use the global minimum
           plausible frame timestamp.
      2) Compute a plausible window for the last timestamp using
         intended * interval (+/- add_window_days) and ignore timestamps
         outside that range or “far future” frames.

    Parameters
    ----------
    file : str or pathlib.Path
        Path to a TOB3 file.
    future_guard_days : int, optional (default=1)
        Reject frames with timestamps beyond now() + this many days.
    add_window_days : int, optional (default=1)
        Slack used both when anchoring to the header start time and when
        defining the plausible window for last timestamp.

    Returns
    -------
    (first_dt, last_dt) : tuple[datetime.datetime | None, datetime.datetime | None]
        first_dt : first (anchor) data timestamp, or None if not found
        last_dt  : last record timestamp, or None if none found within bounds
    """

    # Campbell Scientific epoch
    epoch = dt.datetime(1990, 1, 1)

    file = Path(file)
    # Check if file is empty
    try:
        if file.stat().st_size == 0:
            return None
    except FileNotFoundError:
        raise

    # Read the 6 ASCII header lines
    with open(file, "rb") as f:
        header_lines = []
        for _ in range(6):
            line = f.readline().decode("ascii", errors="replace").strip()
            if not line:
                # Truncated header or unexpected format
                return None
            header_lines.append(line)
        # Mark the start of the binary region
        binary_start = f.tell()

    # Parse line 1 to get the table start datetime (last CSV field)
    line1_fields = next(csv_lib.reader([header_lines[0]]))
    # Expecting something like "YYYY-mm-dd HH:MM:SS"
    header_start = dt.datetime.fromisoformat(line1_fields[-1])

    # Parse line 2 to get Interval, FrameSize, Intended, Validation, Resolution
    line2 = next(csv_lib.reader([header_lines[1]]))
    interval = _parse_interval(line2[1])                 # "100 MSEC" -> timedelta
    frame_size = int(line2[2])                           # major frame size in bytes
    intended = int(line2[3])                             # intended records count (nominal)
    validation = int(line2[4])                           # validation stamp
    res_unit, res_factor = _parse_resolution(line2[5])   # e.g., "Sec100Usec"

    # Parse line 6 to compute one-record payload size
    dtypes = next(csv_lib.reader([header_lines[5]]))
    record_size = sum(_dtype_size(dt_str) for dt_str in dtypes)

    now_utc = dt.datetime.now()

    # Define the anchoring window around the header start time
    # (reuses add_window_days to keep the signature unchanged)
    anchor_min = header_start - dt.timedelta(days=add_window_days)
    anchor_max = header_start + dt.timedelta(days=add_window_days)

    # ------------------------------------------------------------------------------------
    # PASS 1: Find FIRST RECORD TIMESTAMP (robust anchor)
    # ------------------------------------------------------------------------------------
    first_dt = None                 # anchor restricted to header window
    first_dt_any = None             # fallback: global minimum plausible dt

    with open(file, "rb") as f:
        f.seek(binary_start)
        while True:
            major = f.read(frame_size)
            if len(major) < frame_size:
                break

            # Validate major frame via footer stamp
            major_footer = struct.unpack("<I", major[-4:])[0]
            vstamp = (major_footer >> 16) & 0xFFFF
            if vstamp not in (validation, validation ^ 0xFFFF):
                continue

            # Walk (possibly multiple) minor segments within this major frame
            for seg, foot in _iter_minor_segments(major):
                empty_flag = (foot >> 14) & 1
                if empty_flag or len(seg) < 16:
                    continue

                # Frame header: sec (4), sub (4), recno (4)
                sec, sub, _recno0 = struct.unpack("<III", seg[:12])
                dt0 = epoch + dt.timedelta(seconds=sec) + _ticks_to_timedelta(sub, res_unit, res_factor)

                # Guard against far-future frames
                if dt0 > now_utc + dt.timedelta(days=future_guard_days):
                    continue

                # Track the global minimum (previous behavior) for fallback
                if first_dt_any is None or dt0 < first_dt_any:
                    first_dt_any = dt0

                # NEW: restrict the *anchor* to the header-based time window
                if anchor_min <= dt0 <= anchor_max:
                    if first_dt is None or dt0 < first_dt:
                        first_dt = dt0

    # Fallback if no anchor was found within the header window
    if first_dt is None:
        first_dt = first_dt_any

    # If we still don't have an anchor, we can't proceed
    if first_dt is None:
        return None, None

    # ------------------------------------------------------------------------------------
    # Define plausible LAST timestamp window around the anchor
    # ------------------------------------------------------------------------------------
    if interval.total_seconds() > 0 and intended > 0:
        max_span = interval * intended
    else:
        # Free-running / event tables: keep a generous bound
        max_span = dt.timedelta(days=365)

    earliest_allowed = first_dt - dt.timedelta(days=add_window_days)
    latest_allowed = min(
        first_dt + max_span + dt.timedelta(days=add_window_days),
        now_utc + dt.timedelta(days=future_guard_days),
    )

    # ------------------------------------------------------------------------------------
    # PASS 2: Find LAST RECORD TIMESTAMP within plausible window
    # ------------------------------------------------------------------------------------
    last_dt = None
    with open(file, "rb") as f:
        f.seek(binary_start)
        while True:
            major = f.read(frame_size)
            if len(major) < frame_size:
                break

            # Validate major frame via footer stamp
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

                # Compute number of full records in this segment
                data_len = len(seg) - 12 - 4  # header (12) + footer (4)
                nrec = data_len // record_size if record_size > 0 else 0
                if nrec <= 0:
                    continue

                # Last record time in this segment:
                dt_last = dt0 if interval.total_seconds() == 0 else dt0 + (nrec - 1) * interval

                # Keep only plausible last times
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