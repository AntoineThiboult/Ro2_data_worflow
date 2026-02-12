import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import statsmodels.api as sm
from utils import data_loader as dl


def find_uncorrected_files(folder, overwrite=False):
    """
    Find _eddy.csv files without a corresponding _eddy_corr.csv file.
    Returns a list of_eddy.csv files without its corresponding _eddy_corr.csv.
    """
    eddy_files = set()
    eddy_corr_files = set()

    for p in folder.iterdir():
        name = p.name

        # Extract timestamp of _eddy and _eddy_corr
        if name.endswith("_eddy.csv") and not name.endswith("_eddy_corr.csv"):
            timestamp = name.replace("_eddy.csv", "")
            eddy_files.add(timestamp)

        elif name.endswith("_eddy_corr.csv"):
            timestamp = name.replace("_eddy_corr.csv", "")
            eddy_corr_files.add(timestamp)

    # timestamps where eddy exists but eddy_corr is missing
    missing_corr_timestamp = eddy_files - eddy_corr_files

    # corresponding eddy filenames
    missing_corr_eddy_files = [
        folder.joinpath(f"{ts}_eddy.csv")
        for ts in sorted(missing_corr_timestamp)
    ]

    return missing_corr_eddy_files


def correct_densities(station, corr_factors, uncorrected_files):
    """
    Correct water and CO2 concentrations. Correction factors must be provided
    in csv files that contain 'H2O_slope', 'H2O_intercept', and 'CO2_intercept'
    along with a timestamp column.

    Parameters
    ----------
    station : string
        Name of the station
    asciiOutDir : string
        Path to the directory that contains raw (10 Hz) .csv files
    config_dir : string
        Path to the directory that contains the .csv file with correction
        coefficients
    overwrite : Boolean, optional
        Overwrite or not the previously corrected concentations. The default is False.

    Returns
    -------
    None. Write a file file_corr.csv with corrected concentrations

    """

    print(f'Start raw gas concentration correction for the {station} station')

    def resolve_column(df, candidates):
        """
        Return the first candidate column that exists in df.columns.
        If required=True and none exist, raise a KeyError with a helpful message.
        """
        for c in candidates:
            if c in df.columns:
                return c

    # TODO Write option to overwrite last calibration if still open

    # Open log file
    logf = open( os.path.join(
        '.','Logs',f'correct_raw_concentrations_{station}.log'), "w")

    for eddy_file in tqdm(uncorrected_files, desc=f'{station}: Correcting densities'):

        eddy_corr_file = eddy_file.with_name(
            eddy_file.name.replace("_eddy.csv", "_eddy_corr.csv"))
        timestamp = eddy_file.name.replace("_eddy.csv", "")

        try:
            # Load file and header
            df = dl.toa5_file(eddy_file)
            header = dl.toa5_header(eddy_file,False)

            # Write header to destination file
            with open(eddy_corr_file, 'w') as fp:
                for i_line in header:
                    fp.write(i_line)

            # Check closest timestamp in the corr_factors index
            # Log an error if no timestamp is found within a day
            target = pd.to_datetime(timestamp, format="%Y%m%d_%H%M")
            nearest = corr_factors.index.get_indexer([target], method="nearest", tolerance='1d')
            if nearest[0] != -1:
                nearest_date = corr_factors.index[nearest[0]]
            else:
                logf.write(f'Failed to find correction factors for {timestamp}: '
                           +f'{eddy_file}\n')
                continue

            # Handle the change of column names according to instrument and
            # code version
            H2O = resolve_column(df, ['H2O', 'H2O_li', 'H2O_density'])
            t_sonic = resolve_column(df, ['Ts','T_SONIC'])
            # CO2 = resolve_column(df, ['CO2_corr', 'CO2', 'CO2_li', 'CO2_density'])

            # Correction of the H2O densities with identified coeficients
            a, b = corr_factors.loc[nearest_date,['a', 'b']]
            df[H2O] = df[H2O] + (a*df[t_sonic].mean() + b) * linear_func(df[t_sonic].mean())

            # Save
            df.to_csv(eddy_corr_file, header=False,
                      mode='a', quoting=0)

        except Exception as e:
            print(str(e))
            logf.write('Failed to correct concentration for file '+
                       f'{eddy_file}: {str(e)}\n')

    # Close error log file
    logf.close()
    print('Done!')


def absolute_humidity(T, RH):
    """
    Compute absolute humidity (water vapor density) from temperature and
    relative humidity.

    Absolute humidity is defined as the mass of water vapor per unit volume
    of moist air and is expressed in grams per cubic meter (g m⁻³). The
    calculation assumes ideal gas behavior for water vapor.

    Saturation vapor pressure is computed using:
    - Monteith and Unsworth (2013) formulation for temperatures ≥ 0 °C
      (saturation over liquid water),
    - Murray (1967) formulation for temperatures < 0 °C
      (saturation over ice).

    Parameters
    ----------
    T : array_like
        Air temperature in degrees Celsius.
    RH : array_like
        Relative humidity in percent (0–100), defined with respect to
        saturation vapor pressure.

    Returns
    -------
    w : ndarray
        Absolute humidity (water vapor density) in grams per cubic meter
        (g m⁻³).

    Notes
    -----
    This function returns water vapor density, not mixing ratio or specific
    humidity. The calculation does not require atmospheric pressure and
    assumes ideal gas behavior for water vapor.
    """

    T = np.asarray(T, dtype=float)
    RH = np.asarray(RH, dtype=float)
    es = np.empty_like(T, dtype=float)

    mask = T >= 0
    # Vapor pressure of water above 0°C, according to Monteith and Unsworth
    # 2013. Principles of environmental physics: plants, animals, and the atmosphere.
    es[mask] = 0.610781 * np.exp(17.27 * T[mask] / (T[mask] + 237.3))
    # Vaport pressure of water below 0°C, according to Murray 1967. On the
    # computation of saturation vapour pressure. J. Applied Meteorology 6: 203-204
    es[~mask] = 0.610781 * np.exp(21.875 * T[~mask] / (T[~mask] + 265.50))

    e = RH / 100.0 * es
    w = (e * 18.02) / (8.31 * (T + 273.15)) * 1000.0
    return w


def linear_func(T):
    """
    This function generates a value to weight the correction according to
    temperature. If T=-20°C, w= 1, if T=+20°C, w=0
    """
    return -0.5/20 * T + 0.5


def get_correction_coeff(df, gas_analyzer_info, iStation, temperature_bounds=(-30,-10)):
    """
    Get coefficients that will be used to correct the raw gas concentrations
    A set of correction parameters is computed for each calibration periods.

    """

    def iter_time_slices(df: pd.DataFrame, boundaries):
        if not isinstance(df.index, pd.DatetimeIndex):
            raise TypeError("DataFrame index must be a DatetimeIndex")

        # Normalize and sort boundaries
        boundaries = pd.to_datetime(boundaries).sort_values()

        idx_min = df.index.min()
        idx_max = df.index.max()

        # Keep only usable boundaries
        boundaries = boundaries[
            (boundaries > idx_min) & (boundaries < idx_max)
        ]

        # Build all slice edges
        edges = pd.Index([idx_min]).append(boundaries).append(
            pd.Index([idx_max])
        )

        # Yield contiguous slices
        for start, end in zip(edges[:-1], edges[1:]):
            yield df.loc[start:end]


    def build_invalid_datetime_index(periods):
        invalid_periods = pd.DatetimeIndex([])
        if periods:
            for p in periods:
                invalid_periods = invalid_periods.append(
                    pd.date_range(p['start'],p['end'],freq='30min')
                )
        return invalid_periods

    # Initialization of the DataFrame that will store the correction coeficients
    corr_factors = pd.DataFrame(
        index = df.index,
        columns=['a','b'])

    # Check that all necessary data are present in DataFrame to perform
    # quality checks and the regression. If not, do not perform the correction.
    required = {
        'diag_sonic',
        'diag_irga',
        'air_temp_IRGASON',
        'H2O_density_IRGASON'}
    if not required.issubset(df.columns):
        corr_factors = pd.DataFrame(index=df.index, columns=['a','b'])
        corr_factors[['a','b']] = 0.0
        return corr_factors


    # Create time slices between two calibration
    slices = iter_time_slices(df, gas_analyzer_info['calibration_dates'])
    invalid_periods = build_invalid_datetime_index(gas_analyzer_info['invalid_intervals'])

    for s in slices:

        # Remove invalid periods from regression data
        id_invalid = s.index.isin(invalid_periods)

        # Keep data that have no diagnostic flags
        id_diag_sonic = s['diag_sonic'] == 0
        id_diag_irga = s['diag_irga'] == 0

        # Keep data of interest (cold temperature, no NaN)
        low_temp, high_temp = temperature_bounds
        id_temperature = (s['air_temp_IRGASON'] >= low_temp) & (s['air_temp_IRGASON'] <= high_temp)
        id_density = ~np.isnan(s['H2O_density_IRGASON'])

        # Keep data that are not obviously wrong (discard data that are
        # more than 5g/m3 away from their theoretical min/max)
        id_plausible_density = (
            ((s['H2O_density_IRGASON'] - absolute_humidity(s['air_temp_IRGASON'], 100)) < 2.5)
            & (s['H2O_density_IRGASON'] > -2.5 )
            )

        # Add relative signal strength as criteron if available (not for Li-7500)
        rssi_available = any(~np.isnan(s['H2O_sig_strength_IRGASON']))
        if rssi_available:
            id_rssi = s['H2O_sig_strength_IRGASON'] > 0.70
            id_valid = (~id_invalid & id_diag_sonic & id_diag_irga
                        & id_temperature & id_density & id_plausible_density
                        & id_rssi)
        else:
            id_valid = (~id_invalid & id_diag_sonic & id_diag_irga
                        & id_temperature & id_density & id_plausible_density)

        # Check if sample is populated enough to perform a regression
        # Cut the temperature data into 1°C bins
        cut = pd.cut(
            s.loc[id_valid,'air_temp_IRGASON'],
            np.arange(low_temp,high_temp,1))

        # Check if at least 10 bins have 20 points
        if sum(cut.value_counts() > 10) < 10:
            # Not enough data accross the different temperature bins to perform
            # a reliable quantile regression. a = 0 and b = 0 means no
            # correction is performed
            a = b = 0
        else:
            # Perform quantile regression

            # Absolute humidity corresponding to 99% relative humidity (close to saturation)
            q_sat = absolute_humidity(s.loc[id_valid, 'air_temp_IRGASON'], 99.0)
            # Distance between IRGASON density and saturation
            r = q_sat - s.loc[id_valid, 'H2O_density_IRGASON']

            X = sm.add_constant(s.loc[id_valid, 'air_temp_IRGASON'])
            mod = sm.QuantReg(r, X)
            res = mod.fit(q=0.02)
            b, a = res.params

        # Save coeffcients
        corr_factors.loc[s.index,'a'] = a
        corr_factors.loc[s.index,'b'] = b

    # Set coefficient to NaN for invalid periods
    corr_factors.loc[invalid_periods,'a'] = np.nan
    corr_factors.loc[invalid_periods,'b'] = np.nan

    return corr_factors


