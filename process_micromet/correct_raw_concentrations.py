import os
import re
import pandas as pd
import numpy as np
import json
from datetime import datetime

def load_calibration_coefficients(file_dir, station):
    """
    Load calibration coefficients from JSON file.

    Parameters
    ----------
    file_dir : Patlib object
        Path to the folder containing data
    station : String
        Name of the station

    Returns
    -------
    Dictionary
        Returns dictionary built from the JSON file

    """
    with open(os.path.join(file_dir, f'{station}_calibration_correction.json'), 'r') as file:
        return json.load(file)


def get_calibration_coefficients(date_str, corr_coef):
    """
    Get coefficients that will be used to correct the raw gas concentrations

    Parameters
    ----------
    date_str : String
        Date in '%Y-%m-%d %H:%M:%S' format
    corr_coef : Dictionary
        Dictionary returned by the that contains the calibrations periods, the

    Returns
    -------
    dict
        Dictionary that contains the fields: 'H2O_slope', 'H2O_intercept',
        'CO2_slope', 'CO2_intercept'

    """
    input_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

    for period in corr_coef['erroneous_periods']:
        start_date = datetime.strptime(period['start_date'], '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(period['end_date'], '%Y-%m-%d %H:%M:%S') if period['end_date'] != 'None' else None

        if start_date <= input_date <= end_date:
            return {
                'H2O_slope': np.nan,
                'H2O_intercept': np.nan,
                'CO2_slope': np.nan,
                'CO2_intercept': np.nan,
                'temperature_var_name': period['temperature_var_name'],
                'H2O_var_name': period['H2O_var_name'],
                'CO2_var_name': period['CO2_var_name'],
                'cal_period': [start_date, end_date]
            }

    for period in corr_coef['calibration_periods']:
        start_date = datetime.strptime(period['start_date'], '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime(period['end_date'], '%Y-%m-%d %H:%M:%S') if period['end_date'] != 'None' else None

        if end_date is None:
            return {
                'H2O_slope': 0,
                'H2O_intercept': 0,
                'CO2_slope': 0,
                'CO2_intercept': 0,
                'temperature_var_name': period['temperature_var_name'],
                'H2O_var_name': period['H2O_var_name'],
                'CO2_var_name': period['CO2_var_name'],
                'cal_period': [start_date, end_date]
            }

        if start_date <= input_date <= end_date:
            return {
                'H2O_slope': float(period['H2O_slope']),
                'H2O_intercept': float(period['H2O_intercept']),
                'CO2_slope': float(period['CO2_slope']),
                'CO2_intercept': float(period['CO2_intercept']),
                'temperature_var_name': period['temperature_var_name'],
                'H2O_var_name': period['H2O_var_name'],
                'CO2_var_name': period['CO2_var_name'],
                'cal_period': [start_date, end_date]
            }


def correct_gas_concentration(gas_conc, temperature, slope, intercept):
    """
    Correct the gas concentration according to linear regression based
    on air temperature.
    Corrected gas concentation =
        original gas concentration + slope * temperature + intercept

    Parameters
    ----------
    gas_conc : Pandas series
        Gas concentration (H2O, CO2) as measured by the fast response
        gas analyser
    temperature : Pandas series
        Fast temperature (°C)
    slope : Float
        Slope
    intercept : Float
        Intercept

    Returns
    -------
    Pandas series
        Corrected gas concentration
    """
    return gas_conc + slope * temperature + intercept


def correct_raw_concentrations(
        station, asciiOutDir, config_dir, overwrite=False):
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

    calib_coeff = load_calibration_coefficients(config_dir, station)

    # Define a regex pattern to match the date format 'YYYYmmDD_HHMM'
    date_pattern = re.compile(r'(\d{8}_\d{4})')

    # Find files that match the pattern Station_YYYYMMDD_eddy.csv
    eddy_files = [f for f in os.listdir(
        os.path.join(asciiOutDir,station))
                 if re.match(
                         re.compile(date_pattern.pattern + r'_eddy.csv'), f)]

    # Find files that match the pattern Station_YYYYMMDD_eddy_corr.csv
    eddy_corr_files = [f for f in os.listdir(
        os.path.join(asciiOutDir,station))
                if re.match(
                        re.compile(date_pattern.pattern + r'_eddy_corr.csv'), f)]

    logf = open( os.path.join(
        '.','Logs',f'correct_raw_concentrations_{station}.log'), "w")

    #####################
    ### Process files ###
    #####################

    for eddy_file in eddy_files:

        #Check if the file hasn't been processed yet
        eddy_corr_file = eddy_file.replace('eddy', 'eddy_corr')

        if (eddy_corr_file not in eddy_corr_files) | overwrite :
            print('Processing {}'.format(eddy_file))
            try:
                # Load and specify datatype
                df = pd.read_csv(os.path.join(asciiOutDir,station,eddy_file),
                                 skiprows=[0,2,3],
                                 na_values = "NAN")

                 # Get file header
                with open(os.path.join(asciiOutDir,station,eddy_file), 'r') as fp:
                    header = [next(fp) for x in range(4)]

                # Write header to destination file
                with open(os.path.join(asciiOutDir,station,eddy_corr_file), 'w') as fp:
                    for i_line in header:
                        fp.write(i_line)

                # Search for the date pattern in the string
                match = date_pattern.search(eddy_file)
                file_date = datetime.strptime(
                    match.group(1),
                    '%Y%m%d_%H%M'
                    ).strftime('%Y-%m-%d %H:%M:%S')

                # Get calibration coefficients and variables names
                cc = get_calibration_coefficients(file_date, calib_coeff)

                df[cc['H2O_var_name']] = correct_gas_concentration(
                    df[cc['H2O_var_name']].values,
                    df[cc['temperature_var_name']].values,
                    cc['H2O_slope'],
                    cc['H2O_intercept']
                    )

                df[cc['CO2_var_name']] = correct_gas_concentration(
                    df[cc['CO2_var_name']].values,
                    df[cc['temperature_var_name']].values,
                    cc['CO2_slope'],
                    cc['CO2_intercept']
                    )

                df.to_csv(os.path.join(asciiOutDir,station, eddy_corr_file),
                          header=False, index=False, mode='a', quoting=0)

            except Exception as e:
                print(str(e))
                logf.write('Failed to correct concentration for file '+
                           f'{eddy_file}: {str(e)} \n')

    # Close error log file
    logf.close()
    print('Done!')