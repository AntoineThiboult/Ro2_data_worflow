# -*- coding: utf-8 -*-
import os
import time
import pandas as pd
import numpy as np
import yaml
import netCDF4 as ncdf
import cdsapi
import concurrent.futures

def retrieve_routine(variables, dataset, bounding_rectangle, ymd, delay):
    """
    Parameters
    ----------
    variables : list
        Contains the list of variable to extract
    dataset : dictionary
        dictionary containing the keys 'name','dest_folder':'dest_folder',
        'dest_subfolder', and 'product_type'
    bounding_rectangle : list (4,)
        Coordinates of the rectangle to extract (North, West, South, East)
    ymd : Timestamp
        Year Month Date to be extracted

    Returns
    -------
    None.

    """
    cds = cdsapi.Client()
    year = ymd.strftime('%Y')
    month = ymd.strftime('%m')
    day = ymd.strftime('%d')

    export_name = os.path.join(
                dataset['dest_folder'],
                dataset['dest_subfolder'],
                dataset['dest_subfolder'] + f'_{year}{month}{day}.nc')

    if not os.path.isfile(export_name):
        time.sleep((delay%10)/2)
        cds.retrieve(dataset['name'],
                    {'variable': variables,
                     'product_type': dataset['product_type'],
                     'year':   year,
                     'month':  month,
                     'day':    day,
                     'time':   ['{:02}:00'.format(f) for f in range(0,24)],
                     'area':   bounding_rectangle,
                     'format': 'netcdf'}, export_name
                    )


def retrieve_ERA5land(dates, dest_folder):
    """Retrieve data from the ECMWF Copernicus ERA5-land database in a netCDF4
    format. The function checks that the data has not been downloaded yet.
    Requires the cdsapi to be installed. You can install it from the anaconda
    prompt with : conda install -c conda-forge cdsapi
    A file named .cdsapirc that contains user identifiers should be placed
    at the root directory of Python.

    Parameters
    ----------
    dates: dictionnary that contains a 'start' and 'end' dates
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    rawFileDir: path to the directory that contains the .xlsx files

    Returns
    -------
    None.

    References
    ----------
    - General guideline: https://cds.climate.copernicus.eu/api-how-to
    - List of variables available for ERA5 land https://confluence.ecmwf.int/display/CKB/ERA5-Land%3A+data+documentation#ERA5Land:datadocumentation-parameterlistingParameterlistings
    - Check your request progress: https://cds.climate.copernicus.eu/cdsapp#!/yourrequests

    """

    print('Start extracting ERA5 data...')



    # Retrieval parameters
    dataset = {'name':'reanalysis-era5-land',
               'dest_folder':dest_folder,
               'dest_subfolder':'ERA5L',
               'product_type':'reanalysis'}
    datelist = pd.date_range(start=dates['start'], end=dates['end'], freq='D')
    bounding_rectangle = [53, -64.7, 50.2, -62.4 ] # North, West, South, East.
    variables = [
        # Lake variables
        'lake_mix_layer_temperature',
        # Snow variables
        'snow_cover',
        'snow_density',
        'snow_depth',
        'snow_depth_water_equivalent',
        'snowmelt',
        # Standard meteorological variables
        '10m_u_component_of_wind',
        '10m_v_component_of_wind',
        '2m_temperature',
        '2m_dewpoint_temperature',
        'skin_temperature',
        'soil_temperature_level_1',
        'soil_temperature_level_2',
        'volumetric_soil_water_layer_1',
        'volumetric_soil_water_layer_2',
        'snowfall',
        'surface_solar_radiation_downwards',
        'surface_thermal_radiation_downwards',
        'surface_net_solar_radiation',
        'surface_net_thermal_radiation',
        'surface_pressure',
        'total_precipitation',
        # Fluxes
        'surface_sensible_heat_flux',
        'surface_latent_heat_flux',
        'total_evaporation',
        'potential_evaporation',
        'evaporation_from_open_water_surfaces_excluding_oceans',
        'evaporation_from_vegetation_transpiration'
              ]

    # Process to retrieval
    with concurrent.futures.ThreadPoolExecutor(max_workers = 10) as executor:
        [executor.submit(retrieve_routine, variables, dataset, bounding_rectangle, ymd, delay) for delay, ymd in enumerate(datelist)]

    print('Done!\n')


def daily_decumulate(x):
    """Decumulate the daily meteorological variables and linearly interpolate
    the missing values.

    Parameters
    ----------
    x : Pandas series

    Returns
    -------
    x : Pandas series

    """

    # Reshape values by day
    n_rows = len(x)//48
    x_arr = np.reshape(x.values, (n_rows, 48) )

    # Add a 0 value at the beginning of each day for decumulation
    x_arr = np.concatenate(
        ( np.zeros( (n_rows,1) ), x_arr ),
        axis=1)

    # Compute the difference every two items (on the dot), when they are not nans
    diff = np.diff(x_arr[:, ::2])

    # Convert back to half hour timestep and evenly split the quantity
    # over the two half-hours
    tmp = np.repeat(diff, repeats=2, axis=1) / 2

    x[:] = np.reshape(tmp, x.shape)

    return x


def netcdf_to_dataframe(dates, station_name, config_dir, data_folder, dest_folder):
    """ Organize the daily reanalysis file into a single pandas dataframe
    Additionaly, performs a decumulation for variables that require it.

    Parameters
    ----------
    dates : dictionnary that contains a 'start' and 'end' dates
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    dest_folder : string
        Path to folder where netcdf files are saved
    dest_folder : string
        Path to folder where results are saved

    Returns
    -------
    None. Inputs are saved as .csv file in dest_folder

    """

    print('Start handling netcdf files from reanalysis...')

    reanalysis = 'ERA5L'

    variables = {
        ## Instantaneous vars ###

        # Lake variables
        'lake_mix_layer_temperature':
            {'short_name': 'lmlt', 'db_name': 'water_mix_layer_temp', 'unit_conv': lambda x : x - 273.15},

        # Snow variables
        'snow_cover':
            {'short_name': 'snowc', 'db_name': 'snow_cover_frac', 'unit_conv': lambda x : x},
        'snow_density':
            {'short_name': 'rsn', 'db_name': 'snow_density', 'unit_conv': lambda x : x},
        'snow_depth':
            {'short_name': 'sde', 'db_name': 'snow_depth', 'unit_conv': lambda x : x * 1000},
        'snow_depth_water_equivalent':
            {'short_name': 'sd', 'db_name': 'swe', 'unit_conv': lambda x : x * 1000},
        'snowmelt':
            {'short_name': 'smlt', 'db_name': 'snowmelt', 'unit_conv': lambda x : x * 1000},

        # Standard meteorological variables
        '10m_u_component_of_wind':
            {'short_name': 'u10', 'db_name': 'wind_speed_u', 'unit_conv': lambda x : x },
        '10m_v_component_of_wind':
            {'short_name': 'v10', 'db_name': 'wind_speed_v', 'unit_conv': lambda x : x },
        '2m_temperature':
            {'short_name': 't2m', 'db_name': 'air_temp_HMP45C', 'unit_conv': lambda x : x - 273.15},
        '2m_dewpoint_temperature':
            {'short_name': 'd2m', 'db_name': 'air_temp_dewPoint', 'unit_conv': lambda x : x},
        'soil_temperature_level_1':
            {'short_name': 'stl1', 'db_name': 'soil_temp_CS650_1', 'unit_conv': lambda x : x - 273.15},
        'soil_temperature_level_2':
            {'short_name': 'stl2', 'db_name': 'soil_temp_CS650_2', 'unit_conv': lambda x : x - 273.15},
        'volumetric_soil_water_layer_1':
            {'short_name': 'swvl1', 'db_name': 'soil_watercontent_CS650_1', 'unit_conv': lambda x : x},
        'volumetric_soil_water_layer_2':
            {'short_name': 'swvl2', 'db_name': 'soil_watercontent_CS650_2', 'unit_conv': lambda x : x},
        'surface_pressure':
            {'short_name': 'sp', 'db_name': 'air_press_CS106', 'unit_conv': lambda x : x / 100},

        ### Cumulated vars ###

        # Snow variables
        'snowfall':
            {'short_name': 'sf', 'db_name': 'snowfall', 'unit_conv': lambda x : daily_decumulate(x) * 1000},

        # Radiation variables
        'surface_solar_radiation_downwards':
            {'short_name': 'ssrd', 'db_name': 'rad_shortwave_down_CNR4', 'unit_conv': lambda x : daily_decumulate(x) / 1800},
        'surface_thermal_radiation_downwards':
            {'short_name': 'strd', 'db_name': 'rad_longwave_down_CNR4', 'unit_conv': lambda x : daily_decumulate(x) / 1800},
        'surface_net_solar_radiation':
            {'short_name': 'ssr', 'db_name': 'rad_shortwave_net_CNR4', 'unit_conv': lambda x : daily_decumulate(x) / 1800},
        'surface_net_thermal_radiation':
            {'short_name': 'str', 'db_name': 'rad_longwave_net_CNR4', 'unit_conv': lambda x : daily_decumulate(x) / 1800},

        # Precipitation
        'total_precipitation':
            {'short_name': 'tp', 'db_name': 'precipitation', 'unit_conv': lambda x : daily_decumulate(x) * 1000},

        # Fluxes
        'surface_sensible_heat_flux':
            {'short_name': 'sshf', 'db_name': 'H', 'unit_conv': lambda x : -1 * daily_decumulate(x) / 1800},
        'surface_latent_heat_flux':
            {'short_name': 'slhf', 'db_name': 'LE', 'unit_conv': lambda x : -1 * daily_decumulate(x) / 1800},
        'total_evaporation':
            {'short_name': 'e', 'db_name': 'evap', 'unit_conv': lambda x : -1 * daily_decumulate(x) * 1000},
        'potential_evaporation':
            {'short_name': 'pev', 'db_name': 'potential_evap', 'unit_conv': lambda x : -1 * daily_decumulate(x) * 1000},
        'evaporation_from_open_water_surfaces_excluding_oceans':
            {'short_name': 'evaow', 'db_name': 'water_evap', 'unit_conv': lambda x : -1 * daily_decumulate(x) * 1000},
        'evaporation_from_vegetation_transpiration':
            {'short_name': 'evavt', 'db_name': 'plant_evap', 'unit_conv': lambda x : -1 * daily_decumulate(x) * 1000},
              }

    # Load configuration
    config = yaml.safe_load(
        open(os.path.join(config_dir,f'{station_name}_filters.yml')))

    logf = open(os.path.join('.','Logs','reanalysis.log'), "w")

    # Initialize reference reanalysis dataframe. Extend the reference
    # period one day before the start day and one day after the end date.
    # Add 30 min for decumulation.
    d_start = (pd.to_datetime(dates['start'])
               + pd.DateOffset(days = -1)
               + pd.DateOffset(minutes = 30)).strftime('%Y-%m-%d %H:%M:%S')
    d_end = (pd.to_datetime(dates['end'])
             + pd.DateOffset(days = 1)
             + pd.DateOffset(minute = 0)).strftime('%Y-%m-%d %H:%M:%S')
    df = pd.DataFrame( index=pd.date_range(start=d_start, end=d_end, freq='30min') )

    # Expected list of reanalysis files (dates)
    datelist = pd.date_range(start=d_start, end=d_end, freq='D')

    for iDate in datelist:

        file_name = os.path.join(
            data_folder,reanalysis,reanalysis+'_{}.nc'.format(
                iDate.strftime('%Y%m%d')))

        isFilePresent = os.path.isfile(file_name)

        if isFilePresent :
            isFileComplete = os.path.getsize(file_name) > 2e3
        else:
            isFileComplete = False


        if isFilePresent & isFileComplete:

            # Open netcdf file
            rootgrp = ncdf.Dataset(file_name, "r")

            # Retrieve dates and find matching entries in reference Dataframe
            ncdf_time = rootgrp['time']
            f_date = ncdf.num2date(ncdf_time, ncdf_time.units,
                                    ncdf_time.calendar,
                                    only_use_cftime_datetimes=False)

            # Initialize temporary dataframe that will contain netcdf data
            f_date = pd.to_datetime(f_date)
            id_tmp_in_df = f_date.isin(df.index)
            id_df_in_tmp = df.index.isin(f_date)

            # Handle coordinates
            id_long = np.argmin( np.abs(
                (rootgrp['longitude'][:] - config['lon']) ) )
            id_lat = np.argmin( np.abs(
                (rootgrp['latitude'][:] - config['lat']) ) )

            for iVar in variables:
                try:
                    df.loc[id_df_in_tmp, variables[iVar]['db_name']] = rootgrp[
                        variables[iVar]['short_name']][id_tmp_in_df,id_lat,id_long]
                except:
                    logf.write(f'{iVar} not found in file {file_name}\n')

        else:
            logf.write(f'{file_name} not available yet\n')


    # Convert units / decumulate / (interpolate for cumulated vars)
    for iConv in variables:
        try:
            df[variables[iConv]['db_name']] = \
                variables[iConv]['unit_conv'](df[variables[iConv]['db_name']])
        except:
            logf.write(f'Could not convert {iConv}\n')

    # Interpolate for remaining variables
    df = df.interpolate(method='linear', limit=1)

    # Compute wind speed
    df['wind_speed_05103'] = np.sqrt(df['wind_speed_u']**2 +
                               df['wind_speed_v']**2)

    # Compute relative humidity
    p = np.exp( (17.625 * (df['air_temp_dewPoint']-273.15))
               / (243.04 + df['air_temp_dewPoint']-273.15))
    ps = np.exp( (17.625 * df['air_temp_HMP45C'])
               / (243.04 + df['air_temp_HMP45C']))
    df['air_relhum_HMP45C'] = 100*p/ps
    df['air_vpd'] = ps * (1-df['air_relhum_HMP45C']/100)

    # Compute wind direction
    df['wind_dir_05103'] = np.rad2deg(np.arctan2(
        df['wind_speed_v'],df['wind_speed_u'])) + 180

    # Compute albedo and outward radiations
    df['rad_shortwave_up_CNR4'] = \
        df['rad_shortwave_down_CNR4'] - df['rad_shortwave_net_CNR4']

    df['albedo_CNR4'] = np.nan
    id_daylight = df['rad_shortwave_down_CNR4'] > 50
    df.loc[id_daylight, 'albedo_CNR4'] = \
        df.loc[id_daylight,'rad_shortwave_up_CNR4'] \
            / df.loc[id_daylight,'rad_shortwave_down_CNR4']

    df['rad_longwave_up_CNR4'] = \
        df['rad_longwave_down_CNR4'] - df['rad_longwave_net_CNR4']

    # Change timezone (from UTC to UTC-5)
    df.insert(0, 'timestamp', df.index)
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    df['timestamp'] = df['timestamp'].dt.tz_convert('EST')
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    df.index = df['timestamp']

    # Set to nan artefacts created by decumulation
    df.loc[df['air_temp_HMP45C'].last_valid_index():,
           df.columns != 'timestamp'] = np.nan

    # Realign dates on reference dataframe
    d_start = pd.to_datetime(dates['start']).strftime('%Y-%m-%d')
    d_end = pd.to_datetime(dates['end']).strftime('%Y-%m-%d')
    df_ref = pd.DataFrame( index=pd.date_range(start=d_start, end=d_end, freq='30min') )
    df_ref = df_ref.join(df)
    df_ref['timestamp'] = df_ref.index

    # Save
    df_ref.to_csv(os.path.join(
        dest_folder,reanalysis + '_' + station_name + '.csv'), index=False)

    print('Done!\n')

    # Close error log file
    logf.close()
