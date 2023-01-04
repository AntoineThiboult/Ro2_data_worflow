# -*- coding: utf-8 -*-
import cdsapi
import os
import time
import pandas as pd
from calendar import monthrange
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

    for day in range(1,monthrange(ymd.year,ymd.month)[1]+1):
        export_name = os.path.join(
                    dataset['dest_folder'],
                    dataset['dest_subfolder'],
                    dataset['dest_subfolder'] + '_{}{}{:02}.nc'.format(year, month,day))
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
    datelist = pd.date_range(start=dates['start'], end=dates['end'], freq='M')
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
        'relative_humidity',
        'specific humidity',
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