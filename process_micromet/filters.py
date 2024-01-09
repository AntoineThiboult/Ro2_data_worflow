# -*- coding: utf-8 -*-
import os

import pandas as pd
import numpy as np
import yaml
import pysolar # conda install -c conda-forge pysolar


def apply_all(stationName,df,filter_config_dir,proxy_data_dir):

    # Get station information for filtering
    config = get_station_info(stationName,filter_config_dir)

    # Radiations
    if config['radiation']:
        df = radiation(df, config['lat'], config['lon'])

    # Bandpass filter
    for var in config['flux_vars']+config['strg_vars']+config['grnd_vars']:
        id_band = band_pass(df,var)
        df = remove_flux_and_storage(df,var,id_band)

    # Low quality fluxes (Mauder flags)
    for var in config['flux_vars']:
        id_low_qf = low_quality_flux(df, var)
        df = remove_flux_and_storage(df,var,id_low_qf)

    # Remove low RSSI
    for var in config['flux_vars']+config['strg_vars']:
        id_rssi = low_rssi(df,var)
        df = remove_flux_and_storage(df,var,id_rssi)

    # Remove gas fluxes when WPL correction not available
    for var in config['carbon_vars']:
        id_wpl = missing_wpl(df,var)
        df = remove_flux_and_storage(df,var,id_wpl)

    # Remove rainy events
    id_rain = rainfall_events(df,config['proxy_stations'],proxy_data_dir)
    for var in config['flux_vars']+config['strg_vars']:
        df = remove_flux_and_storage(df,var,id_rain)

     # Remove spikes
    for var in config['flux_vars']:
        id_spike = spikes(df,var)
        df = remove_flux_and_storage(df,var,id_spike)

    # Remove large energy violation if flux variables available
    if config['energy_balance']:
        id_balance = energy_balance_violation(
            df,config['proxy_stations'],proxy_data_dir)
        for var in ['LE','H']:
            df = remove_flux_and_storage(df,var,id_balance)

    # Remove flux below the friction velocity threshold for carbon
    for var in config['friction_vel']['vars']:
        id_fric_vel, fvt_series = run_friction_velocity_threshold(
            df,var,config['friction_vel']['temperature_var'],True)
        df = remove_flux_and_storage(df,var,id_fric_vel)
        df['friction_vel_thresholds'] = fvt_series

    return df

def get_station_info(stationName,filter_config_dir):
    config = yaml.safe_load(
        open(os.path.join(filter_config_dir,f'{stationName}_filters.yml')))
    return config


def proxy_station_loader(proxy_station,proxy_data_dir,proxy_var):
    # Load proxy files until it contains variable information
    for proxy in proxy_station:
        df_proxy = pd.read_csv(os.path.join(
            proxy_data_dir, f'{proxy}.csv'), low_memory=False)
        if proxy_var in df_proxy.columns:
            break
    return df_proxy


def radiation(df,lat,lon):
    """Filters radiations
        - cap downward short wave solar radiations with theoretical max value
        - filter out downward short wave solar radiations that are affected
            by snow on sensor during daytime
        - set all negative measurements to 0
        - recompute albedo
        - cap downward short wave solar radiations with albedo
        - removes suspicious upwelling shortwave radiations
        - recompute net radiation

    Parameters
    ----------
    df : Pandas Dataframe
         Pandas dataframe that contains
            'rad_longwave_down_CNR4','rad_longwave_up_CNR4',
            'rad_shortwave_down_CNR4','rad_shortwave_up_CNR4' variables
    lat : Float
          Latitude of the measurement site
    lon : Float
          Longitude of the measurement site

    Returns
    -------
    df : Pandas Dataframe
        Pandas Dataframe with radiation filtered out and corrected
    """

    # Computes sun angle and max theorethical downward shortwave values
    df['solar_angle'] = np.nan
    rad_short_down_max = np.zeros((df.shape[0],))
    dates = df['timestamp'].dt.tz_localize('Etc/GMT+5').dt.to_pydatetime()
    for counter, iDate in enumerate(dates):
        altitude_deg = pysolar.solar.get_altitude(
            lat,lon,iDate)
        altitude_deg = max(0, altitude_deg)
        df.loc[counter,'solar_angle'] = altitude_deg
        max_rad = 1370 * np.sin(np.deg2rad(altitude_deg))
        rad_short_down_max[counter] = max_rad

    # Filter unplausible downward short wave solar radiations
    id_sub = df['rad_shortwave_down_CNR4'] > rad_short_down_max
    df.loc[id_sub,'rad_shortwave_down_CNR4'] = rad_short_down_max[id_sub]
    id_sub = df['rad_shortwave_down_CNR4'] < 0
    df.loc[id_sub,'rad_shortwave_down_CNR4'] = 0

    # Filter upward short wave solar radiations
    id_sub = df['rad_shortwave_up_CNR4'] < 0
    df.loc[id_sub,'rad_shortwave_up_CNR4'] = 0
    id_sub = df['rad_shortwave_down_CNR4'] == 0
    df.loc[id_sub,'rad_shortwave_up_CNR4'] = 0

    # Filter downward radiation for snow obstruction during daytime
    id_sub = (df['rad_shortwave_up_CNR4'] >
              (0.85 * df['rad_shortwave_down_CNR4'])) \
              & (df['rad_shortwave_up_CNR4'] > 25*0.85)
    df.loc[id_sub,'rad_shortwave_down_CNR4'] = np.nan
    df.loc[id_sub,'rad_longwave_down_CNR4'] = np.nan

    # Filter erroneous albedo
    id_albedo = (df['rad_shortwave_down_CNR4'] > 25) & \
        (df['rad_shortwave_down_CNR4'] > df['rad_shortwave_up_CNR4'])

    df.loc[id_albedo,'rolling_albedo'] = \
        df.loc[id_albedo,'rad_shortwave_up_CNR4'].rolling(
            window=48*2,min_periods=12,center=True).median() \
            / df.loc[id_albedo,'rad_shortwave_down_CNR4'].rolling(
                window=48*2,min_periods=12,center=True).median()

    df['rolling_albedo'] = df['rolling_albedo'].interpolate()
    id_sub = (df['rad_shortwave_up_CNR4'] >
              (0.90 * df['rad_shortwave_down_CNR4']))
    df.loc[id_sub,'rad_shortwave_up_CNR4'] = \
        df.loc[id_sub,'rad_shortwave_down_CNR4'] * \
            df.loc[id_sub,'rolling_albedo']
    df=df.drop(columns=['rolling_albedo'])

    # Recompute albedo
    id_daylight = df['rad_shortwave_down_CNR4'] > 25
    df['albedo_CNR4'] = np.nan
    df.loc[id_daylight, 'albedo_CNR4'] = \
        df.loc[id_daylight,'rad_shortwave_up_CNR4'] \
            / df.loc[id_daylight,'rad_shortwave_down_CNR4']

    # Recompute net radiation
    df['rad_net_CNR4'] = \
        df['rad_shortwave_down_CNR4'] + df['rad_longwave_down_CNR4'] \
            - df['rad_shortwave_up_CNR4'] - df['rad_longwave_up_CNR4']

    return df


def band_pass(df, spiky_var):
    """Detect outliers according to a passband filter specific to each variable.

    Parameters
    ----------
    df: pandas DataFrame that contains the spiky variable
    spiky_var: string that designate the spiky variable

    Returns
    -------
    id_outlier: index of outliers"""

    if spiky_var in ['LE','LE_corr']:
        id_bandpass = ( df[spiky_var] < -35 ) | ( df[spiky_var] > 300 )    # in [W+1m-2]
    elif spiky_var in ['H','H_corr']:
        id_bandpass = ( df[spiky_var] < -150 ) | ( df[spiky_var] > 500 )    # in [W+1m-2]
    elif spiky_var == 'CO2_flux':
        id_bandpass = ( df[spiky_var] < -20 ) | ( df[spiky_var] > 20 )    # in [µmol+1s-1m-2]
    elif spiky_var == 'CH4_flux':
        id_bandpass = ( df[spiky_var] < -0.1 ) | ( df[spiky_var] > 0.25 )    # in [µmol+1s-1m-2]
    elif spiky_var == 'LE_strg':
        id_bandpass = ( df[spiky_var] < -60 ) | ( df[spiky_var] > 60 )    # in [W+1m-2]
    elif spiky_var == 'H_strg':
        id_bandpass = ( df[spiky_var] < -50 ) | ( df[spiky_var] > 50 )    # in [W+1m-2]
    elif spiky_var == 'CO2_strg':
        id_bandpass = ( df[spiky_var] < -8 ) | ( df[spiky_var] > 8 )    # in [µmol+1s-1m-2]
    elif spiky_var == 'CH4_strg':
        id_bandpass = ( df[spiky_var] < -0.05 ) | ( df[spiky_var] > 0.05 )    # in [µmol+1s-1m-2]
    elif 'soil_watercontent' in spiky_var:
        id_bandpass = ( df[spiky_var] < 0 ) | ( df[spiky_var] > 1 )    # fraction
    elif 'soil_temp' in spiky_var:
        id_bandpass = ( df[spiky_var] < -20 ) | ( df[spiky_var] > 30 )    # in °C
    elif 'soil_heatflux' in spiky_var:
        id_bandpass = ( df[spiky_var] < -25 ) | ( df[spiky_var] > 100 )    # in W/m2
    elif 'soil_electricconductivity' in spiky_var:
        id_bandpass = ( df[spiky_var] < 0 ) | ( df[spiky_var] > 15 )    # in dS/m
    elif 'air_temp' in spiky_var:
        id_bandpass = ( df[spiky_var] < -50 ) | ( df[spiky_var] > 50 )    # in dS/m
    return id_bandpass


def spikes(df, spiky_var, sliding_window=624, z=5, daynight=False):

    """Detect spikes on slow data according to Papale et al. (2006)

    Parameters
    ----------
    df: pandas DataFrame that contains the spiky variable, a daytime key
        (day=1, night=0)
    spiky_var: string that designate the spiky variable
    sliding_window: sliding window used to compute the moving median (default
                    value = 13*48 -- 13 days in total)
    z: discrimination factor. Default value = 4. Increasing value retains more
       data (up to 7)
    daynight: if true, handle day/night time separately. Default=False. For
              carbon fluxes, it is recommended to switch it to 'True'

    Returns
    -------
    id_outlier: index of outliers

    See also
    --------
    Papale et al. (2006) Towards a standardized processing of Net Ecosystem
    Exchange measured with eddy covariance technique: algorithms and
    uncertainty estimation. Biogeosciences, European Geosciences Union, 2006,
    3 (4), pp.571-583."""

    # Clean the data
    nee = df.dropna(subset=[spiky_var])[spiky_var]

    if daynight: # Filter day and night separately
        daytime = df.dropna(subset=[spiky_var])['daytime']
        nee_day = nee[daytime==1]
        nee_night = nee[daytime==0]

        # Identify outliers during day time
        di = nee_day.diff(periods=1) + nee_day.diff(periods=-1)
        Md = di.rolling(window=sliding_window, center=True, min_periods=1).median()
        MAD = abs(di-Md).median()
        lowerBound = Md - (z*MAD / 0.6745)
        upperBound = Md + (z*MAD / 0.6745)
        id_outlier_day = ( di < lowerBound ) | ( di > upperBound )

        # Identify outliers during night time
        di = nee_night.diff(periods=1) + nee_night.diff(periods=-1)
        Md = di.rolling(window=sliding_window, center=True, min_periods=1).median()
        MAD = abs(di-Md).median()
        lowerBound = Md - (z*MAD / 0.6745)
        upperBound = Md + (z*MAD / 0.6745)
        id_outlier_night = ( di < lowerBound ) | ( di > upperBound )

        # Merge the two outlier indices
        id_outlier = pd.concat([id_outlier_day,id_outlier_night]).sort_index()

    else: # Filter day and night as a whole
        # Identify outliers during day time
        di = nee.diff(periods=1) + nee.diff(periods=-1)
        Md = di.rolling(window=sliding_window, center=True, min_periods=1).median()
        MAD = abs(di-Md).median()
        lowerBound = Md - (z*MAD / 0.6745)
        upperBound = Md + (z*MAD / 0.6745)
        id_outlier = ( di < lowerBound ) | ( di > upperBound )

    # Reindex because of missing values (dropna())
    id_outlier = id_outlier.reindex(df.index, fill_value=False)

    # return id_outlier, lowerBound, upperBound, Md, MAD, di
    return id_outlier


def rainfall_events(df, proxy_station,proxy_data_dir):
    # Remove timestep where rain is measured
    if 'precip_TB4' in df.columns:
        id_rain = df['precip_TB4'] > 0
    else:
        # Load proxy files that contain rain information
        df_proxy = proxy_station_loader(proxy_station, proxy_data_dir, 'precip_TB4')
        id_rain = df_proxy['precip_TB4'] > 0
    return id_rain


def low_rssi(df, var):

    """Detects low rssi values

    Parameters
    ----------
    df: pandas DataFrame that contains the spiky variable
    var: string that designate the variable that should be filtered for its RSSI

    Returns
    -------
    id_rssi: index of low RSSI"""

    rssi_thresholds = {'H':   {'type': 'H2O', 'value':0.7},
                       'LE':  {'type': 'H2O', 'value':0.7},
                       'CO2': {'type': 'CO2', 'value':0.7},
                       'CH4': {'type': 'CH4', 'value':0.15}}

    var_grp = var.split('_')[0]
    var_rssi = f"rssi_{rssi_thresholds[var_grp]['type']}"

    if var_rssi in df.columns:
        id_low_rssi = df[var_rssi] < rssi_thresholds[var_grp]['value']
    else:
        id_low_rssi = []

    return id_low_rssi


def missing_wpl(df, var):
    """Detects values where WPL couldn't be applied

    Parameters
    ----------
    df: pandas DataFrame that contains the spiky variable
    var: string that designate the variable that should be filtered for lack
        of WPL correction

    Returns
    -------
    id_missing_wpl: index of for which WPL could not be applied"""

    if var in ['CO2_flux','CO2_strg','CH4_flux','CH4_strg']:
        id_missing_wpl = df['H2O_mixing_ratio'].isna()
    else:
        id_missing_wpl = []

    return id_missing_wpl


def low_quality_flux(df,var):
    # Remove low quality time step (Mauder et Folken 2004)
    id_low_quality = df[var+'_qf'] == 2
    return id_low_quality


def energy_balance_violation(df,proxy_station,proxy_data_dir):
    # Energy balance violation (i.e., H+λE > 5Rn). Only for forested stations
    if 'rad_net_CNR4' in df.columns:
        id_balance = \
            (np.abs(df['H'] + df['LE']) > np.abs(5 * df['rad_net_CNR4'])) \
                & (df['rad_net_CNR4'] > 50)
    else:
        # Load proxy files that contain radiation information
        df_proxy = proxy_station_loader(proxy_station, proxy_data_dir, 'rad_net_CNR4')
        id_balance = \
            (np.abs(df['H'] + df['LE']) > np.abs(5 * df_proxy['rad_net_CNR4'])) \
                & (df_proxy['rad_net_CNR4'] > 50)
    return id_balance


def remove_flux_and_storage(df,var,id_rm):
    # Set target values to NaN
    df.loc[id_rm,var] = np.nan

    # If var is a storage variable, remove the corresponding flux
    if '_strg' in var:
        df.loc[id_rm, var.split('_strg')[0]] = np.nan
    # If var is a flux, remove the corresponding storage
    elif f'{var}_strg' in df.columns:
        df.loc[id_rm,f'{var}_strg'] = np.nan

    return df


def run_friction_velocity_threshold(df, flux_var, air_temp_var, fvt_values=False):
    """Compute friction velocity threshold per season with bootstrap

    Parameters
    ----------
    df: pandas DataFrame that contains the flux variable
    flux_var: string that designate the flux variable
    air_temp_var: string that designate a cleaned variable temperature
    fvt_values: Boolean swith (True/False) to return or not the friction
        velocity values as time series

    Returns
    id_below_fvt: index of fluxes below friction velocity threshold"""

    # Remove daytime and NaN
    mask = ~( (df['daytime'] == 1)
                | df[air_temp_var].isna()
                | df['friction_velocity'].isna())

    fvt = seasonal_friction_vel_threshold(df.loc[mask], flux_var, air_temp_var)

    # Get all seasonal indexes to one single array of boolean
    index_below_fvt = pd.Series([False] * len(df))
    for s in fvt:
        if fvt[s]['fvt']:
            index_below_fvt[fvt[s]['id_below_fvt'].index] = \
                fvt[s]['id_below_fvt'].values

    if not fvt_values:
        return index_below_fvt

    # Produce a time series containing friction velocity thresholds
    fvt_series = pd.Series([np.nan] * len(df))
    for s in fvt:
        if fvt[s]['fvt']:
            index_season = df[
                df['timestamp'].dt.month.isin( fvt[s]['months'] )].index
            fvt_series[index_season] = fvt[s]['fvt']

    return index_below_fvt, fvt_series


def seasonal_friction_vel_threshold(df, flux_var, air_temp_var):
    """Compute friction velocity threshold per season with bootstrap

    Parameters
    ----------
    df: pandas DataFrame that contains the flux variable
    flux_var: string that designate the flux variable
    air_temp_var: string that designate a cleaned variable temperature

    Returns
    -------
    seasonal_fvt: dictionary that contains seasonal friction velocity thresholds
    with the following keys:
        - months: months corresponding to the seasons
        - fvt: median friction velocity threshold
        - fvt_ci: confidence interval of the friction velocity threshold
        - id_below_fvt : index of fluxes below the median friction velocity
          threshold.

    See also
    --------
    Doc of find_friction_vel_threshold()"""

    # Create a dictionary that will store results by seasons
    seasonal_fvt = {
        'winter': {'months':range(1,4), 'fvt':[], 'fvt_ci':[], 'id_below_fvt':[]},
        'spring': {'months':range(4,7), 'fvt':[], 'fvt_ci':[], 'id_below_fvt':[]},
        'summer': {'months':range(7,10), 'fvt':[], 'fvt_ci':[], 'id_below_fvt':[]},
        'autumn': {'months':range(10,13), 'fvt':[], 'fvt_ci':[], 'id_below_fvt':[]}
        }

    for s in seasonal_fvt:
        # Select months
        index_season = df['timestamp'].dt.month.isin(seasonal_fvt[s]['months'])

        if sum(index_season) > 20*6*2:
            seasonal_fvt[s]['fvt'], seasonal_fvt[s]['fvt_ci'] = \
                bootstrap_fric_vel_threshold(
                    df.loc[index_season], flux_var, air_temp_var)

            seasonal_fvt[s]['id_below_fvt'] = \
                df.loc[index_season, 'friction_velocity'] < seasonal_fvt[s]['fvt']

    return seasonal_fvt


def bootstrap_fric_vel_threshold(df, flux_var, air_temp_var, n_bootstrap=100):
    """Perform bootstrap friction velocity analysis

    Parameters
    ----------
    df: pandas DataFrame that contains the flux variable
    flux_var: string that designate the flux variable
    air_temp_var: string that designate a cleaned variable temperature
    n_bootstrap: number of times bootstraping should be performed (default=100)

    Returns
    -------
    id_below_thresh: index of fluxes below the identified friction velocity
        threshold
    fric_vel_threshold_median: friction velocity threshold (median over
        temperature bins)
    fric_vel_threshold_ci: 5%-95% percetiles friction velocity threshold
        confidance interavls

    See also
    --------
    Doc of find_friction_vel_threshold()"""

    bs_friction_vel_threshold = []

    for i_bootstrap in range(n_bootstrap):

        # Randomly select subset of dataframe for bootstrap
        bs_index = np.random.choice(df.index, int(len(df)/2), replace=False)

        friction_vel_threshold = find_friction_vel_threshold(
            df.loc[bs_index], flux_var, air_temp_var)

        bs_friction_vel_threshold.append(friction_vel_threshold)

    # Compute statistics on the friction velocity thresholds
    fric_vel_threshold_median = np.median(bs_friction_vel_threshold)
    fric_vel_threshold_ci = np.quantile(bs_friction_vel_threshold,[0.05, 0.95])

    return fric_vel_threshold_median, fric_vel_threshold_ci


def find_friction_vel_threshold(df, flux_var, air_temp_var):
    """Identify the friction velocity threshold below which data should be
    discarded according to Papale et al. (2006)

    Parameters
    ----------
    df: pandas DataFrame that contains the flux variable,
    flux_var: string that designate the flux variable
    air_temp_var: string that designate a cleaned variable temperature

    Returns
    -------
    threshold: friction velocity threshold (median)

    See also
    --------
    Papale et al. (2006) Towards a standardized processing of Net Ecosystem
    Exchange measured with eddy covariance technique: algorithms and
    uncertainty estimation. Biogeosciences, European Geosciences Union, 2006,
    3 (4), pp.571-583."""

    # Define the number of quantiles and u* classes
    n_quantiles = 6
    n_u_classes = 20

    # Compute the quantiles of air_temp
    quantiles = pd.qcut(df[air_temp_var], q=n_quantiles, labels=False)

    # Initialize a list to store the thresholds for each temperature class
    thresholds = []

    # Loop over the temperature classes
    for i in range(n_quantiles):
        # Get the data for the current temperature class
        data = df.loc[quantiles == i]

        # Compute the u* classes
        u_classes, u_bins = pd.qcut(data['friction_velocity'], q=n_u_classes,
                                    labels=False, retbins=True)

        # Compute the average night-time flux for each u* class
        fluxes = df[flux_var].groupby(u_classes).mean()

        # Check if the flux that correspond to the i u* class is greater
        # than 99% of the average flux at the higher u∗-classes
        for i_u_class in range(n_u_classes):

            if fluxes[i_u_class] >= np.mean(fluxes[i_u_class+1:i_u_class+11]) * 0.99:

                # Check the correlation between temperature and u* classes
                r = np.abs(np.corrcoef(data[air_temp_var], data['friction_velocity'])[0, 1])

                # Store the threshold if the correlation is weak
                if r < 0.4:
                    thresholds.append(u_bins[i_u_class])
                    break

    threshold = np.median(thresholds)

    return threshold
