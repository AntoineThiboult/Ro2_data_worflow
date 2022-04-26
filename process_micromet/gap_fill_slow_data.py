import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn import linear_model


def train_rf(target_var, input_vars):
    """ Train a random forest regressor

    Parameters
    ----------
    target_var : numpy array (n,)
        target variable (measurements made in situ)
    input_vars : numpy array (n,m)
        input variables (era5 data and insightful variables)

    Returns
    -------
    scalerX : Scikit scaler object
        Scaler of inputs
    scalery : Scikit scaler object
        Scaler of output
    regr : Scikit model object
        Scikit random forest fitted to target
    """

    mask = ~np.isnan(np.column_stack((target_var,input_vars))).any(axis=1)

    X_unscaled = input_vars[mask,:]
    y_unscaled = target_var[mask,np.newaxis]

    scalerX = StandardScaler().fit(X_unscaled)
    scalery = StandardScaler().fit(y_unscaled)

    X = scalerX.transform(X_unscaled)
    y = scalery.transform(y_unscaled)

    regr = RandomForestRegressor(n_estimators=50,random_state=42)
    regr.fit(X, y.flatten())

    return scalerX, scalery, regr


def predict_rf(scalerX, scalery, regr, input_vars):
    """ Predict values with a trained random forest model

    Parameters
    ----------
    scalerX : Scikit scaler object
        Scaler of inputs
    scalery : Scikit scaler object
        Scaler of output
    regr : Scikit model object
        Scikit random forest fitted to target
    input_vars : numpy array (n,m)
        Input variables (era5 data and insightful variables)

    Returns
    -------
    y_pred : numpy array (n,)
        Predicted variable (ERA5 variable corrected with in situ measurements)
    """

    mask = ~np.isnan(input_vars).any(axis=1)
    y_pred = np.zeros((mask.shape[0])) * np.nan

    X = scalerX.transform(input_vars[mask,:])

    y_pred_unindex = scalery.inverse_transform(
        np.expand_dims( regr.predict(X), axis=1))

    y_pred[mask] = y_pred_unindex.flatten()

    return y_pred


def pairwise(iterable):
    """
    Routine to iter over pairs
    s -> (s0, s1), (s2, s3), (s4, s5), ...
    """
    a = iter(iterable)
    return zip(a, a)


def gap_fill_water_temp(df):
    """
    Create an average thermistor chain and gap fill missing data.
    Gaps are filled with several technics applied in the following order
    1) Water surface temperature is derived from linear regression as
        implemented in merge_thermistor.py
    2) Water temperature are derived from a linear regression with
        temperature above and below target.
    3) Remaining missing data are filled with yearly averaged temperature
        to which a linear detrending is applied to ensure reconnection with
        measurements at both ends of the gap.
    4) Remaining missing data are filled with linear interpolation

    Parameters
    ----------
    df : Pandas DataFrame
        Dataframe that contains water temperature variable with the following
        format: water_temp_<d>m<d>_Therm1

    Returns
    -------
    df : Pandas DataFrame
        Dataframe that contains gapfilled temperature variable with the
        following format: water_temp_<d>m<d>_avg

    """

    therm_depths_short = np.array(
        [0, 0.2, 0.4, 0.6, 0.8, 1, 1.4, 1.8, 2.2,
         2.6, 3, 4, 5, 6, 7, 8, 9, 10, 12.5, 15])

    therm_depths_long = np.array(
        [0, 0.2, 0.4, 0.6, 0.8, 1, 1.4, 1.8, 2.2,
         2.6, 3, 4, 5, 6, 7, 8, 9, 10, 12.5, 15,
         20, 30, 40, 50, 60, 70])

    # Names of temperature variables
    therm_depths_names_T1 = ['water_temp_{:d}m{:d}_Therm1'.format(
        int(f), int(np.round((f-np.fix(f))*10))) for f in therm_depths_short ]
    therm_depths_names_T2 = ['water_temp_{:d}m{:d}_Therm2'.format(
        int(f), int(np.round((f-np.fix(f))*10))) for f in therm_depths_long ]
    therm_depths_names = ['water_temp_{:d}m{:d}_avg'.format(
        int(f), int(np.round((f-np.fix(f))*10))) for f in therm_depths_long ]

    # Construct mean temperature dataset
    for count, iVar in enumerate(therm_depths_names):
        if count < len(therm_depths_names_T1):
            df[iVar] = df[[
                therm_depths_names_T1[count],therm_depths_names_T2[count]
                ]].mean(axis=1)
        else:
            df[iVar] = df[therm_depths_names_T2[count]]


    ######################################
    ### Water surface temp replacement ###
    ######################################

    index_na = df.index[df['water_temp_0m0_avg'].isna()]
    df.loc[index_na,'water_temp_0m0_avg'] = df.loc[index_na,'water_temp_sfc']

    ##############################
    ### Vertical extrapolation ###
    ##############################

    for counter, iVar in enumerate(therm_depths_names):

        if df[iVar].isna().sum() == 0:
            continue

        if (counter > 0) & (counter < len(therm_depths_names)-1):
            # Explanatory variables (temperature above and below target)
            list_var_reg = [
                therm_depths_names[counter-1],
                therm_depths_names[counter+1]
                ]
        elif counter == 0:
            # Explanatory variables (temperature below target)
            list_var_reg = [
                therm_depths_names[counter+1],
                therm_depths_names[counter+2]
                ]
        elif counter == len(therm_depths_names)-1:
            # Explanatory variables (temperature above target)
            list_var_reg = [
                therm_depths_names[counter-1],
                therm_depths_names[counter-2]
                ]

        # Data preprocessing
        mask = ~df[list_var_reg+[iVar]].isna().any(axis=1)
        X = df.loc[mask,list_var_reg].values
        y = df.loc[mask,iVar].values

        # Perform regression
        reg =  linear_model.LinearRegression()
        reg.fit(X,y)

        # Predict
        mask = ~df[list_var_reg].isna().any(axis=1) & df[iVar].isna()
        if mask.sum()>0:
            df.loc[mask,iVar] = reg.predict(
                df.loc[mask,list_var_reg].values)
            df.loc[df[iVar]<0,iVar] = 0

    #################################################################
    ### Replacement with yearly averaged data corrected for shift ###
    #################################################################

    for iVar in therm_depths_names:

        if df[iVar].isna().sum() == 0:
            continue

        yearly_avg = df[iVar].groupby([
            df['timestamp'].dt.month,
            df['timestamp'].dt.day,
            df['timestamp'].dt.hour,
            df['timestamp'].dt.minute]).transform('mean')

        # Get starting and ending index of NaN chunks
        id_chunks = df.index[
            df[iVar].isna().astype(int).diff().abs() > 0 ]

        if np.isnan(df.loc[df.index[0], iVar]):
            id_chunks = pd.Int64Index([df.index[0]+1]).append(id_chunks)
        elif np.isnan(df.loc[df.index[-1], iVar]):
            id_chunks = id_chunks.append(pd.Int64Index([df.index[-1]]))

        # Loop of NaN chunks for yearly averaged filling
        for id_start, id_end in pairwise(id_chunks):

            id_start -= 1
            start_offset = df.loc[id_start,iVar] - yearly_avg[id_start]
            end_offset = df.loc[id_end,iVar] - yearly_avg[id_end]

            if np.isnan(start_offset):
                start_offset = 0
            if np.isnan(end_offset):
                end_offset = 0

            replace_chunk = \
                np.linspace(start_offset, end_offset, id_end-id_start) \
                    + yearly_avg[id_start:id_end]

            df.loc[id_start:id_end,iVar] = replace_chunk

    ######################################
    ### Last hope linear extrapolation ###
    ######################################

    df[therm_depths_names] = df[therm_depths_names].interpolate(method='linear')

    return df


def gap_fill_slow_data(station_name, df, dataFileDir):
    """ Gap fill essential data. Linear interpolation is first used with a
    limit window that depends on the variable. The remaining gaps are filled
    with the ERA5 reanalysis data. The reanalysis is corrected with in situ
    measurements via a random forest regressor.

    Parameters
    ----------
    df : pandas DataFrame
        DataFrame that contains the relevant variables
    station_name : string
        Name of the station

    Returns
    -------
    None.

    """

    # Declaration of variables and  maximum window length for linear interpolation
    station_infos = {

        'Water_stations':  {
            'proxy':
                 os.path.join(dataFileDir,'ERA5_Water_stations'),
            'var_to_fill':
                {'air_temp_HMP45C':6,
                 'rad_longwave_down_CNR4':6,
                 'rad_longwave_up_CNR4':6,
                 'rad_shortwave_down_CNR4':4,
                 'rad_shortwave_up_CNR4':4,
                 'wind_speed_05103':6,
                 'wind_dir_05103':6,
                 'air_specificHumidity':12,
                 'air_relativeHumidity':12}},

        'Forest_stations':  {
            'proxy':
                 os.path.join(dataFileDir,'ERA5_Water_stations'),
             'var_to_fill':
                 {'air_temp_HMP45C':6,
                  'rad_longwave_down_CNR4':6,
                  'rad_longwave_up_CNR4':6,
                  'rad_shortwave_down_CNR4':4,
                  'rad_shortwave_up_CNR4':4,
                  'wind_speed_05103':6,
                  'wind_dir_05103':6,
                  'air_specificHumidity':12,
                  'air_relativeHumidity':12,
                  'air_press_CS106':24,
                  'soil_temp_CS650_1':48,
                  'soil_temp_CS650_2':48,
                  'soil_watercontent_CS650_1':96,
                  'soil_watercontent_CS650_2':96,
                  'soil_heatflux_HFP01SC_1': 96}}

                     }


    ############################
    ### Linear interpolation ###
    ############################

    for i_var in station_infos[station_name]['var_to_fill']:

        # Perform linear interpolation with a window specified by variable type
        df[i_var]= df[i_var].interpolate(
            method='linear',
            limit=station_infos[station_name]['var_to_fill'][i_var])


    ########################
    ### Machine learning ###
    ########################

    # Create variables suitable for ML
    df['doy_t'] = np.cos(df['timestamp'].dt.dayofyear/366*2*np.pi)
    df['hour_t'] = np.cos(df['timestamp'].dt.hour/24*2*np.pi)

    # Load meteorological reanalysis
    df_era = pd.read_csv(station_infos[station_name]['proxy']
                           + '.csv')


    for i_var in station_infos[station_name]['var_to_fill']:

        if i_var in df_era.columns:

            if df[i_var].isna().sum() != 0:

                # Input and target variable selection
                if i_var == 'air_temp_HMP45C':
                    input_ml_vars = np.column_stack(
                        (df[['doy_t','hour_t']].values, df_era[i_var].values))
                else:
                    input_ml_vars = np.column_stack(
                        (df[['doy_t','hour_t','air_temp_HMP45C']].values, df_era[i_var].values))
                target_ml_var = df[i_var].values

                # Training
                scalerX, scalery, regr = train_rf(target_ml_var, input_ml_vars)

                # Prediction
                y_pred =  predict_rf(scalerX, scalery, regr, input_ml_vars)
                if i_var in ['soil_temp_CS650_1','soil_temp_CS650_2']:
                    y_pred =  pd.Series(y_pred).rolling(
                        window=6, min_periods=1).mean().values
                id_na = df[i_var].isna()
                df.loc[id_na,i_var] = y_pred[id_na]

        else:
            print(f'{i_var} not available in the ERA5 database')

    # Recompute albedo and rad_net
    df['albedo_CNR4'] = np.nan
    id_daylight = df['rad_shortwave_down_CNR4'] > 50
    df.loc[id_daylight, 'albedo_CNR4'] = \
        df.loc[id_daylight,'rad_shortwave_up_CNR4'] \
            / df.loc[id_daylight,'rad_shortwave_down_CNR4']

    df['rad_net_CNR4'] = \
        df['rad_shortwave_down_CNR4'] - df['rad_shortwave_up_CNR4'] \
        + df['rad_longwave_down_CNR4'] - df['rad_longwave_up_CNR4']

    if station_name == 'Forest_stations':
        # TODO find better way to hand missing soil_heatflux_HFP01SC_1
        id_na = df['soil_heatflux_HFP01SC_1'].isna()
        df.loc[id_na,'soil_heatflux_HFP01SC_1'] = 0

    #########################
    ### Water temperature ###
    #########################

    if station_name == 'Water_stations':
        df = gap_fill_water_temp(df)

    return df