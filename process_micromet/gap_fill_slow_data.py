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

    return df