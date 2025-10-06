import os
import pandas as pd
import numpy as np
import yaml
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor

def load_gap_fill_config(gf_config_dir,station):
    """
    Load gap filling configuration for slow data

    Parameters
    ----------
    gf_config_dir : String
        Path to the directory that contains the .yml file. The name of the
        file should have a format: {station}_lake_slow_data.yml
    station : String
        Name of the station

    Returns
    -------
    config : Dict
        Dictionary that contains the configuration for gap filling slow data

    """
    config = yaml.safe_load(
        open(os.path.join(gf_config_dir,f'{station}_slow_data.yml')))
    return config


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

    mask = ~np.isnan(np.column_stack((target_var,input_vars))).any(axis=1) \
        & np.isfinite(np.column_stack((target_var,input_vars))).all(axis=1)

    X_unscaled = input_vars[mask,:]
    y_unscaled = target_var[mask,np.newaxis]

    scalerX = StandardScaler().fit(X_unscaled)
    scalery = StandardScaler().fit(y_unscaled)

    X = scalerX.transform(X_unscaled)
    y = scalery.transform(y_unscaled)

    regr = RandomForestRegressor(n_estimators=25,random_state=42)
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

    mask = ~np.isnan(input_vars).any(axis=1) \
        & np.isfinite(input_vars).all(axis=1)
    y_pred = np.zeros((mask.shape[0])) * np.nan

    X = scalerX.transform(input_vars[mask,:])

    y_pred_unindex = scalery.inverse_transform(
        np.expand_dims( regr.predict(X), axis=1))

    y_pred[mask] = y_pred_unindex.flatten()

    return y_pred



def gap_fill_meteo(station_name, df, dataFileDir, gf_config_dir):
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
    # Declaration of variables and maximum window length for linear interpolation
    gf_config = load_gap_fill_config(gf_config_dir, station_name)

    ############################
    ### Linear interpolation ###
    ############################

    for i_var in gf_config['vars_to_fill_meteo']:
        # Perform linear interpolation with a window specified by variable type
        df[i_var]= df[i_var].interpolate(
            method='linear',
            limit=gf_config['vars_to_fill_meteo'][i_var])

    ########################
    ### Machine learning ###
    ########################

    # Create variables suitable for ML
    df['doy_t'] = np.cos(df.index.dayofyear/366*2*np.pi)
    df['hour_t'] = np.cos(df.index.hour/24*2*np.pi)

    # Load meteorological reanalysis
    df_era = pd.read_csv(os.path.join(
        dataFileDir, f"{gf_config['proxy']}.csv"))

    if station_name == 'Bernard_lake':
        air_temp = 'air_temp_HC2S3'
        air_relhum = 'air_relhum_HC2S3'
        df_era = df_era.rename(columns={'air_temp_HMP45C' : air_temp,
                                        'air_relhum_HMP45C' : air_relhum})
    else:
        air_temp = 'air_temp_HMP45C'
        air_relhum = 'air_relhum_HMP45C'


    for i_var in gf_config['vars_to_fill_meteo']:

        if i_var in df_era.columns:

            if df[i_var].isna().sum() != 0:

                # Input and target variable selection
                if i_var == air_temp:
                    input_ml_vars = np.column_stack(
                        (df[['doy_t','hour_t']].values, df_era[i_var].values))
                else:
                    input_ml_vars = np.column_stack(
                        (df[['doy_t','hour_t',air_temp]].values, df_era[i_var].values))
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

    if station_name == 'Forest_stations':
        # TODO find better way to hand missing soil_heatflux_HFP01SC_1
        id_na = df['soil_heatflux_HFP01SC_1'].isna()
        df.loc[id_na,'soil_heatflux_HFP01SC_1'] = 0

    return df


def gap_fill_radiation(station_name, df, dataFileDir, gf_config_dir):
    """ Gap fill radiation data. Gaps are filled with the ERA5 reanalysis data.
    The reanalysis is corrected with in situ measurements via a random forest
    regressor.

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

    if station_name == 'Bernard_lake':
        air_temp = 'air_temp_HC2S3'
    else:
        air_temp = 'air_temp_HMP45C'

    # Declaration of variables and maximum window length for linear interpolation
    gf_config = load_gap_fill_config(gf_config_dir, station_name)

    ########################
    ### Machine learning ###
    ########################

    # Create variables suitable for ML
    df['doy_t'] = np.cos(df.index.dayofyear/366*2*np.pi)
    df['hour_t'] = np.cos(df.index.hour/24*2*np.pi)

    # Load meteorological reanalysis
    df_era = pd.read_csv(os.path.join(
        dataFileDir, f"{gf_config['proxy']}.csv"))


    for i_var in gf_config['vars_to_fill_radiation']:

        if i_var in df_era.columns:

            if df[i_var].isna().sum() != 0:

                # Target variable
                target_ml_var = df[i_var].values

                # Input variable
                if i_var in ['rad_longwave_down_CNR4','rad_shortwave_down_CNR4']:
                    input_ml_vars = np.column_stack(
                        (df[['doy_t','hour_t',air_temp]].values,
                         df_era[i_var].values))
                elif i_var == 'rad_longwave_up_CNR4':
                    input_ml_vars = np.column_stack(
                        (df[['doy_t','hour_t',air_temp]].values,
                         df['rad_longwave_down_CNR4'].values))
                    if station_name == 'Water_stations':
                        input_ml_vars = np.column_stack(
                            (input_ml_vars,df['water_frozen_sfc'].values))
                elif i_var == 'rad_shortwave_up_CNR4':
                    input_ml_vars = np.column_stack(
                        (df[['doy_t','hour_t']].values,
                         df['rad_shortwave_down_CNR4'].values))
                    if station_name == 'Water_stations':
                        input_ml_vars = np.column_stack(
                            (input_ml_vars,df['water_frozen_sfc'].values))

                # Training
                scalerX, scalery, regr = train_rf(target_ml_var, input_ml_vars)

                # Prediction
                y_pred =  predict_rf(scalerX, scalery, regr, input_ml_vars)
                id_na = df[i_var].isna()
                df.loc[id_na,i_var] = y_pred[id_na]


    # Recompute albedo and rad_net
    df['albedo_CNR4'] = np.nan
    id_daylight = df['rad_shortwave_down_CNR4'] > 25
    df.loc[id_daylight, 'albedo_CNR4'] = \
        df.loc[id_daylight,'rad_shortwave_up_CNR4'] \
            / df.loc[id_daylight,'rad_shortwave_down_CNR4']

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

    # Recompute albedo and rad_net
    df['albedo_CNR4'] = np.nan
    id_daylight = df['rad_shortwave_down_CNR4'] > 25
    df.loc[id_daylight, 'albedo_CNR4'] = \
        df.loc[id_daylight,'rad_shortwave_up_CNR4'] \
            / df.loc[id_daylight,'rad_shortwave_down_CNR4']

    df['rad_net_CNR4'] = \
        df['rad_shortwave_down_CNR4'] - df['rad_shortwave_up_CNR4'] \
        + df['rad_longwave_down_CNR4'] - df['rad_longwave_up_CNR4']

    return df

def custom_operation(station_name, df, gf_config_dir):
    config = load_gap_fill_config(gf_config_dir,station_name)
    for operation in config['custom_operation']:
        exec(operation, globals(), locals())
    return df
