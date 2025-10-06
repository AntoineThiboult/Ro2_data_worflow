import os
import numpy as np
import yaml
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor

def gap_fill_flux(station_name,df,gf_config_dir):

    """Load gap filling config file, load additional data from other station
    if necessary, prepare data for gap filling, and call the specified gap
    filling algorithm

    Parameters
    ----------
    station_name: string that indicates the name of the station
    merged_df: pandas DataFrame that contains all variables -- slow and eddy
        covariance data -- for the entire measurement period
    gf_config_dir: path to the directory that contains the gap filling
        configuration files

    Returns
    -------
    """

    # Didctionary containing names and gapfilling functions
    gf_methods = {'rf':gap_fill_rf,
                  'mds':gap_fill_mds}

    # Loop over gap filling method
    for i_gf in gf_methods:

        # Load configuration
        config = yaml.safe_load(
            open(os.path.join(gf_config_dir,f'{station_name}_{i_gf}.yml')))

        # Loop over variables
        for var_to_fill in config['vars_to_fill']:

            if var_to_fill in df.columns:

                # Perform gap filling
                print('\nStart gap filling for variable ' +
                      '{:s} and station {:s} with {:s}'.format(
                          var_to_fill, station_name, i_gf))
                df = gf_methods[i_gf](df,var_to_fill,config)

            else:
                print(f'{var_to_fill} not present in data')

    return df


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


def gap_fill_rf(df,var_to_fill,df_config):


    # Add new columns to data frame that contains var_to_fill gapfilled
    gap_fil_col_name = var_to_fill + "_gf_rf"
    df[gap_fil_col_name] = df[var_to_fill]
    gap_fil_quality_col_name = gap_fil_col_name + "_qf"
    df[gap_fil_quality_col_name] = None

    # Identify missing flux indices
    id_missing_flux = df[var_to_fill].isna()

    if id_missing_flux.sum() > 0:

        # Define variables used for Vars_to_fill
        proxy_vars = df_config['vars_to_fill'][var_to_fill]\
            ['proxy_vars'].split(' ')

        # Target flux
        target_ml_var = df[var_to_fill].values

        # Best gap filling option that includes the lagged target variable
        for lag in np.arange(1,2*48):

            # Add delayed target variable
            input_ml_vars = np.concatenate((
                df[proxy_vars].values,
                df[var_to_fill].shift(-lag).values.reshape(df.shape[0],1),
                df[var_to_fill].shift(lag).values.reshape(df.shape[0],1)),
                axis=1)

            # Check if it worth training a model on this dataset
            id_nan_input = ~np.isnan(input_ml_vars).any(axis=1)
            n_target = df.loc[id_nan_input,gap_fil_col_name].isna().sum()

            if n_target > 0:

                # Training
                scalerX, scalery, regr = train_rf(target_ml_var, input_ml_vars)

                # Prediction
                y_pred =  predict_rf(scalerX, scalery, regr, input_ml_vars)

                # Fill missing data
                id_na = df[gap_fil_col_name].isna()
                df.loc[id_na,gap_fil_col_name] = y_pred[id_na]
                df.loc[id_na,gap_fil_quality_col_name] = \
                    f'A{np.divmod(lag,12)[0]+1}'

        # Second choice input variable for gap filling
        input_ml_vars = np.concatenate((
            df[proxy_vars].values,
            df.index.dayofyear.values.reshape(df.shape[0],1),
            df.index.hour.values.reshape(df.shape[0],1)),
            axis=1)

        # Training
        scalerX, scalery, regr = train_rf(target_ml_var, input_ml_vars)

        # Prediction
        y_pred =  predict_rf(scalerX, scalery, regr, input_ml_vars)

        # Fill missing data
        id_na = df[gap_fil_col_name].isna()
        df.loc[id_na,gap_fil_col_name] = y_pred[id_na]
        df.loc[id_na,gap_fil_quality_col_name] = 'B'

    return df


def gap_fill_mds(df,var_to_fill,df_config):
    """
    Performs gap filling with the marginal distribution sampling methodology
    proposed by Reichtein et al. 2005.

    Parameters
    ----------
    df: pandas DataFrame that contains the variable that requires to be gap
        filled.
    var_to_fill: string that indicates the variable that will be gap filled
    df_config: pandas DataFrame that contains the gap filling configuration

    Returns
    -------

    See also
    --------
    Reichstein et al. “On the separation of net ecosystem exchange
    into assimilation and ecosystem respiration: review and improved algorithm.” (2005).

    Algorithm following the Appendix flowchart

      NEE present ?                                                 --> Yes     --> Does nothing
        |
        V
      Rg, T, VPD, NEE available within |dt|<= 7 days                --> Yes     --> Filling quality A (case 1)
        |
        V
      Rg, T, VPD, NEE available within |dt|<= 14 days               --> Yes     --> Filling quality A (case 2)
        |
        V
      Rg, NEE available within |dt|<= 7 days                        --> Yes     --> Filling quality A (case 3)
        |
        V
      NEE available within |dt|<= 1h                                --> Yes     --> Filling quality A (case 4)
        |
        V
      NEE available within |dt|= 1 day & same hour of day           --> Yes     --> Filling quality B (case 5)
        |
        V
      Rg, T, VPD, NEE available within |dt|<= 21, 28,..., 140 days  --> Yes     --> Filling quality B if |dt|<=28, else C (case 6)
        |
        V
      Rg, NEE available within |dt|<= 14, 21, 28,..., 140 days      --> Yes     --> Filling quality B if |dt|<=28, else C (case 7)
        |
        V
      NEE available within |dt|<= 7, 21, 28,...days                 --> Yes     --> Filling quality C (case 8)
      """

    # Submodule to find similar meteorological condition within a given window search
    def find_meteo_proxy_index(df, t, search_window, proxy_vars, proxy_vars_range):
        current_met = df.loc[t,proxy_vars]
        if any(current_met.isna()):
            return None

        t_loc = df.index.get_loc(t)
        t_start = np.max([df.index[0], t_loc-int(search_window*48)])
        t_end = np.min([df.index[-1], t_loc+int(search_window*48)+1])
        time_window = list( range(t_start ,t_end) )
        time_window_met = df.loc[df.index[time_window],proxy_vars]

        # Check if proxy met matches gap filling conditions and that
        # the corresponding var_to_fill is not NaN
        index_proxy_met_bool = \
            abs(time_window_met - current_met).lt(proxy_vars_range).all(axis=1) \
                & ~df.loc[df.index[time_window],var_to_fill].isna()

        index_proxy_met = index_proxy_met_bool.index[index_proxy_met_bool]

        if index_proxy_met.size != 0:
            return index_proxy_met
        else:
            return None

    # Submodule to find a NEE within one day at the same hour of day
    def find_nee_proxy_index(df, t, search_window, exact_time=False):
        t_loc = df.index.get_loc(t)
        t_start = np.max([df.index[0], t_loc-int(search_window*48)])
        t_end = np.min([df.index[-1], t_loc+int(search_window*48)+1])
        if exact_time:
            time_window = list([t_start ,t_end])
        else:
            time_window = list( range(t_start ,t_end) )

        index_proxy_met_bool = ~df.loc[df.index[time_window],var_to_fill].isna()
        index_proxy_met = index_proxy_met_bool.index[index_proxy_met_bool]

        if index_proxy_met.size != 0:
            return index_proxy_met
        else:
            return None

    # Identify missing flux indices
    id_missing_flux = df[var_to_fill].isna()

    # Add new columns to data frame that contains var_to_fill gapfilled
    gap_fil_col_name = var_to_fill + "_gf_mds"
    df[gap_fil_col_name] = df[var_to_fill]
    gap_fil_quality_col_name = gap_fil_col_name + "_qf"
    df[gap_fil_quality_col_name] = None

    # Define variables used for Vars_to_fill
    proxy_vars = df_config['vars_to_fill'][var_to_fill]\
        ['proxy_vars'].keys()
    proxy_vars_range = list(df_config['vars_to_fill'][var_to_fill]\
        ['proxy_vars'].values())
    proxy_vars_subset = df_config['vars_to_fill'][var_to_fill]\
        ['proxy_vars_subset'].keys()
    proxy_vars_subset_range = list(df_config['vars_to_fill'][var_to_fill]\
        ['proxy_vars_subset'].values())

    # Loop over time steps
    for counter, t in enumerate(df.index[id_missing_flux]):

        if not counter%100:
            print("\rGap filling {:s}, progress {:2.1%} ".format(var_to_fill, t/len(df.index)), end='\r')

        # Calculates the number of time steps between t and the next non NaN-value
        non_nan_indices = df.index[~df[var_to_fill].isna()]
        closest_index = np.argmin(np.abs(non_nan_indices - t))
        dist_next_valid = np.abs(t-non_nan_indices[closest_index])

        # Initialize the index_proxy_met
        index_proxy_met = None

        # Case 1
        search_window = 7
        if dist_next_valid < search_window*48:
            index_proxy_met = find_meteo_proxy_index(
                df, t, search_window, proxy_vars, proxy_vars_range)
            if index_proxy_met is not None:
                df.loc[t,gap_fil_col_name] = np.mean(df.loc[index_proxy_met,var_to_fill])
                df.loc[t,gap_fil_quality_col_name] = "A1"
                continue

        # Case 2
        search_window = 14
        if dist_next_valid < search_window*48:
            index_proxy_met = find_meteo_proxy_index(
                df, t, search_window, proxy_vars, proxy_vars_range)
            if index_proxy_met is not None:
                df.loc[t,gap_fil_col_name] = df.loc[index_proxy_met, var_to_fill].mean()
                df.loc[t,gap_fil_quality_col_name] = "A2"
                continue

        # Case 3
        search_window = 7
        if dist_next_valid < search_window*48:
            index_proxy_met = find_meteo_proxy_index(
                df, t, search_window, proxy_vars_subset, proxy_vars_subset_range)
            if index_proxy_met is not None:
                df.loc[t,gap_fil_col_name] = df.loc[index_proxy_met, var_to_fill].mean()
                df.loc[t,gap_fil_quality_col_name] = "A3"
                continue

        # Case 4
        search_window = 1/24
        if dist_next_valid < search_window*48:
            index_proxy_met = find_nee_proxy_index(df, t, search_window)
            if index_proxy_met is not None:
                df.loc[t,gap_fil_col_name] = df.loc[index_proxy_met, var_to_fill].mean()
                df.loc[t,gap_fil_quality_col_name] = "A4"
                continue

        # Case 5
        search_window = 1
        if dist_next_valid < search_window*48:
            index_proxy_met = find_nee_proxy_index(df, t, search_window, True)
            if index_proxy_met is not None:
                df.loc[t,gap_fil_col_name] = df.loc[index_proxy_met, var_to_fill].mean()
                df.loc[t,gap_fil_quality_col_name] = "B1"
                continue

        # Case 6
        search_window = 14
        search_window_max = 140
        if dist_next_valid < search_window_max*48:
            while (index_proxy_met is None) & (search_window <= search_window_max):
                search_window += 7
                index_proxy_met = find_meteo_proxy_index(
                    df, t, search_window, proxy_vars, proxy_vars_range)
                if index_proxy_met is not None:
                    df.loc[t,gap_fil_col_name] = df.loc[index_proxy_met, var_to_fill].mean()
                    if search_window <= 28:
                        df.loc[t,gap_fil_quality_col_name] = "B2"
                    else:
                        df.loc[t,gap_fil_quality_col_name] = "C1"
                    break
        if index_proxy_met is not None:
            continue

        # Case 7
        search_window = 7
        search_window_max = 140
        if dist_next_valid < search_window_max*48:
            while (index_proxy_met is None) & (search_window <= search_window_max):
                search_window += 7
                index_proxy_met = find_meteo_proxy_index(
                    df, t, search_window, proxy_vars_subset, proxy_vars_subset_range)
                if index_proxy_met is not None:
                    df.loc[t,gap_fil_col_name] = df.loc[index_proxy_met, var_to_fill].mean()
                    if search_window <= 14:
                        df.loc[t,gap_fil_quality_col_name] = "B3"
                    else:
                        df.loc[t,gap_fil_quality_col_name] = "C2"
                    break
        if index_proxy_met is not None:
            continue

        # Case 8
        search_window = dist_next_valid//48
        while True:
            search_window += 7
            index_proxy_met = find_nee_proxy_index(df, t, search_window)
            if index_proxy_met is not None:
                df.loc[t,gap_fil_col_name] = df.loc[index_proxy_met, var_to_fill].mean()
                df.loc[t,gap_fil_quality_col_name] = "C3"
                break

    return df