import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor

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
            df['timestamp'].dt.dayofyear.values.reshape(df.shape[0],1),
            df['timestamp'].dt.hour.values.reshape(df.shape[0],1)),
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
        
    