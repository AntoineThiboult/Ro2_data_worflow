# -*- coding: utf-8 -*-
"""
Created on Tue Aug 29 16:04:27 2023

@author: ANTHI182
"""
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model

def train_lm(target_var, input_vars):
    """ Train a linear model

    Parameters
    ----------
    target_var : numpy array (n,)
        target variable
    input_vars : numpy array (n,m)
        input variables

    Returns
    -------
    regr : Scikit model object
        Scikit linear model fitted to target
    """

    mask = ~np.isnan(np.column_stack((target_var,input_vars))).any(axis=1) \
        & np.isfinite(np.column_stack((target_var,input_vars))).all(axis=1)

    if sum(mask) == 0: return None

    X = input_vars[mask,:]
    y = target_var[mask,np.newaxis]

    regr = linear_model.LinearRegression()
    regr.fit(X, y.flatten())

    return regr

def predict_lm(regr, input_vars):
    """ Predict values with a trained linear model

    Parameters
    ----------
    regr : Scikit model object
        Scikit linear model fitted to target
    input_vars : numpy array (n,m)
        Input variables

    Returns
    -------
    y_pred : numpy array (n,)
        Predicted variable
    """

    mask = ~np.isnan(input_vars).any(axis=1) \
        & np.isfinite(input_vars).all(axis=1)
    y_pred = np.zeros((mask.shape[0])) * np.nan

    X = input_vars[mask,:]

    y_pred_unindex = np.expand_dims( regr.predict(X), axis=1)

    y_pred[mask] = y_pred_unindex.flatten()

    return y_pred


def train_rf(target_var, input_vars):
    """ Train a random forest regressor

    Parameters
    ----------
    target_var : numpy array (n,)
        target variable
    input_vars : numpy array (n,m)
        input variables

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
        Input variables

    Returns
    -------
    y_pred : numpy array (n,)
        Predicted variable
    """

    mask = ~np.isnan(input_vars).any(axis=1) \
        & np.isfinite(input_vars).all(axis=1)
    y_pred = np.zeros((mask.shape[0])) * np.nan

    X = scalerX.transform(input_vars[mask,:])

    y_pred_unindex = scalery.inverse_transform(
        np.expand_dims( regr.predict(X), axis=1))

    y_pred[mask] = y_pred_unindex.flatten()

    return y_pred
