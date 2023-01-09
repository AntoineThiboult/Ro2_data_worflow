# -*- coding: utf-8 -*-
import warnings

def rssi_filter(df, var):

    """Detects low rssi values

    Parameters
    ----------
    df: pandas DataFrame that contains the spiky variable
    var: string that designate the variable that should be filtered for its RSSI

    Returns
    -------
    id_rssi: index of low RSSI"""

    try:
        if var in ['H','LE','LE_corr','H_corr','LE_strg','H_strg']:
            id_low_rssi = df['rssi_H2O'] < 0.7
        elif var in ['CO2_flux','CO2_strg']:
            id_low_rssi = df['rssi_CO2'] < 0.7
        elif var in ['CH4_flux','CH4_strg']:
            id_low_rssi = df['rssi_CH4'] < 0.15
    except:
        warnings.warn(f'Unable to filter {var} with RSSI')
        id_low_rssi = []

    return id_low_rssi
