# -*- coding: utf-8 -*-

def bandpass_filter(df, spiky_var):
    """Detect outliers according to a passband filter specific to each variable.

    Parameters
    ----------
    df: pandas DataFrame that contains the spiky variable
    spiky_var: string that designate the spiky variable

    Returns
    -------
    id_outlier: index of outliers"""

    if (spiky_var == 'LE') | (spiky_var == 'LE_corr'):
        id_bandpass = ( df[spiky_var] < -35 ) | ( df[spiky_var] > 300 )     # in [W+1m-2]
    elif (spiky_var == 'H') | (spiky_var == 'H_corr'):
        id_bandpass = ( df[spiky_var] < -150 ) | ( df[spiky_var] > 500 )    # in [W+1m-2]
    elif spiky_var == 'CO2_flux':
        id_bandpass = ( df[spiky_var] < -20 ) | ( df[spiky_var] > 20 )      # in [µmol+1s-1m-2]
    elif spiky_var == 'CH4_flux':
        id_bandpass = ( df[spiky_var] < -0.1 ) | ( df[spiky_var] > 0.25 )   # in [µmol+1s-1m-2]
    elif spiky_var == 'LE_strg':
        id_bandpass = ( df[spiky_var] < -60 ) | ( df[spiky_var] > 60 )     # in [W+1m-2]
    elif spiky_var == 'H_strg':
        id_bandpass = ( df[spiky_var] < -50 ) | ( df[spiky_var] > 50 )    # in [W+1m-2]
    elif spiky_var == 'CO2_strg':
        id_bandpass = ( df[spiky_var] < -8 ) | ( df[spiky_var] > 8 )      # in [µmol+1s-1m-2]
    elif spiky_var == 'CH4_strg':
        id_bandpass = ( df[spiky_var] < -0.05 ) | ( df[spiky_var] > 0.05 )   # in [µmol+1s-1m-2]
    return id_bandpass