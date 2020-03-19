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

    if spiky_var == 'LE':
        id_bandpass = ( df[spiky_var] < -35 ) | ( df[spiky_var] > 300 )     # in [W+1m-2]
    elif spiky_var == 'H':
        id_bandpass = ( df[spiky_var] < -100 ) | ( df[spiky_var] > 400 )     # in [W+1m-2]
    elif spiky_var == 'CO2_flux':
        id_bandpass = ( df[spiky_var] < -10 ) | ( df[spiky_var] > 20 )      # in [µmol+1s-1m-2]
    elif spiky_var == 'CH4_flux':
        id_bandpass = ( df[spiky_var] < -0.1 ) | ( df[spiky_var] > 0.25 )   # in [µmol+1s-1m-2]

    return id_bandpass