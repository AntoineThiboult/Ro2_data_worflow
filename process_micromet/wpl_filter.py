# -*- coding: utf-8 -*-

def wpl_filter(df, var):

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