# -*- coding: utf-8 -*-
import pandas as pd

def detect_spikes(df, spiky_var, sliding_window=624, z=4, daynight=False):
    """Detect spikes on slow data according to Papale et al. (2006)

    Parameters
    ----------
    df: pandas DataFrame that contains the spiky variable, a daytime key
        (day=1, night=0)
    spiky_var: string that designate the spiky variable
    sliding_window: sliding window used to compute the moving median (default
                    value = 13*48 -- 13 days in total)
    z: discrimination factor. Default value = 4. Increasing value retains more
       data (up to 7)
    daynight: if true, handle day/night time separately. Default=False. For
              carbon fluxes, it is recommended to switch it to 'True'

    Returns
    -------
    id_outlier: index of outliers

    See also
    --------
    Papale et al. (2006) Towards a standardized processing of Net Ecosystem
    Exchange measured with eddy covariance technique: algorithms and
    uncertainty estimation. Biogeosciences, European Geosciences Union, 2006,
    3 (4), pp.571-583."""

    # Clean the data
    nee = df.dropna(subset=[spiky_var])[spiky_var]

    if daynight: # Filter day and night separately
        daytime = df.dropna(subset=[spiky_var])['daytime']
        nee_day = nee[daytime==1]
        nee_night = nee[daytime==0]

        # Identify outliers during day time
        di = nee_day.diff(periods=-1) - nee_day.diff(periods=1)
        Md = di.rolling(window=sliding_window, center=True, min_periods=1).median()
        MAD = abs(di-Md).median()
        lowerBound = Md - (z*MAD / 0.6745)
        upperBound = Md + (z*MAD / 0.6745)
        id_outlier_day = ( di < lowerBound ) | ( di > upperBound )

        # Identify outliers during night time
        di = nee_night.diff(periods=-1) - nee_night.diff(periods=1)
        Md = di.rolling(window=sliding_window, center=True, min_periods=1).median()
        MAD = abs(di-Md).median()
        lowerBound = Md - (z*MAD / 0.6745)
        upperBound = Md + (z*MAD / 0.6745)
        id_outlier_night = ( di < lowerBound ) | ( di > upperBound )

        # Merge the two outlier indices
        id_outlier = pd.concat([id_outlier_day,id_outlier_night]).sort_index()

    else: # Filter day and night as a whole
        # Identify outliers during day time
        di = nee.diff(periods=-1) - nee.diff(periods=1)
        Md = di.rolling(window=sliding_window, center=True, min_periods=1).median()
        MAD = abs(di-Md).median()
        lowerBound = Md - (z*MAD / 0.6745)
        upperBound = Md + (z*MAD / 0.6745)
        id_outlier = ( di < lowerBound ) | ( di > upperBound )

    # Reindex because of missing values (dropna())
    id_outlier = id_outlier.reindex(df.index, fill_value=False)

    # return id_outlier, lowerBound, upperBound, Md, MAD, di
    return id_outlier