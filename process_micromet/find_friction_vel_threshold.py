# -*- coding: utf-8 -*-
import numpy as np

def find_friction_vel_threshold(df, flux_var, air_temp_var, n_bootstrap=100):
    """Identify the friction velocity threshold below which data should be
    discarded according to Papale et al. (2006)

    Parameters
    ----------
    df: pandas DataFrame that contains the flux variable, a daytime key
        (day=1, night=0)
    flux_var: string that designate the flux variable
    n_bootstrap: number of times bootstraping should be performed (default=100)

    Returns
    -------
    id_below_thresh: index of fluxes below the identified friction velocity
        threshold
    fric_vel_threshold: friction velocity threshold (median)
    fric_vel_threshold_ci: 5%-95% percetiles friction velocity threshold
        confidance interavls

    See also
    --------
    Papale et al. (2006) Towards a standardized processing of Net Ecosystem
    Exchange measured with eddy covariance technique: algorithms and
    uncertainty estimation. Biogeosciences, European Geosciences Union, 2006,
    3 (4), pp.571-583."""

    print('Start friction velocity threshold for variable :', flux_var, '...', end='\r')
    # Make copy of df
    df_tmp = df.copy()

    # Drop daytime and other unecessary columns
    df_tmp = df_tmp.drop(df_tmp[df_tmp.daytime == 1].index)
    df_tmp = df_tmp[ [flux_var, air_temp_var, 'friction_velocity'] ]
    df_tmp = df_tmp.dropna()

    # Initialize friction threshold list
    bootstrap_fric_vel_threshold=list()


    for i_bootstrap in range(1,n_bootstrap+1):

        # Randomly select subset of dataframe for bootstrap
        df_bootstrap = df.copy()
        selected_index = np.random.choice(df_bootstrap.index, int(len(df_bootstrap)/2))
        df_bootstrap = df_bootstrap.loc[selected_index]

        # Create the 6 equally distributed temperature bins
        air_temp_bins = np.array([np.nanquantile(df_bootstrap[air_temp_var], np.linspace(0,1,7))[0:-1],
                        np.nanquantile(df_bootstrap[air_temp_var], np.linspace(0,1,7))[1:]], np.float32)

        # Initialize friction threshold list
        all_fric_vel_threshold = list()

        for c_air in range(0, len(air_temp_bins[1]) ):

            # Find index of df_bootstrap for which air_temp_var included in the quantile bin
            id_air_temp = ( df_bootstrap[air_temp_var] >= air_temp_bins[0, c_air] )\
                & ( df_bootstrap[air_temp_var] < air_temp_bins[1, c_air] )
            df_air_class = df_bootstrap.loc[id_air_temp]

            # Create the 20 equally distributed friction velocity bins
            fric_vel_bins = np.array([np.nanquantile( df_air_class['friction_velocity'], np.linspace(0,1,21)[0:-1] ),
                                      np.nanquantile( df_air_class['friction_velocity'], np.linspace(0,1,21)[1:]) ])

            # Check that temperature and friction velocities are not or poorly correlated
            corr_air_temp_fric_vel = np.corrcoef(df_air_class[air_temp_var],df_air_class['friction_velocity'])[0,1]

            if corr_air_temp_fric_vel < 0.4:

                for c_fric in range(0, len(fric_vel_bins[0]) ):

                    # Find index of df for which air_temp_var included in the quantile bin
                    id_fric_vel = ( df_air_class['friction_velocity'] >= fric_vel_bins[0, c_fric] )\
                        & ( df_air_class['friction_velocity'] < fric_vel_bins[1, c_fric] )
                    id_fric_vel_upper = ( df_air_class['friction_velocity'] >= fric_vel_bins[0, c_fric] )\
                        & ( df_air_class['friction_velocity'] < fric_vel_bins[1,-1] )

                    # Compute flux associated with this friction velocity bin
                    fric_vel_ratio = np.nanmean(df_air_class.loc[id_fric_vel, flux_var])\
                        /np.nanmean(df_air_class.loc[id_fric_vel_upper, flux_var])

                    if fric_vel_ratio > 0.99:
                        # Defines the bin as the threshold
                        all_fric_vel_threshold.append(fric_vel_bins[0, c_fric])
                        break

        # Store median threshold for each bootstrap evaluation
        bootstrap_fric_vel_threshold.append(np.median(all_fric_vel_threshold))

    # Compute statistics on the friction velocity thresholds
    fric_vel_threshold = np.quantile(bootstrap_fric_vel_threshold,[0.5])[0]
    fric_vel_threshold_ci = np.quantile(bootstrap_fric_vel_threshold,[0.05, 0.95])

    # Get indices of indices not matching threshold requirements
    id_below_thresh = df['friction_velocity'] < fric_vel_threshold
    print('Done!')

    return id_below_thresh, fric_vel_threshold, fric_vel_threshold_ci