import pandas as pd
import yaml
import os
from process_micromet.gap_fill_mds import gap_fill_mds
from process_micromet.gap_fill_rf import gap_fill_rf

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

