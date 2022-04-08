import pandas as pd
from process_micromet.gap_fill_mds import gap_fill_mds

def gap_fill_flux(station_name,df,out_dir,gap_fill_config):

    """Load gap filling config file, load additional data from other station
    if necessary, prepare data for gap filling, and call the specified gap
    filling algorithm

    Parameters
    ----------
    merged_df: pandas DataFrame that contains all variables -- slow and eddy
        covariance data -- for the entire measurement period
    out_dir: path to the directory that contains the final .csv files
    gap_fill_config: path to the directory that contains the gap filling
        configuration files

    Returns
    -------
    """
    # load gap filling config file
    xlsFile = pd.ExcelFile(gap_fill_config+'gapfilling_configuration.xlsx')
    df_config = pd.read_excel(xlsFile,station_name+'_MDS')

    # Loop over variable that will be gapfilled
    var_to_fill = df_config.loc[~df_config['Vars_to_fill'].isna(),'Vars_to_fill']

    for iVar_to_fill in var_to_fill:

        # Perform gap filling
        print('\nStart gap filling for variable {:s} and station {:s}'.format(iVar_to_fill, station_name))
        df = gap_fill_mds(df,iVar_to_fill,df_config,out_dir)

    # Fill missing storage variable with zeros
    strg_vars = [var for var in df.columns if '_strg' in var]
    df[strg_vars] = df[strg_vars].fillna(value=0)

    return df

