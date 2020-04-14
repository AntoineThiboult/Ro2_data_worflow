# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

def gapfill_mds(df,var_to_fill,df_config,mergedCsvOutDir):
    """
    Performs gap filling with the marginal distribution sampling methodology
    proposed by Reichtein et al. 2005.

    Parameters
    ----------
    df: pandas DataFrame that contains the variable that requires to be gap
        filled.
    var_to_fill: string that indicates the variable that will be gap filled
    var_to_fill_trim: similar to var_to_fill except it refers to the data after
        cleaning (dispiked, bandpass filtered, etc)
    df_config: pandas DataFrame that contains the gap filling configuration
    mergedCsvOutDir: path to the output directory as string

    Returns
    -------

    See also
    --------
    Reichstein et al. “On the separation of net ecosystem exchange
    into assimilation and ecosystem respiration: review and improved algorithm.” (2005).

    Algorithm following the Appendix flowchart

      NEE present ?                                                 --> Yes     --> Does nothing
        |
        V
      Rg, T, VPD, NEE available within |dt|<= 7 days                --> Yes     --> Filling quality A (case 1)
        |
        V
      Rg, T, VPD, NEE available within |dt|<= 14 days               --> Yes     --> Filling quality A (case 2)
        |
        V
      Rg, NEE available within |dt|<= 7 days                        --> Yes     --> Filling quality A (case 3)
        |
        V
      NEE available within |dt|<= 1h                                --> Yes     --> Filling quality A (case 4)
        |
        V
      NEE available within |dt|= 1 day & same hour of day           --> Yes     --> Filling quality B (case 5)
        |
        V
      Rg, T, VPD, NEE available within |dt|<= 21, 28,..., 140 days  --> Yes     --> Filling quality B if |dt|<=28, else C (case 6)
        |
        V
      Rg, NEE available within |dt|<= 14, 21, 28,..., 140 days      --> Yes     --> Filling quality B if |dt|<=28, else C (case 7)
        |
        V
      NEE available within |dt|<= 7, 21, 28,...days                 --> Yes     --> Filling quality C (case 8)
      """

    # Submodule to find similar meteorological condition within a given window search
    def find_meteo_proxy_index(df, t, search_window, proxy_vars, proxy_vars_range):
        proxy_vars.dropna(inplace=True)
        current_met = df.loc[t,proxy_vars]
        if any(current_met.isna()):
            index_proxy_met = None
            fail_code = "NaN in current proxy meteorological vars"
        else:
            t_loc = df.index.get_loc(t)
            t_start = np.max([0, t_loc-int(search_window*48)])
            t_end = np.min([df.shape[0]-1, t_loc+int(search_window*48)+1])
            time_window = list( range(t_start ,t_end) )
            time_window_met = df.loc[df.index[time_window],proxy_vars]
            index_proxy_met_bool = pd.DataFrame()
            # Check if proxy met matches gap filling conditions
            for iVar in range(0,proxy_vars.shape[0]):
                index_proxy_met_bool = pd.concat(
                    [index_proxy_met_bool,
                     abs(time_window_met.iloc[:,iVar] - current_met.iloc[iVar]) < proxy_vars_range[iVar]], axis=1)

            # Check that the corresponding var_to_fill is not NaN
            index_proxy_met_bool = pd.concat([index_proxy_met_bool, ~df.loc[df.index[time_window],var_to_fill_trim].isna()], axis=1)
            index_proxy_met_bool = index_proxy_met_bool.all(axis=1)
            # Convert bool to index
            index_proxy_met = index_proxy_met_bool.index[index_proxy_met_bool == True]
            if index_proxy_met.size == 0:
                fail_code = "Proxy met not found"
            else:
                fail_code = None
        return index_proxy_met, fail_code

    # Submodule to find a NEE within one day at the same hour of day
    def find_nee_proxy_index(df, t, search_window, exact_time=False):
        t_loc = df.index.get_loc(t)
        t_start = np.max([0, t_loc-int(search_window*48)])
        t_end = np.min([df.shape[0]-1, t_loc+int(search_window*48)+1])
        if exact_time:
            time_window = list([t_start ,t_end])
        else:
            time_window = list( range(t_start ,t_end) )
        time_window_met = df.loc[df.index[time_window],var_to_fill_trim]
        index_proxy_met_bool = ~time_window_met.isna()
        index_proxy_met = index_proxy_met_bool.index[index_proxy_met_bool == True]
        if index_proxy_met.size == 0:
            fail_code = "Proxy met not found"
        else:
            fail_code = None
        return index_proxy_met, fail_code

    # Column name that contains cleaned flux data
    var_to_fill_trim = var_to_fill+'_trim'

    # Identify missing flux indices
    id_missing_flux = df[var_to_fill_trim].isna()

    # Add new columns to data frame that contains var_to_fill gapfilled
    gap_fil_col_name = var_to_fill + "_gf_mds"
    df[gap_fil_col_name] = df[var_to_fill_trim]
    gap_fil_quality_col_name = gap_fil_col_name + "_qf"
    df[gap_fil_quality_col_name] = None

    # Define variables used for Vars_to_fill
    proxy_vars = var_to_fill + '_proxy_vars'
    proxy_vars_range = var_to_fill + '_proxy_vars_range'
    proxy_vars_subset = var_to_fill + '_proxy_vars_subset'

    # Loop over time steps
    for t in df.index[id_missing_flux]:

        if not t%100:
            print("\rGap filling {:s}, progress {:2.1%} ".format(var_to_fill, t/len(df.index)), end='\r')

        # Case 1
        search_window = 7
        index_proxy_met, fail_code = find_meteo_proxy_index(
            df, t, search_window, df_config[proxy_vars], df_config[proxy_vars_range])
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df.loc[index_proxy_met,var_to_fill_trim])
            df.loc[t,gap_fil_quality_col_name] = "A1"
            continue

        # Case 2
        search_window = 14
        index_proxy_met, fail_code = find_meteo_proxy_index(
            df, t, search_window, df_config[proxy_vars], df_config[proxy_vars_range])
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill_trim][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "A2"
            continue

        # Case 3
        search_window = 7
        index_proxy_met, fail_code = find_meteo_proxy_index(
            df, t, search_window, df_config[proxy_vars_subset], df_config[proxy_vars_range])
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill_trim][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "A3"
            continue

        # Case 4
        search_window = 1/24
        index_proxy_met, fail_code = find_nee_proxy_index(df, t, search_window)
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill_trim][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "A4"
            continue

        # Case 5
        search_window = 1
        index_proxy_met, fail_code = find_nee_proxy_index(df, t, search_window, True)
        if not fail_code:
            df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill_trim][index_proxy_met])
            df.loc[t,gap_fil_quality_col_name] = "B1"
            continue

        # Case 6
        search_window = 14
        while bool(fail_code) & (search_window <= 140):
            search_window += 7
            index_proxy_met, fail_code = find_meteo_proxy_index(
                df, t, search_window, df_config[proxy_vars], df_config[proxy_vars_range])
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill_trim][index_proxy_met])
                if search_window <= 28:
                    df.loc[t,gap_fil_quality_col_name] = "B2"
                else:
                    df.loc[t,gap_fil_quality_col_name] = "C1"
                continue

        # Case 7
        search_window = 7
        while bool(fail_code) & (search_window <= 140):
            search_window += 7
            index_proxy_met, fail_code = find_meteo_proxy_index(
                df, t, search_window, df_config[proxy_vars_subset], df_config[proxy_vars_range])
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill_trim][index_proxy_met])
                if search_window <= 14:
                    df.loc[t,gap_fil_quality_col_name] = "B3"
                else:
                    df.loc[t,gap_fil_quality_col_name] = "C2"
                continue

        # Case 8
        search_window = 0
        while bool(fail_code):
            search_window += 7
            index_proxy_met, fail_code = find_nee_proxy_index(df, t, search_window)
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill_trim][index_proxy_met])
                df.loc[t,gap_fil_quality_col_name] = "C3"
                continue

    return df
