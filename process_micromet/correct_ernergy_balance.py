# -*- coding: utf-8 -*-
"""
Created on Fri Apr  8 12:00:48 2022

@author: ANTHI182
"""
import numpy as np

def correct_energy_balance(df, corr_factor = None):
    """ Correct latent and sensible heat flux according to Mauder 2017
    or with a constant correction factor.

    Parameters
    ----------
    df : Pandas DataFrame
        Dataset that contains columns
            - LE_gf_mds
            - H_gf_mds
            - LE_strg
            - H_strg
            - net_rad_CNR4
            - G
    corr_factor : Float, default None.
        Correction factor (constant) that, if specified, will be used to
        correct the latent and sensible heat flux

    Returns
    -------
    df : Pandas DataFrame
        Dataset with additional columns:
            - LE_corr
            - H_corr

    References
    ----------
    Mauder, M, Genzel, S, Fu, J, et al. Evaluation of energy balance closure
    adjustment methods by independent evapotranspiration estimates from
    lysimeters and hydrological simulations. Hydrological Processes.
    2018; 32: 39– 50. https://doi.org/10.1002/hyp.11397

    """

    df['LE_corr'] = df['LE_gf_mds']
    df['H_corr'] = df['H_gf_mds']
    df['LE_corr_qf'] = df['LE_qf']
    df['H_corr_qf'] = df['H_qf']
    df['LE_corr_strg'] = df['LE_strg']
    df['H_corr_strg'] = df['H_strg']

    if corr_factor: # Correction with specified constant
        df['LE_corr'] = df['LE_corr'] * corr_factor
        df['H_corr']  = df['H_corr'] * corr_factor

    else: # Mauder correction
        id_start = (df.hour == 0).first_valid_index()
        id_end = (df.hour == 0).last_valid_index()
        for i in range(id_start,id_end,48):
            idx_bol = df.loc[i:i+47,'rad_shortwave_down_CNR4'] > 20
            if any(idx_bol):
                idx = idx_bol[idx_bol].index
                C = (df.loc[idx,'H_gf_mds'].sum() + df.loc[idx,'LE_gf_mds'].sum()) / \
                    (-df.loc[idx,'rad_net_CNR4'].sum() + df.loc[idx,'G'].sum()
                     + df.loc[idx,'LE_strg'].sum() + df.loc[idx,'H_strg'].sum())
                if abs(C) < 0.2:
                    C = np.nan
                df.loc[idx,'LE_corr'] = df.loc[idx,'LE_corr'] * -1/C
                df.loc[idx,'H_corr']  = df.loc[idx,'H_corr'] * -1/C

    return df