# -*- coding: utf-8 -*-
import os
import re
import numpy as np
import pandas as pd
from process_micromet.filters import spikes

def rotate_wind(station_name,asciiOutDir):
    """Perform a rotation of the IRGASON 3D wind components according to the
    3DM-25 accelerometer measurements.
    Reads 10Hz foobar.csv files and write new foobar_rot.csv files.
    This function only works for the reservoir station.

    Parameters
    ----------
    station_name: name of the station
    asciiOutDir: path to the directory that contains the converted files (.csv)

    Returns
    -------

    References
    ----------
    Miller, S. D., Hristov, T. S., Edson, J. B., & Friehe, C. A. (2008).
    Platform Motion Effects on Measurements of Turbulence and Air–Sea Exchange
    over the Open Ocean, Journal of Atmospheric and Oceanic Technology, 25(9),
    1683-1694.
    """


    print('Start performing wind correction based on accelerometer data')

    ############################
    ### Constant declaration ###
    ############################

    var_dtypes = {
        'TIMESTAMP':              np.object0,
        'RECORD':                 np.float64,
        'Ux':                     np.float64,
        'Uy':                     np.float64,
        'Uz':                     np.float64,
        'T_SONIC':                np.float64,
        'diag_sonic':             np.float64,
        'CO2_density':            np.float64,
        'CO2_density_fast_tmpr':  np.float64,
        'H2O_density':            np.float64,
        'diag_irga':              np.float64,
        'T_SONIC_corr':           np.float64,
        'TA_1_1_1':               np.float64,
        'PA':                     np.float64,
        'CO2_sig_strgth':         np.float64,
        'H2O_sig_strgth':         np.float64,
        'accel_x':                np.float64,
        'accel_y':                np.float64,
        'accel_z':                np.float64,
        'ang_rate_x':             np.float64,
        'ang_rate_y':             np.float64,
        'ang_rate_z':             np.float64,
        'roll':                   np.float64,
        'pitch':                  np.float64,
        'yaw':                    np.float64,
        'imu_ahrs_checksum_f':    np.float64}
    # Note that some variables are int but must be treated as float since
    # numpy doesn't handle NaN in int, but does for floats.

    accel_vars = ['roll', 'pitch', 'yaw',
                  'accel_x', 'accel_y', 'accel_z',
                  'ang_rate_x', 'ang_rate_y', 'ang_rate_z']

    # Distance between accelerometer and IRGASON
    irga_acc_dist = np.array([0.85, -0.1, 0.1])

    # Gravitational constant
    g = 9.80665

    #Find files that match the pattern Station_YYYYMMDD_eddy.csv
    eddyFilesRegex=r'^[0-9]{8}_[0-9]{4}_eddy.csv$'
    eddyFiles = [f for f in os.listdir(
        os.path.join(asciiOutDir,station_name))
                 if re.match(eddyFilesRegex, f)]

    #Find files that match the pattern Station_YYYYMMDD_eddy_rot.csv
    rotFilesRegex=r'^[0-9]{8}_[0-9]{4}_eddy_rot.csv$'
    rotFiles = [f for f in os.listdir(
        os.path.join(asciiOutDir,station_name))
                if re.match(rotFilesRegex, f)]

    logf = open(os.path.join('.','Logs','rotate_wind.log'), "w")

    #####################
    ### Process files ###
    #####################

    for iFile in eddyFiles:

        #Check if the file hasn't been processed yet
        iRotFile = iFile[:-4]+'_rot.csv'
        if iRotFile not in rotFiles:
            print('Processing {}'.format(iFile))
            try:
                # Load and specify datatype
                df = pd.read_csv(os.path.join(asciiOutDir,station_name,iFile),
                                 skiprows=[0,2,3], dtype=var_dtypes,
                                 na_values = "NAN")

                 # Get file header
                with open(os.path.join(asciiOutDir,station_name,iFile), 'r') as fp:
                    header = [next(fp) for x in range(4)]

                # Write header to destination file
                with open(os.path.join(asciiOutDir,station_name,iRotFile), 'w') as fp:
                    for i_line in header:
                        fp.write(i_line)

                # Pre transformation. IRGASON and accelerometer not in the
                # same referential frame. Transform wind speeds into
                # accelerometer frame
                df['Ux'] = -df['Ux']
                df['Uy'] = -df['Uy']

                #######################################
                ### Data cleaning and interpolation ###
                #######################################

                for iVar in accel_vars:

                    # Filtering
                    if iVar in ['roll', 'pitch', 'yaw']:
                        # Nonsensical values
                        id_rm = df[iVar].abs() > 2.01*np.pi
                        df.loc[id_rm,iVar] = np.nan
                    elif iVar in ['accel_x', 'accel_y', 'accel_z',
                                  'ang_rate_x', 'ang_rate_y', 'ang_rate_z']:
                        # Values that are suspicious (violent mouvement)
                        id_rm = df[iVar].abs() > 2
                        df.loc[id_rm,iVar] = np.nan

                    if iVar == 'roll':
                        # Because the accelerometer is mounted upside down, roll values
                        # are around +/- 2pi. This creates discontinuities with
                        # detect_spikes. Hence, everything it set back around -2*pi
                        df.loc[df['roll']>0,'roll'] = df['roll'] - 2*np.pi

                    if iVar == ['roll', 'pitch']:
                        # Values that are suspicious (too far away from horizontal)
                        id_rm = np.array([np.abs(np.mod(df[iVar],np.pi)),
                                      np.abs(np.mod(df[iVar],np.pi)-np.pi)]).min(axis=0) > 0.5
                        df.loc[id_rm,iVar] = np.nan

                    # Spiker removal
                    id_rm = spikes(df, iVar, 10, 7)
                    df.loc[id_rm,iVar] = np.nan

                    # Interpolation
                    df[iVar] = df[iVar].interpolate(method='linear', limit=2)

                # Nullify all accel var if one is missing
                id_rm = df[accel_vars].isna().any(axis=1)
                df.loc[id_rm, accel_vars] = np.nan

                # Replace remaining accelerometer NaN values with
                # 30min average if at least 5 min is available
                df.loc[id_rm, accel_vars] = df[accel_vars].rolling(
                    window=30*60*10, min_periods=5*60*10,
                    center=True).mean()

                # Replace remaining accelerometer NaN values with
                # theoretical resting values
                id_rm = df[accel_vars].isna().any(axis=1)
                df.loc[id_rm,accel_vars] = 0
                df.loc[id_rm,'accel_z'] = 1
                df.loc[id_rm,'roll'] = -np.pi

                ######################
                ### Initialization ###
                ######################

                # Timestep (s)
                dt = 0.1

                # Rotations (rad)
                phi = (df['roll'] + np.pi).values
                theta = df['pitch'].values
                psi = df['yaw'].values

                # Linear acceleration (m/s²)
                lin_acc = np.array([
                    df['accel_x'].values,
                    df['accel_y'].values,
                    df['accel_z'].values -1
                    ]).transpose() * g

                # Angular rate (rad/s)
                ang_rate = np.array([
                    df['ang_rate_x'].values,
                    df['ang_rate_y'].values,
                    df['ang_rate_z'].values
                    ]).transpose()

                # Measured wind speed
                u = np.array([
                    df['Ux'].values,
                    df['Uy'].values,
                    df['Uz'].values
                    ]).transpose()

                # Empty matrix to contain corrected wind speeds
                u_corr = np.empty((df.shape[0],3))

                for i in range(df.shape[0]):

                    ######################################
                    ### Rotation following Miller 2008 ###
                    ######################################

                    # x-axis rotation (roll)
                    phi_ = np.array([
                        [1, 0, 0],
                        [0, np.cos(phi[i]), -np.sin(phi[i])],
                        [0, np.sin(phi[i]), np.cos(phi[i])]
                        ])

                    # y-axis rotation (pitch)
                    theta_ = np.array([
                        [np.cos(theta[i]), 0, np.sin(theta[i])],
                        [0, 1, 0],
                        [-np.sin(theta[i]), 0, np.cos(theta[i])]
                        ])

                    # z-axis rotation (yaw)
                    psi_ = np.array([
                        [np.cos(psi[i]), -np.sin(psi[i]), 0],
                        [np.sin(psi[i]), np.cos(psi[i]), 0],
                        [0, 0, 1],
                        ])

                    T_ = psi_ @ theta_ @ phi_

                    u_corr[i,:] = T_ @ (u[i,:] + lin_acc[i,:] * dt + ang_rate[i,:] * irga_acc_dist)

                df['Ux'] = -u_corr[:,0]
                df['Uy'] = -u_corr[:,1]
                df['Uz'] = u_corr[:,2]

                df.to_csv(os.path.join(asciiOutDir,station_name, iRotFile),
                          header=False, index=False, mode='a', quoting=0)

            except Exception as e:
                print(str(e))
                logf.write("Failed to rotate file {0}: {1} \n".format(iFile, str(e)))

    # Close error log file
    logf.close()
    print('Done!')

