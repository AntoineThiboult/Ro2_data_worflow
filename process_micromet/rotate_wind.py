# -*- coding: utf-8 -*-
import os
import re
import numpy as np
import pandas as pd
from process_micromet import detect_spikes

def rotate_wind(stationName,asciiOutDir):
    """Perform a rotation of the IRGASON 3D wind components according to the
    3DM-25 accelerometer measurements.
    Reads 10Hz foobar.csv files and write new foobar_rot.csv files.
    This function only works for the reservoir station.

    Parameters
    ----------
    stationName: name of the station
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

    if stationName == 'Reservoir':
        print('Start performing wind rotation for the reservoir station')

        ############################
        ### Constant declaration ###
        ############################

        var_dtypes = {
            'TIMESTAMP':              np.object0,
            'RECORD':                 np.int64,
            'Ux':                     np.float64,
            'Uy':                     np.float64,
            'Uz':                     np.float64,
            'T_SONIC':                np.float64,
            'diag_sonic':             np.int64,
            'CO2_density':            np.float64,
            'CO2_density_fast_tmpr':  np.float64,
            'H2O_density':            np.float64,
            'diag_irga':              np.int64,
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
            'imu_ahrs_checksum_f':    np.int64}

        accel_vars = ['roll', 'pitch', 'yaw',
                      'accel_x', 'accel_y', 'accel_z',
                      'ang_rate_x', 'ang_rate_y', 'ang_rate_z']

        # Distance between accelerometer and IRGASON
        irga_acc_dist = np.array([0.81, -0.07, 0.07])

        # Gravitational constant
        g = 9.80665

        #Find files that match the pattern Station_YYYYMMDD_eddy.csv
        eddyFilesRegex=r'^[0-9]{8}_[0-9]{4}_eddy.csv$'
        eddyFiles = [f for f in os.listdir(
            os.path.join(asciiOutDir,stationName))
                     if re.match(eddyFilesRegex, f)]

        #Find files that match the pattern Station_YYYYMMDD_eddy_rot.csv
        rotFilesRegex=r'^[0-9]{8}_[0-9]{4}_eddy_rot.csv$'
        rotFiles = [f for f in os.listdir(
            os.path.join(asciiOutDir,stationName))
                    if re.match(rotFilesRegex, f)]

        logf = open("rotate_wind.log", "w")

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
                    df = pd.read_csv(os.path.join(asciiOutDir,stationName,iFile),
                                     skiprows=[0,2,3], dtype=var_dtypes,
                                     na_values = "NAN")

                     # Get file header
                    with open(os.path.join(asciiOutDir,stationName,iFile), 'r') as fp:
                        header = [next(fp) for x in range(4)]

                    # Write header to destination file
                    with open(os.path.join(asciiOutDir,stationName,iRotFile), 'w') as fp:
                        for i_line in header:
                            fp.write(i_line)

                    ##########################
                    ### Pre transformation ###
                    ##########################

                    # Because the accelerometer is mounted upside down, roll values
                    # are around +/- 2pi. This creates discontinuities with
                    # detect_spikes. Hence, everything it set back around -2*pi
                    df.loc[df['roll']>0,'roll'] = df['roll'] - 2*np.pi

                    # IRGASON and accelerometer not in the same referential frame
                    # Transform wind speeds into accelerometer frame
                    df['Ux'] = -df['Ux']
                    df['Uy'] = -df['Uy']

                    # Clean data and do linear interpolation (max over 0.5s)
                    for iVar in accel_vars:
                        id_rm = detect_spikes(df, iVar, 10, 7)
                        df.loc[id_rm,iVar] = np.nan
                        if iVar in ['roll', 'pitch', 'yaw']:
                            df[iVar] = df[iVar].interpolate(
                                method='linear', limit=20)
                        else:
                            df[iVar] = df[iVar].interpolate(
                                method='linear', limit=600)

                        # Replace accelerometer NaN values with zeros
                        # (avoid discarding valid Irga data)
                        df.loc[df[iVar].isna(),iVar] = 0


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

                    df.to_csv(os.path.join(asciiOutDir,stationName, iRotFile),
                              header=False, index=False, mode='a', quoting=0)

                except Exception as e:
                    print(str(e))
                    logf.write("Failed to convert {0} from bin to csv: {1} \n".format(iFile, str(e)))

        # Close error log file
        logf.close()
        print('Done!')

