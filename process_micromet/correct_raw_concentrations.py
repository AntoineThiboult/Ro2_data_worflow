import os
import re
import pandas as pd

def get_raw_var_names(stationName,date):
    """
    Get H2O, CO2 concentrations, and temperature names.

    Parameters
    ----------
    stationName : string
        Name of the station
    date : Pandas timestamp
        Date of the file being processed

    Returns
    -------
    temperature : strings
    H2O : array of strings
    CO2 : array of strings

    """

    if stationName == 'Berge':
        if date > pd.to_datetime('20220613_1530',format='%Y%m%d_%H%M'):
            temperature = 'amb_tmpr'
            H2O = 'H2O'
            CO2 = ['CO2','CO2_corr']
        else:
            temperature = 'amb_tmpr'
            H2O = 'H2O'
            CO2 = ['CO2']

    if stationName == 'Foret_est':
        if date > pd.to_datetime('20221022_1130',format='%Y%m%d_%H%M'):
            temperature = 'amb_tmpr'
            H2O = 'H2O'
            CO2 = ['CO2','CO2_corr']
        else:
            temperature = 'Ts'
            H2O = 'H2O_li'
            CO2 = ['CO2_li']

    if stationName == 'Foret_ouest':
        if date > pd.to_datetime('20221022_1400',format='%Y%m%d_%H%M'):
            temperature = 'amb_tmpr'
            H2O = 'H2O'
            CO2 = ['CO2','CO2_corr']
        else:
            temperature = 'Ts'
            H2O = 'H2O_li'
            CO2 = ['CO2_li']

    return temperature, H2O, CO2


def correct_raw_concentrations(
        stationName, asciiOutDir, config_dir, overwrite=False):
    """
    Correct water and CO2 concentrations. Correction factors must be provided
    in csv files that contain 'H2O_slope', 'H2O_intercept', and 'CO2_intercept'
    along with a timestamp column.

    Parameters
    ----------
    stationName : string
        Name of the station
    asciiOutDir : string
        Path to the directory that contains raw (10 Hz) .csv files
    config_dir : string
        Path to the directory that contains the .csv file with correction
        coefficients
    overwrite : Boolean, optional
        Overwrite or not the previously corrected concentations. The default is False.

    Returns
    -------
    None. Write a file file_corr.csv with corrected concentrations

    """

    if stationName not in ['Berge','Foret_ouest','Foret_est']:
        return

    print(f'Start raw gas concentration correction for the {stationName} station')

    # Import dataframe that contains the correction coefficients
    df_corr = pd.read_csv( os.path.join(
        config_dir,f'coeff_corr_calibration_{stationName}.csv'))
    df_corr['timestamp'] = pd.to_datetime(df_corr['timestamp'])

    #Find files that match the pattern Station_YYYYMMDD_eddy.csv
    eddyFilesRegex=r'^[0-9]{8}_[0-9]{4}_eddy.csv$'
    eddyFiles = [f for f in os.listdir(
        os.path.join(asciiOutDir,stationName))
                 if re.match(eddyFilesRegex, f)]

    #Find files that match the pattern Station_YYYYMMDD_eddy_corr.csv
    corrFilesRegex=r'^[0-9]{8}_[0-9]{4}_eddy_corr.csv$'
    corrFiles = [f for f in os.listdir(
        os.path.join(asciiOutDir,stationName))
                if re.match(corrFilesRegex, f)]

    logf = open( os.path.join(
        '.','Logs',f'correct_raw_concentrations_{stationName}.log'), "w")

    #####################
    ### Process files ###
    #####################

    for iFile in eddyFiles:

        #Check if the file hasn't been processed yet
        iCorrFile = iFile[:-4]+'_corr.csv'

        if (iCorrFile not in corrFiles) | overwrite :
            print('Processing {}'.format(iFile))
            try:
                # Load and specify datatype
                df = pd.read_csv(os.path.join(asciiOutDir,stationName,iFile),
                                 skiprows=[0,2,3],
                                 na_values = "NAN")

                 # Get file header
                with open(os.path.join(asciiOutDir,stationName,iFile), 'r') as fp:
                    header = [next(fp) for x in range(4)]

                # Write header to destination file
                with open(os.path.join(asciiOutDir,stationName,iCorrFile), 'w') as fp:
                    for i_line in header:
                        fp.write(i_line)

                # Get appropriate correction factor for the date
                file_date = pd.to_datetime(iFile[0:-9], format='%Y%m%d_%H%M')
                id_corr = df_corr.index[df_corr['timestamp'] == file_date]

                if sum(id_corr) !=0:

                    temperature, H2O, CO2 = get_raw_var_names(stationName, file_date)

                    # Water concentrations
                    df[H2O] = (
                        df[H2O]
                        + df_corr.loc[id_corr,'H2O_slope'].values[0] * df[temperature]
                        + df_corr.loc[id_corr,'H2O_intercept'].values[0]
                        )

                    # CO2 concentrations
                    df[CO2] = df[CO2] + df_corr.loc[id_corr,'CO2_intercept'].values[0]


                df.to_csv(os.path.join(asciiOutDir,stationName, iCorrFile),
                          header=False, index=False, mode='a', quoting=0)

            except Exception as e:
                print(str(e))
                logf.write('Failed to correct concentration for file'+
                           f'{iFile}: {str(e)} \n')

    # Close error log file
    logf.close()
    print('Done!')