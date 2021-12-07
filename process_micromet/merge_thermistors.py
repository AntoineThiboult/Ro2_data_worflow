# -*- coding: utf-8 -*-
import os
import pandas as pd
import re
import warnings

def merge_thermistors(dates, rawFileDir, mergedCsvOutDir):
    """Merge data collected by the thermistors (TidBit and U20 sensors).
    Because HOBOware does not disclose their binary format, this function does
    not provide any conversion from .hobo to a broadly readable format. Thus,
    one needs to first convert .hobo to .xlxs with HOBOware.

    The source directory containing the .xlsx files should be organized as
    follow:
        rawFileDir
        |--Ro2_YYYYMMDD
        |   |--TidBit_YYYYMMDD
        |       |--Excel_exported_YYYYMMDD
        |           |--foo.dat
        |           |--bar.dat
        |--Ro2_YYYYMMDD
        |   |--TidBit_YYYYMMDD
        |       |--Excel_exported_YYYYMMDD
        |           |--foo.dat
        |           |--bar.dat

    Parameters
    ----------
    dates: dictionnary that contains a 'start' and 'end' key to indicates the
        period range to EddyPro.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}
    rawFileDir: path to the directory that contains the .xlsx files
    mergedCsvOutDir: path to the directory that contains final .csv files

    Returns
    -------
    None.
    """

    print('Start merging thermistors data')
    df = pd.DataFrame( index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min') )

    #Find folders that match the pattern Ro2_YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]

    counterField = 0
    for iFieldCampain in listFieldCampains:

        #Find folders that match the pattern TidBit_YYYYMMDD
        sationNameRegex = r'^' + 'TidBit' + r'_[0-9]{8}$'
        listDataCollection  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(sationNameRegex, f)]

        for iDataCollection in listDataCollection:

            #Find all thermistor files in folder
            thermNameRegex = r'^' + 'Therm' + r'[1-2].*xlsx$'
            listThermSensors = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain,iDataCollection,'Excel_exported')) if re.match(thermNameRegex, f)]
            counterSensor = 0

            for iSensor in listThermSensors:

                # Load data and handle annoying warning message
                with warnings.catch_warnings(record=True):
                    warnings.simplefilter("always")
                    df_tmp = pd.read_excel(os.path.join(rawFileDir,iFieldCampain,iDataCollection,'Excel_exported',iSensor), skiprows=[0], engine=('openpyxl'))

                # Remove 12 hours before and after data collection to avoid air contamination
                df_tmp = df_tmp.drop(df_tmp.index[0:24])
                df_tmp = df_tmp.drop(df_tmp.index[-26:])

                # Making nice variable names
                tmp_sensorNiceName = re.sub('\.','m',iSensor,1)[0:-5]
                sensorNiceNameTemp = 'water_temp_' + re.split('_',tmp_sensorNiceName)[1] + '_' + re.split('_',tmp_sensorNiceName)[0]
                sensorNiceNamePress = 'water_height_' + re.split('_',tmp_sensorNiceName)[1] + '_' + re.split('_',tmp_sensorNiceName)[0]


                # Remove log columns
                listCol = [c for c in df_tmp.columns if re.match('.*(Date|Temp|Pres).*', c)]
                df_tmp = df_tmp[listCol]
                df_tmp.index = pd.to_datetime(df_tmp.iloc[:,0])
                df_tmp = df_tmp.loc[~df_tmp.index.duplicated(keep='first')]

                # Fill df with records
                if df_tmp.shape[1] == 2: # Temperature only sensor
                    idDates_RecInRef = df_tmp.index.isin(df.index)
                    idDates_RefInRec = df.index.isin(df_tmp.index)
                    df.loc[idDates_RefInRec,sensorNiceNameTemp] = df_tmp.loc[idDates_RecInRef,df_tmp.columns[1]]

                elif df_tmp.shape[1] == 3: # Temperature and pressure sensor
                    idDates_RecInRef = df_tmp.index.isin(df.index)
                    idDates_RefInRec = df.index.isin(df_tmp.index)
                    df.loc[idDates_RefInRec,sensorNiceNameTemp] = df_tmp.loc[idDates_RecInRef,df_tmp.columns[2]]
                    df.loc[idDates_RefInRec,sensorNiceNamePress] = df_tmp.loc[idDates_RecInRef,df_tmp.columns[1]]

                print("\rMerging thermistors for dataset {:s}. Total progress {:2.1%} ".format(
                    iFieldCampain, 1/len(listFieldCampains)*counterField +
                    1/len(listFieldCampains)*counterSensor/len(listThermSensors)), end='\r')
                counterSensor += 1
        counterField += 1

    df['timestamp'] = df.index
    df = df.reindex(sorted(df.columns), axis=1)

    # Linear interpolation when less than two days are missing
    df.loc[:, df.columns != 'timestamp'] = \
        df.loc[:, df.columns != 'timestamp'].interpolate(method='linear', limit=96)

    # Save file
    df.to_csv(os.path.join(mergedCsvOutDir,'Thermistors.csv'), index=False)
    print('\nDone!')
