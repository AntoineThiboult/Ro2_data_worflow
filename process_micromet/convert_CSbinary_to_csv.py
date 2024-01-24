# -*- coding: utf-8 -*-
# TODO solve issue with shutil.copy that overwrite previous file. Add iDataCollection to name
from datetime import datetime as dt
import os
import re
import subprocess
import shutil
import pandas as pd

def convert_CSbinary_to_csv(station_name_raw, station_name_ascii,
                            rawFileDir, asciiOutDir):
    """Convert Campbell Scientific binary files (.dat) to readable .csv files.
    Csv files are named in the format YYYYMMDD_hhmm_type.csv, where the date
    refers to the first time stamp found in the file and the type can be:
        - eddy for 10Hz data
        - slow for 30 min data
        - slow2, slow3 if there is more than one type of 30 min data
    In the case where the conversion fails, it only copy the file to the
    destination directory

    The source directory containing the .dat files should be organized as
    follow:
        rawFileDir
        |--YYYYMMDD
        |   |--station_name_raw_YYYYMMDD
        |       |--foo.dat
        |       |--bar.dat
        |--YYYYMMDD
        |   |--station_name_raw_YYYYMMDD
        |       |--foo.dat
        |       |--bar.dat

    It logs issues in the convert_CSbinary_to_csv.log file.

    Parameters
    ----------
    station_name_raw: name of the station as stored in the raw data
    station_name_ascii: name of the station as stored in the ascii data
    rawFileDir: path to the directory that contains the Campbell Scientific
        binary files
    asciiOutDir: path to the directory that contains the converted files (.csv)

    Returns
    -------
    """

    print(f'Start converting Campbell binary files to csv for station: {station_name_ascii}')

    # Open error log file
    logf = open(os.path.join('.','Logs','convert_CSbinary_to_csv.log'), "w")

    #Find folders that match the pattern YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'[0-9]{8}$', f)]

    for iFieldCampain in listFieldCampains:

        #Find folders that match the pattern Station_YYYYMMDD
        stationNameRegex=r'^' + station_name_raw + r'_[0-9]{8}$'
        listDataCollection  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(stationNameRegex, f)]

        for iDataCollection in listDataCollection:

            # Check if conversion has already been performed
            destDirContent = os.listdir(os.path.join(asciiOutDir,station_name_ascii))
            subStr = re.search(r'^.*([0-9]{8})$', iDataCollection).group(1)

            if not [f for f in destDirContent if re.match(subStr, f)]:

                for rawFile in os.listdir(os.path.join(rawFileDir,iFieldCampain,iDataCollection)):
                    print(f'\t Currently processing file {rawFile}')

                    inFile=os.path.join(rawFileDir,iFieldCampain,iDataCollection,rawFile)
                    outFile=os.path.join(asciiOutDir,station_name_ascii,rawFile)

                    try:
                        # File type name handling
                        if bool(re.search("ts_data_",rawFile)) | bool(re.search("_Time_Series_",rawFile)):
                            extension="_eddy.csv"
                        elif bool(re.search("alerte",rawFile)):
                            extension="_alert.csv"
                        elif bool(re.search("_Flux_CSIFormat_",rawFile)) | bool(re.search("flux",rawFile)) | bool(re.search("data_",rawFile)):
                            extension="_slow.csv"
                        elif bool(re.search("radiation",rawFile)) | bool(re.search("_Flux_Notes_",rawFile)):
                            extension="_slow2.csv"
                        elif bool(re.search("met30min",rawFile)):
                            extension="_slow3.csv"
                        else:                           # .cr1 / .cr3 / sys_log files / Config_Setting_Notes / Flux_AmeriFluxFormat_12
                            shutil.copy(inFile,outFile)
                            continue

                        # Conversion from the Campbell binary file to csv format
                        process=os.path.join(".\Bin","raw2ascii","csidft_convert.exe")
                        subprocess.call([process, inFile, outFile, 'ToA5'])

                        if extension == "_eddy.csv":

                            # Save the header to respect TOA5 format
                            with open(outFile) as f:
                                header = [next(f) for x in range(4)]

                            # Load file
                            df = pd.read_csv(outFile,sep=',',index_col=None,skiprows=[0,2,3],low_memory=False)
                            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format = 'mixed')

                            # Split the file into 30 minutes files
                            index = df.index[
                                ( (df['TIMESTAMP'].dt.minute == 0) | (df['TIMESTAMP'].dt.minute == 30) ) & \
                                (df['TIMESTAMP'].dt.second == 0) & \
                                (df['TIMESTAMP'].dt.microsecond == 0)]
                            index = index.insert(0,-1)
                            index = zip(index[0:-1],index[1:])

                            # Write splitted files
                            for i in index:
                                if i[1]-i[0] != 18000:
                                    # Save the output file in the incomplete
                                    # folder if the series contains less than
                                    # 18000 records (full 30 min @ 10Hz)
                                    file_name = os.path.join(
                                        asciiOutDir,station_name_ascii,'Incomplete',
                                        df.loc[i[0]+1,'TIMESTAMP'].strftime(
                                        '%Y%m%d_%H%M') + extension)
                                else:
                                    file_name = os.path.join(
                                        asciiOutDir,station_name_ascii,
                                        df.loc[i[0]+1,'TIMESTAMP'].strftime(
                                        '%Y%m%d_%H%M') + extension)
                                # Write header
                                with open(file_name,'w') as f:
                                    for h in header:
                                        f.write(h)
                                df.loc[i[0]+1:i[1],:].to_csv(
                                    file_name, mode='a', header=False, index=False)
                            os.remove(outFile)
                            
                        elif extension == "_slow.csv":

                            # Save the header to respect TOA5 format
                            with open(outFile) as f:
                                header = [next(f) for x in range(4)]

                            # Read file and define timestamp
                            tmp_df = pd.read_csv(outFile, sep=',',skiprows=[0,2,3], low_memory=False, na_values=['NAN'])
                            tmp_df['TIMESTAMP'] = pd.to_datetime(tmp_df['TIMESTAMP'])
                            tmp_df = tmp_df.drop_duplicates(subset='TIMESTAMP', keep='last')
                            tmp_df = tmp_df.set_index('TIMESTAMP')

                            # Identify columns that are summed and columns that are averaged.
                            ind_sum = tmp_df.filter(regex=('\_Tot|\_aggregate')).columns
                            ind_ave = tmp_df.columns.difference(ind_sum, sort=False)
                            func_all = {**{ind_sum[i]: lambda x: x.sum() if len(x) >= 15 else None for i in range(len(ind_sum))}, 
                                        **{ind_ave[i]: lambda x: x.mean() if len(x) >= 15 else None for i in range(len(ind_ave))}}                        

                            # Resample the minute columns into 30-min using function defined for each column
                            df_30min = tmp_df.resample('30T').agg(func_all)
        
                            # Bring back the timestamp on first column and reorder columns as original
                            df_30min = df_30min.reset_index()
                            tmp_df = tmp_df.reset_index()
                            df_30min = df_30min[tmp_df.columns]

                            # Move 1-min file to a new directory
                            fileStartTime=tmp_df.TIMESTAMP[0]
                            newFileName=dt.strftime(fileStartTime,'%Y%m%d_%H%M')+extension
                            shutil.move(outFile, os.path.join(asciiOutDir,stationName,'Slow_min',newFileName))

                            # Define new file name
                            file_name = os.path.join(
                                asciiOutDir,stationName,
                                df_30min['TIMESTAMP'][0].strftime('%Y%m%d_%H%M') + '_slow.csv')
                                
                            # Write header
                            with open(file_name,'w') as f:
                                for h in header:
                                    f.write(h)
                            df_30min.to_csv(
                                file_name, mode='a', header=False, index=False)

                        else:
                            # Rename file according to date
                            fileContent=pd.read_csv(outFile, sep=',', index_col=None, skiprows=[0,2,3], nrows=1)
                            fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S")    # TIMESTAMP format for _alert.csv, _radiation.csv, and _met30min.csv
                            newFileName=dt.strftime(fileStartTime,'%Y%m%d_%H%M')+extension
                            shutil.move(outFile,os.path.join(asciiOutDir,station_name_ascii,newFileName))

                    except Exception as e:
                        print(str(e))
                        logf.write("Failed to convert {0} from bin to csv: {1} \n".format(inFile, str(e)))
            print(f'Folder {iDataCollection} processed')
    # Close error log file
    logf.close()

    print('Done!')