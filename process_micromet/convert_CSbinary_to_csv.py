# -*- coding: utf-8 -*-
# TODO solve issue with shutil.copy that overwrite previous file. Add iDataCollection to name
from datetime import datetime as dt
import os
import re
import subprocess
import shutil
import pandas as pd

def convert_CSbinary_to_csv(stationName,rawFileDir,asciiOutDir):
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
        |--Ro2_YYYYMMDD
        |   |--stationName_YYYYMMDD
        |       |--foo.dat
        |       |--bar.dat
        |--Ro2_YYYYMMDD
        |   |--stationName_YYYYMMDD
        |       |--foo.dat
        |       |--bar.dat

    It logs issues in the convert_CSbinary_to_csv.log file.

    Parameters
    ----------
    stationName: name of the station
    rawFileDir: path to the directory that contains the Campbell Scientific
        binary files
    asciiOutDir: path to the directory that contains the converted files (.csv)

    Returns
    -------
    """

    print('Start converting Campbell binary files to csv for station:', stationName)

    # Open error log file
    logf = open(os.path.join('.','Logs','convert_CSbinary_to_csv.log'), "w")

    #Find folders that match the pattern Ro2_YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]

    for iFieldCampain in listFieldCampains:

        #Find folders that match the pattern Station_YYYYMMDD
        sationNameRegex=r'^' + stationName + r'_[0-9]{8}$'
        listDataCollection  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(sationNameRegex, f)]

        for iDataCollection in listDataCollection:
            print(iDataCollection)

            # Check if conversion has already been performed
            destDirContent = os.listdir(os.path.join(asciiOutDir,stationName))
            subStr = re.search(r'^.*([0-9]{8})$', iDataCollection).group(1)

            if not [f for f in destDirContent if re.match(subStr, f)]:

                for rawFile in os.listdir(os.path.join(rawFileDir,iFieldCampain,iDataCollection)):
                    print('\t'+rawFile)

                    inFile=os.path.join(rawFileDir,iFieldCampain,iDataCollection,rawFile)
                    outFile=os.path.join(asciiOutDir,stationName,rawFile)

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
                            df = pd.read_csv(outFile, sep=',', index_col=None, skiprows=[0,2,3])
                            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
                            
                            # Split the file into 30 minutes files
                            index = df.index[
                                ( (df['TIMESTAMP'].dt.minute == 0) | (df['TIMESTAMP'].dt.minute == 30) ) & \
                                (df['TIMESTAMP'].dt.second == 0) & \
                                (df['TIMESTAMP'].dt.microsecond == 0)]
                            index = index.insert(0,-1)
                            index = zip(index[0:-1],index[1:])                                
                            
                            # Write splitted files
                            for i in index:
                                file_name = os.path.join(
                                    asciiOutDir,stationName,
                                    df.loc[i[0]+1,'TIMESTAMP'].strftime(
                                    '%Y%m%d_%H%M') + extension)
                                # Write header
                                with open(file_name,'w') as f:
                                    for h in header:
                                        f.write(h)        
                                df.loc[i[0]+1:i[1],:].to_csv(
                                    file_name, mode='a', header=False, index=False)
                            os.remove(outFile)

                        else:
                            # Rename file according to date
                            fileContent=pd.read_csv(outFile, sep=',', index_col=None, skiprows=[0,2,3], nrows=1)
                            fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S")    # TIMESTAMP format for _alert.csv, _radiation.csv, and _met30min.csv                        
                            newFileName=dt.strftime(fileStartTime,'%Y%m%d_%H%M')+extension
                            shutil.move(outFile,os.path.join(asciiOutDir,stationName,newFileName))
                            
                    except Exception as e:
                        print(str(e))
                        logf.write("Failed to convert {0} from bin to csv: {1} \n".format(inFile, str(e)))

    # Close error log file
    logf.close()

    print('Done!')