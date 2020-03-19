# -*- coding: utf-8 -*-
# TODO check if the path must be absolute
# TODO manage error code with subprocess.call and add exception
import re
import os
import fileinput
import subprocess

def batch_process_eddypro(stationName,asciiOutDir,eddyproConfigDir,eddyproOutDir,dates):
    """Calls EddyPro software in command lines with arguments

    Parameters
    ----------
    stationName: name of the station
    asciiOutDir: path to the directory that contains the .csv files used as
        input for EddyPro
    eddyproConfigDir: path to the directory that contains the EddyPro .config
        and .metadata files
    eddyproOutDir: path to the directory that will contain EddyPro result files
    dates: dictionnary that contains a 'start' and 'end' key to indicates the
        period range to EddyPro.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}

    Returns
    -------
    """

    print('Start Eddy Pro processing for station:', stationName, '...', end='\r')

    eddyproOutDir   = eddyproOutDir + stationName
    eddyproConfig   = eddyproConfigDir + "Ro2_" + stationName + ".eddypro"
    eddyproMetaData = eddyproConfigDir + "Ro2_" + stationName + ".metadata"
    asciiOutDir     = asciiOutDir + stationName

    # Read in the Eddy Pro config file and replace target strings
    with fileinput.FileInput(eddyproConfig, inplace=True) as file:
        for line in file:
            if re.match(r'file_name',line):
                line = re.sub(r'^file_name=.*$',"file_name="+eddyproConfig, line.rstrip())
                print(line,end='\n')
            elif re.match(r'proj_file',line):
                line = re.sub(r'^proj_file=.*$',"proj_file="+eddyproMetaData, line.rstrip())
                print(line,end='\n')
            elif re.match(r'out_path',line):
                line = re.sub(r'^out_path=.*$',"out_path="+eddyproOutDir, line.rstrip())
                print(line,end='\n')
            elif re.match(r'data_path',line):
                line = re.sub(r'^data_path=.*$',"data_path="+asciiOutDir, line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_start_date',line):
                line = re.sub(r'^pr_start_date=.*$',"pr_start_date="+dates['start'], line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_start_time',line):
                line = re.sub(r'^pr_start_time=.*$',"pr_start_time="+"00:00", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_date',line):
                line = re.sub(r'^pr_end_date=.*$',"pr_end_date="+dates['end'], line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_time',line):
                line = re.sub(r'^pr_end_time=.*$',"pr_end_time="+"00:00", line.rstrip())
                print(line,end='\n')
            else:
                print(line,end='')

    process=os.path.join(".\Bin","EddyPro","bin","eddypro_rp.exe")
    subprocess.call([process, eddyproConfig])

    print('Done!')