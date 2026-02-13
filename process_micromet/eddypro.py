# -*- coding: utf-8 -*-

import sys
import os
import numpy as np
import fileinput
import subprocess
import pandas as pd
from utils import data_loader as dl
from datetime import datetime

def run(station_name,csv_folder,eddypro_config_dir,eddypro_out_dir,dates):
    """Calls EddyPro software in command lines with arguments specified in the
    config file

    Parameters
    ----------
    station_name: name of the station
    csv_folder: path to the directory that contains the .csv files used as
        input for EddyPro
    eddypro_config_dir: path to the directory that contains the EddyPro .config
        and .metadata files
    eddypro_out_dir: path to the directory that will contain EddyPro result files
    dates: dictionnary that contains a 'start' and 'end' key to indicates the
        period range to EddyPro.
        Example: dates{'start': '2018-06-01', 'end': '2020-02-01'}

    Returns
    -------
    """

    print(f'Start Eddy Pro processing for station: {station_name}\n')

    station_eddypro_out_dir = eddypro_out_dir.joinpath(station_name)
    station_csv_folder = csv_folder.joinpath(station_name)
    
    # Construct a datime index with previous EddyPro full_output files.
    output_list = list(station_eddypro_out_dir.glob('*full_output*.csv'))
        
    index_list = []
    for i_file in output_list:
        df = dl.eddypro_fulloutput_file(i_file)
        if not df.index.empty:
            index_list.append(df.index)
    
    if index_list:
        timestamps = pd.DatetimeIndex(np.concatenate(index_list))
        timestamps = timestamps.sort_values().unique()
    else:
        timestamps = pd.DatetimeIndex([], dtype="datetime64[ns]")
        
    # Identify the timestamps in the date range that are not in the EddyPro 
    # output files
    missing_timestamps = pd.DatetimeIndex(
        set(pd.date_range(dates['start'], dates['end'], freq='30min'))
        - set(timestamps)
        ).sort_values()
    
    # Exit if everything has been ran
    if len(missing_timestamps)==0:
        return
            
    # Check the EddyPro configuration folder, list the EddyPro configuration 
    # files for the station, store their path, their validity period
    config_files = list(eddypro_config_dir.glob(f'*{station_name}*.eddypro'))
    eddypro_configs = []
    for path in config_files:
        name = path.stem
        config_dates = name.replace(f'{station_name}_','')
        start_str, end_str = config_dates.split("-")
        start = datetime.strptime(start_str, "%Y%m%d_%H%M")
        if end_str == "current":
            end = datetime(2100, 1, 1)
        else:
            end = datetime.strptime(end_str, "%Y%m%d_%H%M")
    
        eddypro_configs.append({
            "path": path,
            "start": start,
            "end": end,
        })

    for config in eddypro_configs:
        
        # Identify the if there are any missing timestamp for a given config
        missing_timestamps_config = missing_timestamps.intersection(
            pd.date_range(config['start'],config['end'],freq='30min')
            )
        
        if len(missing_timestamps_config)==0: # all dates already processed
            continue
        
        first_timestamp = missing_timestamps_config[0]
        last_timestamp = missing_timestamps_config[-1]
            
        # Read in the Eddy Pro config file and replace target strings
        with fileinput.FileInput(config['path'], inplace=True) as file:
            for line in file:
    
                # Working environment
                if line.startswith('file_name'):
                    line = f"file_name={str(config['path'])}\n"
                elif line.startswith('proj_file'):
                    line = f"proj_file={str(config['path'].with_suffix('.metadata'))}\n"
                elif line.startswith('out_path'):
                    line = f"out_path={station_eddypro_out_dir}\n"
                elif line.startswith('data_path'):
                    line = f"data_path={station_csv_folder}\n"
    
                # Project
                elif line.startswith('pr_start_date'):
                    line = f"pr_start_date={first_timestamp.strftime('%Y-%m-%d')}\n"
                elif line.startswith('pr_start_time'):
                    line = f"pr_start_time={first_timestamp.strftime('%H:%M')}\n"
                elif line.startswith('pr_end_date'):
                    line = f"pr_end_date={last_timestamp.strftime('%Y-%m-%d')}\n"
                elif line.startswith('pr_end_time'):
                    line = f"pr_end_time={last_timestamp.strftime('%H:%M')}\n"
    
                # Statistical analysis
                elif line.startswith('sa_start_date'):
                    line = f"sa_start_date={first_timestamp.strftime('%Y-%m-%d')}\n"
                elif line.startswith('sa_start_time'):
                    line = f"sa_start_time={first_timestamp.strftime('%H:%M')}\n"
                elif line.startswith('sa_end_date'):
                    line = f"sa_end_date={last_timestamp.strftime('%Y-%m-%d')}\n"
                elif line.startswith('sa_end_time'):
                    line = f"sa_end_time={last_timestamp.strftime('%H:%M')}\n"
    
                # Planar fit
                elif line.startswith('pf_start_date'):
                    line = f"pf_start_date={config['start'].strftime('%Y-%m-%d')}\n"
                elif line.startswith('pf_start_time'):
                    line = f"pf_start_time={config['start'].strftime('%H:%M')}\n"
                elif line.startswith('pf_end_date'):
                    line = f"pf_end_date={config['end'].strftime('%Y-%m-%d')}\n"
                elif line.startswith('pf_end_time'):
                    line = f"pf_end_time={config['end'].strftime('%H:%M')}\n"
                    
                sys.stdout.write(line)

        process=os.path.join("./Bin","EddyPro","bin","eddypro_rp.exe")
        subprocess.call([process, config['path']])

    print('Done!')


