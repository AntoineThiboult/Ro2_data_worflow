# -*- coding: utf-8 -*-
# TODO manage error code with subprocess.call and add exception

import os
import re
from glob import glob
import fileinput
import subprocess
import pandas as pd

def run(station_name,ascii_dir,eddypro_config_dir,eddypro_out_dir,dates):
    """Calls EddyPro software in command lines with arguments specified in the
    config file

    Parameters
    ----------
    station_name: name of the station
    ascii_dir: path to the directory that contains the .csv files used as
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

    print('Start Eddy Pro processing for station:', station_name, '...', end='\r')

    eddypro_dates = dates.copy()

    # Check if there are existing EddyPro files and load the newest
    # Overwrite the starting date to run only the necessary
    output_list = glob(eddypro_out_dir+'/'+station_name+'\*full_output*.csv')
    output_list = [sub.replace('\\', '/') for sub in output_list]

    if output_list:
        newest_file = max(output_list, key=os.path.getctime)
        df = pd.read_csv(newest_file,skiprows=[0,2], usecols=['date'])
        last_timestamp =  df.loc[df.index[-1],'date']
        if  eddypro_dates['start'] < last_timestamp:
            eddypro_dates['start'] = last_timestamp


    # Check if there are multiple EddyPro configuration
    config_list = glob(eddypro_config_dir+'/'+'*'+station_name+'*.eddypro')
    config_list = [sub.replace('\\', '/') for sub in config_list]
    config_list = [sub.replace('.eddypro', '') for sub in config_list]
    eddypro_configs = pd.DataFrame(
        {'start': pd.Series(dtype='datetime64[ns]'),
         'end': pd.Series(dtype='datetime64[ns]'),
         'path': pd.Series(dtype='string')},
        index=range(len(config_list))
        )

    # Fill a EddyPro config dataframe
    for count, i_config in enumerate(config_list):
        matching_str = re.findall('\d{8}_\d{4}', i_config)
        if matching_str:
            eddypro_configs.loc[count, 'end'] = pd.to_datetime(
               matching_str[0],format='%Y%m%d_%H%M')
            eddypro_configs.loc[count,'path'] = i_config
        else:
            eddypro_configs.loc[count,'end'] = pd.Timestamp.today()
            eddypro_configs.loc[count,'path'] = i_config
    eddypro_configs = eddypro_configs.sort_values(by='end', ignore_index=True)

    for index in eddypro_configs.index:
        if index > 0:
            eddypro_configs.loc[index,'start'] = \
                eddypro_configs.loc[index-1,'end']
        else:
            eddypro_configs.loc[index,'start'] = \
                pd.to_datetime(0)

    # Find the configuration that matches the EddyPro dates
    id_start = eddypro_configs[
        pd.to_datetime(eddypro_dates['start']) <= eddypro_configs['end']
        ].first_valid_index()
    id_end = eddypro_configs[
        pd.to_datetime(eddypro_dates['end']) <= eddypro_configs['end']
        ].first_valid_index()
    eddypro_configs = eddypro_configs[id_start:id_end+1]

    eddypro_configs.loc[eddypro_configs.index[0],'start'] = eddypro_dates['start']
    eddypro_configs.loc[eddypro_configs.index[-1],'end'] = eddypro_dates['end']


    for i_config in eddypro_configs.index:

        station_out_dir     = eddypro_out_dir + station_name
        station_config      = eddypro_configs.loc[i_config,'path'] + '.eddypro'
        station_metadata    = eddypro_configs.loc[i_config,'path'] + '.metadata'
        station_ascii_dir   = ascii_dir + station_name

        # Read in the Eddy Pro config file and replace target strings
        with fileinput.FileInput(station_config, inplace=True) as file:
            for line in file:

                # Working environment
                if re.match(r'file_name',line):
                    line = re.sub(r'^file_name=.*$',
                                  "file_name=" + station_config,
                                  line.rstrip())
                    print(line,end='\n')
                elif re.match(r'proj_file',line):
                    line = re.sub(r'^proj_file=.*$',
                                  "proj_file=" + station_metadata,
                                  line.rstrip())
                    print(line,end='\n')
                elif re.match(r'out_path',line):
                    line = re.sub(r'^out_path=.*$',
                                  "out_path=" + station_out_dir,
                                  line.rstrip())
                    print(line,end='\n')
                elif re.match(r'data_path',line):
                    line = re.sub(r'^data_path=.*$',
                                  "data_path=" + station_ascii_dir,
                                  line.rstrip())
                    print(line,end='\n')

                # Project
                elif re.match(r'pr_start_date',line):
                    line = re.sub(r'^pr_start_date=.*$',
                                  "pr_start_date=" +
                                  eddypro_configs.loc[
                                      i_config,'start'].strftime('%Y-%m-%d'),
                                          line.rstrip())
                    print(line,end='\n')
                elif re.match(r'pr_start_time',line):
                    line = re.sub(r'^pr_start_time=.*$',
                                  "pr_start_time=" +
                                  eddypro_configs.loc[
                                      i_config,'start'].strftime('%H:%M'),
                                          line.rstrip())
                    print(line,end='\n')
                elif re.match(r'pr_end_date',line):
                    line = re.sub(r'^pr_end_date=.*$',
                                  "pr_end_date=" +
                                  eddypro_configs.loc[
                                      i_config,'end'].strftime('%Y-%m-%d'),
                                          line.rstrip())
                    print(line,end='\n')
                elif re.match(r'pr_end_time',line):
                    line = re.sub(r'^pr_end_time=.*$',
                                  "pr_end_time=" +
                                  eddypro_configs.loc[
                                      i_config,'end'].strftime('%H:%M'),
                                          line.rstrip())
                    print(line,end='\n')

                # Statistical analysis
                elif re.match(r'sa_start_date',line):
                    line = re.sub(r'^sa_start_date=.*$',
                                  "sa_start_date=" "pr_start_date=" +
                                   eddypro_configs.loc[
                                       i_config,'start'].strftime('%Y-%m-%d'),
                                           line.rstrip())
                    print(line,end='\n')
                elif re.match(r'sa_start_time',line):
                    line = re.sub(r'^sa_start_time=.*$',
                                  "sa_start_time=" +
                                  eddypro_configs.loc[
                                      i_config,'start'].strftime('%H:%M'),
                                          line.rstrip())
                    print(line,end='\n')
                elif re.match(r'sa_end_date',line):
                    line = re.sub(r'^sa_end_date=.*$',
                                  "sa_end_date=" +
                                  eddypro_configs.loc[
                                      i_config,'end'].strftime('%Y-%m-%d'),
                                          line.rstrip())
                    print(line,end='\n')
                elif re.match(r'sa_end_time',line):
                    line = re.sub(r'^sa_end_time=.*$',
                                  "sa_end_time=" +
                                  eddypro_configs.loc[
                                      i_config,'end'].strftime('%H:%M'),
                                          line.rstrip())
                    print(line,end='\n')

                # Planar fit
                elif re.match(r'pf_start_date',line):
                    line = re.sub(r'^pf_start_date=.*$',
                                  "pf_start_date=" "pr_start_date=" +
                                   eddypro_configs.loc[
                                       i_config,'start'].strftime('%Y-%m-%d'),
                                           line.rstrip())
                    print(line,end='\n')
                elif re.match(r'pf_start_time',line):
                    line = re.sub(r'^pf_start_time=.*$',
                                  "pf_start_time=" +
                                  eddypro_configs.loc[
                                      i_config,'start'].strftime('%H:%M'),
                                          line.rstrip())
                    print(line,end='\n')
                elif re.match(r'pf_end_date',line):
                    line = re.sub(r'^pf_end_date=.*$',
                                  "pf_end_date=" +
                                  eddypro_configs.loc[
                                      i_config,'end'].strftime('%Y-%m-%d'),
                                          line.rstrip())
                    print(line,end='\n')
                elif re.match(r'pf_end_time',line):
                    line = re.sub(r'^pf_end_time=.*$',
                                  "pf_end_time=" +
                                  eddypro_configs.loc[
                                      i_config,'end'].strftime('%H:%M'),
                                          line.rstrip())
                    print(line,end='\n')
                else:
                    print(line,end='')

        process=os.path.join(".\Bin","EddyPro","bin","eddypro_rp.exe")
        subprocess.call([process, station_config])

    print('Done!')


def merge(station_name,eddypro_out_dir,dates):
    """Load and merge the EddyPro output files found in source directory

    Parameters
    ----------
    station_name: name of the station
    inputDir: path of the directory that contains the EddyPro output files

    Returns
    -------
    df: a nice and tidy pandas DataFrame
    """
    print('Start merging Eddy Pro file for station:', station_name, '...', end='\r')

    # Initialize dataframe
    df = pd.DataFrame(
        index=pd.date_range(start=dates['start'], end=dates['end'], freq='30min')
        )

    # List eddy pro output files and select most recent one
    output_list = glob(eddypro_out_dir+'/'+station_name+'/'+'\*full_output*.csv')

    # Fill dataframe
    for i_file in output_list:

        df_tmp = pd.read_csv(i_file,skiprows=[0,2])

        # Create a standardized time column
        df_tmp['TIMESTAMP'] = pd.to_datetime(
            df_tmp['date'].map(str) + " " + df_tmp['time'].map(str),
            yearfirst=True)
        df_tmp.index = df_tmp['TIMESTAMP']

        idDates_RecInRef = df_tmp.index.isin(df.index)
        idDates_RefInRec = df.index.isin(df_tmp.index)
        df.loc[idDates_RefInRec,df_tmp.columns] = \
            df_tmp.loc[df_tmp.index[idDates_RecInRef],df_tmp.columns]

    df['TIMESTAMP'] = df.index

    print('Done!')

    return df