# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 15:12:45 2019

@author: ANTHI182
"""
import os
import re
import pandas as pd
import numpy as np
import subprocess
import shutil
import fileinput
from datetime import datetime as dt #TODO , timedelta as td
from glob import glob


# TODO manage error and warning codes to final CSV with log file

def convert_CSbinary_to_csv(stationName,rawFileDir,asciiOutDir):
    # Open error log file
    logf = open("convert_CSbinary_to_csv.log", "w")

    #Find folders that match the pattern Ro2_YYYYMMDD
    listFieldCampains = [f for f in os.listdir(rawFileDir) if re.match(r'^Ro2_[0-9]{8}$', f)]

    for iFieldCampain in listFieldCampains:

        #Find folders that match the pattern Station_YYYYMMDD
        sationNameRegex=r'^' + stationName + r'_[0-9]{8}$'
        listDataCollection  = [f for f in os.listdir(os.path.join(rawFileDir,iFieldCampain)) if re.match(sationNameRegex, f)]

        for iDataCollection in listDataCollection:
            print(iDataCollection)
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
                    elif bool(re.search("met30min",rawFile)) | bool(re.search("_Flux_CSIFormat_",rawFile)) | bool(re.search("flux",rawFile)):
                        extension="_slow.csv"
                    elif bool(re.search("radiation",rawFile)) | bool(re.search("_Flux_Notes_",rawFile)):
                        extension="_slow2.csv"
                    else:                           # .cr1 / .cr3 / sys_log files / Config_Setting_Notes / Flux_AmeriFluxFormat_12
                        shutil.copy(inFile,outFile) # TODO solve issue: file with same name will overwrite
                        continue

                    # Conversion from the Campbell binary file to csv format
                    process=os.path.join(".\Bin","raw2ascii","csidft_convert.exe")
                    subprocess.call([process, inFile, outFile, 'ToA5']) # TODO check compatibility with unix and Wine

                    # Rename file according to date
                    fileContent=pd.read_csv(outFile, sep=',', index_col=None, skiprows=[0,2,3], nrows=1)
                    try:
                        fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S")    # TIMESTAMP format for _alert.csv, _radiation.csv, and _met30min.csv
                    except:
                        fileStartTime=dt.strptime(fileContent.TIMESTAMP[0], "%Y-%m-%d %H:%M:%S.%f") # TIMESTAMP format for _eddy.csv file

                    newFileName=dt.strftime(fileStartTime,'%Y%m%d_%H%M')+extension
                    shutil.move(outFile,os.path.join(asciiOutDir,stationName,newFileName))
                except Exception as e:
                    print(str(e))
                    logf.write("Failed to convert {0} from bin to csv: {1} \n".format(inFile, str(e)))

    # Close error log file
    logf.close()

def batch_process_eddypro(iStation,asciiOutDir,eddyproConfig,eddyproMetaData,eddyproOutDir):

    eddyproOutDir   = eddyproOutDir + iStation
    eddyproConfig   = eddyproConfig + "Ro2_" + iStation + ".eddypro"
    eddyproMetaData = eddyproMetaData +"Ro2_"+ iStation + ".metadata"
    asciiOutDir     = asciiOutDir + iStation

    # Read in the Eddy Pro config file and replace target strings
    # TODO check if the path must be absolute
    with fileinput.FileInput(eddyproConfig, inplace=True, backup='.bak') as file:
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
                line = re.sub(r'^pr_start_date=.*$',"pr_start_date="+"2018-06-01", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_start_time',line):
                line = re.sub(r'^pr_start_time=.*$',"pr_start_time="+"00:00", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_date',line):
                line = re.sub(r'^pr_end_date=.*$',"pr_end_date="+"2019-11-01", line.rstrip())
                print(line,end='\n')
            elif re.match(r'pr_end_time',line):
                line = re.sub(r'^pr_end_time=.*$',"pr_end_time="+"00:00", line.rstrip())
                print(line,end='\n')
            else:
                print(line,end='')
            # TODO add line to modify dates + round up to next 30 minutes : dt.now() + (dt.min - dt.now()) % td(minutes=30)
            # TODO pr_subset=0 for "select a different period"

    # TODO check compatibility with unix and Wine
    process=os.path.join(".\Bin","EddyPro","eddypro_rp.exe")
    subprocess.call([process, eddyproConfig])


def merge_eddy_and_slow(iStation,asciiOutDir,eddyproOutDir,mergedCsvOutDir):

    # TODO keep units to csv

    # Module to merge same type of slow data together
    def merge_slow_data(slowList):
        slow_df=pd.read_csv(os.path.join(asciiOutDir,iStation,slowList[0]), sep=',',skiprows=[0,2,3], nrows=0, low_memory=False) # Initialize columns name
        for iSlow in slowList:
            tmp_df=pd.read_csv(os.path.join(asciiOutDir,iStation,iSlow), sep=',',skiprows=[0,2,3], low_memory=False)
            if not tmp_df.TIMESTAMP.shape==tmp_df.TIMESTAMP.unique().shape: # TODO check for more elegant solution that discard less data
                tmp_df=tmp_df.drop_duplicates(subset='TIMESTAMP', keep='last')
            slow_df=slow_df.append(tmp_df, sort=False)
        slow_df.index=pd.to_datetime(slow_df.TIMESTAMP, yearfirst=True)

        return slow_df

    # List all slow csv files and merge them together
    slowFileToLoad = os.listdir(os.path.join(asciiOutDir,iStation))
    slowFileToLoad.sort()

    # Slow data
    slowList=[s for s in slowFileToLoad if re.match('.*slow\.csv', s)]
    slow_df=merge_slow_data(slowList)

    # Slow data 2
    slowList2=[s for s in slowFileToLoad if re.match('.*slow2\.csv', s)]
    if slowList2:
        slow_df2=merge_slow_data(slowList2)
        slow_df=pd.concat([slow_df, slow_df2], axis=1, sort=False)

    # Eddy file to load
    # TODO check Eddy pro file naming conventions. Essential vs full_output
    eddyFileToLoad = glob(eddyproOutDir+'/'+iStation+'/'+'\*full_output*.csv') # TODO chose between glob and os.listdir
    eddy_df=pd.read_csv(eddyFileToLoad[0],skiprows=[0,2], sep=',', index_col=None,low_memory=False)
    eddy_df.index=pd.to_datetime(eddy_df.date.map(str) +" "+ eddy_df.time.map(str), yearfirst=True)

    # Merge and save
    merged_df=pd.concat([eddy_df, slow_df], axis=1)
    merged_df.TIMESTAMP=merged_df.index # overwrite missing timestamp
    merged_df.index.name="date_index"
    merged_df.to_csv(os.path.join(mergedCsvOutDir,iStation,"Merged_data.csv"))


def flux_gap_filling(iStation,var_to_fill,mergedCsvOutDir):
    # Coded from Reichtein et al. 2005
    #
    # Flowchart:
    #
    #   NEE present ?                                                 --> Yes     --> Does nothing
    #    |
    #    V
    #   Rg, T, VPD, NEE available within |dt|<= 7 days                --> Yes     --> Filling quality A (case 1)
    #    |
    #    V
    #   Rg, T, VPD, NEE available within |dt|<= 14 days               --> Yes     --> Filling quality A (case 2)
    #    |
    #    V
    #   Rg, NEE available within |dt|<= 7 days                        --> Yes     --> Filling quality A (case 3)
    #    |
    #    V
    #   NEE available within |dt|<= 1h                                --> Yes     --> Filling quality A (case 4)
    #    |
    #    V
    #   NEE available within |dt|= 1 day & same hour of day           --> Yes     --> Filling quality B (case 5)
    #    |
    #    V
    #   Rg, T, VPD, NEE available within |dt|<= 21, 28,..., 140 days  --> Yes     --> Filling quality B if |dt|<=28, else C (case 6)
    #    |
    #    V
    #   Rg, NEE available within |dt|<= 14, 21, 28,..., 140 days      --> Yes     --> Filling quality B if |dt|<=28, else C (case 7)
    #    |
    #    V
    #   NEE available within |dt|<= 7, 21, 28,...days                 --> Yes     --> Filling quality C (case 8)


    def find_meteo_proxy_index(df, t, search_window, met_vars, var_to_fill):
        current_met = df.loc[t,met_vars.columns]
        if any(current_met.isna()):
            index_proxy_met = None
            fail_code = "NaN in current slow met vars"
        else:
            t_start = np.max([0, t-int(search_window*48)])
            t_end = np.min([df.shape[0], t+int(search_window*48)+1])
            time_window = list( range(t_start ,t_end) )
            time_window_met = df.loc[time_window,met_vars.columns]
            index_proxy_met_bool = pd.DataFrame()
            # Check if proxy met matches gap filling conditions
            for iVar in met_vars.columns:
                index_proxy_met_bool = pd.concat([index_proxy_met_bool, abs(time_window_met[iVar] - current_met[iVar]) < met_vars[iVar][0]], axis=1)
            # Check that var_to_fill is not NaN
            index_proxy_met_bool = pd.concat([index_proxy_met_bool, ~df.isna()[var_to_fill][time_window]], axis=1)
            index_proxy_met_bool = index_proxy_met_bool.all(axis=1)
            # Convert bool to index
            index_proxy_met = index_proxy_met_bool.index[index_proxy_met_bool == True]
            if index_proxy_met.size == 0:
                fail_code = "Proxy met not found"
            else:
                fail_code = None
        return index_proxy_met, fail_code


    # Import file
    df = pd.read_csv(os.path.join(mergedCsvOutDir,iStation, "Merged_data.csv" ), low_memory=False)

    # Add new columns to data frame that contains var_to_fill gapfilled
    gap_fil_col_name = var_to_fill + "_gap_filled"
    df[gap_fil_col_name] = df[var_to_fill]
    gap_fil_quality_col_name = gap_fil_col_name + "_quality"
    df[gap_fil_quality_col_name] = None

    # Identify missing flux
    id_missing_flux=df.isna()[var_to_fill]


    for t in id_missing_flux.index:
        if id_missing_flux[t]:
            print(t)
            # Case 1
            search_window = 7
            met_vars = pd.DataFrame({"Rs_incoming_Avg" : 50, "HMP45C_Sensor_temp_Avg" : 2.5,"vpd" : 500}, index=[0] )
            index_proxy_met, fail_code = find_meteo_proxy_index(df, t, search_window, met_vars, var_to_fill)
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
                df.loc[t,gap_fil_quality_col_name] = "A1"
                print("Case 1 succeeded\n")
                continue
            else:
                print("Case 1 failed\n")

            # Case 2
            search_window = 14
            met_vars = pd.DataFrame({"Rs_incoming_Avg" : 50, "HMP45C_Sensor_temp_Avg" : 2.5,"vpd" : 500}, index=[0] )
            index_proxy_met, fail_code = find_meteo_proxy_index(df, t, search_window, met_vars, var_to_fill)
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
                df.loc[t,gap_fil_quality_col_name] = "A2"
                print("Case 2 succeeded\n")
                continue
            else:
                print("Case 2 failed\n")

            # Case 3
            search_window = 7
            met_vars = pd.DataFrame({"Rs_incoming_Avg" : 50}, index=[0] )
            index_proxy_met, fail_code = find_meteo_proxy_index(df, t, search_window, met_vars, var_to_fill)
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
                df.loc[t,gap_fil_quality_col_name] = "A3"
                print("Case 3 succeeded\n")
                continue
            else:
                print("Case 3 failed\n")

            # Case 4
            search_window = 1/24
            met_vars = pd.DataFrame()
            index_proxy_met, fail_code = find_meteo_proxy_index(df, t, search_window, met_vars, var_to_fill)
            if not fail_code:
                df.loc[t,gap_fil_col_name] = np.mean(df[var_to_fill][index_proxy_met])
                df.loc[t,gap_fil_quality_col_name] = "A4"
                print("Case 4 succeeded\n")
                continue
            else:
                print("Case 4 failed\n")


















